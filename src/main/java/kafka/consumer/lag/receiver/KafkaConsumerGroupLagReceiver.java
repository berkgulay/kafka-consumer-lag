package kafka.consumer.lag.receiver;

import java.util.*;

import kafka.coordinator.group.GroupOverview;
import kafka.admin.AdminClient;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.requests.OffsetFetchResponse;

public class KafkaConsumerGroupLagReceiver {

    static class PartitionInfo {
        long logEndOffset;
        int partition;
        Object currentOffset;
        long lag;
        String consumerId;
        String host;
        String clientId;
    }

    private static final Long CONSUMER_GROUP_STABILIZATION_TIMEOUT = 5000L; //in ms
    private static final Long WAIT_FOR_RESPONSE_TIME = 1000L; //in ms

    private String bootstrapServer;
    private AdminClient baseAdminClient;

    public KafkaConsumerGroupLagReceiver(String bootstrapServer) {
        this.bootstrapServer = bootstrapServer;

        Properties props = new Properties();
        props.put("bootstrap.servers", this.bootstrapServer);
        this.baseAdminClient = AdminClient.create(props);
    }

    public List<String> getConsumerGroupsList() throws LagReceiverException {
        List<String> consumerGroupsAsString = new ArrayList<>();
        try{
            scala.collection.immutable.List<GroupOverview> consumerGroupsListInScala = this.baseAdminClient.listAllConsumerGroupsFlattened();
            List<GroupOverview> consumerGroups = scala.collection.JavaConversions.seqAsJavaList(consumerGroupsListInScala);
            for(int i=0; i<consumerGroups.size(); i++){
                consumerGroupsAsString.add(consumerGroups.get(i).groupId());
            }
        }
        catch (Exception e){
            throw new LagReceiverException("KafkaConsumerGroupLagReceiver - getConsumerGroupsList Exception: " + e.getMessage());
        }

        return consumerGroupsAsString;
    }

    public Map<String, Map<Integer, PartitionInfo>> getConsumerLagForConsumerGroup(String group) throws OffsetFetchException, LagReceiverException {
        AdminClient ac =  getAdminClientWithGroupProperty();

        AdminClient.ConsumerGroupSummary summary = ac.describeConsumerGroup(group, CONSUMER_GROUP_STABILIZATION_TIMEOUT);
        if (summary.state().equals("Dead")) {
            throw new LagReceiverException("KafkaConsumerGroupLagReceiver - getConsumerLagForConsumerGroup - Consumer Group Is Dead");
        }

        List<AdminClient.ConsumerSummary> consumerList = getConsumerSummaries(summary);
        if (consumerList.isEmpty()){
            throw new LagReceiverException("KafkaConsumerGroupLagReceiver - getConsumerLagForConsumerGroup - Consumer List Is Empty");
        }

        Map<TopicPartition, AdminClient.ConsumerSummary> whoOwnsPartition = new HashMap<>();
        List<String> topicNamesList = new ArrayList<>();
        List<TopicPartition> topicPartitionCollection = new ArrayList<>();
        populateTopicBasedStructuresUsingConsumers(consumerList, whoOwnsPartition, topicNamesList, topicPartitionCollection);

        TopicPartitionsOffsetInfoReceiver topicPartitionsOffsetInfo = new TopicPartitionsOffsetInfoReceiver(ac);
        topicPartitionsOffsetInfo.storeTopicPartitionPerNodeInfo(getPropertyWithGroup(), topicNamesList);

        topicPartitionsOffsetInfo.requestEndOffsets();

        topicPartitionsOffsetInfo.findCoordinatorNodeForGroup(group, WAIT_FOR_RESPONSE_TIME);
        topicPartitionsOffsetInfo.requestCommittedOffsets(group, topicPartitionCollection);

        Map<TopicPartition, Long> endOffsets = topicPartitionsOffsetInfo.getEndOffsets();
        Map<TopicPartition, OffsetFetchResponse.PartitionData> committedOffsets = topicPartitionsOffsetInfo.getCommittedOffsets();

        return getResults(whoOwnsPartition, topicPartitionCollection, endOffsets, committedOffsets);
    }

    private AdminClient getAdminClientWithGroupProperty(){
        return AdminClient.create(getPropertyWithGroup());
    }

    private Properties getPropertyWithGroup() {
        Properties props = new Properties();
        props.put("bootstrap.servers", this.bootstrapServer);
        return props;
    }

    private List<AdminClient.ConsumerSummary> getConsumerSummaries(AdminClient.ConsumerGroupSummary summary) {
        scala.collection.immutable.List<AdminClient.ConsumerSummary> scalaList = summary.consumers().get();
        return scala.collection.JavaConversions.seqAsJavaList(scalaList);
    }

    private void populateTopicBasedStructuresUsingConsumers(List<AdminClient.ConsumerSummary> consumerList, Map<TopicPartition, AdminClient.ConsumerSummary> whoOwnsPartition, List<String> topicNamesList, Collection<TopicPartition> topicPartitionCollection) {
        for (AdminClient.ConsumerSummary cs : consumerList) {
            scala.collection.immutable.List<TopicPartition> scalaAssignment = cs.assignment();
            List<TopicPartition> assignment = scala.collection.JavaConversions.seqAsJavaList(scalaAssignment);

            for (TopicPartition tp : assignment) {
                whoOwnsPartition.put(tp, cs);
                topicNamesList.add(tp.topic());
            }
            topicPartitionCollection.addAll(assignment);
        }
    }

    private Map<String, Map<Integer, PartitionInfo>> getResults(Map<TopicPartition, AdminClient.ConsumerSummary> whoOwnsPartition, Collection<TopicPartition> topicPartitionCollection, Map<TopicPartition, Long> endOffsets, Map<TopicPartition, OffsetFetchResponse.PartitionData> commitedOffsets) {
        Map<String, Map<Integer, PartitionInfo>> results = new HashMap<>();
        for (TopicPartition tp : topicPartitionCollection) {

            createResultMapping(results, tp);
            Map<Integer, PartitionInfo> topicMap = createAndGetInnerTopicMap(results, tp);

            OffsetFetchResponse.PartitionData topicPartitionCommittedOffset = getTopicPartitionCommittedOffset(commitedOffsets, tp);
            long end = endOffsets.get(tp);

            PartitionInfo partitionInfo = topicMap.get(tp.partition());
            populatePartitionInfo(partitionInfo, tp, topicPartitionCommittedOffset, end, whoOwnsPartition);
        }
        return results;
    }

    private void createResultMapping(Map<String, Map<Integer, PartitionInfo>> results, TopicPartition tp) {
        if (!results.containsKey(tp.topic())) {
            results.put(tp.topic(), new HashMap<>());
        }
    }

    private Map<Integer, PartitionInfo> createAndGetInnerTopicMap(Map<String, Map<Integer, PartitionInfo>> results, TopicPartition tp) {
        Map<Integer, PartitionInfo> topicMap = results.get(tp.topic());
        if (!topicMap.containsKey(tp.partition())) {
            topicMap.put(tp.partition(), new PartitionInfo());
        }
        return topicMap;
    }

    private OffsetFetchResponse.PartitionData getTopicPartitionCommittedOffset(Map<TopicPartition, OffsetFetchResponse.PartitionData> commitedOffsets, TopicPartition tp) {
        OffsetFetchResponse.PartitionData topicPartitionCommittedOffset = commitedOffsets.get(tp);

        //-1: No Info available
        if(topicPartitionCommittedOffset.offset == -1){
            topicPartitionCommittedOffset = null;
        }
        return topicPartitionCommittedOffset;
    }

    private void populatePartitionInfo(PartitionInfo partitionInfo, TopicPartition tp, OffsetFetchResponse.PartitionData topicPartitionCommittedOffset, long end, Map<TopicPartition, AdminClient.ConsumerSummary> whoOwnsPartition) {
        partitionInfo.logEndOffset = end;
        partitionInfo.partition = tp.partition();

        if (topicPartitionCommittedOffset == null) {
            partitionInfo.currentOffset = "unknown";
        }
        else {
            partitionInfo.currentOffset = topicPartitionCommittedOffset.offset;
        }

        if (topicPartitionCommittedOffset == null || end == -1) {
            partitionInfo.lag = -1;
        } else {
            partitionInfo.lag = end - topicPartitionCommittedOffset.offset;
        }

        AdminClient.ConsumerSummary cs = whoOwnsPartition.get(tp);
        partitionInfo.consumerId = cs.consumerId();
        partitionInfo.host = cs.host();
        partitionInfo.clientId = cs.clientId();
    }
}
