package kafka.consumer.lag.receiver;

import kafka.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.*;

import java.util.*;
import java.util.concurrent.*;

class TopicPartitionsOffsetInfoReceiver{

    static final long DEFAULT_END_OFFSET_VALUE = -1L;

    private final AdminClient adminClient;

    private CountDownLatch endOffsetCountDownLatch;
    static final Object endOffsetMapLock = new Object();
    static Map<TopicPartition, Long> endOffsetsMap = new HashMap<>();

    private CountDownLatch committedOffsetCountDownLatch;
    static AbstractResponse committedOffsetResponse;

    private Map<Node,List<TopicPartition>> nodePartitionMap = new HashMap<>();
    private Node coordinator = null;

    TopicPartitionsOffsetInfoReceiver(final AdminClient adminClient){
        this.adminClient = adminClient;
    }

    void storeTopicPartitionPerNodeInfo(final Properties props, final List<String> topicNamesList) throws OffsetFetchException {
        org.apache.kafka.clients.admin.AdminClient newAc = org.apache.kafka.clients.admin.AdminClient.create(props);
        DescribeTopicsResult describeTopicsResult = newAc.describeTopics(topicNamesList);
        try {
            Map<String, TopicDescription> topicDescriptionMap = describeTopicsResult.all().get();
            for (Map.Entry<String, TopicDescription> entry : topicDescriptionMap.entrySet()) {
                for (TopicPartitionInfo partitionInfo : entry.getValue().partitions()) {
                    List<TopicPartition> topicPartitionList = nodePartitionMap.get(partitionInfo.leader());
                    if (topicPartitionList == null) {
                        topicPartitionList = new ArrayList<>();
                    }
                    TopicPartition tp = new TopicPartition(entry.getKey(), partitionInfo.partition());
                    topicPartitionList.add(tp);
                    nodePartitionMap.put(partitionInfo.leader(), topicPartitionList);
                }
            }
        } catch (InterruptedException e) {
            throw new OffsetFetchException("TopicPartitionsOffsetInfoReceiver - storeTopicPartitionPerNodeInfo InterruptedException:" + e.getMessage());
        } catch (ExecutionException e) {
            throw new OffsetFetchException("TopicPartitionsOffsetInfoReceiver - storeTopicPartitionPerNodeInfo ExecutionException:" + e.getMessage());
        }
    }

    void findCoordinatorNodeForGroup(final String groupName, final long retryTimeMs){
        this.coordinator = this.adminClient.findCoordinator(groupName, retryTimeMs);
    }

    void requestEndOffsets(){
        this.requestPerBrokerPartitionOffsetDetails();
    }

    private void requestPerBrokerPartitionOffsetDetails(){
        this.endOffsetCountDownLatch = new CountDownLatch(nodePartitionMap.size());

        ExecutorService executorService = Executors.newFixedThreadPool(nodePartitionMap.size());
        for(Map.Entry<Node,List<TopicPartition>>nodeListEntry: nodePartitionMap.entrySet()){
            executorService.submit(new EndOffsetsListenerThread(this.adminClient, nodeListEntry.getKey(),nodeListEntry.getValue(), this.endOffsetCountDownLatch, DEFAULT_END_OFFSET_VALUE));
        }
    }

     void requestCommittedOffsets(final String groupName, final List<TopicPartition> topicPartitions) throws OffsetFetchException {
        if(this.coordinator == null){
            throw new OffsetFetchException("TopicPartitionsOffsetInfoReceiver - requestCommittedOffsets - Missing Group Coordinator for group:" + groupName);
        }

        this.committedOffsetCountDownLatch = new CountDownLatch(1);
        Executor executor = Executors.newSingleThreadExecutor();
        executor.execute(new CommittedOffsetListenerThread(this.adminClient, this.committedOffsetCountDownLatch,groupName,topicPartitions,this.coordinator));
    }

    Map<TopicPartition, Long> getEndOffsets() throws OffsetFetchException {
        try {
            //wait for all EndOffsets threads to complete
            endOffsetCountDownLatch.await();
        } catch (InterruptedException e) {
            throw new OffsetFetchException("TopicPartitionsOffsetInfoReceiver - getEndOffsets InterruptedException:" + e.getMessage());
        }

        if(TopicPartitionsOffsetInfoReceiver.endOffsetsMap.isEmpty()){
            throw new OffsetFetchException("TopicPartitionsOffsetInfoReceiver - getEndOffsets - EmptyResult");
        }
        return TopicPartitionsOffsetInfoReceiver.endOffsetsMap;
    }

    Map<TopicPartition, OffsetFetchResponse.PartitionData> getCommittedOffsets() throws OffsetFetchException {
        try {
            //wait for committedOffset thread to complete
            committedOffsetCountDownLatch.await();
        } catch (InterruptedException e) {
            throw new OffsetFetchException("TopicPartitionsOffsetInfoReceiver - getCommittedOffsets InterruptedException:" + e.getMessage());
        }

        OffsetFetchResponse offsetFetchResponse = (OffsetFetchResponse) TopicPartitionsOffsetInfoReceiver.committedOffsetResponse;
        if(offsetFetchResponse==null) {
            throw new OffsetFetchException("TopicPartitionsOffsetInfoReceiver - getCommittedOffsets - EmptyResult");
        }
        else if(offsetFetchResponse.error() == Errors.NONE){
            return offsetFetchResponse.responseData();
        }
        else{
            throw new OffsetFetchException("TopicPartitionsOffsetInfoReceiver - " + offsetFetchResponse.error().message());
        }
    }
}
