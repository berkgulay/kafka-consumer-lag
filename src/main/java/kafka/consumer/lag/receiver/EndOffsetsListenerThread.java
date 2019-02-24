package kafka.consumer.lag.receiver;

import kafka.admin.AdminClient;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequestResponse;
import org.apache.kafka.common.requests.IsolationLevel;
import org.apache.kafka.common.requests.ListOffsetRequest;
import org.apache.kafka.common.requests.ListOffsetResponse;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class EndOffsetsListenerThread implements Runnable, CallBackInstanceForKafka {

    private final KafkaApiRequest kafkaApiRequest;

    private List<TopicPartition> topicPartitionList;
    private CountDownLatch countDownLatch;
    private Node leaderNode;
    private long offsetRequestValue;

    private Map<TopicPartition, Long> requiredTimestamp;

    EndOffsetsListenerThread(final AdminClient adminClient,
                             final Node leaderNode,
                             final List<TopicPartition> topicPartitionList,
                             final CountDownLatch countDownLatch,
                             final long offsetRequestValue){
        this.kafkaApiRequest = new KafkaApiRequest(adminClient.client());
        this.topicPartitionList = topicPartitionList;
        this.countDownLatch = countDownLatch;
        this.leaderNode = leaderNode;
        this.offsetRequestValue = offsetRequestValue;
    }

    public void run() {
        if(this.offsetRequestValue == TopicPartitionsOffsetInfoReceiver.DEFAULT_END_OFFSET_VALUE) {
            this.requestNprocessEndOffsets();
        }
    }

    @Override
    public void callBack(AbstractRequestResponse result) {
        if(result!=null){
            ListOffsetResponse listOffsetResponse = (ListOffsetResponse) result;
            this.addEndoffsets(this.processListOffsetResponse(listOffsetResponse, this.requiredTimestamp));
        }
        this.countDownLatch.countDown();
    }

    private void requestNprocessEndOffsets(){
        this.requiredTimestamp = new HashMap<>();
        for(TopicPartition tp : this.topicPartitionList){
            requiredTimestamp.put(tp, TopicPartitionsOffsetInfoReceiver.DEFAULT_END_OFFSET_VALUE);
        }
        ListOffsetRequest.Builder builder = ListOffsetRequest.Builder
                .forConsumer(false, IsolationLevel.READ_UNCOMMITTED)
                .setTargetTimes(requiredTimestamp);

        this.kafkaApiRequest.sendApiRequest(this.leaderNode, builder);
        this.kafkaApiRequest.getLastApiResponse(this);
    }

    private Map<TopicPartition, Long> processListOffsetResponse(final ListOffsetResponse listOffsetResponse, final Map<TopicPartition, Long>requiredTimestamp) {

        Map<TopicPartition, Long>processTopicPartitionOffsets = new HashMap<>(requiredTimestamp);
        for (Map.Entry<TopicPartition, Long> entry : requiredTimestamp.entrySet()) {
            TopicPartition topicPartition = entry.getKey();
            ListOffsetResponse.PartitionData partitionData = listOffsetResponse.responseData().get(topicPartition);
            Errors error = partitionData.error;
            if (error == Errors.NONE) {
                //supporting kafka version greater than 10 only
                if (partitionData.offset != ListOffsetResponse.UNKNOWN_OFFSET) {
                    processTopicPartitionOffsets.put(topicPartition, partitionData.offset);
                }
            }
        }
        return processTopicPartitionOffsets;
    }

    private void addEndoffsets(Map<TopicPartition, Long> endOffsets){
        synchronized(TopicPartitionsOffsetInfoReceiver.endOffsetMapLock){
            TopicPartitionsOffsetInfoReceiver.endOffsetsMap.putAll(endOffsets);
        }
    }
}
