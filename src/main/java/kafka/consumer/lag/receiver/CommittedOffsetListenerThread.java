package kafka.consumer.lag.receiver;

import kafka.admin.AdminClient;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.requests.AbstractRequestResponse;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.OffsetFetchRequest;

import java.util.List;
import java.util.concurrent.CountDownLatch;

public class CommittedOffsetListenerThread implements Runnable, CallBackInstanceForKafka {

    private final KafkaApiRequest kafkaApiRequest;
    private CountDownLatch countDownLatch;

    private Node coordinator;
    private String groupName;
    private List<TopicPartition> topicPartitions;

    CommittedOffsetListenerThread(final AdminClient adminClient, final CountDownLatch countDownLatch,final String groupName, final List<TopicPartition> topicPartitions, final Node coordinator){
        this.kafkaApiRequest = new KafkaApiRequest(adminClient.client());
        this.countDownLatch = countDownLatch;
        this.groupName = groupName;
        this.topicPartitions = topicPartitions;
        this.coordinator = coordinator;
    }

    @Override
    public void run() {
        OffsetFetchRequest.Builder offsetRequestBuilder =
                new OffsetFetchRequest.Builder(groupName, topicPartitions);
        this.kafkaApiRequest.sendApiRequest(this.coordinator, offsetRequestBuilder);
        this.kafkaApiRequest.getLastApiResponse(this);
    }

    @Override
    public void callBack(AbstractRequestResponse result) {
        if(result != null){
            TopicPartitionsOffsetInfoReceiver.committedOffsetResponse = (AbstractResponse) result;
        }
        this.countDownLatch.countDown();
    }
}
