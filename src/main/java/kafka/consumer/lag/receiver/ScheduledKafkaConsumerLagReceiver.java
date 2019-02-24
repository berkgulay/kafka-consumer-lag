package kafka.consumer.lag.receiver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Service
public class ScheduledKafkaConsumerLagReceiver {

    private Logger logger = LoggerFactory.getLogger(ScheduledKafkaConsumerLagReceiver.class);

    private static final String BOOTSTRAP_SERVER = ""; //TODO: Enter your bootstrap server ip and kafka port in the form <ip:port>
    private KafkaConsumerGroupLagReceiver kafkaConsumerGroupLagReceiver;

    @Scheduled(fixedRate = 120000)   // every 2 minutes
    public void exportToLogstash(){
        kafkaConsumerGroupLagReceiver = new KafkaConsumerGroupLagReceiver(BOOTSTRAP_SERVER);

        try{
            List<String> consumerGroupsList = kafkaConsumerGroupLagReceiver.getConsumerGroupsList();
            for(int i=0;i<consumerGroupsList.size();i++){
                List<String> allConsumersInfoAndLag = getConsumerInfoAndLag(consumerGroupsList.get(i));
                for(int k=0;k<allConsumersInfoAndLag.size();k++){
                    logger.info(allConsumersInfoAndLag.get(k));
                }
            }
        }
        catch (Exception e){
            logger.error(e.getMessage(), e);
        }
    }

    private List<String> getConsumerInfoAndLag(String group) throws OffsetFetchException, LagReceiverException {

        List<String> allConsumersInfoAndLag = new ArrayList<>();
        Map<String, Map<Integer, KafkaConsumerGroupLagReceiver.PartitionInfo>> results = kafkaConsumerGroupLagReceiver.getConsumerLagForConsumerGroup(group);
        for (String topic : results.keySet()) {
            Map<Integer, KafkaConsumerGroupLagReceiver.PartitionInfo> partitionToAssignmentInfo = results.get(topic);
            for (int partition : partitionToAssignmentInfo.keySet()) {
                KafkaConsumerGroupLagReceiver.PartitionInfo partitionInfo = partitionToAssignmentInfo.get(partition);

                long lag = partitionInfo.lag;
                String clientId = partitionInfo.clientId;
                String host = partitionInfo.host.substring(1);

                String consumerInfoAndLag = String.format("CONSUMER_INFO_AND_LAG: %s %s %d %d %s %s", group, topic, partition, lag, clientId, host);
                allConsumersInfoAndLag.add(consumerInfoAndLag);
            }
        }

        return allConsumersInfoAndLag;
    }
}
