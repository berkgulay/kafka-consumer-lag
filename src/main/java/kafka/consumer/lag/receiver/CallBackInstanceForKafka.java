package kafka.consumer.lag.receiver;

import org.apache.kafka.common.requests.AbstractRequestResponse;

public interface CallBackInstanceForKafka {
    void callBack(AbstractRequestResponse result);
}
