package kafka.consumer.lag.receiver;

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.consumer.internals.ConsumerNetworkClient;
import org.apache.kafka.clients.consumer.internals.RequestFuture;
import org.apache.kafka.clients.consumer.internals.RequestFutureListener;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.requests.AbstractRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaApiRequest {

    private Logger logger = LoggerFactory.getLogger(KafkaApiRequest.class);

    private final ConsumerNetworkClient networkClient;
    private RequestFuture<ClientResponse> clientResponse = null;

    KafkaApiRequest(final ConsumerNetworkClient networkClient){
        this.networkClient = networkClient;
    }

    void sendApiRequest(final Node node, final AbstractRequest.Builder<?> requestBuilder){
        this.clientResponse  = this.networkClient.send(node, requestBuilder);
    }

    void getLastApiResponse(final CallBackInstanceForKafka callBackInstanceForKafka){
        this.clientResponse.addListener(new RequestFutureListener<ClientResponse>() {
            @Override
            public void onSuccess(ClientResponse clientResponse) {
                callBackInstanceForKafka.callBack(clientResponse.responseBody());
            }

            @Override
            public void onFailure(RuntimeException e) {
                logger.error("Runtime Exception in Kafka API Request - Exception: " + e.getMessage());
                callBackInstanceForKafka.callBack(null);
            }
        });
    }
}
