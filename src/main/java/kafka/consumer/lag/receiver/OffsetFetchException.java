package kafka.consumer.lag.receiver;

public class OffsetFetchException extends Exception {
    public OffsetFetchException(String message) {
        super(message);
    }
}
