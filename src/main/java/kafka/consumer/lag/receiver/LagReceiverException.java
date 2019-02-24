package kafka.consumer.lag.receiver;

public class LagReceiverException extends Exception  {
    public LagReceiverException(String message) {
        super(message);
    }
}
