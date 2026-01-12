package do_.kafka;

/**
 * Exception thrown when an operation times out.
 */
public class TimeoutException extends KafkaException {

    /**
     * Creates a TimeoutException with a message.
     */
    public TimeoutException(String message) {
        super("TIMEOUT", message, true, null);
    }

    /**
     * Creates a TimeoutException with a message and cause.
     */
    public TimeoutException(String message, Throwable cause) {
        super("TIMEOUT", message, true, cause);
    }
}
