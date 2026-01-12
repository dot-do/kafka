package do_.kafka;

/**
 * Exception thrown when there is a connection error.
 */
public class ConnectionException extends KafkaException {

    /**
     * Creates a ConnectionException with a message.
     */
    public ConnectionException(String message) {
        super("CONNECTION_ERROR", message, true, null);
    }

    /**
     * Creates a ConnectionException with a message and cause.
     */
    public ConnectionException(String message, Throwable cause) {
        super("CONNECTION_ERROR", message, true, cause);
    }
}
