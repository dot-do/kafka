package do_.kafka;

/**
 * The base exception for Kafka operations. All Kafka exceptions extend this class.
 */
public class KafkaException extends RuntimeException {

    private final String code;
    private final boolean retriable;

    /**
     * Creates a KafkaException with a message.
     */
    public KafkaException(String message) {
        this(null, message, false, null);
    }

    /**
     * Creates a KafkaException with a message and cause.
     */
    public KafkaException(String message, Throwable cause) {
        this(null, message, false, cause);
    }

    /**
     * Creates a KafkaException with code and message.
     */
    public KafkaException(String code, String message) {
        this(code, message, false, null);
    }

    /**
     * Creates a KafkaException with all fields.
     */
    public KafkaException(String code, String message, boolean retriable, Throwable cause) {
        super(formatMessage(code, message), cause);
        this.code = code;
        this.retriable = retriable;
    }

    private static String formatMessage(String code, String message) {
        if (code != null && !code.isEmpty()) {
            return "[" + code + "] " + message;
        }
        return message;
    }

    /**
     * Returns the error code, or null if not available.
     */
    public String code() {
        return code;
    }

    /**
     * Returns true if this operation is safe to retry.
     */
    public boolean isRetriable() {
        return retriable;
    }

    /**
     * Returns true if this is a connection-related error.
     */
    public boolean isConnectionError() {
        return this instanceof ConnectionException;
    }

    /**
     * Returns true if this is a timeout error.
     */
    public boolean isTimeoutError() {
        return this instanceof TimeoutException;
    }
}
