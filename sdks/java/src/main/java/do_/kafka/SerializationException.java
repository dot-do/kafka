package do_.kafka;

/**
 * Exception thrown when serialization or deserialization fails.
 */
public class SerializationException extends KafkaException {

    /**
     * Creates a SerializationException with a message.
     */
    public SerializationException(String message) {
        super("SERIALIZATION_ERROR", message, false, null);
    }

    /**
     * Creates a SerializationException with a message and cause.
     */
    public SerializationException(String message, Throwable cause) {
        super("SERIALIZATION_ERROR", message, false, cause);
    }
}
