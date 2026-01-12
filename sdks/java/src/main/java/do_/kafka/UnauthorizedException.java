package do_.kafka;

/**
 * Exception thrown when an operation is not authorized.
 */
public class UnauthorizedException extends KafkaException {

    /**
     * Creates an UnauthorizedException with a message.
     */
    public UnauthorizedException(String message) {
        super("UNAUTHORIZED", message, false, null);
    }

    /**
     * Creates an UnauthorizedException with a message and cause.
     */
    public UnauthorizedException(String message, Throwable cause) {
        super("UNAUTHORIZED", message, false, cause);
    }
}
