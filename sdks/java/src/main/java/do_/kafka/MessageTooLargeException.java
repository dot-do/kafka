package do_.kafka;

/**
 * Exception thrown when a message is too large.
 */
public class MessageTooLargeException extends KafkaException {

    private final int messageSize;
    private final int maxSize;

    /**
     * Creates a MessageTooLargeException.
     */
    public MessageTooLargeException(int messageSize, int maxSize) {
        super("MESSAGE_TOO_LARGE",
              String.format("Message size %d exceeds maximum allowed size %d", messageSize, maxSize),
              false, null);
        this.messageSize = messageSize;
        this.maxSize = maxSize;
    }

    /**
     * Creates a MessageTooLargeException with just a message.
     */
    public MessageTooLargeException(String message) {
        super("MESSAGE_TOO_LARGE", message, false, null);
        this.messageSize = -1;
        this.maxSize = -1;
    }

    /**
     * Returns the message size.
     */
    public int messageSize() {
        return messageSize;
    }

    /**
     * Returns the maximum allowed size.
     */
    public int maxSize() {
        return maxSize;
    }
}
