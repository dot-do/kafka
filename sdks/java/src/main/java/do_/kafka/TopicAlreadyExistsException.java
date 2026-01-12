package do_.kafka;

/**
 * Exception thrown when attempting to create a topic that already exists.
 */
public class TopicAlreadyExistsException extends KafkaException {

    private final String topic;

    /**
     * Creates a TopicAlreadyExistsException.
     */
    public TopicAlreadyExistsException(String topic) {
        super("TOPIC_ALREADY_EXISTS", "Topic already exists: " + topic, false, null);
        this.topic = topic;
    }

    /**
     * Returns the topic that already exists.
     */
    public String topic() {
        return topic;
    }
}
