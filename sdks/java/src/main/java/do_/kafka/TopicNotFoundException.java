package do_.kafka;

/**
 * Exception thrown when a topic is not found.
 */
public class TopicNotFoundException extends KafkaException {

    private final String topic;

    /**
     * Creates a TopicNotFoundException.
     */
    public TopicNotFoundException(String topic) {
        super("TOPIC_NOT_FOUND", "Topic not found: " + topic, false, null);
        this.topic = topic;
    }

    /**
     * Returns the topic that was not found.
     */
    public String topic() {
        return topic;
    }
}
