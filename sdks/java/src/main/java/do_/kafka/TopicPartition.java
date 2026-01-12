package do_.kafka;

import java.util.Objects;

/**
 * A topic name and partition number.
 */
public final class TopicPartition {
    private final String topic;
    private final int partition;

    /**
     * Creates a topic-partition pair.
     *
     * @param topic     The topic name
     * @param partition The partition number
     */
    public TopicPartition(String topic, int partition) {
        Objects.requireNonNull(topic, "topic cannot be null");
        this.topic = topic;
        this.partition = partition;
    }

    /**
     * Returns the topic.
     */
    public String topic() {
        return topic;
    }

    /**
     * Returns the partition.
     */
    public int partition() {
        return partition;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TopicPartition that)) return false;
        return partition == that.partition && Objects.equals(topic, that.topic);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topic, partition);
    }

    @Override
    public String toString() {
        return topic + "-" + partition;
    }
}
