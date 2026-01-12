package do_.kafka;

import java.time.Instant;
import java.util.Objects;

/**
 * Metadata about a record that has been acknowledged by the server.
 */
public final class RecordMetadata {
    private final String topic;
    private final int partition;
    private final long offset;
    private final Instant timestamp;
    private final int serializedKeySize;
    private final int serializedValueSize;

    /**
     * Creates record metadata.
     *
     * @param topic               The topic the record was appended to
     * @param partition           The partition the record was sent to
     * @param offset              The offset of the record in the topic/partition
     * @param timestamp           The timestamp of the record
     * @param serializedKeySize   The size of the serialized key
     * @param serializedValueSize The size of the serialized value
     */
    public RecordMetadata(String topic, int partition, long offset, Instant timestamp,
                          int serializedKeySize, int serializedValueSize) {
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.timestamp = timestamp;
        this.serializedKeySize = serializedKeySize;
        this.serializedValueSize = serializedValueSize;
    }

    /**
     * Creates record metadata with minimal fields.
     */
    public RecordMetadata(String topic, int partition, long offset) {
        this(topic, partition, offset, Instant.now(), -1, -1);
    }

    /**
     * Returns the topic the record was appended to.
     */
    public String topic() {
        return topic;
    }

    /**
     * Returns the partition the record was sent to.
     */
    public int partition() {
        return partition;
    }

    /**
     * Returns the offset of the record in the topic/partition.
     */
    public long offset() {
        return offset;
    }

    /**
     * Returns the timestamp of the record.
     */
    public Instant timestamp() {
        return timestamp;
    }

    /**
     * Returns the size of the serialized key, or -1 if not available.
     */
    public int serializedKeySize() {
        return serializedKeySize;
    }

    /**
     * Returns the size of the serialized value, or -1 if not available.
     */
    public int serializedValueSize() {
        return serializedValueSize;
    }

    /**
     * Returns the TopicPartition for this record.
     */
    public TopicPartition topicPartition() {
        return new TopicPartition(topic, partition);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof RecordMetadata that)) return false;
        return partition == that.partition &&
                offset == that.offset &&
                Objects.equals(topic, that.topic);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topic, partition, offset);
    }

    @Override
    public String toString() {
        return "RecordMetadata{" +
                "topic='" + topic + '\'' +
                ", partition=" + partition +
                ", offset=" + offset +
                ", timestamp=" + timestamp +
                '}';
    }
}
