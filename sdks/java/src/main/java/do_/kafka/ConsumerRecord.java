package do_.kafka;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * A key/value pair received from Kafka. This consists of a topic name, a partition number,
 * the offset that points to the record in a Kafka partition, a key, a value, timestamp, and headers.
 *
 * @param <K> The type of the key
 * @param <V> The type of the value
 */
public final class ConsumerRecord<K, V> {
    private final String topic;
    private final int partition;
    private final long offset;
    private final K key;
    private final V value;
    private final Instant timestamp;
    private final Map<String, byte[]> headers;

    /**
     * Creates a consumer record.
     *
     * @param topic     The topic this record is received from
     * @param partition The partition from which this record is received
     * @param offset    The offset of this record in the corresponding Kafka partition
     * @param key       The key of the record (may be null)
     * @param value     The value
     * @param timestamp The timestamp of the record
     * @param headers   The headers of the record
     */
    public ConsumerRecord(String topic, int partition, long offset, K key, V value,
                          Instant timestamp, Map<String, byte[]> headers) {
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.key = key;
        this.value = value;
        this.timestamp = timestamp;
        this.headers = headers != null ? new HashMap<>(headers) : new HashMap<>();
    }

    /**
     * Creates a consumer record with minimal fields.
     */
    public ConsumerRecord(String topic, int partition, long offset, K key, V value) {
        this(topic, partition, offset, key, value, Instant.now(), null);
    }

    /**
     * Returns the topic this record is received from.
     */
    public String topic() {
        return topic;
    }

    /**
     * Returns the partition from which this record is received.
     */
    public int partition() {
        return partition;
    }

    /**
     * Returns the offset of this record in the corresponding Kafka partition.
     */
    public long offset() {
        return offset;
    }

    /**
     * Returns the key (or null if no key was specified).
     */
    public K key() {
        return key;
    }

    /**
     * Returns the value.
     */
    public V value() {
        return value;
    }

    /**
     * Returns the timestamp of this record.
     */
    public Instant timestamp() {
        return timestamp;
    }

    /**
     * Returns the headers.
     */
    public Map<String, byte[]> headers() {
        return Collections.unmodifiableMap(headers);
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
        if (!(o instanceof ConsumerRecord<?, ?> that)) return false;
        return partition == that.partition &&
                offset == that.offset &&
                Objects.equals(topic, that.topic) &&
                Objects.equals(key, that.key) &&
                Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topic, partition, offset, key, value);
    }

    @Override
    public String toString() {
        return "ConsumerRecord{" +
                "topic='" + topic + '\'' +
                ", partition=" + partition +
                ", offset=" + offset +
                ", key=" + key +
                ", value=" + value +
                ", timestamp=" + timestamp +
                '}';
    }
}
