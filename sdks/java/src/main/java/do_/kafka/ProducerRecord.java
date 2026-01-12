package do_.kafka;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * A key/value pair to be sent to Kafka. This consists of a topic name to which the record is being
 * sent, an optional partition number, an optional key, a value, optional timestamp, and optional headers.
 *
 * @param <K> The type of the key
 * @param <V> The type of the value
 */
public final class ProducerRecord<K, V> {
    private final String topic;
    private final Integer partition;
    private final K key;
    private final V value;
    private final Instant timestamp;
    private final Map<String, byte[]> headers;

    /**
     * Creates a record to be sent to a specified topic with key and value.
     *
     * @param topic The topic the record will be appended to
     * @param key   The key that will be included in the record
     * @param value The record contents
     */
    public ProducerRecord(String topic, K key, V value) {
        this(topic, null, key, value, null, null);
    }

    /**
     * Creates a record to be sent to a specified topic with value only.
     *
     * @param topic The topic the record will be appended to
     * @param value The record contents
     */
    public ProducerRecord(String topic, V value) {
        this(topic, null, null, value, null, null);
    }

    /**
     * Creates a record to be sent to a specified topic and partition with key and value.
     *
     * @param topic     The topic the record will be appended to
     * @param partition The partition to which the record should be sent
     * @param key       The key that will be included in the record
     * @param value     The record contents
     */
    public ProducerRecord(String topic, Integer partition, K key, V value) {
        this(topic, partition, key, value, null, null);
    }

    /**
     * Creates a record with all fields specified.
     *
     * @param topic     The topic the record will be appended to
     * @param partition The partition to which the record should be sent
     * @param key       The key that will be included in the record
     * @param value     The record contents
     * @param timestamp The timestamp of the record
     * @param headers   The headers that will be included in the record
     */
    public ProducerRecord(String topic, Integer partition, K key, V value, Instant timestamp, Map<String, byte[]> headers) {
        Objects.requireNonNull(topic, "topic cannot be null");
        this.topic = topic;
        this.partition = partition;
        this.key = key;
        this.value = value;
        this.timestamp = timestamp;
        this.headers = headers != null ? new HashMap<>(headers) : new HashMap<>();
    }

    /**
     * Returns the topic this record is being sent to.
     */
    public String topic() {
        return topic;
    }

    /**
     * Returns the partition to which this record is being sent, or null if no partition was specified.
     */
    public Integer partition() {
        return partition;
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
     * Returns the timestamp, or null if not specified.
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
     * Creates a new builder for ProducerRecord.
     */
    public static <K, V> Builder<K, V> builder(String topic, V value) {
        return new Builder<>(topic, value);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ProducerRecord<?, ?> that)) return false;
        return Objects.equals(topic, that.topic) &&
                Objects.equals(partition, that.partition) &&
                Objects.equals(key, that.key) &&
                Objects.equals(value, that.value) &&
                Objects.equals(timestamp, that.timestamp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topic, partition, key, value, timestamp);
    }

    @Override
    public String toString() {
        return "ProducerRecord{" +
                "topic='" + topic + '\'' +
                ", partition=" + partition +
                ", key=" + key +
                ", value=" + value +
                ", timestamp=" + timestamp +
                ", headers=" + headers.keySet() +
                '}';
    }

    /**
     * Builder for creating ProducerRecord instances.
     */
    public static final class Builder<K, V> {
        private final String topic;
        private final V value;
        private Integer partition;
        private K key;
        private Instant timestamp;
        private Map<String, byte[]> headers = new HashMap<>();

        private Builder(String topic, V value) {
            this.topic = topic;
            this.value = value;
        }

        public Builder<K, V> partition(Integer partition) {
            this.partition = partition;
            return this;
        }

        public Builder<K, V> key(K key) {
            this.key = key;
            return this;
        }

        public Builder<K, V> timestamp(Instant timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public Builder<K, V> header(String key, byte[] value) {
            this.headers.put(key, value);
            return this;
        }

        public Builder<K, V> header(String key, String value) {
            this.headers.put(key, value.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            return this;
        }

        public Builder<K, V> headers(Map<String, byte[]> headers) {
            this.headers = new HashMap<>(headers);
            return this;
        }

        public ProducerRecord<K, V> build() {
            return new ProducerRecord<>(topic, partition, key, value, timestamp, headers);
        }
    }
}
