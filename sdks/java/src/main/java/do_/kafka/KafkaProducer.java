package do_.kafka;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A Kafka client that publishes records to the Kafka cluster.
 * <p>
 * The producer is thread safe and sharing a single producer instance across threads
 * will generally be faster than having multiple instances.
 * <p>
 * Example usage:
 * <pre>{@code
 * Kafka kafka = new Kafka(Arrays.asList("broker1:9092"));
 * KafkaProducer<String, String> producer = kafka.createProducer();
 *
 * // Send a record
 * producer.send(new ProducerRecord<>("topic", "key", "value")).get();
 *
 * // Send with callback
 * producer.send(new ProducerRecord<>("topic", "key", "value"))
 *     .thenAccept(metadata -> System.out.println("Sent to offset " + metadata.offset()));
 *
 * // Flush and close
 * producer.flush();
 * producer.close();
 * }</pre>
 *
 * @param <K> The type of keys
 * @param <V> The type of values
 */
public class KafkaProducer<K, V> implements AutoCloseable {

    private final Kafka kafka;
    private final ProducerConfig config;
    private volatile RpcClient client;
    private volatile boolean connected;
    private volatile boolean closed;
    private final ReentrantLock lock = new ReentrantLock();

    /**
     * Creates a KafkaProducer.
     */
    KafkaProducer(Kafka kafka, ProducerConfig config) {
        this.kafka = Objects.requireNonNull(kafka, "kafka cannot be null");
        this.config = Objects.requireNonNull(config, "config cannot be null");
    }

    /**
     * Returns true if the producer is connected.
     */
    public boolean isConnected() {
        return connected && !closed;
    }

    /**
     * Connects the producer to the Kafka cluster.
     */
    public void connect() {
        lock.lock();
        try {
            if (connected) {
                return;
            }
            if (closed) {
                throw new KafkaException("Producer is closed");
            }
            client = kafka.getRpcClient();
            connected = true;
        } finally {
            lock.unlock();
        }
    }

    private void ensureConnected() {
        if (!connected) {
            connect();
        }
        if (closed) {
            throw new KafkaException("Producer is closed");
        }
    }

    /**
     * Sends a record to the Kafka cluster asynchronously.
     *
     * @param record The record to send
     * @return A CompletableFuture that will complete with the record metadata
     */
    public CompletableFuture<RecordMetadata> send(ProducerRecord<K, V> record) {
        Objects.requireNonNull(record, "record cannot be null");
        ensureConnected();

        Map<String, Object> serialized = serializeRecord(record);

        return client.call("kafka.producer.send", serialized)
                .thenApply(this::parseRecordMetadata)
                .exceptionally(e -> {
                    throw wrapException(e, "Failed to send message");
                });
    }

    /**
     * Sends a batch of records to the Kafka cluster asynchronously.
     *
     * @param records The records to send
     * @return A CompletableFuture that will complete with the list of record metadata
     */
    public CompletableFuture<List<RecordMetadata>> sendBatch(List<ProducerRecord<K, V>> records) {
        Objects.requireNonNull(records, "records cannot be null");
        ensureConnected();

        List<Map<String, Object>> serialized = new ArrayList<>();
        for (ProducerRecord<K, V> record : records) {
            serialized.add(serializeRecord(record));
        }

        return client.call("kafka.producer.sendBatch", serialized)
                .thenApply(result -> parseBatchResult(result))
                .exceptionally(e -> {
                    throw wrapException(e, "Failed to send batch");
                });
    }

    /**
     * Flushes any pending records.
     */
    public void flush() {
        if (!connected || closed) {
            return;
        }
        try {
            client.call("kafka.producer.flush").join();
        } catch (Exception e) {
            // Ignore flush errors
        }
    }

    /**
     * Returns the partitions for a topic.
     *
     * @param topic The topic name
     * @return A CompletableFuture that will complete with the list of partition numbers
     */
    public CompletableFuture<List<Integer>> partitionsFor(String topic) {
        ensureConnected();
        return client.call("kafka.producer.partitionsFor", topic)
                .thenApply(this::parsePartitions)
                .exceptionally(e -> {
                    throw wrapException(e, "Failed to get partitions");
                });
    }

    @Override
    public void close() {
        lock.lock();
        try {
            if (closed) {
                return;
            }
            if (connected && client != null) {
                flush();
            }
            closed = true;
            connected = false;
            client = null;
        } finally {
            lock.unlock();
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> serializeRecord(ProducerRecord<K, V> record) {
        Map<String, Object> data = new HashMap<>();
        data.put("topic", record.topic());

        // Serialize value
        if (record.value() != null) {
            data.put("value", encodeValue(record.value()));
        }

        // Serialize key
        if (record.key() != null) {
            data.put("key", encodeValue(record.key()));
        }

        if (record.partition() != null) {
            data.put("partition", record.partition());
        }

        if (record.timestamp() != null) {
            data.put("timestamp", record.timestamp().toEpochMilli());
        }

        if (!record.headers().isEmpty()) {
            List<Map<String, String>> headers = new ArrayList<>();
            for (Map.Entry<String, byte[]> entry : record.headers().entrySet()) {
                Map<String, String> header = new HashMap<>();
                header.put("key", entry.getKey());
                header.put("value", Base64.getEncoder().encodeToString(entry.getValue()));
                headers.add(header);
            }
            data.put("headers", headers);
        }

        return data;
    }

    private String encodeValue(Object value) {
        if (value instanceof byte[]) {
            return Base64.getEncoder().encodeToString((byte[]) value);
        } else if (value instanceof String) {
            return Base64.getEncoder().encodeToString(((String) value).getBytes(java.nio.charset.StandardCharsets.UTF_8));
        } else {
            // For other types, convert to string then encode
            return Base64.getEncoder().encodeToString(value.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8));
        }
    }

    @SuppressWarnings("unchecked")
    private RecordMetadata parseRecordMetadata(Object result) {
        if (!(result instanceof Map)) {
            throw new KafkaException("Invalid response format");
        }
        Map<String, Object> m = (Map<String, Object>) result;

        String topic = (String) m.get("topic");
        int partition = ((Number) m.getOrDefault("partition", 0)).intValue();
        long offset = ((Number) m.getOrDefault("offset", 0L)).longValue();

        Instant timestamp = Instant.now();
        if (m.containsKey("timestamp")) {
            timestamp = Instant.ofEpochMilli(((Number) m.get("timestamp")).longValue());
        }

        int keySize = m.containsKey("serialized_key_size")
                ? ((Number) m.get("serialized_key_size")).intValue()
                : -1;
        int valueSize = m.containsKey("serialized_value_size")
                ? ((Number) m.get("serialized_value_size")).intValue()
                : -1;

        return new RecordMetadata(topic, partition, offset, timestamp, keySize, valueSize);
    }

    @SuppressWarnings("unchecked")
    private List<RecordMetadata> parseBatchResult(Object result) {
        if (!(result instanceof List)) {
            throw new KafkaException("Invalid batch response format");
        }
        List<Object> results = (List<Object>) result;
        List<RecordMetadata> metadata = new ArrayList<>();

        for (Object r : results) {
            if (r instanceof Map) {
                Map<String, Object> m = (Map<String, Object>) r;
                if (m.containsKey("error")) {
                    // Skip failed records for now
                    continue;
                }
                metadata.add(parseRecordMetadata(r));
            }
        }

        return metadata;
    }

    @SuppressWarnings("unchecked")
    private List<Integer> parsePartitions(Object result) {
        if (result instanceof List) {
            List<Object> list = (List<Object>) result;
            List<Integer> partitions = new ArrayList<>();
            for (Object item : list) {
                if (item instanceof Number) {
                    partitions.add(((Number) item).intValue());
                }
            }
            return partitions;
        }
        throw new KafkaException("Invalid partitions response");
    }

    private RuntimeException wrapException(Throwable e, String message) {
        Throwable cause = e;
        if (e instanceof java.util.concurrent.CompletionException) {
            cause = e.getCause();
        }
        if (cause instanceof KafkaException) {
            return (KafkaException) cause;
        }
        return new KafkaException(message, cause);
    }
}
