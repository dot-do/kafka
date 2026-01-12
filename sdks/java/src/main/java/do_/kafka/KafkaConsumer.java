package do_.kafka;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A Kafka client that consumes records from a Kafka cluster.
 * <p>
 * The consumer is NOT thread-safe. Calling poll() from multiple threads
 * is not supported.
 * <p>
 * Example usage:
 * <pre>{@code
 * Kafka kafka = new Kafka(Arrays.asList("broker1:9092"));
 * KafkaConsumer<String, String> consumer = kafka.createConsumer("my-group");
 *
 * consumer.subscribe(Arrays.asList("my-topic"));
 *
 * while (true) {
 *     ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
 *     for (ConsumerRecord<String, String> record : records) {
 *         System.out.printf("offset = %d, key = %s, value = %s%n",
 *             record.offset(), record.key(), record.value());
 *     }
 * }
 * }</pre>
 *
 * @param <K> The type of keys
 * @param <V> The type of values
 */
public class KafkaConsumer<K, V> implements AutoCloseable {

    private final Kafka kafka;
    private final String groupId;
    private final ConsumerConfig config;
    private volatile RpcClient client;
    private volatile boolean connected;
    private volatile boolean closed;
    private final Set<String> subscriptions = new HashSet<>();
    private final Set<TopicPartition> assignments = new HashSet<>();
    private final Set<TopicPartition> paused = new HashSet<>();
    private final ReentrantLock lock = new ReentrantLock();

    /**
     * Creates a KafkaConsumer.
     */
    KafkaConsumer(Kafka kafka, String groupId, ConsumerConfig config) {
        this.kafka = Objects.requireNonNull(kafka, "kafka cannot be null");
        this.groupId = Objects.requireNonNull(groupId, "groupId cannot be null");
        this.config = Objects.requireNonNull(config, "config cannot be null");
    }

    /**
     * Returns the consumer group ID.
     */
    public String groupId() {
        return groupId;
    }

    /**
     * Returns true if the consumer is connected.
     */
    public boolean isConnected() {
        return connected && !closed;
    }

    /**
     * Connects the consumer to the Kafka cluster.
     */
    public void connect() {
        lock.lock();
        try {
            if (connected) {
                return;
            }
            if (closed) {
                throw new KafkaException("Consumer is closed");
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
            throw new KafkaException("Consumer is closed");
        }
    }

    /**
     * Subscribe to the given list of topics.
     *
     * @param topics The list of topics to subscribe to
     */
    public void subscribe(Collection<String> topics) {
        Objects.requireNonNull(topics, "topics cannot be null");
        ensureConnected();

        lock.lock();
        try {
            Map<String, Object> args = new HashMap<>();
            args.put("topics", new ArrayList<>(topics));
            args.put("groupId", groupId);
            args.put("autoOffsetReset", config.offsetReset().name().toLowerCase());

            client.call("kafka.consumer.subscribe", args).join();

            subscriptions.clear();
            subscriptions.addAll(topics);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Unsubscribe from all topics.
     */
    public void unsubscribe() {
        ensureConnected();

        lock.lock();
        try {
            client.call("kafka.consumer.unsubscribe").join();
            subscriptions.clear();
            assignments.clear();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Returns the current subscription.
     */
    public Set<String> subscription() {
        lock.lock();
        try {
            return new HashSet<>(subscriptions);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Manually assign partitions to this consumer.
     *
     * @param partitions The partitions to assign
     */
    public void assign(Collection<TopicPartition> partitions) {
        Objects.requireNonNull(partitions, "partitions cannot be null");
        ensureConnected();

        lock.lock();
        try {
            List<Map<String, Object>> partitionData = new ArrayList<>();
            for (TopicPartition tp : partitions) {
                Map<String, Object> p = new HashMap<>();
                p.put("topic", tp.topic());
                p.put("partition", tp.partition());
                partitionData.add(p);
            }

            Map<String, Object> args = new HashMap<>();
            args.put("partitions", partitionData);

            client.call("kafka.consumer.assign", args).join();

            assignments.clear();
            assignments.addAll(partitions);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Returns the currently assigned partitions.
     */
    public Set<TopicPartition> assignment() {
        lock.lock();
        try {
            return new HashSet<>(assignments);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Fetches data for the topics or partitions specified using one of the subscribe/assign APIs.
     *
     * @param timeout The maximum time to block
     * @return The records, or empty if no data is available
     */
    public ConsumerRecords<K, V> poll(Duration timeout) {
        Objects.requireNonNull(timeout, "timeout cannot be null");
        ensureConnected();

        Map<String, Object> args = new HashMap<>();
        args.put("timeoutMs", timeout.toMillis());
        args.put("maxRecords", config.maxPollRecords());

        try {
            Object result = client.call("kafka.consumer.poll", args).join();
            return parseRecords(result);
        } catch (Exception e) {
            throw wrapException(e, "Failed to poll");
        }
    }

    /**
     * Commit offsets synchronously.
     */
    public void commitSync() {
        commitSync(null);
    }

    /**
     * Commit specific offsets synchronously.
     *
     * @param offsets The offsets to commit, or null to commit all
     */
    public void commitSync(Map<TopicPartition, Long> offsets) {
        ensureConnected();

        Map<String, Object> args = null;
        if (offsets != null) {
            List<Map<String, Object>> offsetData = new ArrayList<>();
            for (Map.Entry<TopicPartition, Long> entry : offsets.entrySet()) {
                Map<String, Object> o = new HashMap<>();
                o.put("topic", entry.getKey().topic());
                o.put("partition", entry.getKey().partition());
                o.put("offset", entry.getValue());
                offsetData.add(o);
            }
            args = Map.of("offsets", offsetData);
        }

        try {
            client.call("kafka.consumer.commit", args).join();
        } catch (Exception e) {
            throw wrapException(e, "Failed to commit");
        }
    }

    /**
     * Commit offsets asynchronously.
     *
     * @return A CompletableFuture that completes when the commit is done
     */
    public CompletableFuture<Void> commitAsync() {
        return commitAsync(null);
    }

    /**
     * Commit specific offsets asynchronously.
     *
     * @param offsets The offsets to commit, or null to commit all
     * @return A CompletableFuture that completes when the commit is done
     */
    public CompletableFuture<Void> commitAsync(Map<TopicPartition, Long> offsets) {
        ensureConnected();

        Map<String, Object> args = null;
        if (offsets != null) {
            List<Map<String, Object>> offsetData = new ArrayList<>();
            for (Map.Entry<TopicPartition, Long> entry : offsets.entrySet()) {
                Map<String, Object> o = new HashMap<>();
                o.put("topic", entry.getKey().topic());
                o.put("partition", entry.getKey().partition());
                o.put("offset", entry.getValue());
                offsetData.add(o);
            }
            args = Map.of("offsets", offsetData);
        }

        return client.call("kafka.consumer.commit", args)
                .thenApply(r -> null)
                .exceptionally(e -> {
                    throw wrapException(e, "Failed to commit");
                });
    }

    /**
     * Get the committed offset for a partition.
     *
     * @param partition The partition
     * @return The committed offset, or null if no offset is committed
     */
    public Long committed(TopicPartition partition) {
        ensureConnected();

        Map<String, Object> p = new HashMap<>();
        p.put("topic", partition.topic());
        p.put("partition", partition.partition());

        Map<String, Object> args = Map.of("partitions", List.of(p));

        try {
            Object result = client.call("kafka.consumer.committed", args).join();
            return parseCommittedOffset(result, partition);
        } catch (Exception e) {
            throw wrapException(e, "Failed to get committed offset");
        }
    }

    /**
     * Seek to a specific offset for a partition.
     *
     * @param partition The partition
     * @param offset    The offset to seek to
     */
    public void seek(TopicPartition partition, long offset) {
        ensureConnected();

        Map<String, Object> args = new HashMap<>();
        args.put("topic", partition.topic());
        args.put("partition", partition.partition());
        args.put("offset", offset);

        try {
            client.call("kafka.consumer.seek", args).join();
        } catch (Exception e) {
            throw wrapException(e, "Failed to seek");
        }
    }

    /**
     * Seek to the beginning of partitions.
     *
     * @param partitions The partitions to seek to beginning
     */
    public void seekToBeginning(Collection<TopicPartition> partitions) {
        ensureConnected();

        List<Map<String, Object>> partitionData = new ArrayList<>();
        for (TopicPartition tp : partitions) {
            Map<String, Object> p = new HashMap<>();
            p.put("topic", tp.topic());
            p.put("partition", tp.partition());
            partitionData.add(p);
        }

        Map<String, Object> args = Map.of("partitions", partitionData);

        try {
            client.call("kafka.consumer.seekToBeginning", args).join();
        } catch (Exception e) {
            throw wrapException(e, "Failed to seek to beginning");
        }
    }

    /**
     * Seek to the end of partitions.
     *
     * @param partitions The partitions to seek to end
     */
    public void seekToEnd(Collection<TopicPartition> partitions) {
        ensureConnected();

        List<Map<String, Object>> partitionData = new ArrayList<>();
        for (TopicPartition tp : partitions) {
            Map<String, Object> p = new HashMap<>();
            p.put("topic", tp.topic());
            p.put("partition", tp.partition());
            partitionData.add(p);
        }

        Map<String, Object> args = Map.of("partitions", partitionData);

        try {
            client.call("kafka.consumer.seekToEnd", args).join();
        } catch (Exception e) {
            throw wrapException(e, "Failed to seek to end");
        }
    }

    /**
     * Get the position (offset) of the next record to consume.
     *
     * @param partition The partition
     * @return The position
     */
    public long position(TopicPartition partition) {
        ensureConnected();

        Map<String, Object> args = new HashMap<>();
        args.put("topic", partition.topic());
        args.put("partition", partition.partition());

        try {
            Object result = client.call("kafka.consumer.position", args).join();
            if (result instanceof Number) {
                return ((Number) result).longValue();
            }
            throw new KafkaException("Invalid position response");
        } catch (Exception e) {
            throw wrapException(e, "Failed to get position");
        }
    }

    /**
     * Pause consumption from the specified partitions.
     *
     * @param partitions The partitions to pause
     */
    public void pause(Collection<TopicPartition> partitions) {
        ensureConnected();

        lock.lock();
        try {
            List<Map<String, Object>> partitionData = new ArrayList<>();
            for (TopicPartition tp : partitions) {
                Map<String, Object> p = new HashMap<>();
                p.put("topic", tp.topic());
                p.put("partition", tp.partition());
                partitionData.add(p);
            }

            Map<String, Object> args = Map.of("partitions", partitionData);
            client.call("kafka.consumer.pause", args).join();

            paused.addAll(partitions);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Resume consumption from the specified partitions.
     *
     * @param partitions The partitions to resume
     */
    public void resume(Collection<TopicPartition> partitions) {
        ensureConnected();

        lock.lock();
        try {
            List<Map<String, Object>> partitionData = new ArrayList<>();
            for (TopicPartition tp : partitions) {
                Map<String, Object> p = new HashMap<>();
                p.put("topic", tp.topic());
                p.put("partition", tp.partition());
                partitionData.add(p);
            }

            Map<String, Object> args = Map.of("partitions", partitionData);
            client.call("kafka.consumer.resume", args).join();

            paused.removeAll(partitions);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Returns the set of paused partitions.
     */
    public Set<TopicPartition> paused() {
        lock.lock();
        try {
            return new HashSet<>(paused);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() {
        lock.lock();
        try {
            if (closed) {
                return;
            }

            // Commit if auto-commit is enabled
            if (connected && config.autoCommit() && groupId != null && !groupId.isEmpty()) {
                try {
                    commitSync();
                } catch (Exception e) {
                    // Ignore commit errors on close
                }
            }

            closed = true;
            connected = false;
            client = null;
            subscriptions.clear();
            assignments.clear();
            paused.clear();
        } finally {
            lock.unlock();
        }
    }

    @SuppressWarnings("unchecked")
    private ConsumerRecords<K, V> parseRecords(Object result) {
        if (result == null) {
            return ConsumerRecords.empty();
        }

        if (!(result instanceof List)) {
            return ConsumerRecords.empty();
        }

        List<Object> results = (List<Object>) result;
        List<ConsumerRecord<K, V>> records = new ArrayList<>();

        for (Object r : results) {
            if (r instanceof Map) {
                ConsumerRecord<K, V> record = parseRecord((Map<String, Object>) r);
                if (record != null) {
                    records.add(record);
                }
            }
        }

        return ConsumerRecords.fromList(records);
    }

    @SuppressWarnings("unchecked")
    private ConsumerRecord<K, V> parseRecord(Map<String, Object> m) {
        String topic = (String) m.get("topic");
        int partition = ((Number) m.getOrDefault("partition", 0)).intValue();
        long offset = ((Number) m.getOrDefault("offset", 0L)).longValue();

        K key = null;
        if (m.containsKey("key") && m.get("key") != null) {
            String keyStr = (String) m.get("key");
            key = (K) decodeValue(keyStr);
        }

        V value = null;
        if (m.containsKey("value") && m.get("value") != null) {
            String valueStr = (String) m.get("value");
            value = (V) decodeValue(valueStr);
        }

        Instant timestamp = Instant.now();
        if (m.containsKey("timestamp")) {
            timestamp = Instant.ofEpochMilli(((Number) m.get("timestamp")).longValue());
        }

        Map<String, byte[]> headers = new HashMap<>();
        if (m.containsKey("headers") && m.get("headers") instanceof List) {
            List<Object> headerList = (List<Object>) m.get("headers");
            for (Object h : headerList) {
                if (h instanceof Map) {
                    Map<String, Object> hm = (Map<String, Object>) h;
                    String hKey = (String) hm.get("key");
                    String hValue = (String) hm.get("value");
                    if (hKey != null && hValue != null) {
                        headers.put(hKey, Base64.getDecoder().decode(hValue));
                    }
                }
            }
        }

        return new ConsumerRecord<>(topic, partition, offset, key, value, timestamp, headers);
    }

    private Object decodeValue(String encoded) {
        try {
            byte[] decoded = Base64.getDecoder().decode(encoded);
            return new String(decoded, java.nio.charset.StandardCharsets.UTF_8);
        } catch (Exception e) {
            return encoded;
        }
    }

    @SuppressWarnings("unchecked")
    private Long parseCommittedOffset(Object result, TopicPartition partition) {
        if (!(result instanceof List)) {
            return null;
        }

        List<Object> results = (List<Object>) result;
        for (Object r : results) {
            if (r instanceof Map) {
                Map<String, Object> m = (Map<String, Object>) r;
                String topic = (String) m.get("topic");
                int part = ((Number) m.getOrDefault("partition", -1)).intValue();

                if (partition.topic().equals(topic) && partition.partition() == part) {
                    if (m.containsKey("offset")) {
                        return ((Number) m.get("offset")).longValue();
                    }
                }
            }
        }

        return null;
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
