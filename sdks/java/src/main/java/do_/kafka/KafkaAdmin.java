package do_.kafka;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A Kafka admin client for managing topics and consumer groups.
 * <p>
 * Example usage:
 * <pre>{@code
 * Kafka kafka = new Kafka(Arrays.asList("broker1:9092"));
 * KafkaAdmin admin = kafka.createAdmin();
 *
 * // Create topics
 * admin.createTopics(Arrays.asList(new NewTopic("my-topic", 3, (short) 1))).get();
 *
 * // List topics
 * List<String> topics = admin.listTopics().get();
 *
 * // Describe topics
 * List<TopicMetadata> metadata = admin.describeTopics(Arrays.asList("my-topic")).get();
 *
 * // Delete topics
 * admin.deleteTopics(Arrays.asList("my-topic")).get();
 * }</pre>
 */
public class KafkaAdmin implements AutoCloseable {

    private final Kafka kafka;
    private final AdminConfig config;
    private volatile RpcClient client;
    private volatile boolean connected;
    private volatile boolean closed;
    private final ReentrantLock lock = new ReentrantLock();

    /**
     * Creates a KafkaAdmin.
     */
    KafkaAdmin(Kafka kafka, AdminConfig config) {
        this.kafka = Objects.requireNonNull(kafka, "kafka cannot be null");
        this.config = Objects.requireNonNull(config, "config cannot be null");
    }

    /**
     * Returns true if the admin client is connected.
     */
    public boolean isConnected() {
        return connected && !closed;
    }

    /**
     * Connects the admin client to the Kafka cluster.
     */
    public void connect() {
        lock.lock();
        try {
            if (connected) {
                return;
            }
            if (closed) {
                throw new KafkaException("Admin client is closed");
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
            throw new KafkaException("Admin client is closed");
        }
    }

    /**
     * Creates new topics.
     *
     * @param topics The topics to create
     * @return A CompletableFuture that completes when the operation is done
     */
    public CompletableFuture<Void> createTopics(Collection<NewTopic> topics) {
        return createTopics(topics, false);
    }

    /**
     * Creates new topics.
     *
     * @param topics       The topics to create
     * @param validateOnly If true, only validate the request without creating topics
     * @return A CompletableFuture that completes when the operation is done
     */
    public CompletableFuture<Void> createTopics(Collection<NewTopic> topics, boolean validateOnly) {
        Objects.requireNonNull(topics, "topics cannot be null");
        ensureConnected();

        List<Map<String, Object>> topicConfigs = new ArrayList<>();
        for (NewTopic topic : topics) {
            Map<String, Object> tc = new HashMap<>();
            tc.put("name", topic.name());
            tc.put("numPartitions", topic.numPartitions());
            tc.put("replicationFactor", topic.replicationFactor());
            tc.put("config", topic.config());
            topicConfigs.add(tc);
        }

        Map<String, Object> args = new HashMap<>();
        args.put("topics", topicConfigs);
        args.put("validateOnly", validateOnly);

        return client.call("kafka.admin.createTopics", args)
                .thenApply(result -> {
                    checkCreateTopicsResult(result);
                    return null;
                })
                .exceptionally(e -> {
                    throw wrapException(e, "Failed to create topics");
                });
    }

    /**
     * Deletes topics.
     *
     * @param topics The topic names to delete
     * @return A CompletableFuture that completes when the operation is done
     */
    public CompletableFuture<Void> deleteTopics(Collection<String> topics) {
        Objects.requireNonNull(topics, "topics cannot be null");
        ensureConnected();

        Map<String, Object> args = Map.of("topics", new ArrayList<>(topics));

        return client.call("kafka.admin.deleteTopics", args)
                .thenApply(result -> {
                    checkDeleteTopicsResult(result);
                    return null;
                })
                .exceptionally(e -> {
                    throw wrapException(e, "Failed to delete topics");
                });
    }

    /**
     * Lists all topics.
     *
     * @return A CompletableFuture that completes with the list of topic names
     */
    public CompletableFuture<List<String>> listTopics() {
        return listTopics(false);
    }

    /**
     * Lists topics.
     *
     * @param includeInternal If true, include internal topics
     * @return A CompletableFuture that completes with the list of topic names
     */
    public CompletableFuture<List<String>> listTopics(boolean includeInternal) {
        ensureConnected();

        Map<String, Object> args = Map.of("includeInternal", includeInternal);

        return client.call("kafka.admin.listTopics", args)
                .thenApply(this::parseStringList)
                .exceptionally(e -> {
                    throw wrapException(e, "Failed to list topics");
                });
    }

    /**
     * Describes topics.
     *
     * @param topics The topic names to describe
     * @return A CompletableFuture that completes with the topic metadata
     */
    public CompletableFuture<List<TopicMetadata>> describeTopics(Collection<String> topics) {
        Objects.requireNonNull(topics, "topics cannot be null");
        ensureConnected();

        Map<String, Object> args = Map.of("topics", new ArrayList<>(topics));

        return client.call("kafka.admin.describeTopics", args)
                .thenApply(this::parseTopicMetadataList)
                .exceptionally(e -> {
                    throw wrapException(e, "Failed to describe topics");
                });
    }

    /**
     * Describes partitions for a topic.
     *
     * @param topic The topic name
     * @return A CompletableFuture that completes with the partition metadata
     */
    public CompletableFuture<List<PartitionMetadata>> describePartitions(String topic) {
        Objects.requireNonNull(topic, "topic cannot be null");
        ensureConnected();

        Map<String, Object> args = Map.of("topic", topic);

        return client.call("kafka.admin.describePartitions", args)
                .thenApply(result -> parsePartitionMetadataList(result, topic))
                .exceptionally(e -> {
                    throw wrapException(e, "Failed to describe partitions");
                });
    }

    /**
     * Lists all consumer groups.
     *
     * @return A CompletableFuture that completes with the list of group IDs
     */
    public CompletableFuture<List<String>> listConsumerGroups() {
        ensureConnected();

        return client.call("kafka.admin.listConsumerGroups")
                .thenApply(this::parseStringList)
                .exceptionally(e -> {
                    throw wrapException(e, "Failed to list consumer groups");
                });
    }

    /**
     * Describes consumer groups.
     *
     * @param groupIds The group IDs to describe
     * @return A CompletableFuture that completes with the group metadata
     */
    public CompletableFuture<List<ConsumerGroupMetadata>> describeConsumerGroups(Collection<String> groupIds) {
        Objects.requireNonNull(groupIds, "groupIds cannot be null");
        ensureConnected();

        Map<String, Object> args = Map.of("groupIds", new ArrayList<>(groupIds));

        return client.call("kafka.admin.describeConsumerGroups", args)
                .thenApply(this::parseConsumerGroupMetadataList)
                .exceptionally(e -> {
                    throw wrapException(e, "Failed to describe consumer groups");
                });
    }

    /**
     * Deletes consumer groups.
     *
     * @param groupIds The group IDs to delete
     * @return A CompletableFuture that completes when the operation is done
     */
    public CompletableFuture<Void> deleteConsumerGroups(Collection<String> groupIds) {
        Objects.requireNonNull(groupIds, "groupIds cannot be null");
        ensureConnected();

        Map<String, Object> args = Map.of("groupIds", new ArrayList<>(groupIds));

        return client.call("kafka.admin.deleteConsumerGroups", args)
                .thenApply(result -> {
                    checkDeleteConsumerGroupsResult(result);
                    return null;
                })
                .exceptionally(e -> {
                    throw wrapException(e, "Failed to delete consumer groups");
                });
    }

    /**
     * Alters topic configuration.
     *
     * @param topic  The topic name
     * @param config The configuration to set
     * @return A CompletableFuture that completes when the operation is done
     */
    public CompletableFuture<Void> alterTopicConfig(String topic, Map<String, String> config) {
        Objects.requireNonNull(topic, "topic cannot be null");
        Objects.requireNonNull(config, "config cannot be null");
        ensureConnected();

        Map<String, Object> args = new HashMap<>();
        args.put("topic", topic);
        args.put("config", config);

        return client.call("kafka.admin.alterTopicConfig", args)
                .thenApply(result -> {
                    checkAlterTopicConfigResult(result, topic);
                    return null;
                })
                .exceptionally(e -> {
                    throw wrapException(e, "Failed to alter topic config");
                });
    }

    @Override
    public void close() {
        lock.lock();
        try {
            if (closed) {
                return;
            }
            closed = true;
            connected = false;
            client = null;
        } finally {
            lock.unlock();
        }
    }

    @SuppressWarnings("unchecked")
    private void checkCreateTopicsResult(Object result) {
        if (!(result instanceof List)) {
            return;
        }
        List<Object> results = (List<Object>) result;
        for (Object r : results) {
            if (r instanceof Map) {
                Map<String, Object> rm = (Map<String, Object>) r;
                if (rm.containsKey("error") && rm.get("error") != null) {
                    Map<String, Object> errMap = (Map<String, Object>) rm.get("error");
                    String code = (String) errMap.get("code");
                    String message = (String) errMap.get("message");
                    String topicName = (String) rm.get("name");

                    if ("TOPIC_ALREADY_EXISTS".equals(code)) {
                        throw new TopicAlreadyExistsException(topicName);
                    }
                    throw new KafkaException(code, message);
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void checkDeleteTopicsResult(Object result) {
        if (!(result instanceof List)) {
            return;
        }
        List<Object> results = (List<Object>) result;
        for (Object r : results) {
            if (r instanceof Map) {
                Map<String, Object> rm = (Map<String, Object>) r;
                if (rm.containsKey("error") && rm.get("error") != null) {
                    Map<String, Object> errMap = (Map<String, Object>) rm.get("error");
                    String code = (String) errMap.get("code");
                    String message = (String) errMap.get("message");
                    String topicName = (String) rm.get("name");

                    if ("TOPIC_NOT_FOUND".equals(code)) {
                        throw new TopicNotFoundException(topicName);
                    }
                    throw new KafkaException(code, message);
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void checkDeleteConsumerGroupsResult(Object result) {
        if (!(result instanceof List)) {
            return;
        }
        List<Object> results = (List<Object>) result;
        for (Object r : results) {
            if (r instanceof Map) {
                Map<String, Object> rm = (Map<String, Object>) r;
                if (rm.containsKey("error") && rm.get("error") != null) {
                    Map<String, Object> errMap = (Map<String, Object>) rm.get("error");
                    String code = (String) errMap.get("code");
                    String message = (String) errMap.get("message");
                    throw new KafkaException(code, message);
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void checkAlterTopicConfigResult(Object result, String topic) {
        if (!(result instanceof Map)) {
            return;
        }
        Map<String, Object> rm = (Map<String, Object>) result;
        if (rm.containsKey("error") && rm.get("error") != null) {
            Map<String, Object> errMap = (Map<String, Object>) rm.get("error");
            String code = (String) errMap.get("code");
            String message = (String) errMap.get("message");

            if ("TOPIC_NOT_FOUND".equals(code)) {
                throw new TopicNotFoundException(topic);
            }
            throw new KafkaException(code, message);
        }
    }

    @SuppressWarnings("unchecked")
    private List<String> parseStringList(Object result) {
        if (result instanceof List) {
            List<Object> list = (List<Object>) result;
            List<String> strings = new ArrayList<>();
            for (Object item : list) {
                if (item instanceof String) {
                    strings.add((String) item);
                }
            }
            return strings;
        }
        return List.of();
    }

    @SuppressWarnings("unchecked")
    private List<TopicMetadata> parseTopicMetadataList(Object result) {
        if (!(result instanceof List)) {
            return List.of();
        }

        List<Object> results = (List<Object>) result;
        List<TopicMetadata> metadata = new ArrayList<>();

        for (Object r : results) {
            if (!(r instanceof Map)) {
                continue;
            }
            Map<String, Object> m = (Map<String, Object>) r;

            // Check for error
            if (m.containsKey("error") && m.get("error") != null) {
                Map<String, Object> errMap = (Map<String, Object>) m.get("error");
                String code = (String) errMap.get("code");
                String topicName = (String) m.get("name");

                if ("TOPIC_NOT_FOUND".equals(code)) {
                    throw new TopicNotFoundException(topicName);
                }
                String message = (String) errMap.get("message");
                throw new KafkaException(code, message);
            }

            String name = (String) m.get("name");
            int partitions = ((Number) m.getOrDefault("partitions", 0)).intValue();
            int rf = ((Number) m.getOrDefault("replicationFactor", 0)).intValue();

            Map<String, String> config = new HashMap<>();
            if (m.containsKey("config") && m.get("config") instanceof Map) {
                Map<String, Object> configMap = (Map<String, Object>) m.get("config");
                for (Map.Entry<String, Object> entry : configMap.entrySet()) {
                    if (entry.getValue() instanceof String) {
                        config.put(entry.getKey(), (String) entry.getValue());
                    }
                }
            }

            metadata.add(new TopicMetadata(name, partitions, rf, config));
        }

        return metadata;
    }

    @SuppressWarnings("unchecked")
    private List<PartitionMetadata> parsePartitionMetadataList(Object result, String topic) {
        if (!(result instanceof Map)) {
            return List.of();
        }

        Map<String, Object> m = (Map<String, Object>) result;

        // Check for error
        if (m.containsKey("error") && m.get("error") != null) {
            Map<String, Object> errMap = (Map<String, Object>) m.get("error");
            String code = (String) errMap.get("code");

            if ("TOPIC_NOT_FOUND".equals(code)) {
                throw new TopicNotFoundException(topic);
            }
            String message = (String) errMap.get("message");
            throw new KafkaException(code, message);
        }

        if (!(m.get("partitions") instanceof List)) {
            return List.of();
        }

        List<Object> partitions = (List<Object>) m.get("partitions");
        List<PartitionMetadata> metadata = new ArrayList<>();

        for (Object p : partitions) {
            if (!(p instanceof Map)) {
                continue;
            }
            Map<String, Object> pm = (Map<String, Object>) p;

            int partition = ((Number) pm.getOrDefault("partition", 0)).intValue();
            int leader = ((Number) pm.getOrDefault("leader", 0)).intValue();

            List<Integer> replicas = new ArrayList<>();
            if (pm.get("replicas") instanceof List) {
                for (Object r : (List<Object>) pm.get("replicas")) {
                    if (r instanceof Number) {
                        replicas.add(((Number) r).intValue());
                    }
                }
            }

            List<Integer> isr = new ArrayList<>();
            if (pm.get("isr") instanceof List) {
                for (Object r : (List<Object>) pm.get("isr")) {
                    if (r instanceof Number) {
                        isr.add(((Number) r).intValue());
                    }
                }
            }

            metadata.add(new PartitionMetadata(topic, partition, leader, replicas, isr));
        }

        return metadata;
    }

    @SuppressWarnings("unchecked")
    private List<ConsumerGroupMetadata> parseConsumerGroupMetadataList(Object result) {
        if (!(result instanceof List)) {
            return List.of();
        }

        List<Object> results = (List<Object>) result;
        List<ConsumerGroupMetadata> metadata = new ArrayList<>();

        for (Object r : results) {
            if (!(r instanceof Map)) {
                continue;
            }
            Map<String, Object> m = (Map<String, Object>) r;

            // Check for error
            if (m.containsKey("error") && m.get("error") != null) {
                Map<String, Object> errMap = (Map<String, Object>) m.get("error");
                String code = (String) errMap.get("code");
                String message = (String) errMap.get("message");
                throw new KafkaException(code, message);
            }

            String groupId = (String) m.get("groupId");
            String state = (String) m.get("state");
            int coordinator = ((Number) m.getOrDefault("coordinator", 0)).intValue();

            List<String> members = new ArrayList<>();
            if (m.get("members") instanceof List) {
                for (Object member : (List<Object>) m.get("members")) {
                    if (member instanceof String) {
                        members.add((String) member);
                    }
                }
            }

            metadata.add(new ConsumerGroupMetadata(groupId, state, members, coordinator));
        }

        return metadata;
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
