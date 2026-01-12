package do_.kafka;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * A mock RPC client for testing.
 */
public class MockRpcClient implements RpcClient {

    private boolean connected = true;
    private final Map<String, Function<Object[], Object>> handlers = new ConcurrentHashMap<>();
    private final List<MethodCall> calls = Collections.synchronizedList(new ArrayList<>());
    private final AtomicLong offsetCounter = new AtomicLong(0);
    private final Map<String, List<Map<String, Object>>> topicMessages = new ConcurrentHashMap<>();
    private final Set<String> topics = ConcurrentHashMap.newKeySet();

    public MockRpcClient() {
        setupDefaultHandlers();
    }

    private void setupDefaultHandlers() {
        // Producer handlers
        handlers.put("kafka.producer.send", args -> {
            if (args.length > 0 && args[0] instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> record = (Map<String, Object>) args[0];
                String topic = (String) record.get("topic");
                int partition = record.containsKey("partition") ? ((Number) record.get("partition")).intValue() : 0;
                long offset = offsetCounter.getAndIncrement();

                // Store the message
                topicMessages.computeIfAbsent(topic, k -> Collections.synchronizedList(new ArrayList<>()))
                        .add(record);

                Map<String, Object> result = new HashMap<>();
                result.put("topic", topic);
                result.put("partition", partition);
                result.put("offset", offset);
                result.put("timestamp", System.currentTimeMillis());
                return result;
            }
            throw new RuntimeException("Invalid send args");
        });

        handlers.put("kafka.producer.sendBatch", args -> {
            if (args.length > 0 && args[0] instanceof List) {
                @SuppressWarnings("unchecked")
                List<Map<String, Object>> records = (List<Map<String, Object>>) args[0];
                List<Map<String, Object>> results = new ArrayList<>();

                for (Map<String, Object> record : records) {
                    String topic = (String) record.get("topic");
                    int partition = record.containsKey("partition") ? ((Number) record.get("partition")).intValue() : 0;
                    long offset = offsetCounter.getAndIncrement();

                    topicMessages.computeIfAbsent(topic, k -> Collections.synchronizedList(new ArrayList<>()))
                            .add(record);

                    Map<String, Object> result = new HashMap<>();
                    result.put("topic", topic);
                    result.put("partition", partition);
                    result.put("offset", offset);
                    result.put("timestamp", System.currentTimeMillis());
                    results.add(result);
                }

                return results;
            }
            throw new RuntimeException("Invalid sendBatch args");
        });

        handlers.put("kafka.producer.flush", args -> null);

        handlers.put("kafka.producer.partitionsFor", args -> {
            return List.of(0, 1, 2);
        });

        // Consumer handlers
        handlers.put("kafka.consumer.subscribe", args -> null);
        handlers.put("kafka.consumer.unsubscribe", args -> null);
        handlers.put("kafka.consumer.assign", args -> null);

        handlers.put("kafka.consumer.poll", args -> {
            // Return empty by default, tests can override
            return List.of();
        });

        handlers.put("kafka.consumer.commit", args -> null);

        handlers.put("kafka.consumer.committed", args -> {
            return List.of();
        });

        handlers.put("kafka.consumer.seek", args -> null);
        handlers.put("kafka.consumer.seekToBeginning", args -> null);
        handlers.put("kafka.consumer.seekToEnd", args -> null);

        handlers.put("kafka.consumer.position", args -> {
            return 0L;
        });

        handlers.put("kafka.consumer.pause", args -> null);
        handlers.put("kafka.consumer.resume", args -> null);

        // Admin handlers
        handlers.put("kafka.admin.createTopics", args -> {
            if (args.length > 0 && args[0] instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> request = (Map<String, Object>) args[0];
                @SuppressWarnings("unchecked")
                List<Map<String, Object>> topicConfigs = (List<Map<String, Object>>) request.get("topics");

                List<Map<String, Object>> results = new ArrayList<>();
                for (Map<String, Object> tc : topicConfigs) {
                    String name = (String) tc.get("name");
                    if (topics.contains(name)) {
                        Map<String, Object> result = new HashMap<>();
                        result.put("name", name);
                        Map<String, Object> error = new HashMap<>();
                        error.put("code", "TOPIC_ALREADY_EXISTS");
                        error.put("message", "Topic already exists: " + name);
                        result.put("error", error);
                        results.add(result);
                    } else {
                        topics.add(name);
                        Map<String, Object> result = new HashMap<>();
                        result.put("name", name);
                        results.add(result);
                    }
                }
                return results;
            }
            return List.of();
        });

        handlers.put("kafka.admin.deleteTopics", args -> {
            if (args.length > 0 && args[0] instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> request = (Map<String, Object>) args[0];
                @SuppressWarnings("unchecked")
                List<String> topicNames = (List<String>) request.get("topics");

                List<Map<String, Object>> results = new ArrayList<>();
                for (String name : topicNames) {
                    if (!topics.contains(name)) {
                        Map<String, Object> result = new HashMap<>();
                        result.put("name", name);
                        Map<String, Object> error = new HashMap<>();
                        error.put("code", "TOPIC_NOT_FOUND");
                        error.put("message", "Topic not found: " + name);
                        result.put("error", error);
                        results.add(result);
                    } else {
                        topics.remove(name);
                        Map<String, Object> result = new HashMap<>();
                        result.put("name", name);
                        results.add(result);
                    }
                }
                return results;
            }
            return List.of();
        });

        handlers.put("kafka.admin.listTopics", args -> {
            return new ArrayList<>(topics);
        });

        handlers.put("kafka.admin.describeTopics", args -> {
            if (args.length > 0 && args[0] instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> request = (Map<String, Object>) args[0];
                @SuppressWarnings("unchecked")
                List<String> topicNames = (List<String>) request.get("topics");

                List<Map<String, Object>> results = new ArrayList<>();
                for (String name : topicNames) {
                    if (!topics.contains(name)) {
                        Map<String, Object> result = new HashMap<>();
                        result.put("name", name);
                        Map<String, Object> error = new HashMap<>();
                        error.put("code", "TOPIC_NOT_FOUND");
                        error.put("message", "Topic not found: " + name);
                        result.put("error", error);
                        results.add(result);
                    } else {
                        Map<String, Object> result = new HashMap<>();
                        result.put("name", name);
                        result.put("partitions", 3);
                        result.put("replicationFactor", 1);
                        result.put("config", Map.of("retention.ms", "604800000"));
                        results.add(result);
                    }
                }
                return results;
            }
            return List.of();
        });

        handlers.put("kafka.admin.describePartitions", args -> {
            if (args.length > 0 && args[0] instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> request = (Map<String, Object>) args[0];
                String topic = (String) request.get("topic");

                if (!topics.contains(topic)) {
                    Map<String, Object> result = new HashMap<>();
                    Map<String, Object> error = new HashMap<>();
                    error.put("code", "TOPIC_NOT_FOUND");
                    error.put("message", "Topic not found: " + topic);
                    result.put("error", error);
                    return result;
                }

                List<Map<String, Object>> partitions = new ArrayList<>();
                for (int i = 0; i < 3; i++) {
                    Map<String, Object> p = new HashMap<>();
                    p.put("partition", i);
                    p.put("leader", 0);
                    p.put("replicas", List.of(0));
                    p.put("isr", List.of(0));
                    partitions.add(p);
                }

                Map<String, Object> result = new HashMap<>();
                result.put("topic", topic);
                result.put("partitions", partitions);
                return result;
            }
            return Map.of();
        });

        handlers.put("kafka.admin.listConsumerGroups", args -> {
            return List.of("test-group-1", "test-group-2");
        });

        handlers.put("kafka.admin.describeConsumerGroups", args -> {
            if (args.length > 0 && args[0] instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> request = (Map<String, Object>) args[0];
                @SuppressWarnings("unchecked")
                List<String> groupIds = (List<String>) request.get("groupIds");

                List<Map<String, Object>> results = new ArrayList<>();
                for (String groupId : groupIds) {
                    Map<String, Object> result = new HashMap<>();
                    result.put("groupId", groupId);
                    result.put("state", "Stable");
                    result.put("members", List.of("consumer-1", "consumer-2"));
                    result.put("coordinator", 0);
                    results.add(result);
                }
                return results;
            }
            return List.of();
        });

        handlers.put("kafka.admin.deleteConsumerGroups", args -> {
            return List.of();
        });

        handlers.put("kafka.admin.alterTopicConfig", args -> {
            if (args.length > 0 && args[0] instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> request = (Map<String, Object>) args[0];
                String topic = (String) request.get("topic");

                if (!topics.contains(topic)) {
                    Map<String, Object> result = new HashMap<>();
                    Map<String, Object> error = new HashMap<>();
                    error.put("code", "TOPIC_NOT_FOUND");
                    error.put("message", "Topic not found: " + topic);
                    result.put("error", error);
                    return result;
                }
            }
            return Map.of();
        });
    }

    /**
     * Sets a handler for a specific method.
     */
    public void setHandler(String method, Function<Object[], Object> handler) {
        handlers.put(method, handler);
    }

    /**
     * Adds a topic for testing.
     */
    public void addTopic(String topic) {
        topics.add(topic);
    }

    /**
     * Returns the list of method calls.
     */
    public List<MethodCall> getCalls() {
        return new ArrayList<>(calls);
    }

    /**
     * Returns the messages for a topic.
     */
    public List<Map<String, Object>> getMessages(String topic) {
        return topicMessages.getOrDefault(topic, List.of());
    }

    /**
     * Clears all recorded calls.
     */
    public void clearCalls() {
        calls.clear();
    }

    @Override
    public CompletableFuture<Object> call(String method, Object... args) {
        calls.add(new MethodCall(method, args));

        if (!connected) {
            return CompletableFuture.failedFuture(new ConnectionException("Not connected"));
        }

        Function<Object[], Object> handler = handlers.get(method);
        if (handler != null) {
            try {
                Object result = handler.apply(args);
                return CompletableFuture.completedFuture(result);
            } catch (Exception e) {
                return CompletableFuture.failedFuture(e);
            }
        }

        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void close() {
        connected = false;
    }

    @Override
    public boolean isConnected() {
        return connected;
    }

    public void setConnected(boolean connected) {
        this.connected = connected;
    }

    /**
     * Represents a recorded method call.
     */
    public static class MethodCall {
        private final String method;
        private final Object[] args;

        public MethodCall(String method, Object[] args) {
            this.method = method;
            this.args = args;
        }

        public String method() {
            return method;
        }

        public Object[] args() {
            return args;
        }

        @Override
        public String toString() {
            return method + "(" + Arrays.toString(args) + ")";
        }
    }
}
