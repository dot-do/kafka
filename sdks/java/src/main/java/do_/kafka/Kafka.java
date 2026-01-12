package do_.kafka;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The main Kafka client for creating producers, consumers, and admin clients.
 * <p>
 * Example usage:
 * <pre>{@code
 * Kafka kafka = new Kafka(Arrays.asList("broker1:9092"));
 * KafkaProducer<String, String> producer = kafka.createProducer();
 * producer.send(new ProducerRecord<>("topic", "key", "value")).get();
 *
 * KafkaConsumer<String, String> consumer = kafka.createConsumer("group");
 * consumer.subscribe(Arrays.asList("topic"));
 * ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
 * }</pre>
 */
public class Kafka implements AutoCloseable {

    private final Config config;
    private volatile RpcClient client;
    private volatile boolean closed;
    private final ReentrantLock lock = new ReentrantLock();

    /**
     * Creates a Kafka client with the specified brokers.
     *
     * @param brokers The list of broker addresses
     */
    public Kafka(List<String> brokers) {
        this(new Config.Builder().brokers(brokers).build());
    }

    /**
     * Creates a Kafka client with the specified configuration.
     *
     * @param config The configuration
     */
    public Kafka(Config config) {
        Objects.requireNonNull(config, "config cannot be null");
        this.config = config;
    }

    /**
     * Returns the configuration.
     */
    public Config config() {
        return config;
    }

    /**
     * Returns the list of broker addresses.
     */
    public List<String> brokers() {
        return config.brokers();
    }

    /**
     * Sets the RPC client. This is primarily used for testing.
     */
    public void setRpcClient(RpcClient client) {
        lock.lock();
        try {
            this.client = client;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Gets the RPC client, connecting if necessary.
     */
    RpcClient getRpcClient() {
        if (closed) {
            throw new KafkaException("Kafka client is closed");
        }
        if (client != null) {
            return client;
        }

        lock.lock();
        try {
            if (closed) {
                throw new KafkaException("Kafka client is closed");
            }
            if (client != null) {
                return client;
            }
            // In a real implementation, this would connect via kafka.do RPC
            throw new ConnectionException("RPC client not configured");
        } finally {
            lock.unlock();
        }
    }

    /**
     * Creates a new producer with default configuration.
     *
     * @param <K> The key type
     * @param <V> The value type
     * @return A new KafkaProducer
     */
    public <K, V> KafkaProducer<K, V> createProducer() {
        return createProducer(new ProducerConfig.Builder().build());
    }

    /**
     * Creates a new producer with the specified configuration.
     *
     * @param config The producer configuration
     * @param <K>    The key type
     * @param <V>    The value type
     * @return A new KafkaProducer
     */
    public <K, V> KafkaProducer<K, V> createProducer(ProducerConfig config) {
        if (closed) {
            throw new KafkaException("Kafka client is closed");
        }
        return new KafkaProducer<>(this, config);
    }

    /**
     * Creates a new consumer with the specified group ID and default configuration.
     *
     * @param groupId The consumer group ID
     * @param <K>     The key type
     * @param <V>     The value type
     * @return A new KafkaConsumer
     */
    public <K, V> KafkaConsumer<K, V> createConsumer(String groupId) {
        return createConsumer(groupId, new ConsumerConfig.Builder().build());
    }

    /**
     * Creates a new consumer with the specified group ID and configuration.
     *
     * @param groupId The consumer group ID
     * @param config  The consumer configuration
     * @param <K>     The key type
     * @param <V>     The value type
     * @return A new KafkaConsumer
     */
    public <K, V> KafkaConsumer<K, V> createConsumer(String groupId, ConsumerConfig config) {
        if (closed) {
            throw new KafkaException("Kafka client is closed");
        }
        return new KafkaConsumer<>(this, groupId, config);
    }

    /**
     * Creates a new admin client with default configuration.
     *
     * @return A new KafkaAdmin
     */
    public KafkaAdmin createAdmin() {
        return createAdmin(new AdminConfig.Builder().build());
    }

    /**
     * Creates a new admin client with the specified configuration.
     *
     * @param config The admin configuration
     * @return A new KafkaAdmin
     */
    public KafkaAdmin createAdmin(AdminConfig config) {
        if (closed) {
            throw new KafkaException("Kafka client is closed");
        }
        return new KafkaAdmin(this, config);
    }

    /**
     * Returns true if the client is closed.
     */
    public boolean isClosed() {
        return closed;
    }

    @Override
    public void close() {
        lock.lock();
        try {
            if (closed) {
                return;
            }
            closed = true;
            if (client != null) {
                client.close();
                client = null;
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Configuration for the Kafka client.
     */
    public static final class Config {
        private final List<String> brokers;
        private final String clientId;
        private final Duration timeout;
        private final String connectionUrl;

        private Config(Builder builder) {
            this.brokers = Collections.unmodifiableList(new ArrayList<>(builder.brokers));
            this.clientId = builder.clientId;
            this.timeout = builder.timeout;
            this.connectionUrl = builder.connectionUrl;
        }

        public List<String> brokers() {
            return brokers;
        }

        public String clientId() {
            return clientId;
        }

        public Duration timeout() {
            return timeout;
        }

        public String connectionUrl() {
            return connectionUrl;
        }

        public static final class Builder {
            private List<String> brokers = new ArrayList<>();
            private String clientId;
            private Duration timeout = Duration.ofSeconds(30);
            private String connectionUrl = "kafka.do";

            public Builder brokers(List<String> brokers) {
                this.brokers = new ArrayList<>(brokers);
                return this;
            }

            public Builder broker(String broker) {
                this.brokers.add(broker);
                return this;
            }

            public Builder clientId(String clientId) {
                this.clientId = clientId;
                return this;
            }

            public Builder timeout(Duration timeout) {
                this.timeout = timeout;
                return this;
            }

            public Builder connectionUrl(String connectionUrl) {
                this.connectionUrl = connectionUrl;
                return this;
            }

            public Config build() {
                return new Config(this);
            }
        }
    }
}
