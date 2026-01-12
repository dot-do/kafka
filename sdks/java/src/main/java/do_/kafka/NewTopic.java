package do_.kafka;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * A specification for a new topic to be created.
 */
public final class NewTopic {
    private final String name;
    private final int numPartitions;
    private final short replicationFactor;
    private final Map<String, String> config;

    /**
     * Creates a builder for a NewTopic.
     *
     * @param name The topic name
     * @return a new Builder
     */
    public static Builder builder(String name) {
        return new Builder(name);
    }

    /**
     * Creates a new topic specification.
     *
     * @param name              The topic name
     * @param numPartitions     The number of partitions for the topic
     * @param replicationFactor The replication factor for each partition
     */
    public NewTopic(String name, int numPartitions, short replicationFactor) {
        this(name, numPartitions, replicationFactor, null);
    }

    /**
     * Creates a new topic specification with configuration.
     *
     * @param name              The topic name
     * @param numPartitions     The number of partitions for the topic
     * @param replicationFactor The replication factor for each partition
     * @param config            The topic configuration
     */
    public NewTopic(String name, int numPartitions, short replicationFactor, Map<String, String> config) {
        Objects.requireNonNull(name, "name cannot be null");
        if (numPartitions < 1) {
            throw new IllegalArgumentException("numPartitions must be at least 1");
        }
        if (replicationFactor < 1) {
            throw new IllegalArgumentException("replicationFactor must be at least 1");
        }
        this.name = name;
        this.numPartitions = numPartitions;
        this.replicationFactor = replicationFactor;
        this.config = config != null ? new HashMap<>(config) : new HashMap<>();
    }

    /**
     * Returns the topic name.
     */
    public String name() {
        return name;
    }

    /**
     * Returns the number of partitions.
     */
    public int numPartitions() {
        return numPartitions;
    }

    /**
     * Returns the replication factor.
     */
    public short replicationFactor() {
        return replicationFactor;
    }

    /**
     * Returns the topic configuration.
     */
    public Map<String, String> config() {
        return Collections.unmodifiableMap(config);
    }

    /**
     * Adds a configuration entry.
     */
    public NewTopic config(String key, String value) {
        Map<String, String> newConfig = new HashMap<>(config);
        newConfig.put(key, value);
        return new NewTopic(name, numPartitions, replicationFactor, newConfig);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof NewTopic that)) return false;
        return numPartitions == that.numPartitions &&
                replicationFactor == that.replicationFactor &&
                Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, numPartitions, replicationFactor);
    }

    @Override
    public String toString() {
        return "NewTopic{" +
                "name='" + name + '\'' +
                ", numPartitions=" + numPartitions +
                ", replicationFactor=" + replicationFactor +
                ", config=" + config +
                '}';
    }

    /**
     * Builder for NewTopic.
     */
    public static final class Builder {
        private final String name;
        private int numPartitions = 1;
        private short replicationFactor = 1;
        private final Map<String, String> config = new HashMap<>();

        Builder(String name) {
            this.name = Objects.requireNonNull(name, "name cannot be null");
        }

        /**
         * Sets the number of partitions.
         */
        public Builder numPartitions(int numPartitions) {
            this.numPartitions = numPartitions;
            return this;
        }

        /**
         * Sets the replication factor.
         */
        public Builder replicationFactor(short replicationFactor) {
            this.replicationFactor = replicationFactor;
            return this;
        }

        /**
         * Adds a configuration entry.
         */
        public Builder config(String key, String value) {
            this.config.put(key, value);
            return this;
        }

        /**
         * Adds multiple configuration entries.
         */
        public Builder configs(Map<String, String> configs) {
            this.config.putAll(configs);
            return this;
        }

        /**
         * Builds the NewTopic.
         */
        public NewTopic build() {
            return new NewTopic(name, numPartitions, replicationFactor, config);
        }
    }
}
