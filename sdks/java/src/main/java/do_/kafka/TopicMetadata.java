package do_.kafka;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Metadata about a topic.
 */
public final class TopicMetadata {
    private final String name;
    private final int partitions;
    private final int replicationFactor;
    private final Map<String, String> config;

    /**
     * Creates topic metadata.
     */
    public TopicMetadata(String name, int partitions, int replicationFactor, Map<String, String> config) {
        this.name = name;
        this.partitions = partitions;
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
    public int partitions() {
        return partitions;
    }

    /**
     * Returns the replication factor.
     */
    public int replicationFactor() {
        return replicationFactor;
    }

    /**
     * Returns the topic configuration.
     */
    public Map<String, String> config() {
        return Collections.unmodifiableMap(config);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TopicMetadata that)) return false;
        return partitions == that.partitions &&
                replicationFactor == that.replicationFactor &&
                Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, partitions, replicationFactor);
    }

    @Override
    public String toString() {
        return "TopicMetadata{" +
                "name='" + name + '\'' +
                ", partitions=" + partitions +
                ", replicationFactor=" + replicationFactor +
                '}';
    }
}
