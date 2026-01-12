package do_.kafka;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Metadata about a partition.
 */
public final class PartitionMetadata {
    private final String topic;
    private final int partition;
    private final int leader;
    private final List<Integer> replicas;
    private final List<Integer> isr;

    /**
     * Creates partition metadata.
     */
    public PartitionMetadata(String topic, int partition, int leader, List<Integer> replicas, List<Integer> isr) {
        this.topic = topic;
        this.partition = partition;
        this.leader = leader;
        this.replicas = replicas != null ? List.copyOf(replicas) : List.of();
        this.isr = isr != null ? List.copyOf(isr) : List.of();
    }

    /**
     * Returns the topic name.
     */
    public String topic() {
        return topic;
    }

    /**
     * Returns the partition number.
     */
    public int partition() {
        return partition;
    }

    /**
     * Returns the broker ID of the leader.
     */
    public int leader() {
        return leader;
    }

    /**
     * Returns the list of replica broker IDs.
     */
    public List<Integer> replicas() {
        return replicas;
    }

    /**
     * Returns the list of in-sync replica broker IDs.
     */
    public List<Integer> isr() {
        return isr;
    }

    /**
     * Returns the TopicPartition for this metadata.
     */
    public TopicPartition topicPartition() {
        return new TopicPartition(topic, partition);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PartitionMetadata that)) return false;
        return partition == that.partition &&
                leader == that.leader &&
                Objects.equals(topic, that.topic);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topic, partition, leader);
    }

    @Override
    public String toString() {
        return "PartitionMetadata{" +
                "topic='" + topic + '\'' +
                ", partition=" + partition +
                ", leader=" + leader +
                ", replicas=" + replicas +
                ", isr=" + isr +
                '}';
    }
}
