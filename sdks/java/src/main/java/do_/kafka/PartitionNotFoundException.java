package do_.kafka;

/**
 * Exception thrown when a partition is not found.
 */
public class PartitionNotFoundException extends KafkaException {

    private final TopicPartition partition;

    /**
     * Creates a PartitionNotFoundException.
     */
    public PartitionNotFoundException(TopicPartition partition) {
        super("PARTITION_NOT_FOUND", "Partition not found: " + partition, false, null);
        this.partition = partition;
    }

    /**
     * Returns the partition that was not found.
     */
    public TopicPartition partition() {
        return partition;
    }
}
