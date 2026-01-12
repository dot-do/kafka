package do_.kafka;

/**
 * Exception thrown when an offset is out of range.
 */
public class OffsetOutOfRangeException extends KafkaException {

    private final TopicPartition partition;
    private final long offset;

    /**
     * Creates an OffsetOutOfRangeException.
     */
    public OffsetOutOfRangeException(TopicPartition partition, long offset) {
        super("OFFSET_OUT_OF_RANGE",
              String.format("Offset %d is out of range for %s", offset, partition),
              false, null);
        this.partition = partition;
        this.offset = offset;
    }

    /**
     * Creates an OffsetOutOfRangeException with just a message.
     */
    public OffsetOutOfRangeException(String message) {
        super("OFFSET_OUT_OF_RANGE", message, false, null);
        this.partition = null;
        this.offset = -1;
    }

    /**
     * Returns the partition.
     */
    public TopicPartition partition() {
        return partition;
    }

    /**
     * Returns the offset.
     */
    public long offset() {
        return offset;
    }
}
