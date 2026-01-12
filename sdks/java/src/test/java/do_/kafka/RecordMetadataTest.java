package do_.kafka;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests for the RecordMetadata class.
 */
class RecordMetadataTest {

    @Test
    void shouldCreateWithAllFields() {
        Instant timestamp = Instant.parse("2025-01-01T12:00:00Z");

        RecordMetadata metadata = new RecordMetadata(
                "my-topic", 2, 100L, timestamp, 10, 256
        );

        assertThat(metadata.topic()).isEqualTo("my-topic");
        assertThat(metadata.partition()).isEqualTo(2);
        assertThat(metadata.offset()).isEqualTo(100L);
        assertThat(metadata.timestamp()).isEqualTo(timestamp);
        assertThat(metadata.serializedKeySize()).isEqualTo(10);
        assertThat(metadata.serializedValueSize()).isEqualTo(256);
    }

    @Test
    void shouldCreateWithMinimalFields() {
        RecordMetadata metadata = new RecordMetadata("topic", 0, 50L);

        assertThat(metadata.topic()).isEqualTo("topic");
        assertThat(metadata.partition()).isEqualTo(0);
        assertThat(metadata.offset()).isEqualTo(50L);
        assertThat(metadata.timestamp()).isNotNull();
        assertThat(metadata.serializedKeySize()).isEqualTo(-1);
        assertThat(metadata.serializedValueSize()).isEqualTo(-1);
    }

    @Test
    void shouldReturnTopicPartition() {
        RecordMetadata metadata = new RecordMetadata("my-topic", 3, 100L);

        TopicPartition tp = metadata.topicPartition();

        assertThat(tp.topic()).isEqualTo("my-topic");
        assertThat(tp.partition()).isEqualTo(3);
    }

    @Test
    void shouldBeEqualWhenCoreFieldsMatch() {
        RecordMetadata metadata1 = new RecordMetadata("topic", 1, 100L);
        RecordMetadata metadata2 = new RecordMetadata("topic", 1, 100L);

        assertThat(metadata1).isEqualTo(metadata2);
        assertThat(metadata1.hashCode()).isEqualTo(metadata2.hashCode());
    }

    @Test
    void shouldNotBeEqualWhenTopicDiffers() {
        RecordMetadata metadata1 = new RecordMetadata("topic-a", 0, 0L);
        RecordMetadata metadata2 = new RecordMetadata("topic-b", 0, 0L);

        assertThat(metadata1).isNotEqualTo(metadata2);
    }

    @Test
    void shouldNotBeEqualWhenPartitionDiffers() {
        RecordMetadata metadata1 = new RecordMetadata("topic", 0, 0L);
        RecordMetadata metadata2 = new RecordMetadata("topic", 1, 0L);

        assertThat(metadata1).isNotEqualTo(metadata2);
    }

    @Test
    void shouldNotBeEqualWhenOffsetDiffers() {
        RecordMetadata metadata1 = new RecordMetadata("topic", 0, 100L);
        RecordMetadata metadata2 = new RecordMetadata("topic", 0, 200L);

        assertThat(metadata1).isNotEqualTo(metadata2);
    }

    @Test
    void shouldNotBeEqualToNull() {
        RecordMetadata metadata = new RecordMetadata("topic", 0, 0L);

        assertThat(metadata).isNotEqualTo(null);
    }

    @Test
    void shouldNotBeEqualToDifferentType() {
        RecordMetadata metadata = new RecordMetadata("topic", 0, 0L);

        assertThat(metadata).isNotEqualTo("topic");
    }

    @Test
    void shouldHaveReadableToString() {
        Instant timestamp = Instant.parse("2025-01-01T12:00:00Z");
        RecordMetadata metadata = new RecordMetadata(
                "my-topic", 2, 100L, timestamp, 10, 256
        );

        String str = metadata.toString();

        assertThat(str).contains("my-topic");
        assertThat(str).contains("2");
        assertThat(str).contains("100");
        assertThat(str).contains("2025-01-01");
    }

    @Test
    void shouldHaveConsistentHashCode() {
        RecordMetadata metadata = new RecordMetadata("topic", 0, 100L);
        int hash1 = metadata.hashCode();
        int hash2 = metadata.hashCode();

        assertThat(hash1).isEqualTo(hash2);
    }

    @Test
    void shouldBeEqualToItself() {
        RecordMetadata metadata = new RecordMetadata("topic", 0, 100L);

        assertThat(metadata).isEqualTo(metadata);
    }

    @Test
    void shouldAllowNullTopic() {
        // The implementation doesn't prevent null topics,
        // but real Kafka would never return null
        RecordMetadata metadata = new RecordMetadata(null, 0, 0L);

        assertThat(metadata.topic()).isNull();
    }

    @Test
    void shouldHandleZeroOffset() {
        RecordMetadata metadata = new RecordMetadata("topic", 0, 0L);

        assertThat(metadata.offset()).isEqualTo(0L);
    }

    @Test
    void shouldHandleLargeOffset() {
        long largeOffset = Long.MAX_VALUE;
        RecordMetadata metadata = new RecordMetadata("topic", 0, largeOffset);

        assertThat(metadata.offset()).isEqualTo(largeOffset);
    }

    @Test
    void shouldHandleNegativeSerializedSizes() {
        // -1 indicates size is not available
        RecordMetadata metadata = new RecordMetadata(
                "topic", 0, 0L, Instant.now(), -1, -1
        );

        assertThat(metadata.serializedKeySize()).isEqualTo(-1);
        assertThat(metadata.serializedValueSize()).isEqualTo(-1);
    }

    @Test
    void shouldHandleZeroSerializedSizes() {
        // 0 indicates null key/value
        RecordMetadata metadata = new RecordMetadata(
                "topic", 0, 0L, Instant.now(), 0, 0
        );

        assertThat(metadata.serializedKeySize()).isEqualTo(0);
        assertThat(metadata.serializedValueSize()).isEqualTo(0);
    }

    @Test
    void shouldHandleLargeSerializedSizes() {
        RecordMetadata metadata = new RecordMetadata(
                "topic", 0, 0L, Instant.now(), Integer.MAX_VALUE, Integer.MAX_VALUE
        );

        assertThat(metadata.serializedKeySize()).isEqualTo(Integer.MAX_VALUE);
        assertThat(metadata.serializedValueSize()).isEqualTo(Integer.MAX_VALUE);
    }

    @Test
    void shouldCreateTopicPartitionFromMetadata() {
        RecordMetadata metadata = new RecordMetadata("test-topic", 7, 999L);

        TopicPartition tp = metadata.topicPartition();

        assertThat(tp).isEqualTo(new TopicPartition("test-topic", 7));
    }

    @Test
    void equalityIgnoresTimestampAndSizes() {
        Instant time1 = Instant.parse("2025-01-01T00:00:00Z");
        Instant time2 = Instant.parse("2025-06-01T00:00:00Z");

        RecordMetadata metadata1 = new RecordMetadata("topic", 0, 0L, time1, 10, 100);
        RecordMetadata metadata2 = new RecordMetadata("topic", 0, 0L, time2, 20, 200);

        // Equality is based on topic, partition, offset only
        assertThat(metadata1).isEqualTo(metadata2);
    }
}
