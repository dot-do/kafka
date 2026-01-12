package do_.kafka;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests for the ConsumerRecord class.
 */
class ConsumerRecordTest {

    @Test
    void shouldCreateRecordWithAllFields() {
        Instant timestamp = Instant.parse("2025-01-01T12:00:00Z");
        Map<String, byte[]> headers = Map.of(
                "correlation-id", "abc-123".getBytes(StandardCharsets.UTF_8)
        );

        ConsumerRecord<String, String> record = new ConsumerRecord<>(
                "my-topic", 2, 100L, "my-key", "my-value", timestamp, headers
        );

        assertThat(record.topic()).isEqualTo("my-topic");
        assertThat(record.partition()).isEqualTo(2);
        assertThat(record.offset()).isEqualTo(100L);
        assertThat(record.key()).isEqualTo("my-key");
        assertThat(record.value()).isEqualTo("my-value");
        assertThat(record.timestamp()).isEqualTo(timestamp);
        assertThat(record.headers()).containsKey("correlation-id");
    }

    @Test
    void shouldCreateRecordWithMinimalFields() {
        ConsumerRecord<String, String> record = new ConsumerRecord<>(
                "my-topic", 0, 50L, "key", "value"
        );

        assertThat(record.topic()).isEqualTo("my-topic");
        assertThat(record.partition()).isEqualTo(0);
        assertThat(record.offset()).isEqualTo(50L);
        assertThat(record.key()).isEqualTo("key");
        assertThat(record.value()).isEqualTo("value");
        assertThat(record.timestamp()).isNotNull();
        assertThat(record.headers()).isEmpty();
    }

    @Test
    void shouldAllowNullKey() {
        ConsumerRecord<String, String> record = new ConsumerRecord<>(
                "topic", 0, 0L, null, "value"
        );

        assertThat(record.key()).isNull();
    }

    @Test
    void shouldReturnTopicPartition() {
        ConsumerRecord<String, String> record = new ConsumerRecord<>(
                "my-topic", 3, 100L, "key", "value"
        );

        TopicPartition tp = record.topicPartition();

        assertThat(tp.topic()).isEqualTo("my-topic");
        assertThat(tp.partition()).isEqualTo(3);
    }

    @Test
    void shouldReturnUnmodifiableHeaders() {
        Map<String, byte[]> headers = new HashMap<>();
        headers.put("key", "value".getBytes());

        ConsumerRecord<String, String> record = new ConsumerRecord<>(
                "topic", 0, 0L, null, "value", Instant.now(), headers
        );

        assertThatThrownBy(() -> record.headers().put("new-key", new byte[0]))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void shouldHandleNullHeaders() {
        ConsumerRecord<String, String> record = new ConsumerRecord<>(
                "topic", 0, 0L, null, "value", Instant.now(), null
        );

        assertThat(record.headers()).isEmpty();
    }

    @Test
    void shouldHandleEmptyHeaders() {
        ConsumerRecord<String, String> record = new ConsumerRecord<>(
                "topic", 0, 0L, null, "value", Instant.now(), Map.of()
        );

        assertThat(record.headers()).isEmpty();
    }

    @Test
    void shouldBeEqualWhenCoreFieldsMatch() {
        ConsumerRecord<String, String> record1 = new ConsumerRecord<>(
                "topic", 1, 100L, "key", "value"
        );
        ConsumerRecord<String, String> record2 = new ConsumerRecord<>(
                "topic", 1, 100L, "key", "value"
        );

        assertThat(record1).isEqualTo(record2);
        assertThat(record1.hashCode()).isEqualTo(record2.hashCode());
    }

    @Test
    void shouldNotBeEqualWhenTopicDiffers() {
        ConsumerRecord<String, String> record1 = new ConsumerRecord<>(
                "topic-1", 0, 0L, "key", "value"
        );
        ConsumerRecord<String, String> record2 = new ConsumerRecord<>(
                "topic-2", 0, 0L, "key", "value"
        );

        assertThat(record1).isNotEqualTo(record2);
    }

    @Test
    void shouldNotBeEqualWhenPartitionDiffers() {
        ConsumerRecord<String, String> record1 = new ConsumerRecord<>(
                "topic", 0, 0L, "key", "value"
        );
        ConsumerRecord<String, String> record2 = new ConsumerRecord<>(
                "topic", 1, 0L, "key", "value"
        );

        assertThat(record1).isNotEqualTo(record2);
    }

    @Test
    void shouldNotBeEqualWhenOffsetDiffers() {
        ConsumerRecord<String, String> record1 = new ConsumerRecord<>(
                "topic", 0, 100L, "key", "value"
        );
        ConsumerRecord<String, String> record2 = new ConsumerRecord<>(
                "topic", 0, 200L, "key", "value"
        );

        assertThat(record1).isNotEqualTo(record2);
    }

    @Test
    void shouldNotBeEqualWhenKeyDiffers() {
        ConsumerRecord<String, String> record1 = new ConsumerRecord<>(
                "topic", 0, 0L, "key1", "value"
        );
        ConsumerRecord<String, String> record2 = new ConsumerRecord<>(
                "topic", 0, 0L, "key2", "value"
        );

        assertThat(record1).isNotEqualTo(record2);
    }

    @Test
    void shouldNotBeEqualWhenValueDiffers() {
        ConsumerRecord<String, String> record1 = new ConsumerRecord<>(
                "topic", 0, 0L, "key", "value1"
        );
        ConsumerRecord<String, String> record2 = new ConsumerRecord<>(
                "topic", 0, 0L, "key", "value2"
        );

        assertThat(record1).isNotEqualTo(record2);
    }

    @Test
    void shouldHaveReadableToString() {
        ConsumerRecord<String, String> record = new ConsumerRecord<>(
                "my-topic", 2, 100L, "my-key", "my-value"
        );

        String str = record.toString();

        assertThat(str).contains("my-topic");
        assertThat(str).contains("2");
        assertThat(str).contains("100");
        assertThat(str).contains("my-key");
        assertThat(str).contains("my-value");
    }

    @Test
    void shouldWorkWithDifferentTypes() {
        ConsumerRecord<Integer, byte[]> record = new ConsumerRecord<>(
                "topic", 0, 0L, 42, "data".getBytes()
        );

        assertThat(record.key()).isEqualTo(42);
        assertThat(record.value()).isEqualTo("data".getBytes());
    }

    @Test
    void shouldDefensivelyCloneHeaders() {
        Map<String, byte[]> headers = new HashMap<>();
        headers.put("key", "original".getBytes());

        ConsumerRecord<String, String> record = new ConsumerRecord<>(
                "topic", 0, 0L, null, "value", Instant.now(), headers
        );

        // Modify original map
        headers.put("new-key", "new-value".getBytes());

        // Record should not be affected
        assertThat(record.headers()).doesNotContainKey("new-key");
    }
}
