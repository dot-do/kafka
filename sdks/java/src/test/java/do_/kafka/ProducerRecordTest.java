package do_.kafka;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests for the ProducerRecord class.
 */
class ProducerRecordTest {

    @Test
    void shouldCreateRecordWithTopicAndValue() {
        ProducerRecord<String, String> record = new ProducerRecord<>("my-topic", "my-value");

        assertThat(record.topic()).isEqualTo("my-topic");
        assertThat(record.value()).isEqualTo("my-value");
        assertThat(record.key()).isNull();
        assertThat(record.partition()).isNull();
        assertThat(record.timestamp()).isNull();
        assertThat(record.headers()).isEmpty();
    }

    @Test
    void shouldCreateRecordWithTopicKeyAndValue() {
        ProducerRecord<String, String> record = new ProducerRecord<>("my-topic", "my-key", "my-value");

        assertThat(record.topic()).isEqualTo("my-topic");
        assertThat(record.key()).isEqualTo("my-key");
        assertThat(record.value()).isEqualTo("my-value");
    }

    @Test
    void shouldCreateRecordWithPartition() {
        ProducerRecord<String, String> record = new ProducerRecord<>("my-topic", 2, "my-key", "my-value");

        assertThat(record.partition()).isEqualTo(2);
    }

    @Test
    void shouldCreateRecordWithBuilder() {
        Instant now = Instant.now();
        byte[] headerValue = "header-value".getBytes(StandardCharsets.UTF_8);

        ProducerRecord<String, String> record = ProducerRecord.<String, String>builder("my-topic", "my-value")
                .key("my-key")
                .partition(1)
                .timestamp(now)
                .header("correlation-id", headerValue)
                .header("type", "order")
                .build();

        assertThat(record.topic()).isEqualTo("my-topic");
        assertThat(record.key()).isEqualTo("my-key");
        assertThat(record.value()).isEqualTo("my-value");
        assertThat(record.partition()).isEqualTo(1);
        assertThat(record.timestamp()).isEqualTo(now);
        assertThat(record.headers()).containsKey("correlation-id");
        assertThat(record.headers()).containsKey("type");
    }

    @Test
    void shouldThrowOnNullTopic() {
        assertThatThrownBy(() -> new ProducerRecord<String, String>(null, "value"))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("topic");
    }

    @Test
    void shouldBeEqualWhenFieldsMatch() {
        ProducerRecord<String, String> record1 = new ProducerRecord<>("topic", "key", "value");
        ProducerRecord<String, String> record2 = new ProducerRecord<>("topic", "key", "value");

        assertThat(record1).isEqualTo(record2);
        assertThat(record1.hashCode()).isEqualTo(record2.hashCode());
    }

    @Test
    void shouldNotBeEqualWhenFieldsDiffer() {
        ProducerRecord<String, String> record1 = new ProducerRecord<>("topic1", "key", "value");
        ProducerRecord<String, String> record2 = new ProducerRecord<>("topic2", "key", "value");

        assertThat(record1).isNotEqualTo(record2);
    }

    @Test
    void shouldHaveReadableToString() {
        ProducerRecord<String, String> record = new ProducerRecord<>("my-topic", "my-key", "my-value");
        String str = record.toString();

        assertThat(str).contains("my-topic");
        assertThat(str).contains("my-key");
        assertThat(str).contains("my-value");
    }
}
