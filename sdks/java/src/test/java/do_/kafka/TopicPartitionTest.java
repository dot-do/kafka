package do_.kafka;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests for the TopicPartition class.
 */
class TopicPartitionTest {

    @Test
    void shouldCreateTopicPartition() {
        TopicPartition tp = new TopicPartition("my-topic", 3);

        assertThat(tp.topic()).isEqualTo("my-topic");
        assertThat(tp.partition()).isEqualTo(3);
    }

    @Test
    void shouldCreateWithPartitionZero() {
        TopicPartition tp = new TopicPartition("topic", 0);

        assertThat(tp.partition()).isEqualTo(0);
    }

    @Test
    void shouldThrowOnNullTopic() {
        assertThatThrownBy(() -> new TopicPartition(null, 0))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("topic");
    }

    @Test
    void shouldBeEqualWhenFieldsMatch() {
        TopicPartition tp1 = new TopicPartition("topic", 5);
        TopicPartition tp2 = new TopicPartition("topic", 5);

        assertThat(tp1).isEqualTo(tp2);
        assertThat(tp1.hashCode()).isEqualTo(tp2.hashCode());
    }

    @Test
    void shouldNotBeEqualWhenTopicDiffers() {
        TopicPartition tp1 = new TopicPartition("topic-a", 0);
        TopicPartition tp2 = new TopicPartition("topic-b", 0);

        assertThat(tp1).isNotEqualTo(tp2);
    }

    @Test
    void shouldNotBeEqualWhenPartitionDiffers() {
        TopicPartition tp1 = new TopicPartition("topic", 0);
        TopicPartition tp2 = new TopicPartition("topic", 1);

        assertThat(tp1).isNotEqualTo(tp2);
    }

    @Test
    void shouldNotBeEqualToNull() {
        TopicPartition tp = new TopicPartition("topic", 0);

        assertThat(tp).isNotEqualTo(null);
    }

    @Test
    void shouldNotBeEqualToDifferentType() {
        TopicPartition tp = new TopicPartition("topic", 0);

        assertThat(tp).isNotEqualTo("topic-0");
    }

    @Test
    void shouldWorkInHashSet() {
        Set<TopicPartition> set = new HashSet<>();
        set.add(new TopicPartition("topic-a", 0));
        set.add(new TopicPartition("topic-a", 1));
        set.add(new TopicPartition("topic-b", 0));
        set.add(new TopicPartition("topic-a", 0)); // Duplicate

        assertThat(set).hasSize(3);
    }

    @Test
    void shouldWorkAsMapKey() {
        Map<TopicPartition, Long> offsets = new HashMap<>();
        offsets.put(new TopicPartition("topic", 0), 100L);
        offsets.put(new TopicPartition("topic", 1), 200L);
        offsets.put(new TopicPartition("topic", 0), 150L); // Update

        assertThat(offsets).hasSize(2);
        assertThat(offsets.get(new TopicPartition("topic", 0))).isEqualTo(150L);
        assertThat(offsets.get(new TopicPartition("topic", 1))).isEqualTo(200L);
    }

    @Test
    void shouldHaveReadableToString() {
        TopicPartition tp = new TopicPartition("my-topic", 7);

        String str = tp.toString();

        assertThat(str).isEqualTo("my-topic-7");
    }

    @Test
    void shouldHandleEmptyTopicName() {
        TopicPartition tp = new TopicPartition("", 0);

        assertThat(tp.topic()).isEmpty();
        assertThat(tp.toString()).isEqualTo("-0");
    }

    @Test
    void shouldHandleSpecialCharactersInTopicName() {
        TopicPartition tp = new TopicPartition("my.topic_name-1", 0);

        assertThat(tp.topic()).isEqualTo("my.topic_name-1");
        assertThat(tp.toString()).isEqualTo("my.topic_name-1-0");
    }

    @Test
    void shouldBeEqualToItself() {
        TopicPartition tp = new TopicPartition("topic", 0);

        assertThat(tp).isEqualTo(tp);
    }

    @Test
    void shouldHaveConsistentHashCode() {
        TopicPartition tp = new TopicPartition("topic", 5);
        int hash1 = tp.hashCode();
        int hash2 = tp.hashCode();

        assertThat(hash1).isEqualTo(hash2);
    }

    @Test
    void shouldHaveDifferentHashCodesForDifferentObjects() {
        TopicPartition tp1 = new TopicPartition("topic-a", 0);
        TopicPartition tp2 = new TopicPartition("topic-b", 0);
        TopicPartition tp3 = new TopicPartition("topic-a", 1);

        // Note: Hash collisions are possible but unlikely for these simple cases
        // We just verify they're different for obviously different objects
        assertThat(tp1.hashCode()).isNotEqualTo(tp2.hashCode());
        assertThat(tp1.hashCode()).isNotEqualTo(tp3.hashCode());
    }
}
