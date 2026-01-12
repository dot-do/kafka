package do_.kafka;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests for the NewTopic class.
 */
class NewTopicTest {

    @Test
    void shouldCreateWithBasicFields() {
        NewTopic topic = new NewTopic("my-topic", 3, (short) 1);

        assertThat(topic.name()).isEqualTo("my-topic");
        assertThat(topic.numPartitions()).isEqualTo(3);
        assertThat(topic.replicationFactor()).isEqualTo((short) 1);
        assertThat(topic.config()).isEmpty();
    }

    @Test
    void shouldCreateWithConfig() {
        Map<String, String> config = Map.of(
                "retention.ms", "86400000",
                "cleanup.policy", "compact"
        );

        NewTopic topic = new NewTopic("my-topic", 3, (short) 1, config);

        assertThat(topic.config()).containsEntry("retention.ms", "86400000");
        assertThat(topic.config()).containsEntry("cleanup.policy", "compact");
    }

    @Test
    void shouldCreateWithBuilder() {
        NewTopic topic = NewTopic.builder("my-topic")
                .numPartitions(6)
                .replicationFactor((short) 2)
                .config("retention.ms", "86400000")
                .config("cleanup.policy", "compact")
                .build();

        assertThat(topic.name()).isEqualTo("my-topic");
        assertThat(topic.numPartitions()).isEqualTo(6);
        assertThat(topic.replicationFactor()).isEqualTo((short) 2);
        assertThat(topic.config()).hasSize(2);
    }

    @Test
    void shouldCreateWithBuilderDefaults() {
        NewTopic topic = NewTopic.builder("minimal-topic").build();

        assertThat(topic.name()).isEqualTo("minimal-topic");
        assertThat(topic.numPartitions()).isEqualTo(1);
        assertThat(topic.replicationFactor()).isEqualTo((short) 1);
        assertThat(topic.config()).isEmpty();
    }

    @Test
    void shouldAddConfigWithBuilderConfigs() {
        Map<String, String> configs = Map.of(
                "key1", "value1",
                "key2", "value2"
        );

        NewTopic topic = NewTopic.builder("topic")
                .configs(configs)
                .build();

        assertThat(topic.config()).containsAllEntriesOf(configs);
    }

    @Test
    void shouldThrowOnNullName() {
        assertThatThrownBy(() -> new NewTopic(null, 1, (short) 1))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("name");
    }

    @Test
    void shouldThrowOnNullNameInBuilder() {
        assertThatThrownBy(() -> NewTopic.builder(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void shouldThrowOnZeroPartitions() {
        assertThatThrownBy(() -> new NewTopic("topic", 0, (short) 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("numPartitions");
    }

    @Test
    void shouldThrowOnNegativePartitions() {
        assertThatThrownBy(() -> new NewTopic("topic", -1, (short) 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("numPartitions");
    }

    @Test
    void shouldThrowOnZeroReplicationFactor() {
        assertThatThrownBy(() -> new NewTopic("topic", 1, (short) 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("replicationFactor");
    }

    @Test
    void shouldThrowOnNegativeReplicationFactor() {
        assertThatThrownBy(() -> new NewTopic("topic", 1, (short) -1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("replicationFactor");
    }

    @Test
    void shouldReturnUnmodifiableConfig() {
        NewTopic topic = new NewTopic("topic", 1, (short) 1, Map.of("key", "value"));

        assertThatThrownBy(() -> topic.config().put("new-key", "new-value"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void shouldAddConfigImmutably() {
        NewTopic topic1 = new NewTopic("topic", 1, (short) 1);
        NewTopic topic2 = topic1.config("retention.ms", "3600000");

        assertThat(topic1.config()).isEmpty();
        assertThat(topic2.config()).containsEntry("retention.ms", "3600000");
    }

    @Test
    void shouldChainConfigAdditions() {
        NewTopic topic = new NewTopic("topic", 1, (short) 1)
                .config("key1", "value1")
                .config("key2", "value2")
                .config("key3", "value3");

        assertThat(topic.config()).hasSize(3);
    }

    @Test
    void shouldBeEqualWhenCoreFieldsMatch() {
        NewTopic topic1 = new NewTopic("topic", 3, (short) 2);
        NewTopic topic2 = new NewTopic("topic", 3, (short) 2);

        assertThat(topic1).isEqualTo(topic2);
        assertThat(topic1.hashCode()).isEqualTo(topic2.hashCode());
    }

    @Test
    void equalityIgnoresConfig() {
        NewTopic topic1 = new NewTopic("topic", 3, (short) 2, Map.of("key1", "value1"));
        NewTopic topic2 = new NewTopic("topic", 3, (short) 2, Map.of("key2", "value2"));

        // Equality is based on name, partitions, replication factor only
        assertThat(topic1).isEqualTo(topic2);
    }

    @Test
    void shouldNotBeEqualWhenNameDiffers() {
        NewTopic topic1 = new NewTopic("topic-a", 1, (short) 1);
        NewTopic topic2 = new NewTopic("topic-b", 1, (short) 1);

        assertThat(topic1).isNotEqualTo(topic2);
    }

    @Test
    void shouldNotBeEqualWhenPartitionsDiffer() {
        NewTopic topic1 = new NewTopic("topic", 1, (short) 1);
        NewTopic topic2 = new NewTopic("topic", 2, (short) 1);

        assertThat(topic1).isNotEqualTo(topic2);
    }

    @Test
    void shouldNotBeEqualWhenReplicationFactorDiffers() {
        NewTopic topic1 = new NewTopic("topic", 1, (short) 1);
        NewTopic topic2 = new NewTopic("topic", 1, (short) 2);

        assertThat(topic1).isNotEqualTo(topic2);
    }

    @Test
    void shouldNotBeEqualToNull() {
        NewTopic topic = new NewTopic("topic", 1, (short) 1);

        assertThat(topic).isNotEqualTo(null);
    }

    @Test
    void shouldNotBeEqualToDifferentType() {
        NewTopic topic = new NewTopic("topic", 1, (short) 1);

        assertThat(topic).isNotEqualTo("topic");
    }

    @Test
    void shouldHaveReadableToString() {
        NewTopic topic = new NewTopic("my-topic", 3, (short) 2, Map.of("key", "value"));

        String str = topic.toString();

        assertThat(str).contains("my-topic");
        assertThat(str).contains("3");
        assertThat(str).contains("2");
        assertThat(str).contains("key");
        assertThat(str).contains("value");
    }

    @Test
    void shouldDefensivelyCloneConfig() {
        Map<String, String> config = new java.util.HashMap<>();
        config.put("key", "original");

        NewTopic topic = new NewTopic("topic", 1, (short) 1, config);

        // Modify original map
        config.put("new-key", "new-value");

        // Topic should not be affected
        assertThat(topic.config()).doesNotContainKey("new-key");
    }

    @Test
    void shouldHandleNullConfig() {
        NewTopic topic = new NewTopic("topic", 1, (short) 1, null);

        assertThat(topic.config()).isEmpty();
    }
}
