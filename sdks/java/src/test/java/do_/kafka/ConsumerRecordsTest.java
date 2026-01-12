package do_.kafka;

import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests for the ConsumerRecords class.
 */
class ConsumerRecordsTest {

    @Test
    void shouldCreateEmptyRecords() {
        ConsumerRecords<String, String> records = ConsumerRecords.empty();

        assertThat(records.isEmpty()).isTrue();
        assertThat(records.count()).isEqualTo(0);
    }

    @Test
    void shouldCreateFromMap() {
        Map<TopicPartition, List<ConsumerRecord<String, String>>> map = new HashMap<>();
        TopicPartition tp = new TopicPartition("topic", 0);
        map.put(tp, List.of(
                new ConsumerRecord<>("topic", 0, 0L, "k1", "v1"),
                new ConsumerRecord<>("topic", 0, 1L, "k2", "v2")
        ));

        ConsumerRecords<String, String> records = new ConsumerRecords<>(map);

        assertThat(records.count()).isEqualTo(2);
        assertThat(records.isEmpty()).isFalse();
    }

    @Test
    void shouldCreateFromList() {
        List<ConsumerRecord<String, String>> list = List.of(
                new ConsumerRecord<>("topic", 0, 0L, "k1", "v1"),
                new ConsumerRecord<>("topic", 0, 1L, "k2", "v2"),
                new ConsumerRecord<>("topic", 1, 0L, "k3", "v3")
        );

        ConsumerRecords<String, String> records = ConsumerRecords.fromList(list);

        assertThat(records.count()).isEqualTo(3);
        assertThat(records.partitions()).hasSize(2);
    }

    @Test
    void shouldReturnRecordsForPartition() {
        List<ConsumerRecord<String, String>> list = List.of(
                new ConsumerRecord<>("topic", 0, 0L, "k1", "v1"),
                new ConsumerRecord<>("topic", 0, 1L, "k2", "v2"),
                new ConsumerRecord<>("topic", 1, 0L, "k3", "v3")
        );

        ConsumerRecords<String, String> records = ConsumerRecords.fromList(list);

        TopicPartition tp0 = new TopicPartition("topic", 0);
        TopicPartition tp1 = new TopicPartition("topic", 1);

        List<ConsumerRecord<String, String>> partition0 = records.records(tp0);
        List<ConsumerRecord<String, String>> partition1 = records.records(tp1);

        assertThat(partition0).hasSize(2);
        assertThat(partition1).hasSize(1);
    }

    @Test
    void shouldReturnEmptyListForNonExistentPartition() {
        ConsumerRecords<String, String> records = ConsumerRecords.fromList(
                List.of(new ConsumerRecord<>("topic", 0, 0L, "k", "v"))
        );

        TopicPartition tp = new TopicPartition("other-topic", 0);

        List<ConsumerRecord<String, String>> partitionRecords = records.records(tp);

        assertThat(partitionRecords).isEmpty();
    }

    @Test
    void shouldReturnRecordsForTopic() {
        List<ConsumerRecord<String, String>> list = List.of(
                new ConsumerRecord<>("topic-a", 0, 0L, "k1", "v1"),
                new ConsumerRecord<>("topic-a", 1, 0L, "k2", "v2"),
                new ConsumerRecord<>("topic-b", 0, 0L, "k3", "v3")
        );

        ConsumerRecords<String, String> records = ConsumerRecords.fromList(list);

        List<ConsumerRecord<String, String>> topicARecords = new ArrayList<>();
        records.records("topic-a").forEach(topicARecords::add);

        assertThat(topicARecords).hasSize(2);
    }

    @Test
    void shouldReturnEmptyIterableForNonExistentTopic() {
        ConsumerRecords<String, String> records = ConsumerRecords.fromList(
                List.of(new ConsumerRecord<>("topic", 0, 0L, "k", "v"))
        );

        Iterable<ConsumerRecord<String, String>> topicRecords = records.records("other-topic");

        assertThat(topicRecords).isEmpty();
    }

    @Test
    void shouldReturnPartitions() {
        List<ConsumerRecord<String, String>> list = List.of(
                new ConsumerRecord<>("topic-a", 0, 0L, "k1", "v1"),
                new ConsumerRecord<>("topic-a", 1, 0L, "k2", "v2"),
                new ConsumerRecord<>("topic-b", 0, 0L, "k3", "v3")
        );

        ConsumerRecords<String, String> records = ConsumerRecords.fromList(list);

        Set<TopicPartition> partitions = records.partitions();

        assertThat(partitions).containsExactlyInAnyOrder(
                new TopicPartition("topic-a", 0),
                new TopicPartition("topic-a", 1),
                new TopicPartition("topic-b", 0)
        );
    }

    @Test
    void shouldReturnUnmodifiablePartitions() {
        ConsumerRecords<String, String> records = ConsumerRecords.fromList(
                List.of(new ConsumerRecord<>("topic", 0, 0L, "k", "v"))
        );

        Set<TopicPartition> partitions = records.partitions();

        assertThatThrownBy(() -> partitions.add(new TopicPartition("x", 0)))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void shouldReturnUnmodifiableRecordsForPartition() {
        ConsumerRecords<String, String> records = ConsumerRecords.fromList(
                List.of(new ConsumerRecord<>("topic", 0, 0L, "k", "v"))
        );

        List<ConsumerRecord<String, String>> partitionRecords = records.records(
                new TopicPartition("topic", 0));

        assertThatThrownBy(() -> partitionRecords.add(null))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void shouldIterateOverAllRecords() {
        List<ConsumerRecord<String, String>> list = List.of(
                new ConsumerRecord<>("topic", 0, 0L, "k1", "v1"),
                new ConsumerRecord<>("topic", 0, 1L, "k2", "v2"),
                new ConsumerRecord<>("topic", 1, 0L, "k3", "v3")
        );

        ConsumerRecords<String, String> records = ConsumerRecords.fromList(list);

        List<String> values = new ArrayList<>();
        for (ConsumerRecord<String, String> record : records) {
            values.add(record.value());
        }

        assertThat(values).containsExactlyInAnyOrder("v1", "v2", "v3");
    }

    @Test
    void shouldStreamAllRecords() {
        List<ConsumerRecord<String, String>> list = List.of(
                new ConsumerRecord<>("topic", 0, 0L, "k1", "v1"),
                new ConsumerRecord<>("topic", 0, 1L, "k2", "v2"),
                new ConsumerRecord<>("topic", 1, 0L, "k3", "v3")
        );

        ConsumerRecords<String, String> records = ConsumerRecords.fromList(list);

        List<String> values = records.stream()
                .map(ConsumerRecord::value)
                .collect(Collectors.toList());

        assertThat(values).containsExactlyInAnyOrder("v1", "v2", "v3");
    }

    @Test
    void shouldHandleNullMap() {
        ConsumerRecords<String, String> records = new ConsumerRecords<>(null);

        assertThat(records.isEmpty()).isTrue();
        assertThat(records.count()).isEqualTo(0);
    }

    @Test
    void shouldThrowNoSuchElementExceptionWhenIteratorExhausted() {
        ConsumerRecords<String, String> records = ConsumerRecords.fromList(
                List.of(new ConsumerRecord<>("topic", 0, 0L, "k", "v"))
        );

        Iterator<ConsumerRecord<String, String>> iterator = records.iterator();
        iterator.next(); // Consume the one record

        assertThatThrownBy(iterator::next)
                .isInstanceOf(NoSuchElementException.class);
    }

    @Test
    void shouldIterateAcrossMultiplePartitions() {
        // Create records across multiple partitions
        List<ConsumerRecord<String, String>> list = List.of(
                new ConsumerRecord<>("t1", 0, 0L, "k1", "v1"),
                new ConsumerRecord<>("t1", 0, 1L, "k2", "v2"),
                new ConsumerRecord<>("t1", 1, 0L, "k3", "v3"),
                new ConsumerRecord<>("t2", 0, 0L, "k4", "v4"),
                new ConsumerRecord<>("t2", 0, 1L, "k5", "v5")
        );

        ConsumerRecords<String, String> records = ConsumerRecords.fromList(list);

        int count = 0;
        for (ConsumerRecord<String, String> ignored : records) {
            count++;
        }

        assertThat(count).isEqualTo(5);
    }

    @Test
    void shouldHaveReadableToString() {
        ConsumerRecords<String, String> records = ConsumerRecords.fromList(
                List.of(
                        new ConsumerRecord<>("my-topic", 0, 0L, "k", "v"),
                        new ConsumerRecord<>("my-topic", 1, 0L, "k", "v")
                )
        );

        String str = records.toString();

        assertThat(str).contains("partitions=");
        assertThat(str).contains("count=2");
    }

    @Test
    void shouldReturnEmptyRecordsAsSingleton() {
        ConsumerRecords<String, String> empty1 = ConsumerRecords.empty();
        ConsumerRecords<String, String> empty2 = ConsumerRecords.empty();

        assertThat(empty1).isSameAs(empty2);
    }

    @Test
    void shouldIterateEmptyRecordsWithoutException() {
        ConsumerRecords<String, String> records = ConsumerRecords.empty();

        int count = 0;
        for (ConsumerRecord<String, String> ignored : records) {
            count++;
        }

        assertThat(count).isEqualTo(0);
    }

    @Test
    void shouldStreamEmptyRecords() {
        ConsumerRecords<String, String> records = ConsumerRecords.empty();

        long count = records.stream().count();

        assertThat(count).isEqualTo(0);
    }

    @Test
    void shouldCreateFromEmptyList() {
        ConsumerRecords<String, String> records = ConsumerRecords.fromList(List.of());

        assertThat(records.isEmpty()).isTrue();
        assertThat(records.count()).isEqualTo(0);
        assertThat(records.partitions()).isEmpty();
    }
}
