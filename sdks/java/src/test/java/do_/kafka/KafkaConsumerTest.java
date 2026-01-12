package do_.kafka;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests for the KafkaConsumer class.
 */
class KafkaConsumerTest {

    private Kafka kafka;
    private MockRpcClient mockClient;
    private KafkaConsumer<String, String> consumer;

    @BeforeEach
    void setUp() {
        kafka = new Kafka(Arrays.asList("broker1:9092"));
        mockClient = new MockRpcClient();
        kafka.setRpcClient(mockClient);
        consumer = kafka.createConsumer("test-group");
    }

    @Test
    void shouldCreateConsumerWithGroupId() {
        assertThat(consumer.groupId()).isEqualTo("test-group");
    }

    @Test
    void shouldSubscribeToTopics() {
        consumer.subscribe(List.of("topic1", "topic2"));

        Set<String> subscription = consumer.subscription();
        assertThat(subscription).containsExactlyInAnyOrder("topic1", "topic2");
    }

    @Test
    void shouldUpdateSubscription() {
        consumer.subscribe(List.of("topic1"));
        consumer.subscribe(List.of("topic2", "topic3"));

        Set<String> subscription = consumer.subscription();
        assertThat(subscription).containsExactlyInAnyOrder("topic2", "topic3");
    }

    @Test
    void shouldUnsubscribe() {
        consumer.subscribe(List.of("topic1", "topic2"));
        consumer.unsubscribe();

        Set<String> subscription = consumer.subscription();
        assertThat(subscription).isEmpty();
    }

    @Test
    void shouldAssignPartitions() {
        consumer.assign(List.of(
                new TopicPartition("topic1", 0),
                new TopicPartition("topic1", 1)
        ));

        Set<TopicPartition> assignment = consumer.assignment();
        assertThat(assignment).hasSize(2);
        assertThat(assignment).contains(
                new TopicPartition("topic1", 0),
                new TopicPartition("topic1", 1)
        );
    }

    @Test
    void shouldPollForRecords() {
        setupPollResponse();
        consumer.subscribe(List.of("my-topic"));

        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));

        assertThat(records).isNotNull();
    }

    @Test
    void shouldPollAndReturnRecords() {
        setupPollResponseWithRecords();
        consumer.subscribe(List.of("my-topic"));

        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));

        assertThat(records.count()).isEqualTo(3);
    }

    @Test
    void shouldIterateOverPolledRecords() {
        setupPollResponseWithRecords();
        consumer.subscribe(List.of("my-topic"));

        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));

        List<String> values = new ArrayList<>();
        for (ConsumerRecord<String, String> record : records) {
            values.add((String) record.value());
        }
        assertThat(values).containsExactly("value1", "value2", "value3");
    }

    @Test
    void shouldCommitSynchronously() {
        consumer.subscribe(List.of("my-topic"));
        consumer.poll(Duration.ofMillis(100));

        assertThatCode(() -> consumer.commitSync()).doesNotThrowAnyException();

        List<MockRpcClient.MethodCall> calls = mockClient.getCalls();
        assertThat(calls.stream().anyMatch(c -> c.method().equals("kafka.consumer.commit"))).isTrue();
    }

    @Test
    void shouldCommitSpecificOffsets() {
        consumer.subscribe(List.of("my-topic"));
        Map<TopicPartition, Long> offsets = Map.of(
                new TopicPartition("my-topic", 0), 100L,
                new TopicPartition("my-topic", 1), 200L
        );

        assertThatCode(() -> consumer.commitSync(offsets)).doesNotThrowAnyException();
    }

    @Test
    void shouldCommitAsync() throws Exception {
        consumer.subscribe(List.of("my-topic"));

        CompletableFuture<Void> future = consumer.commitAsync();
        future.get();

        assertThat(future.isDone()).isTrue();
    }

    @Test
    void shouldGetCommittedOffset() {
        setupCommittedOffsetResponse(50L);
        consumer.subscribe(List.of("my-topic"));

        Long offset = consumer.committed(new TopicPartition("my-topic", 0));

        assertThat(offset).isEqualTo(50L);
    }

    @Test
    void shouldSeekToOffset() {
        consumer.assign(List.of(new TopicPartition("my-topic", 0)));

        assertThatCode(() -> consumer.seek(new TopicPartition("my-topic", 0), 100L))
                .doesNotThrowAnyException();
    }

    @Test
    void shouldSeekToBeginning() {
        consumer.assign(List.of(
                new TopicPartition("my-topic", 0),
                new TopicPartition("my-topic", 1)
        ));

        assertThatCode(() -> consumer.seekToBeginning(List.of(
                new TopicPartition("my-topic", 0),
                new TopicPartition("my-topic", 1)
        ))).doesNotThrowAnyException();

        List<MockRpcClient.MethodCall> calls = mockClient.getCalls();
        assertThat(calls.stream().anyMatch(c -> c.method().equals("kafka.consumer.seekToBeginning"))).isTrue();
    }

    @Test
    void shouldSeekToEnd() {
        consumer.assign(List.of(new TopicPartition("my-topic", 0)));

        assertThatCode(() -> consumer.seekToEnd(List.of(new TopicPartition("my-topic", 0))))
                .doesNotThrowAnyException();

        List<MockRpcClient.MethodCall> calls = mockClient.getCalls();
        assertThat(calls.stream().anyMatch(c -> c.method().equals("kafka.consumer.seekToEnd"))).isTrue();
    }

    @Test
    void shouldGetPosition() {
        mockClient.setHandler("kafka.consumer.position", args -> 42L);
        consumer.assign(List.of(new TopicPartition("my-topic", 0)));

        long position = consumer.position(new TopicPartition("my-topic", 0));

        assertThat(position).isEqualTo(42L);
    }

    @Test
    void shouldPausePartitions() {
        consumer.assign(List.of(new TopicPartition("my-topic", 0)));

        consumer.pause(List.of(new TopicPartition("my-topic", 0)));

        Set<TopicPartition> paused = consumer.paused();
        assertThat(paused).contains(new TopicPartition("my-topic", 0));
    }

    @Test
    void shouldResumePartitions() {
        consumer.assign(List.of(new TopicPartition("my-topic", 0)));
        consumer.pause(List.of(new TopicPartition("my-topic", 0)));

        consumer.resume(List.of(new TopicPartition("my-topic", 0)));

        Set<TopicPartition> paused = consumer.paused();
        assertThat(paused).isEmpty();
    }

    @Test
    void shouldCloseConsumer() {
        consumer.subscribe(List.of("my-topic"));
        consumer.poll(Duration.ofMillis(100));
        assertThat(consumer.isConnected()).isTrue();

        consumer.close();

        assertThat(consumer.isConnected()).isFalse();
    }

    @Test
    void shouldCommitOnCloseWhenAutoCommitEnabled() {
        ConsumerConfig config = new ConsumerConfig.Builder()
                .autoCommit(true)
                .build();
        KafkaConsumer<String, String> autoCommitConsumer = kafka.createConsumer("group", config);

        autoCommitConsumer.subscribe(List.of("topic"));
        autoCommitConsumer.poll(Duration.ofMillis(100));
        mockClient.clearCalls();

        autoCommitConsumer.close();

        List<MockRpcClient.MethodCall> calls = mockClient.getCalls();
        assertThat(calls.stream().anyMatch(c -> c.method().equals("kafka.consumer.commit"))).isTrue();
    }

    @Test
    void shouldThrowWhenPollAfterClose() {
        consumer.close();

        assertThatThrownBy(() -> consumer.poll(Duration.ofMillis(100)))
                .isInstanceOf(KafkaException.class)
                .hasMessageContaining("closed");
    }

    @Test
    void shouldThrowOnNullTopics() {
        assertThatThrownBy(() -> consumer.subscribe(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void shouldThrowOnNullTimeout() {
        consumer.subscribe(List.of("topic"));

        assertThatThrownBy(() -> consumer.poll(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void shouldCreateConsumerWithConfig() {
        ConsumerConfig config = new ConsumerConfig.Builder()
                .autoCommit(false)
                .maxPollRecords(100)
                .offsetReset(ConsumerConfig.OffsetReset.EARLIEST)
                .sessionTimeout(Duration.ofSeconds(45))
                .build();

        KafkaConsumer<String, String> configuredConsumer = kafka.createConsumer("group", config);

        assertThat(configuredConsumer).isNotNull();
    }

    @Test
    void shouldConnectLazilyOnSubscribe() {
        assertThat(consumer.isConnected()).isFalse();

        consumer.subscribe(List.of("topic"));

        assertThat(consumer.isConnected()).isTrue();
    }

    @Test
    void shouldHandlePollFailure() {
        mockClient.setHandler("kafka.consumer.poll", args -> {
            throw new RuntimeException("Network error");
        });
        consumer.subscribe(List.of("topic"));

        assertThatThrownBy(() -> consumer.poll(Duration.ofSeconds(1)))
                .isInstanceOf(KafkaException.class);
    }

    @Test
    void shouldGetEmptyRecordsFromEmptyPoll() {
        consumer.subscribe(List.of("my-topic"));

        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

        assertThat(records.isEmpty()).isTrue();
        assertThat(records.count()).isEqualTo(0);
    }

    private void setupPollResponse() {
        mockClient.setHandler("kafka.consumer.poll", args -> List.of());
    }

    private void setupPollResponseWithRecords() {
        mockClient.setHandler("kafka.consumer.poll", args -> {
            List<Map<String, Object>> records = new ArrayList<>();

            for (int i = 1; i <= 3; i++) {
                Map<String, Object> record = new HashMap<>();
                record.put("topic", "my-topic");
                record.put("partition", 0);
                record.put("offset", (long) i);
                record.put("key", Base64.getEncoder().encodeToString(("key" + i).getBytes()));
                record.put("value", Base64.getEncoder().encodeToString(("value" + i).getBytes()));
                record.put("timestamp", System.currentTimeMillis());
                records.add(record);
            }

            return records;
        });
    }

    private void setupCommittedOffsetResponse(long offset) {
        mockClient.setHandler("kafka.consumer.committed", args -> {
            List<Map<String, Object>> results = new ArrayList<>();
            Map<String, Object> result = new HashMap<>();
            result.put("topic", "my-topic");
            result.put("partition", 0);
            result.put("offset", offset);
            results.add(result);
            return results;
        });
    }
}
