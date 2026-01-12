package do_.kafka;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests for the KafkaProducer class.
 */
class KafkaProducerTest {

    private Kafka kafka;
    private MockRpcClient mockClient;
    private KafkaProducer<String, String> producer;

    @BeforeEach
    void setUp() {
        kafka = new Kafka(Arrays.asList("broker1:9092"));
        mockClient = new MockRpcClient();
        kafka.setRpcClient(mockClient);
        producer = kafka.createProducer();
    }

    @Test
    void shouldSendRecord() throws Exception {
        ProducerRecord<String, String> record = new ProducerRecord<>("my-topic", "my-key", "my-value");

        RecordMetadata metadata = producer.send(record).get();

        assertThat(metadata).isNotNull();
        assertThat(metadata.topic()).isEqualTo("my-topic");
        assertThat(metadata.partition()).isGreaterThanOrEqualTo(0);
        assertThat(metadata.offset()).isGreaterThanOrEqualTo(0);
        assertThat(metadata.timestamp()).isNotNull();
    }

    @Test
    void shouldSendRecordWithoutKey() throws Exception {
        ProducerRecord<String, String> record = new ProducerRecord<>("my-topic", "my-value");

        RecordMetadata metadata = producer.send(record).get();

        assertThat(metadata).isNotNull();
        assertThat(metadata.topic()).isEqualTo("my-topic");
    }

    @Test
    void shouldSendRecordWithPartition() throws Exception {
        ProducerRecord<String, String> record = new ProducerRecord<>("my-topic", 2, "my-key", "my-value");

        RecordMetadata metadata = producer.send(record).get();

        assertThat(metadata).isNotNull();
        assertThat(metadata.partition()).isEqualTo(2);
    }

    @Test
    void shouldSendRecordWithHeaders() throws Exception {
        ProducerRecord<String, String> record = ProducerRecord.<String, String>builder("my-topic", "my-value")
                .key("my-key")
                .header("correlation-id", "abc-123".getBytes())
                .header("type", "order".getBytes())
                .build();

        RecordMetadata metadata = producer.send(record).get();

        assertThat(metadata).isNotNull();

        // Verify the call was made with headers
        List<MockRpcClient.MethodCall> calls = mockClient.getCalls();
        assertThat(calls).isNotEmpty();
        MockRpcClient.MethodCall sendCall = calls.stream()
                .filter(c -> c.method().equals("kafka.producer.send"))
                .findFirst()
                .orElseThrow();
        assertThat(sendCall.args()).isNotEmpty();
    }

    @Test
    void shouldSendRecordWithTimestamp() throws Exception {
        Instant timestamp = Instant.parse("2025-01-01T12:00:00Z");
        ProducerRecord<String, String> record = ProducerRecord.<String, String>builder("my-topic", "my-value")
                .timestamp(timestamp)
                .build();

        RecordMetadata metadata = producer.send(record).get();

        assertThat(metadata).isNotNull();
    }

    @Test
    void shouldSendBatch() throws Exception {
        List<ProducerRecord<String, String>> records = List.of(
                new ProducerRecord<>("my-topic", "key1", "value1"),
                new ProducerRecord<>("my-topic", "key2", "value2"),
                new ProducerRecord<>("my-topic", "key3", "value3")
        );

        List<RecordMetadata> metadataList = producer.sendBatch(records).get();

        assertThat(metadataList).hasSize(3);
        assertThat(metadataList.get(0).offset()).isLessThan(metadataList.get(1).offset());
        assertThat(metadataList.get(1).offset()).isLessThan(metadataList.get(2).offset());
    }

    @Test
    void shouldGetPartitionsForTopic() throws Exception {
        mockClient.addTopic("my-topic");

        List<Integer> partitions = producer.partitionsFor("my-topic").get();

        assertThat(partitions).containsExactly(0, 1, 2);
    }

    @Test
    void shouldFlush() {
        // Send a record first
        producer.send(new ProducerRecord<>("my-topic", "value"));

        // Flush should not throw
        assertThatCode(() -> producer.flush()).doesNotThrowAnyException();

        // Verify flush was called
        List<MockRpcClient.MethodCall> calls = mockClient.getCalls();
        assertThat(calls.stream().anyMatch(c -> c.method().equals("kafka.producer.flush"))).isTrue();
    }

    @Test
    void shouldConnectLazilyOnSend() {
        assertThat(producer.isConnected()).isFalse();

        producer.send(new ProducerRecord<>("my-topic", "value"));

        assertThat(producer.isConnected()).isTrue();
    }

    @Test
    void shouldCloseProducer() {
        producer.send(new ProducerRecord<>("my-topic", "value"));
        assertThat(producer.isConnected()).isTrue();

        producer.close();

        assertThat(producer.isConnected()).isFalse();
    }

    @Test
    void shouldThrowWhenSendingAfterClose() {
        producer.close();

        assertThatThrownBy(() -> producer.send(new ProducerRecord<>("topic", "value")))
                .isInstanceOf(KafkaException.class)
                .hasMessageContaining("closed");
    }

    @Test
    void shouldThrowOnNullRecord() {
        assertThatThrownBy(() -> producer.send(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void shouldThrowOnNullBatch() {
        assertThatThrownBy(() -> producer.sendBatch(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void shouldHandleSendFailure() {
        mockClient.setHandler("kafka.producer.send", args -> {
            throw new RuntimeException("Network error");
        });

        CompletableFuture<RecordMetadata> future = producer.send(new ProducerRecord<>("topic", "value"));

        assertThatThrownBy(future::get)
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(KafkaException.class);
    }

    @Test
    void shouldRecordMessages() {
        producer.send(new ProducerRecord<>("my-topic", "key1", "value1"));
        producer.send(new ProducerRecord<>("my-topic", "key2", "value2"));

        List<Map<String, Object>> messages = mockClient.getMessages("my-topic");
        assertThat(messages).hasSize(2);
    }

    @Test
    void shouldCreateProducerWithConfig() {
        ProducerConfig config = new ProducerConfig.Builder()
                .acks(ProducerConfig.Acks.ALL)
                .compression(ProducerConfig.Compression.GZIP)
                .batchSize(32768)
                .lingerMs(10)
                .retries(3)
                .build();

        KafkaProducer<String, String> configuredProducer = kafka.createProducer(config);

        assertThat(configuredProducer).isNotNull();
    }

    @Test
    void shouldSendRecordAsync() {
        ProducerRecord<String, String> record = new ProducerRecord<>("my-topic", "my-value");

        CompletableFuture<RecordMetadata> future = producer.send(record);

        assertThat(future).isNotNull();
        assertThat(future.isDone()).isTrue(); // Mock completes immediately
    }

    @Test
    void shouldHandleMultipleSendsInParallel() throws Exception {
        CompletableFuture<RecordMetadata> future1 = producer.send(new ProducerRecord<>("topic", "value1"));
        CompletableFuture<RecordMetadata> future2 = producer.send(new ProducerRecord<>("topic", "value2"));
        CompletableFuture<RecordMetadata> future3 = producer.send(new ProducerRecord<>("topic", "value3"));

        CompletableFuture.allOf(future1, future2, future3).get();

        assertThat(future1.get().offset()).isNotEqualTo(future2.get().offset());
        assertThat(future2.get().offset()).isNotEqualTo(future3.get().offset());
    }
}
