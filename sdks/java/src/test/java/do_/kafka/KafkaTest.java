package do_.kafka;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests for the Kafka class.
 */
class KafkaTest {

    private Kafka kafka;
    private MockRpcClient mockClient;

    @BeforeEach
    void setUp() {
        kafka = new Kafka(Arrays.asList("broker1:9092", "broker2:9092"));
        mockClient = new MockRpcClient();
        kafka.setRpcClient(mockClient);
    }

    @Test
    void shouldCreateKafkaWithBrokers() {
        List<String> brokers = kafka.brokers();
        assertThat(brokers).containsExactly("broker1:9092", "broker2:9092");
    }

    @Test
    void shouldCreateKafkaWithConfig() {
        Kafka.Config config = new Kafka.Config.Builder()
                .brokers(Arrays.asList("broker1:9092"))
                .clientId("my-client")
                .timeout(Duration.ofSeconds(60))
                .connectionUrl("custom.kafka.do")
                .build();

        Kafka customKafka = new Kafka(config);
        assertThat(customKafka.config().brokers()).containsExactly("broker1:9092");
        assertThat(customKafka.config().clientId()).isEqualTo("my-client");
        assertThat(customKafka.config().timeout()).isEqualTo(Duration.ofSeconds(60));
        assertThat(customKafka.config().connectionUrl()).isEqualTo("custom.kafka.do");
    }

    @Test
    void shouldCreateProducer() {
        KafkaProducer<String, String> producer = kafka.createProducer();
        assertThat(producer).isNotNull();
    }

    @Test
    void shouldCreateProducerWithConfig() {
        ProducerConfig config = new ProducerConfig.Builder()
                .acks(ProducerConfig.Acks.ALL)
                .compression(ProducerConfig.Compression.GZIP)
                .batchSize(32768)
                .lingerMs(10)
                .build();

        KafkaProducer<String, String> producer = kafka.createProducer(config);
        assertThat(producer).isNotNull();
    }

    @Test
    void shouldCreateConsumer() {
        KafkaConsumer<String, String> consumer = kafka.createConsumer("my-group");
        assertThat(consumer).isNotNull();
        assertThat(consumer.groupId()).isEqualTo("my-group");
    }

    @Test
    void shouldCreateConsumerWithConfig() {
        ConsumerConfig config = new ConsumerConfig.Builder()
                .autoCommit(false)
                .maxPollRecords(100)
                .offsetReset(ConsumerConfig.OffsetReset.EARLIEST)
                .build();

        KafkaConsumer<String, String> consumer = kafka.createConsumer("my-group", config);
        assertThat(consumer).isNotNull();
    }

    @Test
    void shouldCreateAdmin() {
        KafkaAdmin admin = kafka.createAdmin();
        assertThat(admin).isNotNull();
    }

    @Test
    void shouldCreateAdminWithConfig() {
        AdminConfig config = new AdminConfig.Builder()
                .requestTimeout(Duration.ofSeconds(60))
                .build();

        KafkaAdmin admin = kafka.createAdmin(config);
        assertThat(admin).isNotNull();
    }

    @Test
    void shouldCloseKafka() {
        assertThat(kafka.isClosed()).isFalse();
        kafka.close();
        assertThat(kafka.isClosed()).isTrue();
    }

    @Test
    void shouldThrowWhenCreatingProducerAfterClose() {
        kafka.close();
        assertThatThrownBy(() -> kafka.createProducer())
                .isInstanceOf(KafkaException.class)
                .hasMessageContaining("closed");
    }

    @Test
    void shouldThrowWhenCreatingConsumerAfterClose() {
        kafka.close();
        assertThatThrownBy(() -> kafka.createConsumer("group"))
                .isInstanceOf(KafkaException.class)
                .hasMessageContaining("closed");
    }

    @Test
    void shouldThrowWhenCreatingAdminAfterClose() {
        kafka.close();
        assertThatThrownBy(() -> kafka.createAdmin())
                .isInstanceOf(KafkaException.class)
                .hasMessageContaining("closed");
    }
}
