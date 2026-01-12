package do_.kafka;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests for the configuration classes (ProducerConfig, ConsumerConfig, AdminConfig).
 */
class ConfigTest {

    // =========================================================================
    // ProducerConfig Tests
    // =========================================================================

    @Test
    void shouldCreateProducerConfigWithDefaults() {
        ProducerConfig config = new ProducerConfig.Builder().build();

        assertThat(config.acks()).isEqualTo(ProducerConfig.Acks.ONE);
        assertThat(config.compression()).isEqualTo(ProducerConfig.Compression.NONE);
        assertThat(config.batchSize()).isEqualTo(16384);
        assertThat(config.lingerMs()).isEqualTo(0);
        assertThat(config.retries()).isEqualTo(0);
        assertThat(config.retryBackoff()).isEqualTo(Duration.ofMillis(100));
        assertThat(config.clientId()).isNull();
    }

    @Test
    void shouldCreateProducerConfigWithAllFields() {
        ProducerConfig config = new ProducerConfig.Builder()
                .acks(ProducerConfig.Acks.ALL)
                .compression(ProducerConfig.Compression.GZIP)
                .batchSize(32768)
                .lingerMs(10)
                .retries(5)
                .retryBackoff(Duration.ofMillis(500))
                .clientId("my-producer")
                .build();

        assertThat(config.acks()).isEqualTo(ProducerConfig.Acks.ALL);
        assertThat(config.compression()).isEqualTo(ProducerConfig.Compression.GZIP);
        assertThat(config.batchSize()).isEqualTo(32768);
        assertThat(config.lingerMs()).isEqualTo(10);
        assertThat(config.retries()).isEqualTo(5);
        assertThat(config.retryBackoff()).isEqualTo(Duration.ofMillis(500));
        assertThat(config.clientId()).isEqualTo("my-producer");
    }

    @Test
    void shouldSupportAllAcksValues() {
        assertThat(ProducerConfig.Acks.NONE.value()).isEqualTo("0");
        assertThat(ProducerConfig.Acks.ONE.value()).isEqualTo("1");
        assertThat(ProducerConfig.Acks.ALL.value()).isEqualTo("all");
    }

    @Test
    void shouldSupportAllCompressionTypes() {
        ProducerConfig.Compression[] compressions = {
                ProducerConfig.Compression.NONE,
                ProducerConfig.Compression.GZIP,
                ProducerConfig.Compression.SNAPPY,
                ProducerConfig.Compression.LZ4,
                ProducerConfig.Compression.ZSTD
        };

        for (ProducerConfig.Compression compression : compressions) {
            ProducerConfig config = new ProducerConfig.Builder()
                    .compression(compression)
                    .build();
            assertThat(config.compression()).isEqualTo(compression);
        }
    }

    @Test
    void shouldChainProducerConfigBuilderMethods() {
        ProducerConfig config = new ProducerConfig.Builder()
                .acks(ProducerConfig.Acks.ALL)
                .compression(ProducerConfig.Compression.LZ4)
                .batchSize(1000)
                .lingerMs(5)
                .retries(3)
                .clientId("chained")
                .build();

        assertThat(config.acks()).isEqualTo(ProducerConfig.Acks.ALL);
        assertThat(config.compression()).isEqualTo(ProducerConfig.Compression.LZ4);
    }

    // =========================================================================
    // ConsumerConfig Tests
    // =========================================================================

    @Test
    void shouldCreateConsumerConfigWithDefaults() {
        ConsumerConfig config = new ConsumerConfig.Builder().build();

        assertThat(config.autoCommit()).isTrue();
        assertThat(config.autoCommitInterval()).isEqualTo(Duration.ofMillis(5000));
        assertThat(config.sessionTimeout()).isEqualTo(Duration.ofSeconds(10));
        assertThat(config.heartbeatInterval()).isEqualTo(Duration.ofSeconds(3));
        assertThat(config.maxPollRecords()).isEqualTo(500);
        assertThat(config.maxPollInterval()).isEqualTo(Duration.ofMinutes(5));
        assertThat(config.isolationLevel()).isEqualTo(ConsumerConfig.IsolationLevel.READ_UNCOMMITTED);
        assertThat(config.offsetReset()).isEqualTo(ConsumerConfig.OffsetReset.LATEST);
        assertThat(config.clientId()).isNull();
    }

    @Test
    void shouldCreateConsumerConfigWithAllFields() {
        ConsumerConfig config = new ConsumerConfig.Builder()
                .autoCommit(false)
                .autoCommitInterval(Duration.ofMillis(10000))
                .sessionTimeout(Duration.ofSeconds(30))
                .heartbeatInterval(Duration.ofSeconds(5))
                .maxPollRecords(100)
                .maxPollInterval(Duration.ofMinutes(10))
                .isolationLevel(ConsumerConfig.IsolationLevel.READ_COMMITTED)
                .offsetReset(ConsumerConfig.OffsetReset.EARLIEST)
                .clientId("my-consumer")
                .build();

        assertThat(config.autoCommit()).isFalse();
        assertThat(config.autoCommitInterval()).isEqualTo(Duration.ofMillis(10000));
        assertThat(config.sessionTimeout()).isEqualTo(Duration.ofSeconds(30));
        assertThat(config.heartbeatInterval()).isEqualTo(Duration.ofSeconds(5));
        assertThat(config.maxPollRecords()).isEqualTo(100);
        assertThat(config.maxPollInterval()).isEqualTo(Duration.ofMinutes(10));
        assertThat(config.isolationLevel()).isEqualTo(ConsumerConfig.IsolationLevel.READ_COMMITTED);
        assertThat(config.offsetReset()).isEqualTo(ConsumerConfig.OffsetReset.EARLIEST);
        assertThat(config.clientId()).isEqualTo("my-consumer");
    }

    @Test
    void shouldSupportAllOffsetResetValues() {
        ConsumerConfig.OffsetReset[] resets = {
                ConsumerConfig.OffsetReset.EARLIEST,
                ConsumerConfig.OffsetReset.LATEST,
                ConsumerConfig.OffsetReset.NONE
        };

        for (ConsumerConfig.OffsetReset reset : resets) {
            ConsumerConfig config = new ConsumerConfig.Builder()
                    .offsetReset(reset)
                    .build();
            assertThat(config.offsetReset()).isEqualTo(reset);
        }
    }

    @Test
    void shouldSupportAllIsolationLevels() {
        ConsumerConfig.IsolationLevel[] levels = {
                ConsumerConfig.IsolationLevel.READ_UNCOMMITTED,
                ConsumerConfig.IsolationLevel.READ_COMMITTED
        };

        for (ConsumerConfig.IsolationLevel level : levels) {
            ConsumerConfig config = new ConsumerConfig.Builder()
                    .isolationLevel(level)
                    .build();
            assertThat(config.isolationLevel()).isEqualTo(level);
        }
    }

    @Test
    void shouldChainConsumerConfigBuilderMethods() {
        ConsumerConfig config = new ConsumerConfig.Builder()
                .autoCommit(false)
                .maxPollRecords(200)
                .offsetReset(ConsumerConfig.OffsetReset.EARLIEST)
                .clientId("chained")
                .build();

        assertThat(config.autoCommit()).isFalse();
        assertThat(config.maxPollRecords()).isEqualTo(200);
    }

    // =========================================================================
    // AdminConfig Tests
    // =========================================================================

    @Test
    void shouldCreateAdminConfigWithDefaults() {
        AdminConfig config = new AdminConfig.Builder().build();

        assertThat(config.requestTimeout()).isEqualTo(Duration.ofSeconds(30));
        assertThat(config.clientId()).isNull();
    }

    @Test
    void shouldCreateAdminConfigWithAllFields() {
        AdminConfig config = new AdminConfig.Builder()
                .requestTimeout(Duration.ofMinutes(2))
                .clientId("my-admin")
                .build();

        assertThat(config.requestTimeout()).isEqualTo(Duration.ofMinutes(2));
        assertThat(config.clientId()).isEqualTo("my-admin");
    }

    @Test
    void shouldChainAdminConfigBuilderMethods() {
        AdminConfig config = new AdminConfig.Builder()
                .requestTimeout(Duration.ofSeconds(60))
                .clientId("chained-admin")
                .build();

        assertThat(config.requestTimeout()).isEqualTo(Duration.ofSeconds(60));
        assertThat(config.clientId()).isEqualTo("chained-admin");
    }

    // =========================================================================
    // Edge Case Tests
    // =========================================================================

    @Test
    void shouldAllowZeroBatchSize() {
        ProducerConfig config = new ProducerConfig.Builder()
                .batchSize(0)
                .build();

        assertThat(config.batchSize()).isEqualTo(0);
    }

    @Test
    void shouldAllowLargeBatchSize() {
        ProducerConfig config = new ProducerConfig.Builder()
                .batchSize(1048576) // 1MB
                .build();

        assertThat(config.batchSize()).isEqualTo(1048576);
    }

    @Test
    void shouldAllowZeroLingerMs() {
        ProducerConfig config = new ProducerConfig.Builder()
                .lingerMs(0)
                .build();

        assertThat(config.lingerMs()).isEqualTo(0);
    }

    @Test
    void shouldAllowHighLingerMs() {
        ProducerConfig config = new ProducerConfig.Builder()
                .lingerMs(60000)
                .build();

        assertThat(config.lingerMs()).isEqualTo(60000);
    }

    @Test
    void shouldAllowZeroRetries() {
        ProducerConfig config = new ProducerConfig.Builder()
                .retries(0)
                .build();

        assertThat(config.retries()).isEqualTo(0);
    }

    @Test
    void shouldAllowHighRetries() {
        ProducerConfig config = new ProducerConfig.Builder()
                .retries(Integer.MAX_VALUE)
                .build();

        assertThat(config.retries()).isEqualTo(Integer.MAX_VALUE);
    }

    @Test
    void shouldAllowZeroMaxPollRecords() {
        ConsumerConfig config = new ConsumerConfig.Builder()
                .maxPollRecords(0)
                .build();

        assertThat(config.maxPollRecords()).isEqualTo(0);
    }

    @Test
    void shouldAllowHighMaxPollRecords() {
        ConsumerConfig config = new ConsumerConfig.Builder()
                .maxPollRecords(10000)
                .build();

        assertThat(config.maxPollRecords()).isEqualTo(10000);
    }

    @Test
    void shouldAllowShortSessionTimeout() {
        ConsumerConfig config = new ConsumerConfig.Builder()
                .sessionTimeout(Duration.ofMillis(100))
                .build();

        assertThat(config.sessionTimeout()).isEqualTo(Duration.ofMillis(100));
    }

    @Test
    void shouldAllowLongSessionTimeout() {
        ConsumerConfig config = new ConsumerConfig.Builder()
                .sessionTimeout(Duration.ofMinutes(30))
                .build();

        assertThat(config.sessionTimeout()).isEqualTo(Duration.ofMinutes(30));
    }

    @Test
    void shouldAllowNullClientId() {
        ProducerConfig producerConfig = new ProducerConfig.Builder().build();
        ConsumerConfig consumerConfig = new ConsumerConfig.Builder().build();
        AdminConfig adminConfig = new AdminConfig.Builder().build();

        assertThat(producerConfig.clientId()).isNull();
        assertThat(consumerConfig.clientId()).isNull();
        assertThat(adminConfig.clientId()).isNull();
    }

    @Test
    void shouldAllowEmptyClientId() {
        ProducerConfig config = new ProducerConfig.Builder()
                .clientId("")
                .build();

        assertThat(config.clientId()).isEmpty();
    }

    // =========================================================================
    // Immutability Tests
    // =========================================================================

    @Test
    void builderShouldCreateNewConfigOnEachBuild() {
        ProducerConfig.Builder builder = new ProducerConfig.Builder();

        ProducerConfig config1 = builder.batchSize(1000).build();
        ProducerConfig config2 = builder.batchSize(2000).build();

        assertThat(config1.batchSize()).isEqualTo(1000);
        assertThat(config2.batchSize()).isEqualTo(2000);
    }

    @Test
    void consumerBuilderShouldCreateNewConfigOnEachBuild() {
        ConsumerConfig.Builder builder = new ConsumerConfig.Builder();

        ConsumerConfig config1 = builder.maxPollRecords(100).build();
        ConsumerConfig config2 = builder.maxPollRecords(200).build();

        assertThat(config1.maxPollRecords()).isEqualTo(100);
        assertThat(config2.maxPollRecords()).isEqualTo(200);
    }

    @Test
    void adminBuilderShouldCreateNewConfigOnEachBuild() {
        AdminConfig.Builder builder = new AdminConfig.Builder();

        AdminConfig config1 = builder.requestTimeout(Duration.ofSeconds(10)).build();
        AdminConfig config2 = builder.requestTimeout(Duration.ofSeconds(20)).build();

        assertThat(config1.requestTimeout()).isEqualTo(Duration.ofSeconds(10));
        assertThat(config2.requestTimeout()).isEqualTo(Duration.ofSeconds(20));
    }
}
