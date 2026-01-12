package do_.kafka;

import java.time.Duration;

/**
 * Configuration for a Kafka producer.
 */
public final class ProducerConfig {

    /**
     * Acknowledgment levels.
     */
    public enum Acks {
        /** No acknowledgment required */
        NONE("0"),
        /** Leader acknowledgment required */
        ONE("1"),
        /** All replicas must acknowledge */
        ALL("all");

        private final String value;

        Acks(String value) {
            this.value = value;
        }

        public String value() {
            return value;
        }
    }

    /**
     * Compression types.
     */
    public enum Compression {
        NONE, GZIP, SNAPPY, LZ4, ZSTD
    }

    private final Acks acks;
    private final Compression compression;
    private final int batchSize;
    private final int lingerMs;
    private final int retries;
    private final Duration retryBackoff;
    private final String clientId;

    private ProducerConfig(Builder builder) {
        this.acks = builder.acks;
        this.compression = builder.compression;
        this.batchSize = builder.batchSize;
        this.lingerMs = builder.lingerMs;
        this.retries = builder.retries;
        this.retryBackoff = builder.retryBackoff;
        this.clientId = builder.clientId;
    }

    public Acks acks() {
        return acks;
    }

    public Compression compression() {
        return compression;
    }

    public int batchSize() {
        return batchSize;
    }

    public int lingerMs() {
        return lingerMs;
    }

    public int retries() {
        return retries;
    }

    public Duration retryBackoff() {
        return retryBackoff;
    }

    public String clientId() {
        return clientId;
    }

    public static final class Builder {
        private Acks acks = Acks.ONE;
        private Compression compression = Compression.NONE;
        private int batchSize = 16384;
        private int lingerMs = 0;
        private int retries = 0;
        private Duration retryBackoff = Duration.ofMillis(100);
        private String clientId;

        public Builder acks(Acks acks) {
            this.acks = acks;
            return this;
        }

        public Builder compression(Compression compression) {
            this.compression = compression;
            return this;
        }

        public Builder batchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public Builder lingerMs(int lingerMs) {
            this.lingerMs = lingerMs;
            return this;
        }

        public Builder retries(int retries) {
            this.retries = retries;
            return this;
        }

        public Builder retryBackoff(Duration retryBackoff) {
            this.retryBackoff = retryBackoff;
            return this;
        }

        public Builder clientId(String clientId) {
            this.clientId = clientId;
            return this;
        }

        public ProducerConfig build() {
            return new ProducerConfig(this);
        }
    }
}
