package do_.kafka;

import java.time.Duration;

/**
 * Configuration for a Kafka consumer.
 */
public final class ConsumerConfig {

    /**
     * Offset reset policies.
     */
    public enum OffsetReset {
        EARLIEST, LATEST, NONE
    }

    /**
     * Isolation levels.
     */
    public enum IsolationLevel {
        READ_UNCOMMITTED, READ_COMMITTED
    }

    private final boolean autoCommit;
    private final Duration autoCommitInterval;
    private final Duration sessionTimeout;
    private final Duration heartbeatInterval;
    private final int maxPollRecords;
    private final Duration maxPollInterval;
    private final IsolationLevel isolationLevel;
    private final OffsetReset offsetReset;
    private final String clientId;

    private ConsumerConfig(Builder builder) {
        this.autoCommit = builder.autoCommit;
        this.autoCommitInterval = builder.autoCommitInterval;
        this.sessionTimeout = builder.sessionTimeout;
        this.heartbeatInterval = builder.heartbeatInterval;
        this.maxPollRecords = builder.maxPollRecords;
        this.maxPollInterval = builder.maxPollInterval;
        this.isolationLevel = builder.isolationLevel;
        this.offsetReset = builder.offsetReset;
        this.clientId = builder.clientId;
    }

    public boolean autoCommit() {
        return autoCommit;
    }

    public Duration autoCommitInterval() {
        return autoCommitInterval;
    }

    public Duration sessionTimeout() {
        return sessionTimeout;
    }

    public Duration heartbeatInterval() {
        return heartbeatInterval;
    }

    public int maxPollRecords() {
        return maxPollRecords;
    }

    public Duration maxPollInterval() {
        return maxPollInterval;
    }

    public IsolationLevel isolationLevel() {
        return isolationLevel;
    }

    public OffsetReset offsetReset() {
        return offsetReset;
    }

    public String clientId() {
        return clientId;
    }

    public static final class Builder {
        private boolean autoCommit = true;
        private Duration autoCommitInterval = Duration.ofMillis(5000);
        private Duration sessionTimeout = Duration.ofSeconds(10);
        private Duration heartbeatInterval = Duration.ofSeconds(3);
        private int maxPollRecords = 500;
        private Duration maxPollInterval = Duration.ofMinutes(5);
        private IsolationLevel isolationLevel = IsolationLevel.READ_UNCOMMITTED;
        private OffsetReset offsetReset = OffsetReset.LATEST;
        private String clientId;

        public Builder autoCommit(boolean autoCommit) {
            this.autoCommit = autoCommit;
            return this;
        }

        public Builder autoCommitInterval(Duration autoCommitInterval) {
            this.autoCommitInterval = autoCommitInterval;
            return this;
        }

        public Builder sessionTimeout(Duration sessionTimeout) {
            this.sessionTimeout = sessionTimeout;
            return this;
        }

        public Builder heartbeatInterval(Duration heartbeatInterval) {
            this.heartbeatInterval = heartbeatInterval;
            return this;
        }

        public Builder maxPollRecords(int maxPollRecords) {
            this.maxPollRecords = maxPollRecords;
            return this;
        }

        public Builder maxPollInterval(Duration maxPollInterval) {
            this.maxPollInterval = maxPollInterval;
            return this;
        }

        public Builder isolationLevel(IsolationLevel isolationLevel) {
            this.isolationLevel = isolationLevel;
            return this;
        }

        public Builder offsetReset(OffsetReset offsetReset) {
            this.offsetReset = offsetReset;
            return this;
        }

        public Builder clientId(String clientId) {
            this.clientId = clientId;
            return this;
        }

        public ConsumerConfig build() {
            return new ConsumerConfig(this);
        }
    }
}
