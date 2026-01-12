package do_.kafka;

import java.time.Duration;

/**
 * Configuration for a Kafka admin client.
 */
public final class AdminConfig {

    private final Duration requestTimeout;
    private final String clientId;

    private AdminConfig(Builder builder) {
        this.requestTimeout = builder.requestTimeout;
        this.clientId = builder.clientId;
    }

    public Duration requestTimeout() {
        return requestTimeout;
    }

    public String clientId() {
        return clientId;
    }

    public static final class Builder {
        private Duration requestTimeout = Duration.ofSeconds(30);
        private String clientId;

        public Builder requestTimeout(Duration requestTimeout) {
            this.requestTimeout = requestTimeout;
            return this;
        }

        public Builder clientId(String clientId) {
            this.clientId = clientId;
            return this;
        }

        public AdminConfig build() {
            return new AdminConfig(this);
        }
    }
}
