//! # kafka-do
//!
//! Event Streaming for Rust. Zero-Copy. Async. Zero Ops.
//!
//! This SDK provides a rdkafka-compatible API for interacting with Kafka-DO,
//! using RPC-DO as the underlying transport.
//!
//! ## Quick Start
//!
//! ```ignore
//! use kafka_do::{Kafka, Message};
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Debug, Serialize, Deserialize)]
//! struct Order {
//!     order_id: String,
//!     amount: f64,
//! }
//!
//! #[tokio::main]
//! async fn main() -> kafka_do::Result<()> {
//!     let kafka = Kafka::new(&["broker1:9092"]);
//!
//!     // Produce
//!     let producer = kafka.producer().await?;
//!     producer.send("orders", "key", Order {
//!         order_id: "123".into(),
//!         amount: 99.99,
//!     }).await?;
//!
//!     // Consume
//!     let mut consumer = kafka.consumer("my-group").await?;
//!     consumer.subscribe(&["orders"]).await?;
//!     while let Some(msg) = consumer.next().await {
//!         let msg = msg?;
//!         let order: Order = msg.value()?;
//!         println!("Received: {:?}", order);
//!         msg.commit().await?;
//!     }
//!
//!     // Admin
//!     let admin = kafka.admin().await?;
//!     admin.create_topics(&["new-topic"]).await?;
//!
//!     Ok(())
//! }
//! ```

pub mod admin;
pub mod consumer;
pub mod error;
pub mod kafka;
pub mod producer;

// Re-exports
pub use admin::{Admin, GroupInfo, GroupMember, PartitionAssignment, PartitionOffset, TopicConfig, TopicInfo};
pub use consumer::{Consumer, ConsumerConfig, Message, Offset};
pub use error::{Error, ErrorKind, Result};
pub use kafka::{Config, Kafka};
pub use producer::{Producer, RecordMetadata, SendOptions};

/// Prelude module for common imports.
pub mod prelude {
    pub use super::admin::{Admin, TopicConfig, TopicInfo};
    pub use super::consumer::{Consumer, ConsumerConfig, Message, Offset};
    pub use super::error::{Error, ErrorKind, Result};
    pub use super::kafka::{Config, Kafka};
    pub use super::producer::{Producer, RecordMetadata, SendOptions};
    pub use serde::{Deserialize, Serialize};
}

/// Check if the SDK is fully implemented.
pub fn is_implemented() -> bool {
    true
}

/// Get the SDK version.
pub fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_version() {
        assert_eq!(version(), "0.1.0");
    }

    #[test]
    fn test_is_implemented() {
        assert!(is_implemented());
    }

    #[test]
    fn test_prelude_exports() {
        // Ensure prelude exports are accessible
        use prelude::*;
        let _ = Config::new();
        let _ = ConsumerConfig::new();
        let _ = TopicConfig::new();
        let _ = SendOptions::new();
    }

    #[test]
    fn test_error_exports() {
        // Verify error types are exported
        let err = Error::topic_not_found("test");
        assert_eq!(err.kind(), &ErrorKind::TopicNotFound);
    }

    #[test]
    fn test_config_exports() {
        // Verify config types are exported
        let config = Config::new()
            .url("https://kafka.do")
            .api_key("test-key");
        assert!(config.url.is_some());
    }

    #[test]
    fn test_consumer_config_exports() {
        // Verify consumer config is exported
        let config = ConsumerConfig::new()
            .offset(Offset::Earliest)
            .auto_commit(true);
        assert_eq!(config.offset, Offset::Earliest);
        assert!(config.auto_commit);
    }

    #[test]
    fn test_topic_config_exports() {
        // Verify topic config is exported
        let config = TopicConfig::new().partitions(5);
        assert_eq!(config.partitions, 5);
    }

    #[test]
    fn test_send_options_exports() {
        // Verify send options is exported
        let options = SendOptions::new()
            .key("test-key")
            .header("h1", "v1");
        assert_eq!(options.key, Some("test-key".to_string()));
    }
}
