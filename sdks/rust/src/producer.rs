//! Kafka producer for sending messages.

use crate::error::{Error, Result};
use crate::kafka::RpcTransport;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

/// Metadata returned after successfully sending a message.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordMetadata {
    /// Topic the message was sent to.
    pub topic: String,
    /// Partition the message was written to.
    pub partition: i32,
    /// Offset of the message within the partition.
    pub offset: i64,
    /// Timestamp of the message (if available).
    #[serde(default)]
    pub timestamp: Option<i64>,
}

/// Options for sending messages.
#[derive(Debug, Clone, Default)]
pub struct SendOptions {
    /// Message key for partitioning.
    pub key: Option<String>,
    /// Custom headers.
    pub headers: HashMap<String, String>,
    /// Target partition (if specified).
    pub partition: Option<i32>,
}

impl SendOptions {
    /// Create new send options.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the message key.
    pub fn key(mut self, key: impl Into<String>) -> Self {
        self.key = Some(key.into());
        self
    }

    /// Add a header.
    pub fn header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.insert(key.into(), value.into());
        self
    }

    /// Set the target partition.
    pub fn partition(mut self, partition: i32) -> Self {
        self.partition = Some(partition);
        self
    }
}

/// Kafka producer for sending messages to topics.
pub struct Producer {
    transport: Arc<dyn RpcTransport>,
}

impl Producer {
    /// Create a new producer.
    pub(crate) fn new(transport: Arc<dyn RpcTransport>) -> Self {
        Self { transport }
    }

    /// Send a message to a topic.
    ///
    /// # Example
    /// ```ignore
    /// producer.send("my-topic", "key", "value").await?;
    /// ```
    pub async fn send(
        &self,
        topic: &str,
        key: impl Into<Option<&str>>,
        value: impl Serialize,
    ) -> Result<RecordMetadata> {
        let key_str: Option<&str> = key.into();
        let value_json = serde_json::to_value(&value)?;

        let args = serde_json::json!([topic, key_str, value_json]);
        let response = self.transport.call("kafka.produce", args).await?;

        serde_json::from_value(response).map_err(Error::from)
    }

    /// Send a message with custom options.
    pub async fn send_with_options(
        &self,
        topic: &str,
        value: impl Serialize,
        options: SendOptions,
    ) -> Result<RecordMetadata> {
        let value_json = serde_json::to_value(&value)?;

        let args = serde_json::json!([
            topic,
            options.key,
            value_json,
            {
                "headers": options.headers,
                "partition": options.partition
            }
        ]);

        let response = self.transport.call("kafka.produce", args).await?;
        serde_json::from_value(response).map_err(Error::from)
    }

    /// Send multiple messages in a batch.
    pub async fn send_batch<T: Serialize>(
        &self,
        topic: &str,
        messages: Vec<(Option<String>, T)>,
    ) -> Result<Vec<RecordMetadata>> {
        let mut results = Vec::with_capacity(messages.len());

        for (key, value) in messages {
            let metadata = self.send(topic, key.as_deref(), value).await?;
            results.push(metadata);
        }

        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kafka::mock::*;
    use crate::kafka::MockRpcTransport;

    fn create_mock_producer() -> (Producer, Arc<MockRpcClient>) {
        let mock_client = Arc::new(MockRpcClient::new());
        let transport = Arc::new(MockRpcTransport {
            client: mock_client.clone(),
        });
        let producer = Producer::new(transport);
        (producer, mock_client)
    }

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct TestMessage {
        id: String,
        value: i32,
    }

    #[tokio::test]
    async fn test_send_simple_message() {
        let (producer, mock_client) = create_mock_producer();

        let result = producer
            .send("test-topic", "key1", "hello world")
            .await;

        assert!(result.is_ok());
        let metadata = result.unwrap();
        assert_eq!(metadata.topic, "test-topic");
        assert_eq!(metadata.partition, 0);
        assert_eq!(metadata.offset, 0);

        // Verify message was stored
        let messages = mock_client.get_messages("test-topic");
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].key, Some("key1".to_string()));
    }

    #[tokio::test]
    async fn test_send_without_key() {
        let (producer, mock_client) = create_mock_producer();

        let result = producer.send("test-topic", None, "value").await;

        assert!(result.is_ok());
        let messages = mock_client.get_messages("test-topic");
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].key, None);
    }

    #[tokio::test]
    async fn test_send_struct_message() {
        let (producer, mock_client) = create_mock_producer();

        let msg = TestMessage {
            id: "123".to_string(),
            value: 42,
        };

        let result = producer.send("test-topic", "struct-key", &msg).await;

        assert!(result.is_ok());
        let messages = mock_client.get_messages("test-topic");
        assert_eq!(messages.len(), 1);

        // Verify the value was serialized correctly
        let stored_value: TestMessage =
            serde_json::from_slice(&messages[0].value).unwrap();
        assert_eq!(stored_value, msg);
    }

    #[tokio::test]
    async fn test_send_with_options() {
        let (producer, mock_client) = create_mock_producer();

        let options = SendOptions::new()
            .key("custom-key")
            .header("correlation-id", "abc-123")
            .header("source", "test");

        let result = producer
            .send_with_options("test-topic", "value", options)
            .await;

        assert!(result.is_ok());
        let messages = mock_client.get_messages("test-topic");
        assert_eq!(messages.len(), 1);
    }

    #[tokio::test]
    async fn test_send_batch() {
        let (producer, mock_client) = create_mock_producer();

        let messages = vec![
            (Some("key1".to_string()), "value1"),
            (Some("key2".to_string()), "value2"),
            (None, "value3"),
        ];

        let result = producer.send_batch("test-topic", messages).await;

        assert!(result.is_ok());
        let metadata_list = result.unwrap();
        assert_eq!(metadata_list.len(), 3);

        // Verify offsets are sequential
        assert_eq!(metadata_list[0].offset, 0);
        assert_eq!(metadata_list[1].offset, 1);
        assert_eq!(metadata_list[2].offset, 2);

        // Verify all messages stored
        let stored = mock_client.get_messages("test-topic");
        assert_eq!(stored.len(), 3);
    }

    #[tokio::test]
    async fn test_send_error_handling() {
        let (producer, mock_client) = create_mock_producer();

        // Set up error
        mock_client.set_error(crate::error::ErrorKind::TopicNotFound);

        let result = producer.send("test-topic", "key", "value").await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.kind(), &crate::error::ErrorKind::TopicNotFound);
    }

    #[tokio::test]
    async fn test_send_multiple_topics() {
        let (producer, mock_client) = create_mock_producer();

        producer.send("topic-a", "key1", "value1").await.unwrap();
        producer.send("topic-b", "key2", "value2").await.unwrap();
        producer.send("topic-a", "key3", "value3").await.unwrap();

        let topic_a_messages = mock_client.get_messages("topic-a");
        let topic_b_messages = mock_client.get_messages("topic-b");

        assert_eq!(topic_a_messages.len(), 2);
        assert_eq!(topic_b_messages.len(), 1);
    }

    #[test]
    fn test_send_options_builder() {
        let options = SendOptions::new()
            .key("my-key")
            .header("h1", "v1")
            .header("h2", "v2")
            .partition(5);

        assert_eq!(options.key, Some("my-key".to_string()));
        assert_eq!(options.headers.len(), 2);
        assert_eq!(options.headers.get("h1"), Some(&"v1".to_string()));
        assert_eq!(options.partition, Some(5));
    }

    #[test]
    fn test_record_metadata_deserialize() {
        let json = r#"{"topic":"test","partition":1,"offset":100,"timestamp":1234567890}"#;
        let metadata: RecordMetadata = serde_json::from_str(json).unwrap();

        assert_eq!(metadata.topic, "test");
        assert_eq!(metadata.partition, 1);
        assert_eq!(metadata.offset, 100);
        assert_eq!(metadata.timestamp, Some(1234567890));
    }

    #[test]
    fn test_record_metadata_without_timestamp() {
        let json = r#"{"topic":"test","partition":0,"offset":0}"#;
        let metadata: RecordMetadata = serde_json::from_str(json).unwrap();

        assert_eq!(metadata.topic, "test");
        assert_eq!(metadata.timestamp, None);
    }
}
