//! Kafka client configuration and main entry point.

use crate::admin::Admin;
use crate::consumer::{Consumer, ConsumerConfig};
use crate::error::{Error, ErrorKind, Result};
use crate::producer::Producer;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;

/// Configuration for the Kafka client.
#[derive(Debug, Clone)]
pub struct Config {
    /// Broker endpoints.
    pub brokers: Vec<String>,
    /// API URL for Kafka-DO service.
    pub url: Option<String>,
    /// API key for authentication.
    pub api_key: Option<String>,
    /// Request timeout.
    pub timeout: Duration,
    /// Number of retry attempts.
    pub retries: u32,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            brokers: Vec::new(),
            url: None,
            api_key: None,
            timeout: Duration::from_secs(30),
            retries: 3,
        }
    }
}

impl Config {
    /// Create a new configuration.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the API URL.
    pub fn url(mut self, url: impl Into<String>) -> Self {
        self.url = Some(url.into());
        self
    }

    /// Set the API key.
    pub fn api_key(mut self, api_key: impl Into<String>) -> Self {
        self.api_key = Some(api_key.into());
        self
    }

    /// Set the request timeout.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Set the number of retries.
    pub fn retries(mut self, retries: u32) -> Self {
        self.retries = retries;
        self
    }

    /// Set the broker endpoints.
    pub fn brokers(mut self, brokers: Vec<String>) -> Self {
        self.brokers = brokers;
        self
    }
}

/// Mock RPC client for testing.
#[cfg(test)]
pub mod mock {
    use super::*;
    use serde_json::Value as JsonValue;
    use std::collections::HashMap;
    use std::sync::Mutex;

    /// Mock RPC client that stores messages in memory.
    #[derive(Debug, Default)]
    pub struct MockRpcClient {
        /// Messages stored by topic.
        pub messages: Arc<Mutex<HashMap<String, Vec<MockMessage>>>>,
        /// Topics that exist.
        pub topics: Arc<Mutex<Vec<TopicInfo>>>,
        /// Consumer groups.
        pub groups: Arc<Mutex<HashMap<String, GroupInfo>>>,
        /// Whether to simulate errors.
        pub should_error: Arc<Mutex<Option<ErrorKind>>>,
        /// Consumer offsets by group and topic.
        pub offsets: Arc<Mutex<HashMap<String, HashMap<String, i64>>>>,
    }

    /// Mock message stored in memory.
    #[derive(Debug, Clone)]
    pub struct MockMessage {
        pub topic: String,
        pub partition: i32,
        pub offset: i64,
        pub key: Option<String>,
        pub value: Vec<u8>,
        pub headers: HashMap<String, String>,
        pub timestamp: i64,
    }

    /// Topic information.
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct TopicInfo {
        pub name: String,
        pub partitions: u32,
        pub retention_ms: Option<u64>,
    }

    /// Consumer group information.
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct GroupInfo {
        pub id: String,
        pub members: Vec<String>,
        pub state: String,
    }

    impl MockRpcClient {
        /// Create a new mock RPC client.
        pub fn new() -> Self {
            Self::default()
        }

        /// Set an error to be returned on the next operation.
        pub fn set_error(&self, kind: ErrorKind) {
            *self.should_error.lock().unwrap() = Some(kind);
        }

        /// Clear any pending error.
        pub fn clear_error(&self) {
            *self.should_error.lock().unwrap() = None;
        }

        /// Check if an error should be returned.
        pub fn check_error(&self) -> Option<Error> {
            let mut guard = self.should_error.lock().unwrap();
            guard.take().map(|kind| Error::new(kind, "Mock error"))
        }

        /// Store a message.
        pub fn store_message(&self, msg: MockMessage) {
            let mut messages = self.messages.lock().unwrap();
            let topic_messages = messages.entry(msg.topic.clone()).or_insert_with(Vec::new);
            topic_messages.push(msg);
        }

        /// Get messages for a topic.
        pub fn get_messages(&self, topic: &str) -> Vec<MockMessage> {
            let messages = self.messages.lock().unwrap();
            messages.get(topic).cloned().unwrap_or_default()
        }

        /// Create a topic.
        pub fn create_topic(&self, name: &str, partitions: u32, retention_ms: Option<u64>) {
            let mut topics = self.topics.lock().unwrap();
            topics.push(TopicInfo {
                name: name.to_string(),
                partitions,
                retention_ms,
            });
        }

        /// Check if a topic exists.
        pub fn topic_exists(&self, name: &str) -> bool {
            let topics = self.topics.lock().unwrap();
            topics.iter().any(|t| t.name == name)
        }

        /// Get all topics.
        pub fn list_topics(&self) -> Vec<TopicInfo> {
            self.topics.lock().unwrap().clone()
        }

        /// Delete a topic.
        pub fn delete_topic(&self, name: &str) -> bool {
            let mut topics = self.topics.lock().unwrap();
            let initial_len = topics.len();
            topics.retain(|t| t.name != name);
            topics.len() < initial_len
        }

        /// Create a consumer group.
        pub fn create_group(&self, id: &str) {
            let mut groups = self.groups.lock().unwrap();
            groups.insert(
                id.to_string(),
                GroupInfo {
                    id: id.to_string(),
                    members: Vec::new(),
                    state: "Empty".to_string(),
                },
            );
        }

        /// Add a member to a group.
        pub fn add_group_member(&self, group_id: &str, member_id: &str) {
            let mut groups = self.groups.lock().unwrap();
            if let Some(group) = groups.get_mut(group_id) {
                group.members.push(member_id.to_string());
                group.state = "Stable".to_string();
            }
        }

        /// Get current offset for a group/topic.
        pub fn get_offset(&self, group: &str, topic: &str) -> i64 {
            let offsets = self.offsets.lock().unwrap();
            offsets
                .get(group)
                .and_then(|topics| topics.get(topic))
                .copied()
                .unwrap_or(0)
        }

        /// Set offset for a group/topic.
        pub fn set_offset(&self, group: &str, topic: &str, offset: i64) {
            let mut offsets = self.offsets.lock().unwrap();
            let group_offsets = offsets.entry(group.to_string()).or_insert_with(HashMap::new);
            group_offsets.insert(topic.to_string(), offset);
        }
    }
}

/// Internal RPC client trait for abstraction.
#[async_trait::async_trait]
pub trait RpcTransport: Send + Sync {
    /// Call an RPC method.
    async fn call(&self, method: &str, args: serde_json::Value) -> Result<serde_json::Value>;
}

/// Real RPC transport using rpc-do.
pub struct RealRpcTransport {
    client: rpc_do::RpcClient,
}

#[async_trait::async_trait]
impl RpcTransport for RealRpcTransport {
    async fn call(&self, method: &str, args: serde_json::Value) -> Result<serde_json::Value> {
        let args_vec = if let serde_json::Value::Array(arr) = args {
            arr
        } else {
            vec![args]
        };
        self.client.call_raw(method, args_vec).await.map_err(Into::into)
    }
}

/// Mock RPC transport for testing.
#[cfg(test)]
pub struct MockRpcTransport {
    pub client: Arc<mock::MockRpcClient>,
}

#[cfg(test)]
#[async_trait::async_trait]
impl RpcTransport for MockRpcTransport {
    async fn call(&self, method: &str, args: serde_json::Value) -> Result<serde_json::Value> {
        // Check for configured error
        if let Some(err) = self.client.check_error() {
            return Err(err);
        }

        // Handle different methods
        match method {
            "kafka.produce" => {
                // Extract topic, key, value from args
                if let serde_json::Value::Array(arr) = &args {
                    if arr.len() >= 3 {
                        let topic = arr[0].as_str().unwrap_or("").to_string();
                        let key = arr[1].as_str().map(|s| s.to_string());
                        let value = serde_json::to_vec(&arr[2]).unwrap_or_default();

                        let partition = 0i32;
                        let offset = {
                            let messages = self.client.messages.lock().unwrap();
                            messages.get(&topic).map(|m| m.len() as i64).unwrap_or(0)
                        };

                        let msg = mock::MockMessage {
                            topic: topic.clone(),
                            partition,
                            offset,
                            key,
                            value,
                            headers: HashMap::new(),
                            timestamp: chrono::Utc::now().timestamp_millis(),
                        };

                        self.client.store_message(msg);

                        return Ok(serde_json::json!({
                            "topic": topic,
                            "partition": partition,
                            "offset": offset
                        }));
                    }
                }
                Ok(serde_json::json!({}))
            }
            "kafka.consume" => {
                // Return pending messages for the topic
                if let serde_json::Value::Array(arr) = &args {
                    if let Some(topic) = arr.get(0).and_then(|v| v.as_str()) {
                        let group = arr.get(1).and_then(|v| v.as_str()).unwrap_or("default");
                        let current_offset = self.client.get_offset(group, topic);
                        let messages = self.client.get_messages(topic);

                        let pending: Vec<_> = messages
                            .into_iter()
                            .filter(|m| m.offset >= current_offset)
                            .take(10)
                            .map(|m| {
                                serde_json::json!({
                                    "topic": m.topic,
                                    "partition": m.partition,
                                    "offset": m.offset,
                                    "key": m.key,
                                    "value": serde_json::from_slice::<serde_json::Value>(&m.value).unwrap_or(serde_json::Value::Null),
                                    "timestamp": m.timestamp,
                                    "headers": m.headers
                                })
                            })
                            .collect();

                        return Ok(serde_json::json!({ "messages": pending }));
                    }
                }
                Ok(serde_json::json!({ "messages": [] }))
            }
            "kafka.commit" => {
                if let serde_json::Value::Array(arr) = &args {
                    if arr.len() >= 3 {
                        let group = arr[0].as_str().unwrap_or("default");
                        let topic = arr[1].as_str().unwrap_or("");
                        let offset = arr[2].as_i64().unwrap_or(0);
                        self.client.set_offset(group, topic, offset + 1);
                    }
                }
                Ok(serde_json::json!({}))
            }
            "kafka.createTopic" => {
                if let serde_json::Value::Array(arr) = &args {
                    if let Some(topic) = arr.get(0).and_then(|v| v.as_str()) {
                        let partitions = arr.get(1).and_then(|v| v.as_u64()).unwrap_or(1) as u32;
                        self.client.create_topic(topic, partitions, None);
                        return Ok(serde_json::json!({ "created": true }));
                    }
                }
                Ok(serde_json::json!({ "created": false }))
            }
            "kafka.deleteTopic" => {
                if let serde_json::Value::Array(arr) = &args {
                    if let Some(topic) = arr.get(0).and_then(|v| v.as_str()) {
                        let deleted = self.client.delete_topic(topic);
                        return Ok(serde_json::json!({ "deleted": deleted }));
                    }
                }
                Ok(serde_json::json!({ "deleted": false }))
            }
            "kafka.listTopics" => {
                let topics = self.client.list_topics();
                Ok(serde_json::json!({ "topics": topics }))
            }
            "kafka.describeTopic" => {
                if let serde_json::Value::Array(arr) = &args {
                    if let Some(topic_name) = arr.get(0).and_then(|v| v.as_str()) {
                        let topics = self.client.list_topics();
                        if let Some(topic) = topics.iter().find(|t| t.name == topic_name) {
                            return Ok(serde_json::json!({
                                "name": topic.name,
                                "partitions": topic.partitions,
                                "retention_ms": topic.retention_ms
                            }));
                        } else {
                            return Err(Error::topic_not_found(topic_name));
                        }
                    }
                }
                Err(Error::other("Invalid arguments"))
            }
            _ => Ok(serde_json::json!({})),
        }
    }
}

#[cfg(test)]
use std::collections::HashMap;

/// Main Kafka client.
pub struct Kafka {
    config: Config,
    transport: Arc<dyn RpcTransport>,
}

impl Kafka {
    /// Create a new Kafka client with broker endpoints.
    ///
    /// # Example
    /// ```ignore
    /// let kafka = Kafka::new(&["broker1:9092", "broker2:9092"]);
    /// ```
    pub fn new(brokers: &[&str]) -> Self {
        let config = Config {
            brokers: brokers.iter().map(|s| s.to_string()).collect(),
            ..Default::default()
        };

        // In production, this would connect to the actual RPC service
        // For now, we create a placeholder that will be replaced in tests
        Self {
            config,
            transport: Arc::new(PlaceholderTransport),
        }
    }

    /// Create a Kafka client with custom configuration.
    pub fn with_config(config: Config) -> Self {
        Self {
            config,
            transport: Arc::new(PlaceholderTransport),
        }
    }

    /// Create a Kafka client with a custom transport (for testing).
    #[cfg(test)]
    pub fn with_transport(config: Config, transport: Arc<dyn RpcTransport>) -> Self {
        Self { config, transport }
    }

    /// Get the configuration.
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// Create a producer for the given topic.
    pub async fn producer(&self) -> Result<Producer> {
        Ok(Producer::new(self.transport.clone()))
    }

    /// Create a consumer for the given consumer group.
    pub async fn consumer(&self, group_id: &str) -> Result<Consumer> {
        Ok(Consumer::new(self.transport.clone(), group_id.to_string(), ConsumerConfig::default()))
    }

    /// Create a consumer with custom configuration.
    pub async fn consumer_with_config(
        &self,
        group_id: &str,
        config: ConsumerConfig,
    ) -> Result<Consumer> {
        Ok(Consumer::new(self.transport.clone(), group_id.to_string(), config))
    }

    /// Get an admin client for topic management.
    pub async fn admin(&self) -> Result<Admin> {
        Ok(Admin::new(self.transport.clone()))
    }
}

/// Placeholder transport that returns errors until properly connected.
struct PlaceholderTransport;

#[async_trait::async_trait]
impl RpcTransport for PlaceholderTransport {
    async fn call(&self, _method: &str, _args: serde_json::Value) -> Result<serde_json::Value> {
        Err(Error::new(
            ErrorKind::Disconnected,
            "Not connected to Kafka-DO service",
        ))
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use super::mock::*;

    fn create_mock_kafka() -> (Kafka, Arc<MockRpcClient>) {
        let mock_client = Arc::new(MockRpcClient::new());
        let transport = Arc::new(MockRpcTransport {
            client: mock_client.clone(),
        });
        let kafka = Kafka::with_transport(Config::default(), transport);
        (kafka, mock_client)
    }

    #[test]
    fn test_config_builder() {
        let config = Config::new()
            .url("https://kafka.do")
            .api_key("test-key")
            .timeout(Duration::from_secs(60))
            .retries(5)
            .brokers(vec!["broker1:9092".to_string()]);

        assert_eq!(config.url, Some("https://kafka.do".to_string()));
        assert_eq!(config.api_key, Some("test-key".to_string()));
        assert_eq!(config.timeout, Duration::from_secs(60));
        assert_eq!(config.retries, 5);
        assert_eq!(config.brokers, vec!["broker1:9092"]);
    }

    #[test]
    fn test_config_default() {
        let config = Config::default();
        assert!(config.brokers.is_empty());
        assert!(config.url.is_none());
        assert!(config.api_key.is_none());
        assert_eq!(config.timeout, Duration::from_secs(30));
        assert_eq!(config.retries, 3);
    }

    #[test]
    fn test_kafka_new() {
        let kafka = Kafka::new(&["broker1:9092", "broker2:9092"]);
        assert_eq!(kafka.config().brokers.len(), 2);
        assert_eq!(kafka.config().brokers[0], "broker1:9092");
    }

    #[test]
    fn test_kafka_with_config() {
        let config = Config::new().url("https://test.kafka.do");
        let kafka = Kafka::with_config(config);
        assert_eq!(kafka.config().url, Some("https://test.kafka.do".to_string()));
    }

    #[tokio::test]
    async fn test_kafka_producer() {
        let (kafka, _mock) = create_mock_kafka();
        let producer = kafka.producer().await;
        assert!(producer.is_ok());
    }

    #[tokio::test]
    async fn test_kafka_consumer() {
        let (kafka, _mock) = create_mock_kafka();
        let consumer = kafka.consumer("test-group").await;
        assert!(consumer.is_ok());
    }

    #[tokio::test]
    async fn test_kafka_admin() {
        let (kafka, _mock) = create_mock_kafka();
        let admin = kafka.admin().await;
        assert!(admin.is_ok());
    }

    #[test]
    fn test_mock_rpc_client() {
        let mock = MockRpcClient::new();

        // Test topic creation
        mock.create_topic("test-topic", 3, Some(86400000));
        assert!(mock.topic_exists("test-topic"));

        let topics = mock.list_topics();
        assert_eq!(topics.len(), 1);
        assert_eq!(topics[0].name, "test-topic");
        assert_eq!(topics[0].partitions, 3);

        // Test topic deletion
        assert!(mock.delete_topic("test-topic"));
        assert!(!mock.topic_exists("test-topic"));
    }

    #[test]
    fn test_mock_message_storage() {
        let mock = MockRpcClient::new();

        let msg = MockMessage {
            topic: "test-topic".to_string(),
            partition: 0,
            offset: 0,
            key: Some("key1".to_string()),
            value: b"value1".to_vec(),
            headers: HashMap::new(),
            timestamp: 1234567890,
        };

        mock.store_message(msg);

        let messages = mock.get_messages("test-topic");
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].key, Some("key1".to_string()));
    }

    #[test]
    fn test_mock_offset_tracking() {
        let mock = MockRpcClient::new();

        // Initial offset should be 0
        assert_eq!(mock.get_offset("group1", "topic1"), 0);

        // Set and verify offset
        mock.set_offset("group1", "topic1", 100);
        assert_eq!(mock.get_offset("group1", "topic1"), 100);

        // Different group should have different offset
        assert_eq!(mock.get_offset("group2", "topic1"), 0);
    }

    #[test]
    fn test_mock_error_injection() {
        let mock = MockRpcClient::new();

        // No error initially
        assert!(mock.check_error().is_none());

        // Set error
        mock.set_error(ErrorKind::TopicNotFound);
        let err = mock.check_error();
        assert!(err.is_some());
        assert_eq!(err.unwrap().kind(), &ErrorKind::TopicNotFound);

        // Error should be cleared after check
        assert!(mock.check_error().is_none());

        // Test clear_error
        mock.set_error(ErrorKind::Timeout);
        mock.clear_error();
        assert!(mock.check_error().is_none());
    }

    #[test]
    fn test_mock_consumer_groups() {
        let mock = MockRpcClient::new();

        mock.create_group("test-group");
        mock.add_group_member("test-group", "member-1");

        let groups = mock.groups.lock().unwrap();
        let group = groups.get("test-group").unwrap();
        assert_eq!(group.id, "test-group");
        assert_eq!(group.members.len(), 1);
        assert_eq!(group.state, "Stable");
    }
}
