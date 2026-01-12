//! Kafka admin operations for topic management.

use crate::consumer::Offset;
use crate::error::{Error, Result};
use crate::kafka::RpcTransport;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;

/// Configuration for creating a topic.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TopicConfig {
    /// Number of partitions.
    pub partitions: u32,
    /// Replication factor.
    pub replication_factor: Option<u32>,
    /// Retention time in milliseconds.
    pub retention_ms: Option<u64>,
    /// Cleanup policy ("delete" or "compact").
    pub cleanup_policy: Option<String>,
    /// Maximum message size.
    pub max_message_bytes: Option<u32>,
}

impl TopicConfig {
    /// Create a new topic configuration.
    pub fn new() -> Self {
        Self {
            partitions: 1,
            ..Default::default()
        }
    }

    /// Set the number of partitions.
    pub fn partitions(mut self, partitions: u32) -> Self {
        self.partitions = partitions;
        self
    }

    /// Set the replication factor.
    pub fn replication_factor(mut self, factor: u32) -> Self {
        self.replication_factor = Some(factor);
        self
    }

    /// Set the retention time.
    pub fn retention(mut self, duration: Duration) -> Self {
        self.retention_ms = Some(duration.as_millis() as u64);
        self
    }

    /// Set the cleanup policy.
    pub fn cleanup_policy(mut self, policy: impl Into<String>) -> Self {
        self.cleanup_policy = Some(policy.into());
        self
    }

    /// Set the maximum message size.
    pub fn max_message_bytes(mut self, size: u32) -> Self {
        self.max_message_bytes = Some(size);
        self
    }
}

/// Information about a topic.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicInfo {
    /// Topic name.
    pub name: String,
    /// Number of partitions.
    pub partitions: u32,
    /// Replication factor.
    #[serde(default)]
    pub replication_factor: u32,
    /// Retention time in milliseconds.
    pub retention_ms: Option<u64>,
}

/// Information about a consumer group.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupInfo {
    /// Group ID.
    pub id: String,
    /// Group state (e.g., "Stable", "Empty", "Rebalancing").
    pub state: String,
    /// Number of members.
    #[serde(default)]
    pub member_count: u32,
    /// Members of the group.
    #[serde(default)]
    pub members: Vec<GroupMember>,
    /// Total lag across all partitions.
    #[serde(default)]
    pub total_lag: i64,
}

/// Information about a consumer group member.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupMember {
    /// Member ID.
    pub id: String,
    /// Client ID.
    #[serde(default)]
    pub client_id: String,
    /// Client host.
    #[serde(default)]
    pub host: String,
    /// Assigned partitions.
    #[serde(default)]
    pub partitions: Vec<PartitionAssignment>,
}

/// Partition assignment for a group member.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionAssignment {
    /// Topic name.
    pub topic: String,
    /// Partition number.
    pub partition: i32,
}

/// Kafka admin client for topic and group management.
pub struct Admin {
    transport: Arc<dyn RpcTransport>,
}

impl Admin {
    /// Create a new admin client.
    pub(crate) fn new(transport: Arc<dyn RpcTransport>) -> Self {
        Self { transport }
    }

    /// Create a new topic.
    pub async fn create_topic(&self, name: &str, config: TopicConfig) -> Result<()> {
        let args = serde_json::json!([name, config.partitions, config]);
        self.transport.call("kafka.createTopic", args).await?;
        Ok(())
    }

    /// Create multiple topics at once.
    pub async fn create_topics(&self, topics: &[&str]) -> Result<()> {
        for topic in topics {
            self.create_topic(topic, TopicConfig::new()).await?;
        }
        Ok(())
    }

    /// Create multiple topics with configuration.
    pub async fn create_topics_with_config(
        &self,
        topics: &[(&str, TopicConfig)],
    ) -> Result<()> {
        for (name, config) in topics {
            self.create_topic(name, config.clone()).await?;
        }
        Ok(())
    }

    /// Delete a topic.
    pub async fn delete_topic(&self, name: &str) -> Result<()> {
        let args = serde_json::json!([name]);
        self.transport.call("kafka.deleteTopic", args).await?;
        Ok(())
    }

    /// Delete multiple topics.
    pub async fn delete_topics(&self, topics: &[&str]) -> Result<()> {
        for topic in topics {
            self.delete_topic(topic).await?;
        }
        Ok(())
    }

    /// List all topics.
    pub async fn list_topics(&self) -> Result<Vec<TopicInfo>> {
        let args = serde_json::json!([]);
        let response = self.transport.call("kafka.listTopics", args).await?;

        let topics = response
            .get("topics")
            .and_then(|v| serde_json::from_value::<Vec<TopicInfo>>(v.clone()).ok())
            .unwrap_or_default();

        Ok(topics)
    }

    /// Describe a specific topic.
    pub async fn describe_topic(&self, name: &str) -> Result<TopicInfo> {
        let args = serde_json::json!([name]);
        let response = self.transport.call("kafka.describeTopic", args).await?;
        serde_json::from_value(response).map_err(Error::from)
    }

    /// Alter a topic's configuration.
    pub async fn alter_topic(&self, name: &str, config: TopicConfig) -> Result<()> {
        let args = serde_json::json!([name, config]);
        self.transport.call("kafka.alterTopic", args).await?;
        Ok(())
    }

    /// List all consumer groups.
    pub async fn list_groups(&self) -> Result<Vec<GroupInfo>> {
        let args = serde_json::json!([]);
        let response = self.transport.call("kafka.listGroups", args).await?;

        let groups = response
            .get("groups")
            .and_then(|v| serde_json::from_value::<Vec<GroupInfo>>(v.clone()).ok())
            .unwrap_or_default();

        Ok(groups)
    }

    /// Describe a specific consumer group.
    pub async fn describe_group(&self, group_id: &str) -> Result<GroupInfo> {
        let args = serde_json::json!([group_id]);
        let response = self.transport.call("kafka.describeGroup", args).await?;
        serde_json::from_value(response).map_err(Error::from)
    }

    /// Delete a consumer group.
    pub async fn delete_group(&self, group_id: &str) -> Result<()> {
        let args = serde_json::json!([group_id]);
        self.transport.call("kafka.deleteGroup", args).await?;
        Ok(())
    }

    /// Reset offsets for a consumer group.
    pub async fn reset_offsets(
        &self,
        group_id: &str,
        topic: &str,
        offset: Offset,
    ) -> Result<()> {
        let offset_value = match offset {
            Offset::Earliest => serde_json::json!("earliest"),
            Offset::Latest => serde_json::json!("latest"),
            Offset::Offset(o) => serde_json::json!(o),
            Offset::Timestamp(ts) => serde_json::json!({ "timestamp": ts.timestamp_millis() }),
        };

        let args = serde_json::json!([group_id, topic, offset_value]);
        self.transport.call("kafka.resetOffsets", args).await?;
        Ok(())
    }

    /// Reset offsets to a specific timestamp.
    pub async fn reset_offsets_to_time(
        &self,
        group_id: &str,
        topic: &str,
        timestamp: chrono::DateTime<chrono::Utc>,
    ) -> Result<()> {
        self.reset_offsets(group_id, topic, Offset::Timestamp(timestamp))
            .await
    }

    /// Get the current offset for a consumer group on a topic.
    pub async fn get_offsets(
        &self,
        group_id: &str,
        topic: &str,
    ) -> Result<Vec<PartitionOffset>> {
        let args = serde_json::json!([group_id, topic]);
        let response = self.transport.call("kafka.getOffsets", args).await?;

        let offsets = response
            .get("offsets")
            .and_then(|v| serde_json::from_value::<Vec<PartitionOffset>>(v.clone()).ok())
            .unwrap_or_default();

        Ok(offsets)
    }
}

/// Offset information for a partition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionOffset {
    /// Partition number.
    pub partition: i32,
    /// Current committed offset.
    pub offset: i64,
    /// High watermark (end offset).
    #[serde(default)]
    pub high_watermark: i64,
    /// Lag (high_watermark - offset).
    #[serde(default)]
    pub lag: i64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kafka::mock::MockRpcClient;
    use crate::kafka::MockRpcTransport;

    fn create_mock_admin() -> (Admin, Arc<MockRpcClient>) {
        let mock_client = Arc::new(MockRpcClient::new());
        let transport = Arc::new(MockRpcTransport {
            client: mock_client.clone(),
        });
        let admin = Admin::new(transport);
        (admin, mock_client)
    }

    #[tokio::test]
    async fn test_create_topic() {
        let (admin, mock_client) = create_mock_admin();

        let config = TopicConfig::new()
            .partitions(3)
            .retention(Duration::from_secs(86400));

        let result = admin.create_topic("test-topic", config).await;
        assert!(result.is_ok());

        // Verify topic was created
        assert!(mock_client.topic_exists("test-topic"));
    }

    #[tokio::test]
    async fn test_create_topics() {
        let (admin, mock_client) = create_mock_admin();

        let result = admin.create_topics(&["topic-a", "topic-b", "topic-c"]).await;
        assert!(result.is_ok());

        assert!(mock_client.topic_exists("topic-a"));
        assert!(mock_client.topic_exists("topic-b"));
        assert!(mock_client.topic_exists("topic-c"));
    }

    #[tokio::test]
    async fn test_delete_topic() {
        let (admin, mock_client) = create_mock_admin();

        // Create topic first
        mock_client.create_topic("to-delete", 1, None);
        assert!(mock_client.topic_exists("to-delete"));

        // Delete it
        let result = admin.delete_topic("to-delete").await;
        assert!(result.is_ok());
        assert!(!mock_client.topic_exists("to-delete"));
    }

    #[tokio::test]
    async fn test_list_topics() {
        let (admin, mock_client) = create_mock_admin();

        // Create some topics
        mock_client.create_topic("topic-1", 1, None);
        mock_client.create_topic("topic-2", 3, Some(86400000));

        let topics = admin.list_topics().await.unwrap();
        assert_eq!(topics.len(), 2);

        let topic_names: Vec<&str> = topics.iter().map(|t| t.name.as_str()).collect();
        assert!(topic_names.contains(&"topic-1"));
        assert!(topic_names.contains(&"topic-2"));
    }

    #[tokio::test]
    async fn test_describe_topic() {
        let (admin, mock_client) = create_mock_admin();

        mock_client.create_topic("described-topic", 5, Some(604800000));

        let info = admin.describe_topic("described-topic").await.unwrap();
        assert_eq!(info.name, "described-topic");
        assert_eq!(info.partitions, 5);
        assert_eq!(info.retention_ms, Some(604800000));
    }

    #[tokio::test]
    async fn test_describe_topic_not_found() {
        let (admin, _mock) = create_mock_admin();

        let result = admin.describe_topic("nonexistent-topic").await;
        assert!(result.is_err());
    }

    #[test]
    fn test_topic_config_builder() {
        let config = TopicConfig::new()
            .partitions(10)
            .replication_factor(3)
            .retention(Duration::from_secs(86400 * 7))
            .cleanup_policy("compact")
            .max_message_bytes(1048576);

        assert_eq!(config.partitions, 10);
        assert_eq!(config.replication_factor, Some(3));
        assert_eq!(config.retention_ms, Some(604800000));
        assert_eq!(config.cleanup_policy, Some("compact".to_string()));
        assert_eq!(config.max_message_bytes, Some(1048576));
    }

    #[test]
    fn test_topic_config_default() {
        let config = TopicConfig::default();
        assert_eq!(config.partitions, 0);
        assert!(config.replication_factor.is_none());
        assert!(config.retention_ms.is_none());
    }

    #[test]
    fn test_topic_info_deserialize() {
        let json = r#"{
            "name": "test-topic",
            "partitions": 3,
            "replication_factor": 2,
            "retention_ms": 86400000
        }"#;

        let info: TopicInfo = serde_json::from_str(json).unwrap();
        assert_eq!(info.name, "test-topic");
        assert_eq!(info.partitions, 3);
        assert_eq!(info.replication_factor, 2);
        assert_eq!(info.retention_ms, Some(86400000));
    }

    #[test]
    fn test_group_info_deserialize() {
        let json = r#"{
            "id": "test-group",
            "state": "Stable",
            "member_count": 3,
            "members": [],
            "total_lag": 100
        }"#;

        let info: GroupInfo = serde_json::from_str(json).unwrap();
        assert_eq!(info.id, "test-group");
        assert_eq!(info.state, "Stable");
        assert_eq!(info.member_count, 3);
        assert_eq!(info.total_lag, 100);
    }

    #[test]
    fn test_group_member_deserialize() {
        let json = r#"{
            "id": "member-1",
            "client_id": "client-1",
            "host": "localhost",
            "partitions": [
                {"topic": "test", "partition": 0},
                {"topic": "test", "partition": 1}
            ]
        }"#;

        let member: GroupMember = serde_json::from_str(json).unwrap();
        assert_eq!(member.id, "member-1");
        assert_eq!(member.client_id, "client-1");
        assert_eq!(member.partitions.len(), 2);
    }

    #[test]
    fn test_partition_offset_deserialize() {
        let json = r#"{
            "partition": 0,
            "offset": 100,
            "high_watermark": 150,
            "lag": 50
        }"#;

        let offset: PartitionOffset = serde_json::from_str(json).unwrap();
        assert_eq!(offset.partition, 0);
        assert_eq!(offset.offset, 100);
        assert_eq!(offset.high_watermark, 150);
        assert_eq!(offset.lag, 50);
    }

    #[tokio::test]
    async fn test_create_topics_with_config() {
        let (admin, mock_client) = create_mock_admin();

        let topics = vec![
            ("high-throughput", TopicConfig::new().partitions(10)),
            ("low-latency", TopicConfig::new().partitions(3)),
        ];

        let result = admin.create_topics_with_config(&topics).await;
        assert!(result.is_ok());

        assert!(mock_client.topic_exists("high-throughput"));
        assert!(mock_client.topic_exists("low-latency"));
    }

    #[tokio::test]
    async fn test_delete_topics() {
        let (admin, mock_client) = create_mock_admin();

        mock_client.create_topic("delete-1", 1, None);
        mock_client.create_topic("delete-2", 1, None);

        let result = admin.delete_topics(&["delete-1", "delete-2"]).await;
        assert!(result.is_ok());

        assert!(!mock_client.topic_exists("delete-1"));
        assert!(!mock_client.topic_exists("delete-2"));
    }

    #[tokio::test]
    async fn test_admin_error_propagation() {
        let (admin, mock_client) = create_mock_admin();

        mock_client.set_error(crate::error::ErrorKind::Unauthorized);

        let result = admin.create_topic("test", TopicConfig::new()).await;
        assert!(result.is_err());
    }

    #[test]
    fn test_partition_assignment_deserialize() {
        let json = r#"{"topic": "orders", "partition": 5}"#;
        let assignment: PartitionAssignment = serde_json::from_str(json).unwrap();

        assert_eq!(assignment.topic, "orders");
        assert_eq!(assignment.partition, 5);
    }

    #[test]
    fn test_topic_config_serialize() {
        let config = TopicConfig::new()
            .partitions(5)
            .retention(Duration::from_secs(3600));

        let json = serde_json::to_string(&config).unwrap();
        assert!(json.contains("\"partitions\":5"));
        assert!(json.contains("3600000")); // 3600 seconds in ms
    }
}
