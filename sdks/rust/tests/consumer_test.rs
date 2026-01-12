//! Integration tests for the Kafka consumer.

use chrono::Utc;
use kafka_do::prelude::*;
use kafka_do::kafka::mock::{MockRpcClient, MockMessage};
use kafka_do::kafka::MockRpcTransport;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

/// Helper to create a mock Kafka client for testing.
fn create_mock_kafka() -> (kafka_do::Kafka, Arc<MockRpcClient>) {
    let mock_client = Arc::new(MockRpcClient::new());
    let transport = Arc::new(MockRpcTransport {
        client: mock_client.clone(),
    });
    let kafka = kafka_do::Kafka::with_transport(kafka_do::Config::default(), transport);
    (kafka, mock_client)
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
struct TestOrder {
    order_id: String,
    amount: f64,
    customer: String,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
struct TestEvent {
    event_type: String,
    payload: String,
}

fn create_mock_message(topic: &str, offset: i64, key: Option<&str>, value: impl Serialize) -> MockMessage {
    MockMessage {
        topic: topic.to_string(),
        partition: 0,
        offset,
        key: key.map(|s| s.to_string()),
        value: serde_json::to_vec(&value).unwrap(),
        headers: HashMap::new(),
        timestamp: Utc::now().timestamp_millis(),
    }
}

#[tokio::test]
async fn test_consumer_subscribe() {
    let (kafka, _mock) = create_mock_kafka();
    let mut consumer = kafka.consumer("test-group").await.unwrap();

    consumer.subscribe(&["topic1", "topic2"]).await.unwrap();

    let subscriptions = consumer.subscriptions().await;
    assert_eq!(subscriptions.len(), 2);
    assert!(subscriptions.contains(&"topic1".to_string()));
    assert!(subscriptions.contains(&"topic2".to_string()));
}

#[tokio::test]
async fn test_consumer_unsubscribe() {
    let (kafka, _mock) = create_mock_kafka();
    let mut consumer = kafka.consumer("test-group").await.unwrap();

    consumer.subscribe(&["topic1", "topic2"]).await.unwrap();
    assert_eq!(consumer.subscriptions().await.len(), 2);

    consumer.unsubscribe().await.unwrap();
    assert!(consumer.subscriptions().await.is_empty());
}

#[tokio::test]
async fn test_consumer_receive_messages() {
    let (kafka, mock_client) = create_mock_kafka();

    // Add messages
    mock_client.store_message(create_mock_message("orders", 0, Some("ORD-001"), TestOrder {
        order_id: "ORD-001".to_string(),
        amount: 99.99,
        customer: "Alice".to_string(),
    }));

    mock_client.store_message(create_mock_message("orders", 1, Some("ORD-002"), TestOrder {
        order_id: "ORD-002".to_string(),
        amount: 149.99,
        customer: "Bob".to_string(),
    }));

    let mut consumer = kafka.consumer("order-processor").await.unwrap();
    consumer.subscribe(&["orders"]).await.unwrap();

    // Receive first message
    let msg1 = consumer.next().await.unwrap().unwrap();
    assert_eq!(msg1.topic(), "orders");
    assert_eq!(msg1.offset(), 0);
    assert_eq!(msg1.key(), Some("ORD-001"));

    let order1: TestOrder = msg1.value().unwrap();
    assert_eq!(order1.order_id, "ORD-001");

    // Receive second message
    let msg2 = consumer.next().await.unwrap().unwrap();
    assert_eq!(msg2.offset(), 1);
    let order2: TestOrder = msg2.value().unwrap();
    assert_eq!(order2.order_id, "ORD-002");
}

#[tokio::test]
async fn test_consumer_commit() {
    let (kafka, mock_client) = create_mock_kafka();

    mock_client.store_message(create_mock_message("events", 0, None, "event-1"));
    mock_client.store_message(create_mock_message("events", 1, None, "event-2"));

    let mut consumer = kafka.consumer("event-group").await.unwrap();
    consumer.subscribe(&["events"]).await.unwrap();

    let msg1 = consumer.next().await.unwrap().unwrap();
    msg1.commit().await.unwrap();

    // Verify offset was committed
    let committed = mock_client.get_offset("event-group", "events");
    assert_eq!(committed, 1); // offset + 1

    let msg2 = consumer.next().await.unwrap().unwrap();
    msg2.commit().await.unwrap();

    let committed = mock_client.get_offset("event-group", "events");
    assert_eq!(committed, 2);
}

#[tokio::test]
async fn test_consumer_message_headers() {
    let (kafka, mock_client) = create_mock_kafka();

    let mut headers = HashMap::new();
    headers.insert("correlation-id".to_string(), "corr-123".to_string());
    headers.insert("source".to_string(), "test-service".to_string());

    mock_client.store_message(MockMessage {
        topic: "events".to_string(),
        partition: 0,
        offset: 0,
        key: Some("evt-1".to_string()),
        value: serde_json::to_vec(&"test-value").unwrap(),
        headers: headers.clone(),
        timestamp: Utc::now().timestamp_millis(),
    });

    let mut consumer = kafka.consumer("test-group").await.unwrap();
    consumer.subscribe(&["events"]).await.unwrap();

    let msg = consumer.next().await.unwrap().unwrap();
    assert_eq!(msg.topic(), "events");
}

#[tokio::test]
async fn test_consumer_close() {
    let (kafka, _mock) = create_mock_kafka();
    let mut consumer = kafka.consumer("test-group").await.unwrap();

    consumer.subscribe(&["topic"]).await.unwrap();
    assert!(consumer.is_active());

    consumer.close().await.unwrap();
    assert!(!consumer.is_active());
    assert!(consumer.subscriptions().await.is_empty());

    // Next should return None after close
    let result = consumer.next().await;
    assert!(result.is_none());
}

#[tokio::test]
async fn test_consumer_empty_topic() {
    let (kafka, _mock) = create_mock_kafka();
    let mut consumer = kafka.consumer("test-group").await.unwrap();

    consumer.subscribe(&["empty-topic"]).await.unwrap();

    let result = consumer.next().await;
    assert!(result.is_none());
}

#[tokio::test]
async fn test_consumer_no_subscription() {
    let (kafka, _mock) = create_mock_kafka();
    let mut consumer = kafka.consumer("test-group").await.unwrap();

    // Don't subscribe
    let result = consumer.next().await;
    assert!(result.is_none());
}

#[tokio::test]
async fn test_consumer_config() {
    let (kafka, _mock) = create_mock_kafka();

    let config = ConsumerConfig::new()
        .offset(Offset::Earliest)
        .auto_commit(true)
        .max_poll_records(100)
        .session_timeout(Duration::from_secs(60))
        .heartbeat_interval(Duration::from_secs(5));

    let consumer = kafka.consumer_with_config("test-group", config).await.unwrap();
    assert!(consumer.is_active());
}

#[tokio::test]
async fn test_consumer_group_id() {
    let (kafka, _mock) = create_mock_kafka();
    let consumer = kafka.consumer("my-unique-group").await.unwrap();

    assert_eq!(consumer.group_id().await, "my-unique-group");
}

#[tokio::test]
async fn test_consumer_deserialize_struct() {
    let (kafka, mock_client) = create_mock_kafka();

    let event = TestEvent {
        event_type: "user.created".to_string(),
        payload: "user-123".to_string(),
    };

    mock_client.store_message(create_mock_message("events", 0, Some("evt-1"), &event));

    let mut consumer = kafka.consumer("test-group").await.unwrap();
    consumer.subscribe(&["events"]).await.unwrap();

    let msg = consumer.next().await.unwrap().unwrap();
    let received: TestEvent = msg.value().unwrap();

    assert_eq!(received, event);
}

#[tokio::test]
async fn test_consumer_raw_value() {
    let (kafka, mock_client) = create_mock_kafka();

    mock_client.store_message(create_mock_message("raw-topic", 0, None, "raw-data"));

    let mut consumer = kafka.consumer("test-group").await.unwrap();
    consumer.subscribe(&["raw-topic"]).await.unwrap();

    let msg = consumer.next().await.unwrap().unwrap();
    let raw = msg.raw_value();

    assert!(!raw.is_empty());
}

#[tokio::test]
async fn test_consumer_timestamp() {
    let (kafka, mock_client) = create_mock_kafka();

    let timestamp = Utc::now().timestamp_millis();

    mock_client.store_message(MockMessage {
        topic: "time-topic".to_string(),
        partition: 0,
        offset: 0,
        key: None,
        value: serde_json::to_vec(&"value").unwrap(),
        headers: HashMap::new(),
        timestamp,
    });

    let mut consumer = kafka.consumer("test-group").await.unwrap();
    consumer.subscribe(&["time-topic"]).await.unwrap();

    let msg = consumer.next().await.unwrap().unwrap();
    let msg_ts = msg.timestamp();

    // Timestamp should be close to what we set
    assert!((msg_ts.timestamp_millis() - timestamp).abs() < 1000);
}

#[tokio::test]
async fn test_consumer_multiple_topics() {
    let (kafka, mock_client) = create_mock_kafka();

    mock_client.store_message(create_mock_message("topic-a", 0, None, "value-a"));
    mock_client.store_message(create_mock_message("topic-b", 0, None, "value-b"));

    let mut consumer = kafka.consumer("multi-topic-group").await.unwrap();
    consumer.subscribe(&["topic-a", "topic-b"]).await.unwrap();

    let msg1 = consumer.next().await.unwrap().unwrap();
    let msg2 = consumer.next().await.unwrap().unwrap();

    let topics: Vec<&str> = vec![msg1.topic(), msg2.topic()];
    assert!(topics.contains(&"topic-a"));
    assert!(topics.contains(&"topic-b"));
}

#[tokio::test]
async fn test_consumer_error_propagation() {
    let (kafka, mock_client) = create_mock_kafka();

    mock_client.store_message(create_mock_message("topic", 0, None, "value"));

    let mut consumer = kafka.consumer("test-group").await.unwrap();
    consumer.subscribe(&["topic"]).await.unwrap();

    // Set error before polling
    mock_client.set_error(kafka_do::ErrorKind::Disconnected);

    let result = consumer.next().await;
    assert!(result.is_some());
    assert!(result.unwrap().is_err());
}

#[tokio::test]
async fn test_consumer_config_offset_earliest() {
    let config = ConsumerConfig::new().offset(Offset::Earliest);
    assert_eq!(config.offset, Offset::Earliest);
}

#[tokio::test]
async fn test_consumer_config_offset_latest() {
    let config = ConsumerConfig::new().offset(Offset::Latest);
    assert_eq!(config.offset, Offset::Latest);
}

#[tokio::test]
async fn test_consumer_config_offset_specific() {
    let config = ConsumerConfig::new().offset(Offset::Offset(100));
    assert_eq!(config.offset, Offset::Offset(100));
}

#[tokio::test]
async fn test_consumer_config_offset_timestamp() {
    let ts = Utc::now();
    let config = ConsumerConfig::new().offset(Offset::Timestamp(ts));
    assert!(matches!(config.offset, Offset::Timestamp(_)));
}

#[tokio::test]
async fn test_consumer_resubscribe() {
    let (kafka, _mock) = create_mock_kafka();
    let mut consumer = kafka.consumer("test-group").await.unwrap();

    // Initial subscription
    consumer.subscribe(&["topic-a"]).await.unwrap();
    assert_eq!(consumer.subscriptions().await, vec!["topic-a".to_string()]);

    // Resubscribe to different topics
    consumer.subscribe(&["topic-b", "topic-c"]).await.unwrap();
    let subs = consumer.subscriptions().await;
    assert_eq!(subs.len(), 2);
    assert!(subs.contains(&"topic-b".to_string()));
    assert!(subs.contains(&"topic-c".to_string()));
    assert!(!subs.contains(&"topic-a".to_string()));
}

#[tokio::test]
async fn test_consumer_message_partition() {
    let (kafka, mock_client) = create_mock_kafka();

    mock_client.store_message(MockMessage {
        topic: "partitioned".to_string(),
        partition: 5,
        offset: 100,
        key: Some("key".to_string()),
        value: serde_json::to_vec(&"value").unwrap(),
        headers: HashMap::new(),
        timestamp: Utc::now().timestamp_millis(),
    });

    let mut consumer = kafka.consumer("test-group").await.unwrap();
    consumer.subscribe(&["partitioned"]).await.unwrap();

    let msg = consumer.next().await.unwrap().unwrap();
    assert_eq!(msg.partition(), 5);
    assert_eq!(msg.offset(), 100);
}

#[tokio::test]
async fn test_consumer_config_all_options() {
    let config = ConsumerConfig::new()
        .offset(Offset::Earliest)
        .auto_commit(true)
        .max_poll_records(1000)
        .session_timeout(Duration::from_secs(45))
        .heartbeat_interval(Duration::from_secs(10))
        .fetch_min_bytes(1024)
        .fetch_max_wait(Duration::from_millis(250));

    assert_eq!(config.offset, Offset::Earliest);
    assert!(config.auto_commit);
    assert_eq!(config.max_poll_records, 1000);
    assert_eq!(config.session_timeout, Duration::from_secs(45));
    assert_eq!(config.heartbeat_interval, Duration::from_secs(10));
    assert_eq!(config.fetch_min_bytes, 1024);
    assert_eq!(config.fetch_max_wait, Duration::from_millis(250));
}

#[tokio::test]
async fn test_consumer_recovers_after_error() {
    let (kafka, mock_client) = create_mock_kafka();

    mock_client.store_message(create_mock_message("topic", 0, None, "value1"));
    mock_client.store_message(create_mock_message("topic", 1, None, "value2"));

    let mut consumer = kafka.consumer("test-group").await.unwrap();
    consumer.subscribe(&["topic"]).await.unwrap();

    // First poll with error
    mock_client.set_error(kafka_do::ErrorKind::Timeout);
    let result1 = consumer.next().await;
    assert!(result1.unwrap().is_err());

    // Second poll should succeed (error was one-shot)
    let result2 = consumer.next().await;
    assert!(result2.is_some());
    assert!(result2.unwrap().is_ok());
}

#[tokio::test]
async fn test_message_all_accessors() {
    let (kafka, mock_client) = create_mock_kafka();

    let timestamp = Utc::now().timestamp_millis();
    let mut headers = HashMap::new();
    headers.insert("h1".to_string(), "v1".to_string());

    mock_client.store_message(MockMessage {
        topic: "full-msg".to_string(),
        partition: 3,
        offset: 42,
        key: Some("my-key".to_string()),
        value: serde_json::to_vec(&serde_json::json!({"field": "value"})).unwrap(),
        headers,
        timestamp,
    });

    let mut consumer = kafka.consumer("test-group").await.unwrap();
    consumer.subscribe(&["full-msg"]).await.unwrap();

    let msg = consumer.next().await.unwrap().unwrap();

    // Test all accessors
    assert_eq!(msg.topic(), "full-msg");
    assert_eq!(msg.partition(), 3);
    assert_eq!(msg.offset(), 42);
    assert_eq!(msg.key(), Some("my-key"));
    assert!(!msg.raw_value().is_empty());
    let _ = msg.timestamp();
    let _ = msg.headers();
}

#[tokio::test]
async fn test_consumer_inactive_after_multiple_closes() {
    let (kafka, _mock) = create_mock_kafka();
    let mut consumer = kafka.consumer("test-group").await.unwrap();

    consumer.subscribe(&["topic"]).await.unwrap();

    // Close multiple times should be safe
    consumer.close().await.unwrap();
    consumer.close().await.unwrap();

    assert!(!consumer.is_active());
}
