//! Integration tests for the Kafka producer.

use kafka_do::prelude::*;
use kafka_do::kafka::mock::{MockRpcClient, MockMessage};
use kafka_do::kafka::MockRpcTransport;
use std::collections::HashMap;
use std::sync::Arc;

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

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct TestEvent {
    event_type: String,
    timestamp: i64,
    data: HashMap<String, String>,
}

#[tokio::test]
async fn test_producer_send_simple_string() {
    let (kafka, mock_client) = create_mock_kafka();
    let producer = kafka.producer().await.unwrap();

    let result = producer.send("events", "key-1", "Hello, Kafka!").await;

    assert!(result.is_ok());
    let metadata = result.unwrap();
    assert_eq!(metadata.topic, "events");
    assert_eq!(metadata.partition, 0);

    let messages = mock_client.get_messages("events");
    assert_eq!(messages.len(), 1);
}

#[tokio::test]
async fn test_producer_send_struct() {
    let (kafka, mock_client) = create_mock_kafka();
    let producer = kafka.producer().await.unwrap();

    let order = TestOrder {
        order_id: "ORD-001".to_string(),
        amount: 149.99,
        customer: "John Doe".to_string(),
    };

    let result = producer.send("orders", Some("ORD-001"), &order).await;

    assert!(result.is_ok());

    let messages = mock_client.get_messages("orders");
    assert_eq!(messages.len(), 1);

    let stored: TestOrder = serde_json::from_slice(&messages[0].value).unwrap();
    assert_eq!(stored, order);
}

#[tokio::test]
async fn test_producer_send_without_key() {
    let (kafka, mock_client) = create_mock_kafka();
    let producer = kafka.producer().await.unwrap();

    let result = producer.send("logs", None, "Log message").await;

    assert!(result.is_ok());

    let messages = mock_client.get_messages("logs");
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].key, None);
}

#[tokio::test]
async fn test_producer_send_batch() {
    let (kafka, mock_client) = create_mock_kafka();
    let producer = kafka.producer().await.unwrap();

    let orders = vec![
        (Some("ORD-001".to_string()), TestOrder {
            order_id: "ORD-001".to_string(),
            amount: 99.99,
            customer: "Alice".to_string(),
        }),
        (Some("ORD-002".to_string()), TestOrder {
            order_id: "ORD-002".to_string(),
            amount: 149.99,
            customer: "Bob".to_string(),
        }),
        (Some("ORD-003".to_string()), TestOrder {
            order_id: "ORD-003".to_string(),
            amount: 199.99,
            customer: "Charlie".to_string(),
        }),
    ];

    let result = producer.send_batch("orders", orders).await;

    assert!(result.is_ok());
    let metadata_list = result.unwrap();
    assert_eq!(metadata_list.len(), 3);

    // Verify offsets are sequential
    assert_eq!(metadata_list[0].offset, 0);
    assert_eq!(metadata_list[1].offset, 1);
    assert_eq!(metadata_list[2].offset, 2);

    let messages = mock_client.get_messages("orders");
    assert_eq!(messages.len(), 3);
}

#[tokio::test]
async fn test_producer_send_with_options() {
    let (kafka, mock_client) = create_mock_kafka();
    let producer = kafka.producer().await.unwrap();

    let event = TestEvent {
        event_type: "user.created".to_string(),
        timestamp: 1234567890,
        data: HashMap::from([
            ("user_id".to_string(), "U-123".to_string()),
            ("email".to_string(), "user@example.com".to_string()),
        ]),
    };

    let options = SendOptions::new()
        .key("user-123")
        .header("correlation-id", "corr-abc-123")
        .header("source", "user-service");

    let result = producer.send_with_options("user-events", &event, options).await;

    assert!(result.is_ok());

    let messages = mock_client.get_messages("user-events");
    assert_eq!(messages.len(), 1);
}

#[tokio::test]
async fn test_producer_multiple_topics() {
    let (kafka, mock_client) = create_mock_kafka();
    let producer = kafka.producer().await.unwrap();

    producer.send("topic-a", "key-1", "value-1").await.unwrap();
    producer.send("topic-b", "key-2", "value-2").await.unwrap();
    producer.send("topic-a", "key-3", "value-3").await.unwrap();
    producer.send("topic-c", "key-4", "value-4").await.unwrap();

    assert_eq!(mock_client.get_messages("topic-a").len(), 2);
    assert_eq!(mock_client.get_messages("topic-b").len(), 1);
    assert_eq!(mock_client.get_messages("topic-c").len(), 1);
}

#[tokio::test]
async fn test_producer_error_handling() {
    let (kafka, mock_client) = create_mock_kafka();
    let producer = kafka.producer().await.unwrap();

    // Set error
    mock_client.set_error(kafka_do::ErrorKind::TopicNotFound);

    let result = producer.send("nonexistent", "key", "value").await;

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert_eq!(err.kind(), &kafka_do::ErrorKind::TopicNotFound);
}

#[tokio::test]
async fn test_producer_timeout_error() {
    let (kafka, mock_client) = create_mock_kafka();
    let producer = kafka.producer().await.unwrap();

    mock_client.set_error(kafka_do::ErrorKind::Timeout);

    let result = producer.send("topic", "key", "value").await;

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.is_retriable());
}

#[tokio::test]
async fn test_producer_message_too_large() {
    let (kafka, mock_client) = create_mock_kafka();
    let producer = kafka.producer().await.unwrap();

    mock_client.set_error(kafka_do::ErrorKind::MessageTooLarge);

    let result = producer.send("topic", "key", "large message").await;

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(!err.is_retriable());
}

#[tokio::test]
async fn test_producer_json_values() {
    let (kafka, mock_client) = create_mock_kafka();
    let producer = kafka.producer().await.unwrap();

    let json_value = serde_json::json!({
        "type": "notification",
        "priority": 1,
        "tags": ["urgent", "system"]
    });

    let result = producer.send("notifications", "notif-1", &json_value).await;

    assert!(result.is_ok());

    let messages = mock_client.get_messages("notifications");
    assert_eq!(messages.len(), 1);

    let stored: serde_json::Value = serde_json::from_slice(&messages[0].value).unwrap();
    assert_eq!(stored["type"], "notification");
    assert_eq!(stored["priority"], 1);
}

#[tokio::test]
async fn test_producer_empty_batch() {
    let (kafka, _mock_client) = create_mock_kafka();
    let producer = kafka.producer().await.unwrap();

    let empty: Vec<(Option<String>, String)> = vec![];
    let result = producer.send_batch("topic", empty).await;

    assert!(result.is_ok());
    let metadata = result.unwrap();
    assert!(metadata.is_empty());
}

#[tokio::test]
async fn test_send_options_builder_chain() {
    let options = SendOptions::new()
        .key("my-key")
        .header("h1", "v1")
        .header("h2", "v2")
        .header("h3", "v3")
        .partition(2);

    assert_eq!(options.key, Some("my-key".to_string()));
    assert_eq!(options.headers.len(), 3);
    assert_eq!(options.partition, Some(2));
}

#[tokio::test]
async fn test_record_metadata_fields() {
    let (kafka, _mock_client) = create_mock_kafka();
    let producer = kafka.producer().await.unwrap();

    let metadata = producer.send("test", "key", "value").await.unwrap();

    // Verify all fields are accessible
    let _ = metadata.topic;
    let _ = metadata.partition;
    let _ = metadata.offset;
    let _ = metadata.timestamp;
}

#[tokio::test]
async fn test_producer_concurrent_sends() {
    let (kafka, mock_client) = create_mock_kafka();
    let producer = Arc::new(kafka.producer().await.unwrap());

    let mut handles = vec![];

    for i in 0..10 {
        let producer = producer.clone();
        let handle = tokio::spawn(async move {
            producer.send("concurrent", format!("key-{}", i).as_str(), format!("value-{}", i)).await
        });
        handles.push(handle);
    }

    for handle in handles {
        let result = handle.await.unwrap();
        assert!(result.is_ok());
    }

    let messages = mock_client.get_messages("concurrent");
    assert_eq!(messages.len(), 10);
}

#[tokio::test]
async fn test_producer_recovers_after_error() {
    let (kafka, mock_client) = create_mock_kafka();
    let producer = kafka.producer().await.unwrap();

    // First send with error
    mock_client.set_error(kafka_do::ErrorKind::Timeout);
    let result1 = producer.send("topic", "key1", "value1").await;
    assert!(result1.is_err());

    // Second send should succeed (error was cleared)
    let result2 = producer.send("topic", "key2", "value2").await;
    assert!(result2.is_ok());
}

#[tokio::test]
async fn test_producer_preserves_key_order() {
    let (kafka, mock_client) = create_mock_kafka();
    let producer = kafka.producer().await.unwrap();

    for i in 0..5 {
        producer.send("ordered", format!("key-{}", i).as_str(), format!("value-{}", i)).await.unwrap();
    }

    let messages = mock_client.get_messages("ordered");

    for (i, msg) in messages.iter().enumerate() {
        assert_eq!(msg.key, Some(format!("key-{}", i)));
        assert_eq!(msg.offset, i as i64);
    }
}
