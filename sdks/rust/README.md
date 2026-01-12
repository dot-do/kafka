# kafka-do

> Event Streaming for Rust. Zero-Copy. Async. Zero Ops.

```rust
let producer = kafka.producer("orders");
producer.send(Order { id: "123".into(), amount: 99.99 }).await?;
```

Type-safe. Memory-safe. The Rust way.

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
kafka-do = "0.1"
tokio = { version = "1", features = ["full"] }
```

## Quick Start

```rust
use kafka_do::{Kafka, Message};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct Order {
    order_id: String,
    amount: f64,
}

#[tokio::main]
async fn main() -> Result<(), kafka_do::Error> {
    let kafka = Kafka::new().await?;

    // Produce
    let producer = kafka.producer("orders");
    producer.send(Order {
        order_id: "123".into(),
        amount: 99.99,
    }).await?;

    // Consume
    let mut consumer = kafka.consumer("orders", "my-processor").await?;
    while let Some(msg) = consumer.next().await {
        let msg = msg?;
        let order: Order = msg.value()?;
        println!("Received: {:?}", order);
        msg.commit().await?;
    }

    Ok(())
}
```

## Producing Messages

### Simple Producer

```rust
use kafka_do::{Kafka, Message};

let kafka = Kafka::new().await?;
let producer = kafka.producer("orders");

// Send with automatic serialization
producer.send(Order {
    order_id: "123".into(),
    amount: 99.99,
}).await?;

// Send with key for partitioning
producer.send_with_key(
    "customer-456",
    Order { order_id: "123".into(), amount: 99.99 },
).await?;

// Send with headers
producer.send_with_options(
    Order { order_id: "123".into(), amount: 99.99 },
    SendOptions::default()
        .key("customer-456")
        .header("correlation-id", "abc-123"),
).await?;
```

### Batch Producer

```rust
let producer = kafka.producer("orders");

// Send batch for high throughput
producer.send_batch(vec![
    Order { order_id: "124".into(), amount: 49.99 },
    Order { order_id: "125".into(), amount: 149.99 },
    Order { order_id: "126".into(), amount: 29.99 },
]).await?;
```

### Buffered Producer

```rust
use kafka_do::{Kafka, BufferedProducer};

let producer = kafka.buffered_producer("orders", BufferedConfig {
    batch_size: 100,
    linger: Duration::from_millis(5),
});

// Non-blocking send
producer.send(order1)?;
producer.send(order2)?;
producer.send(order3)?;

// Flush all buffered messages
producer.flush().await?;
```

### Transactional Producer

```rust
let tx = kafka.transaction("orders").await?;

tx.send(Order { order_id: "123".into(), amount: 99.99 }).await?;
tx.send(Order { order_id: "124".into(), amount: 49.99 }).await?;

// Commit transaction
tx.commit().await?;

// Or abort
// tx.abort().await?;
```

## Consuming Messages

### Basic Consumer

```rust
use kafka_do::{Kafka, ConsumerConfig};
use futures::StreamExt;

let kafka = Kafka::new().await?;
let mut consumer = kafka.consumer("orders", "order-processor").await?;

while let Some(result) = consumer.next().await {
    let msg = result?;

    println!("Topic: {}", msg.topic());
    println!("Partition: {}", msg.partition());
    println!("Offset: {}", msg.offset());
    println!("Key: {:?}", msg.key());
    println!("Timestamp: {:?}", msg.timestamp());
    println!("Headers: {:?}", msg.headers());

    // Deserialize value
    let order: Order = msg.value()?;
    println!("Order: {:?}", order);

    // Process
    process_order(&order).await?;

    // Commit
    msg.commit().await?;
}
```

### Typed Consumer

```rust
// Consumer with explicit type
let mut consumer = kafka.typed_consumer::<Order>("orders", "processor").await?;

while let Some(result) = consumer.next().await {
    let msg = result?;
    let order: &Order = msg.value(); // Already deserialized
    println!("Order {}: ${}", order.order_id, order.amount);
    msg.commit().await?;
}
```

### Consumer with Options

```rust
use kafka_do::{Kafka, ConsumerConfig, Offset};

let config = ConsumerConfig::default()
    .offset(Offset::Earliest)
    .auto_commit(true)
    .max_poll_records(100)
    .session_timeout(Duration::from_secs(30));

let mut consumer = kafka.consumer_with_config("orders", "processor", config).await?;

while let Some(result) = consumer.next().await {
    let msg = result?;
    process(&msg.value::<Order>()?).await?;
    // Auto-committed
}
```

### Consumer from Timestamp

```rust
use chrono::{Utc, Duration};

let yesterday = Utc::now() - Duration::days(1);

let config = ConsumerConfig::default()
    .offset(Offset::Timestamp(yesterday));

let mut consumer = kafka.consumer_with_config("orders", "replay", config).await?;

while let Some(result) = consumer.next().await {
    println!("Replaying: {:?}", result?.value::<Order>()?);
}
```

### Batch Consumer

```rust
use kafka_do::{Kafka, BatchConfig};

let config = BatchConfig {
    size: 100,
    timeout: Duration::from_secs(5),
};

let mut consumer = kafka.batch_consumer("orders", "batch-processor", config).await?;

while let Some(batch) = consumer.next_batch().await? {
    // Process batch
    for msg in &batch {
        process(&msg.value::<Order>()?).await?;
    }

    // Commit all at once
    batch.commit().await?;
}
```

### Parallel Consumer

```rust
use futures::stream::StreamExt;
use tokio::sync::mpsc;

let mut consumer = kafka.consumer("orders", "parallel-processor").await?;
let (tx, mut rx) = mpsc::channel(100);

// Spawn workers
for _ in 0..10 {
    let mut rx = rx.clone();
    tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            process(&msg.value::<Order>()?).await?;
            msg.commit().await?;
        }
        Ok::<_, Error>(())
    });
}

// Feed messages to workers
while let Some(result) = consumer.next().await {
    tx.send(result?).await?;
}
```

## Stream Processing

### Filter and Transform

```rust
use kafka_do::{Kafka, Stream};

let kafka = Kafka::new().await?;

Stream::new(&kafka, "orders")
    .filter(|msg| {
        let order: Order = msg.value().ok()?;
        Some(order.amount > 100.0)
    })
    .map(|msg| {
        let mut order: Order = msg.value()?;
        order.tier = Some("premium".into());
        Ok(order)
    })
    .to("high-value-orders")
    .await?;
```

### Typed Stream Processing

```rust
use kafka_do::{Kafka, TypedStream};

#[derive(Debug, Serialize, Deserialize)]
struct PremiumOrder {
    #[serde(flatten)]
    order: Order,
    tier: String,
}

TypedStream::<Order>::new(&kafka, "orders")
    .filter(|order| order.amount > 100.0)
    .map(|order| PremiumOrder {
        order,
        tier: "premium".into(),
    })
    .to("high-value-orders")
    .await?;
```

### Windowed Aggregations

```rust
use kafka_do::{Kafka, Stream, Window};

Stream::new(&kafka, "orders")
    .window(Window::tumbling(Duration::from_secs(300))) // 5 minutes
    .group_by(|msg| {
        let order: Order = msg.value()?;
        Ok(order.customer_id)
    })
    .count()
    .for_each(|window| async move {
        println!(
            "Customer {}: {} orders in {:?}-{:?}",
            window.key, window.value, window.start, window.end
        );
        Ok(())
    })
    .await?;
```

### Joins

```rust
use kafka_do::{Kafka, Stream, JoinType};

let orders = Stream::new(&kafka, "orders");
let customers = Stream::new(&kafka, "customers");

orders
    .join(
        customers,
        JoinType::Inner,
        |order_msg, customer_msg| {
            let order: Order = order_msg.value()?;
            let customer: Customer = customer_msg.value()?;
            Ok(order.customer_id == customer.id)
        },
        Duration::from_secs(3600), // 1 hour window
    )
    .for_each(|(order_msg, customer_msg)| async move {
        let order: Order = order_msg.value()?;
        let customer: Customer = customer_msg.value()?;
        println!("Order {} by {}", order.order_id, customer.name);
        Ok(())
    })
    .await?;
```

### Branching

```rust
use kafka_do::{Kafka, Stream, Branch};

Stream::new(&kafka, "orders")
    .branch(vec![
        Branch::new(
            |msg| {
                let order: Order = msg.value()?;
                Ok(order.region == "us")
            },
            "us-orders",
        ),
        Branch::new(
            |msg| {
                let order: Order = msg.value()?;
                Ok(order.region == "eu")
            },
            "eu-orders",
        ),
        Branch::default("other-orders"),
    ])
    .await?;
```

## Topic Administration

```rust
use kafka_do::{Kafka, TopicConfig};

let kafka = Kafka::new().await?;
let admin = kafka.admin();

// Create topic
admin.create_topic("orders", TopicConfig {
    partitions: 3,
    retention_ms: Some(7 * 24 * 60 * 60 * 1000), // 7 days
    ..Default::default()
}).await?;

// List topics
let topics = admin.list_topics().await?;
for topic in topics {
    println!("{}: {} partitions", topic.name, topic.partitions);
}

// Describe topic
let info = admin.describe_topic("orders").await?;
println!("Partitions: {}", info.partitions);
println!("Retention: {:?}ms", info.retention_ms);

// Alter topic
admin.alter_topic("orders", TopicConfig {
    retention_ms: Some(30 * 24 * 60 * 60 * 1000), // 30 days
    ..Default::default()
}).await?;

// Delete topic
admin.delete_topic("old-events").await?;
```

## Consumer Groups

```rust
let admin = kafka.admin();

// List groups
let groups = admin.list_groups().await?;
for group in groups {
    println!("Group: {}, Members: {}", group.id, group.member_count);
}

// Describe group
let info = admin.describe_group("order-processor").await?;
println!("State: {:?}", info.state);
println!("Members: {:?}", info.members);
println!("Total Lag: {}", info.total_lag);

// Reset offsets
admin.reset_offsets("order-processor", "orders", Offset::Earliest).await?;

// Reset to timestamp
admin.reset_offsets_to_time("order-processor", "orders", yesterday).await?;
```

## Error Handling

```rust
use kafka_do::{Error, ErrorKind};

let producer = kafka.producer("orders");

match producer.send(order).await {
    Ok(metadata) => {
        println!("Sent to partition {} offset {}",
            metadata.partition, metadata.offset);
    }
    Err(e) => {
        match e.kind() {
            ErrorKind::TopicNotFound => {
                eprintln!("Topic not found");
            }
            ErrorKind::MessageTooLarge => {
                eprintln!("Message too large");
            }
            ErrorKind::Timeout => {
                eprintln!("Request timed out");
            }
            ErrorKind::Serialization(se) => {
                eprintln!("Serialization failed: {}", se);
            }
            _ => {
                eprintln!("Kafka error: {}", e);
            }
        }

        if e.is_retriable() {
            // Safe to retry
        }
    }
}
```

### Error Types

```rust
#[derive(Debug)]
pub enum ErrorKind {
    TopicNotFound,
    PartitionNotFound,
    MessageTooLarge,
    NotLeader,
    OffsetOutOfRange,
    GroupCoordinator,
    RebalanceInProgress,
    Unauthorized,
    QuotaExceeded,
    Timeout,
    Disconnected,
    Serialization(serde_json::Error),
    Io(std::io::Error),
    Other(String),
}

impl Error {
    pub fn kind(&self) -> &ErrorKind;
    pub fn is_retriable(&self) -> bool;
    pub fn message(&self) -> &str;
}
```

### Dead Letter Queue

```rust
let mut consumer = kafka.consumer("orders", "processor").await?;
let dlq_producer = kafka.producer("orders-dlq");

while let Some(result) = consumer.next().await {
    let msg = result?;

    if let Err(e) = process_order(&msg.value::<Order>()?).await {
        // Send to DLQ
        dlq_producer.send_with_options(
            DlqRecord {
                original: msg.raw_value().to_vec(),
                error: e.to_string(),
                timestamp: Utc::now(),
            },
            SendOptions::default()
                .header("original-topic", msg.topic())
                .header("original-partition", &msg.partition().to_string())
                .header("original-offset", &msg.offset().to_string()),
        ).await?;
    }

    msg.commit().await?;
}
```

## Configuration

### Environment Variables

```bash
export KAFKA_DO_URL="https://kafka.do"
export KAFKA_DO_API_KEY="your-api-key"
```

### Client Configuration

```rust
use kafka_do::{Kafka, Config};

let config = Config::new()
    .url("https://kafka.do")
    .api_key("your-api-key")
    .timeout(Duration::from_secs(30))
    .retries(3);

let kafka = Kafka::with_config(config).await?;
```

### Producer Configuration

```rust
use kafka_do::{ProducerConfig, Compression, Acks};

let config = ProducerConfig::default()
    .batch_size(16384)
    .linger(Duration::from_millis(5))
    .compression(Compression::Gzip)
    .acks(Acks::All)
    .retries(3);

let producer = kafka.producer_with_config("orders", config);
```

### Consumer Configuration

```rust
use kafka_do::{ConsumerConfig, Offset};

let config = ConsumerConfig::default()
    .offset(Offset::Latest)
    .auto_commit(false)
    .fetch_min_bytes(1)
    .fetch_max_wait(Duration::from_millis(500))
    .max_poll_records(500)
    .session_timeout(Duration::from_secs(30))
    .heartbeat_interval(Duration::from_secs(3));

let consumer = kafka.consumer_with_config("orders", "processor", config).await?;
```

## Testing

### Mock Client

```rust
use kafka_do::mock::MockKafka;

#[tokio::test]
async fn test_order_processing() {
    let kafka = MockKafka::new();

    // Seed test data
    kafka.seed("orders", vec![
        Order { order_id: "123".into(), amount: 99.99 },
        Order { order_id: "124".into(), amount: 149.99 },
    ]);

    // Process
    let mut consumer = kafka.consumer("orders", "test").await.unwrap();
    let mut count = 0;

    while let Some(result) = consumer.next().await {
        let msg = result.unwrap();
        msg.commit().await.unwrap();
        count += 1;
        if count == 2 {
            break;
        }
    }

    assert_eq!(count, 2);
}

#[tokio::test]
async fn test_producer() {
    let kafka = MockKafka::new();
    let producer = kafka.producer("orders");

    producer.send(Order { order_id: "125".into(), amount: 99.99 })
        .await
        .unwrap();

    let messages = kafka.get_messages::<Order>("orders");
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].order_id, "125");
}
```

### Integration Testing

```rust
#[tokio::test]
#[ignore] // Run with: cargo test -- --ignored
async fn test_integration() {
    let kafka = Kafka::new().await.unwrap();

    let topic = format!("test-orders-{}", uuid::Uuid::new_v4());

    // Cleanup on drop
    let _cleanup = scopeguard::guard((), |_| {
        // Delete topic
    });

    // Produce
    let producer = kafka.producer(&topic);
    producer.send(Order { order_id: "123".into(), amount: 99.99 })
        .await
        .unwrap();

    // Consume
    let mut consumer = kafka.consumer(&topic, "test").await.unwrap();
    if let Some(result) = consumer.next().await {
        let msg = result.unwrap();
        let order: Order = msg.value().unwrap();
        assert_eq!(order.order_id, "123");
    }
}
```

## API Reference

### Kafka

```rust
impl Kafka {
    pub async fn new() -> Result<Self, Error>;
    pub async fn with_config(config: Config) -> Result<Self, Error>;

    pub fn producer<T: Serialize>(&self, topic: &str) -> Producer<T>;
    pub fn producer_with_config<T: Serialize>(
        &self, topic: &str, config: ProducerConfig
    ) -> Producer<T>;

    pub async fn consumer(&self, topic: &str, group: &str)
        -> Result<Consumer, Error>;
    pub async fn consumer_with_config(
        &self, topic: &str, group: &str, config: ConsumerConfig
    ) -> Result<Consumer, Error>;
    pub async fn typed_consumer<T: DeserializeOwned>(
        &self, topic: &str, group: &str
    ) -> Result<TypedConsumer<T>, Error>;

    pub async fn transaction(&self, topic: &str) -> Result<Transaction, Error>;
    pub fn admin(&self) -> Admin;
}
```

### Producer

```rust
impl<T: Serialize> Producer<T> {
    pub async fn send(&self, value: T) -> Result<RecordMetadata, Error>;
    pub async fn send_with_key(&self, key: &str, value: T)
        -> Result<RecordMetadata, Error>;
    pub async fn send_with_options(&self, value: T, opts: SendOptions)
        -> Result<RecordMetadata, Error>;
    pub async fn send_batch(&self, values: Vec<T>)
        -> Result<Vec<RecordMetadata>, Error>;
}
```

### Consumer

```rust
impl Consumer {
    pub async fn next(&mut self) -> Option<Result<Message, Error>>;
    pub fn value<T: DeserializeOwned>(&self) -> Result<T, Error>;
    pub async fn commit(&self) -> Result<(), Error>;
}

impl<T: DeserializeOwned> TypedConsumer<T> {
    pub async fn next(&mut self) -> Option<Result<TypedMessage<T>, Error>>;
}
```

### Message

```rust
pub struct Message {
    pub fn topic(&self) -> &str;
    pub fn partition(&self) -> i32;
    pub fn offset(&self) -> i64;
    pub fn key(&self) -> Option<&str>;
    pub fn value<T: DeserializeOwned>(&self) -> Result<T, Error>;
    pub fn raw_value(&self) -> &[u8];
    pub fn timestamp(&self) -> DateTime<Utc>;
    pub fn headers(&self) -> &HashMap<String, String>;
    pub async fn commit(&self) -> Result<(), Error>;
}
```

### Stream

```rust
impl<T> Stream<T> {
    pub fn new(kafka: &Kafka, topic: &str) -> Self;
    pub fn filter<F>(self, f: F) -> Self where F: Fn(&Message) -> Option<bool>;
    pub fn map<F, R>(self, f: F) -> Stream<R> where F: Fn(&Message) -> Result<R, Error>;
    pub fn flat_map<F, R>(self, f: F) -> Stream<R> where F: Fn(&Message) -> Result<Vec<R>, Error>;
    pub fn window(self, w: Window) -> WindowedStream<T>;
    pub fn group_by<F, K>(self, f: F) -> GroupedStream<K, T> where F: Fn(&Message) -> Result<K, Error>;
    pub fn branch(self, branches: Vec<Branch>) -> BranchedStream<T>;
    pub fn join<R>(self, other: Stream<R>, join_type: JoinType, on: F, window: Duration)
        -> JoinedStream<T, R>;
    pub async fn to(self, topic: &str) -> Result<(), Error>;
    pub async fn for_each<F, Fut>(self, f: F) -> Result<(), Error>
        where F: Fn(T) -> Fut, Fut: Future<Output = Result<(), Error>>;
}
```

## License

MIT
