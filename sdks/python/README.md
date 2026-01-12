# kafka-do

> Event Streaming for Python. Pythonic. Async-Native. Zero Ops.

```python
async with kafka.topic("orders") as producer:
    await producer.send({"order_id": "123", "amount": 99.99})
```

One import. Async context managers. The Python way.

## Installation

```bash
pip install kafka-do
```

Requires Python 3.10+.

## Quick Start

```python
import asyncio
from kafka_do import Kafka

async def main():
    kafka = Kafka()

    # Produce messages
    async with kafka.producer("orders") as producer:
        await producer.send({"order_id": "123", "amount": 99.99})

    # Consume messages
    async for record in kafka.consumer("orders", group="my-processor"):
        print(f"Received: {record.value}")
        await record.commit()

if __name__ == "__main__":
    asyncio.run(main())
```

## Producing Messages

### Simple Producer

```python
from kafka_do import Kafka

kafka = Kafka()

# Context manager handles connection lifecycle
async with kafka.producer("orders") as producer:
    # Send single message
    await producer.send({"order_id": "123", "amount": 99.99})

    # Send with key for partitioning
    await producer.send(
        value={"order_id": "123", "amount": 99.99},
        key="customer-456"
    )

    # Send with headers
    await producer.send(
        value={"order_id": "123"},
        headers={"correlation-id": "abc-123"}
    )
```

### Batch Producer

```python
async with kafka.producer("orders") as producer:
    # Batch send for high throughput
    await producer.send_batch([
        {"order_id": "124", "amount": 49.99},
        {"order_id": "125", "amount": 149.99},
        {"order_id": "126", "amount": 29.99},
    ])

    # Batch with keys
    await producer.send_batch([
        {"key": "customer-1", "value": {"order_id": "124"}},
        {"key": "customer-2", "value": {"order_id": "125"}},
    ])
```

### Transactional Producer

```python
async with kafka.transaction("orders") as tx:
    await tx.send({"order_id": "123", "status": "created"})
    await tx.send({"order_id": "123", "status": "validated"})
    # Automatically commits on exit, rolls back on exception
```

## Consuming Messages

### Basic Consumer

```python
from kafka_do import Kafka

kafka = Kafka()

# Async iteration - Pythonic and clean
async for record in kafka.consumer("orders", group="order-processor"):
    print(f"Topic: {record.topic}")
    print(f"Partition: {record.partition}")
    print(f"Offset: {record.offset}")
    print(f"Key: {record.key}")
    print(f"Value: {record.value}")
    print(f"Timestamp: {record.timestamp}")
    print(f"Headers: {record.headers}")

    # Process the message
    await process_order(record.value)

    # Commit offset
    await record.commit()
```

### Consumer with Auto-Commit

```python
# Auto-commit after successful processing
async for record in kafka.consumer(
    "orders",
    group="order-processor",
    auto_commit=True
):
    await process_order(record.value)
    # Offset committed automatically
```

### Consumer from Beginning

```python
# Read all messages from the beginning
async for record in kafka.consumer(
    "orders",
    group="replay-processor",
    offset="earliest"
):
    await reprocess(record.value)
```

### Consumer with Timestamp

```python
from datetime import datetime, timedelta

# Start from a specific timestamp
yesterday = datetime.now() - timedelta(days=1)

async for record in kafka.consumer(
    "orders",
    group="replay-processor",
    offset=yesterday
):
    print(f"Replaying: {record.value}")
```

### Batch Consumer

```python
# Process messages in batches for efficiency
async for batch in kafka.consumer(
    "orders",
    group="batch-processor",
    batch_size=100,
    batch_timeout=5.0  # seconds
):
    # batch is a list of records
    await process_batch([r.value for r in batch])

    # Commit all at once
    await batch.commit()
```

## Stream Processing

### Filter and Transform

```python
from kafka_do import Kafka, Stream

kafka = Kafka()

# Build a processing pipeline
stream = (
    Stream(kafka, "orders")
    .filter(lambda r: r.value["amount"] > 100)
    .map(lambda r: {**r.value, "tier": "premium"})
)

# Materialize to a new topic
await stream.to("high-value-orders")

# Or iterate results
async for record in stream:
    print(f"High value order: {record.value}")
```

### Windowed Aggregations

```python
from kafka_do import Kafka, Stream, Windows

kafka = Kafka()

# Count orders per customer in 5-minute windows
stream = (
    Stream(kafka, "orders")
    .window(Windows.tumbling("5m"))
    .group_by(lambda r: r.value["customer_id"])
    .count()
)

async for window in stream:
    print(f"Customer {window.key}: {window.value} orders in {window.start}-{window.end}")
```

### Joins

```python
from kafka_do import Kafka, Stream

kafka = Kafka()

# Join orders with customers
orders = Stream(kafka, "orders")
customers = Stream(kafka, "customers")

joined = orders.join(
    customers,
    on=lambda order, customer: order.value["customer_id"] == customer.key,
    window="1h"
)

async for record in joined:
    order, customer = record.value
    print(f"Order {order['order_id']} by {customer['name']}")
```

### Branching

```python
from kafka_do import Kafka, Stream

kafka = Kafka()

# Route messages to different topics based on conditions
await (
    Stream(kafka, "orders")
    .branch(
        (lambda r: r.value["region"] == "us", "us-orders"),
        (lambda r: r.value["region"] == "eu", "eu-orders"),
        (lambda r: True, "other-orders"),  # default branch
    )
)
```

## Topic Administration

```python
from kafka_do import Kafka, TopicConfig

kafka = Kafka()

# Create a topic
await kafka.admin.create_topic(
    "orders",
    partitions=3,
    config=TopicConfig(
        retention_ms=7 * 24 * 60 * 60 * 1000,  # 7 days
        cleanup_policy="delete"
    )
)

# List topics
topics = await kafka.admin.list_topics()
for topic in topics:
    print(f"{topic.name}: {topic.partitions} partitions")

# Describe a topic
info = await kafka.admin.describe_topic("orders")
print(f"Partitions: {info.partitions}")
print(f"Retention: {info.retention_ms}ms")

# Modify a topic
await kafka.admin.alter_topic(
    "orders",
    config=TopicConfig(retention_ms=30 * 24 * 60 * 60 * 1000)  # 30 days
)

# Delete a topic
await kafka.admin.delete_topic("old-events")
```

## Consumer Groups

```python
from kafka_do import Kafka

kafka = Kafka()

# List consumer groups
groups = await kafka.admin.list_groups()
for group in groups:
    print(f"Group: {group.id}, Members: {group.member_count}")

# Describe a consumer group
info = await kafka.admin.describe_group("order-processor")
print(f"State: {info.state}")
print(f"Members: {info.members}")
print(f"Lag: {info.total_lag}")

# Reset offsets
await kafka.admin.reset_offsets(
    group="order-processor",
    topic="orders",
    offset="earliest"
)

# Reset to timestamp
await kafka.admin.reset_offsets(
    group="order-processor",
    topic="orders",
    offset=datetime(2024, 1, 1)
)
```

## Error Handling

```python
from kafka_do import (
    Kafka,
    KafkaError,
    ProducerError,
    ConsumerError,
    SerializationError,
    TimeoutError,
)

kafka = Kafka()

try:
    async with kafka.producer("orders") as producer:
        await producer.send({"order_id": "123"})
except SerializationError as e:
    print(f"Failed to serialize message: {e}")
except ProducerError as e:
    print(f"Failed to produce: {e}")
    if e.retriable:
        # Safe to retry
        pass
except TimeoutError:
    print("Request timed out")
except KafkaError as e:
    print(f"Kafka error: {e}")
```

### Dead Letter Queue Pattern

```python
from kafka_do import Kafka

kafka = Kafka()

async for record in kafka.consumer("orders", group="processor"):
    try:
        await process_order(record.value)
        await record.commit()
    except ProcessingError as e:
        # Send to dead letter queue
        async with kafka.producer("orders-dlq") as dlq:
            await dlq.send({
                "original_record": record.value,
                "error": str(e),
                "timestamp": datetime.now().isoformat(),
            })
        await record.commit()  # Commit to avoid reprocessing
```

### Retry with Backoff

```python
from kafka_do import Kafka
import asyncio

kafka = Kafka()

async def process_with_retry(record, max_retries=3):
    for attempt in range(max_retries):
        try:
            await process_order(record.value)
            return
        except TransientError:
            if attempt < max_retries - 1:
                await asyncio.sleep(2 ** attempt)  # Exponential backoff
            else:
                raise

async for record in kafka.consumer("orders", group="processor"):
    try:
        await process_with_retry(record)
        await record.commit()
    except Exception as e:
        print(f"Failed after retries: {e}")
```

## Configuration

### Environment Variables

```bash
export KAFKA_DO_URL="https://kafka.do"
export KAFKA_DO_API_KEY="your-api-key"
```

### Programmatic Configuration

```python
from kafka_do import Kafka, Config

# Using config object
config = Config(
    url="https://kafka.do",
    api_key="your-api-key",
    timeout=30.0,
    retries=3,
)

kafka = Kafka(config)

# Or inline
kafka = Kafka(
    url="https://kafka.do",
    api_key="your-api-key",
)
```

### Producer Configuration

```python
from kafka_do import Kafka, ProducerConfig

kafka = Kafka()

config = ProducerConfig(
    batch_size=16384,           # Batch size in bytes
    linger_ms=5,                # Wait time before sending batch
    compression="gzip",         # none, gzip, snappy, lz4, zstd
    acks="all",                 # 0, 1, all
    retries=3,                  # Number of retries
    retry_backoff_ms=100,       # Backoff between retries
)

async with kafka.producer("orders", config=config) as producer:
    await producer.send({"order_id": "123"})
```

### Consumer Configuration

```python
from kafka_do import Kafka, ConsumerConfig

kafka = Kafka()

config = ConsumerConfig(
    fetch_min_bytes=1,              # Minimum bytes to fetch
    fetch_max_wait_ms=500,          # Max wait time
    max_poll_records=500,           # Max records per poll
    session_timeout_ms=30000,       # Session timeout
    heartbeat_interval_ms=3000,     # Heartbeat interval
    auto_offset_reset="latest",     # earliest, latest, none
)

async for record in kafka.consumer("orders", group="processor", config=config):
    await process(record)
```

## Type Hints

kafka-do is fully typed for IDE support and static analysis:

```python
from kafka_do import Kafka, Record
from dataclasses import dataclass
from typing import AsyncIterator

@dataclass
class Order:
    order_id: str
    customer_id: str
    amount: float
    items: list[str]

kafka = Kafka()

# Typed producer
async with kafka.producer[Order]("orders") as producer:
    await producer.send(Order(
        order_id="123",
        customer_id="cust-456",
        amount=99.99,
        items=["item-1", "item-2"]
    ))

# Typed consumer
async for record in kafka.consumer[Order]("orders", group="processor"):
    order: Order = record.value  # IDE knows this is Order
    print(f"Order {order.order_id}: ${order.amount}")
```

## Testing

### Mock Client

```python
from kafka_do.testing import MockKafka
import pytest

@pytest.fixture
def kafka():
    return MockKafka()

async def test_order_processing(kafka):
    # Seed test data
    kafka.seed("orders", [
        {"order_id": "123", "amount": 99.99},
        {"order_id": "124", "amount": 149.99},
    ])

    # Process
    processed = []
    async for record in kafka.consumer("orders", group="test"):
        processed.append(record.value)
        await record.commit()
        if len(processed) == 2:
            break

    assert len(processed) == 2
    assert processed[0]["order_id"] == "123"

async def test_producer(kafka):
    async with kafka.producer("orders") as producer:
        await producer.send({"order_id": "125"})

    # Verify
    messages = kafka.get_messages("orders")
    assert len(messages) == 1
    assert messages[0]["order_id"] == "125"
```

### Integration Testing

```python
import pytest
import os
from kafka_do import Kafka

@pytest.fixture
async def kafka():
    kafka = Kafka(
        url=os.environ.get("TEST_KAFKA_URL"),
        api_key=os.environ.get("TEST_KAFKA_API_KEY"),
    )
    yield kafka
    # Cleanup
    await kafka.admin.delete_topic("test-orders")

async def test_end_to_end(kafka):
    topic = f"test-orders-{uuid4()}"

    # Produce
    async with kafka.producer(topic) as producer:
        await producer.send({"order_id": "123"})

    # Consume
    async for record in kafka.consumer(topic, group="test"):
        assert record.value["order_id"] == "123"
        break
```

## API Reference

### Kafka

```python
class Kafka:
    def __init__(
        self,
        url: str | None = None,
        api_key: str | None = None,
        config: Config | None = None,
    ) -> None: ...

    def producer(
        self,
        topic: str,
        config: ProducerConfig | None = None,
    ) -> AsyncContextManager[Producer]: ...

    def consumer(
        self,
        topic: str,
        group: str,
        config: ConsumerConfig | None = None,
        offset: str | datetime | None = None,
        auto_commit: bool = False,
        batch_size: int | None = None,
        batch_timeout: float | None = None,
    ) -> AsyncIterator[Record]: ...

    def transaction(self, topic: str) -> AsyncContextManager[Transaction]: ...

    @property
    def admin(self) -> Admin: ...
```

### Producer

```python
class Producer[T]:
    async def send(
        self,
        value: T,
        key: str | None = None,
        headers: dict[str, str] | None = None,
        partition: int | None = None,
    ) -> RecordMetadata: ...

    async def send_batch(
        self,
        records: list[T] | list[dict[str, Any]],
    ) -> list[RecordMetadata]: ...
```

### Record

```python
@dataclass
class Record[T]:
    topic: str
    partition: int
    offset: int
    key: str | None
    value: T
    timestamp: datetime
    headers: dict[str, str]

    async def commit(self) -> None: ...
```

### Stream

```python
class Stream[T]:
    def filter(self, predicate: Callable[[Record[T]], bool]) -> Stream[T]: ...
    def map(self, mapper: Callable[[Record[T]], R]) -> Stream[R]: ...
    def flat_map(self, mapper: Callable[[Record[T]], Iterable[R]]) -> Stream[R]: ...
    def window(self, window: Window) -> WindowedStream[T]: ...
    def group_by(self, key_selector: Callable[[Record[T]], K]) -> GroupedStream[K, T]: ...
    def branch(self, *branches: tuple[Callable[[Record[T]], bool], str]) -> None: ...
    def join(self, other: Stream[R], on: Callable, window: str) -> Stream[tuple[T, R]]: ...
    async def to(self, topic: str) -> None: ...
    def __aiter__(self) -> AsyncIterator[Record]: ...
```

## License

MIT
