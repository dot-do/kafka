# kafkado

> Event Streaming for Nim. Async. Templates. Zero Ops.

```nim
kafka.produce("orders", Order(orderId: "123", amount: 99.99))
```

Zero-cost abstractions. Compile-time magic. The Nim way.

## Installation

```bash
nimble install kafkado
```

Requires Nim 2.0+.

## Quick Start

```nim
import kafkado

type Order = object
  orderId: string
  amount: float

let kafka = newKafkaClient()

# Produce
kafka.produce("orders", Order(orderId: "123", amount: 99.99))

# Consume
for record in kafka.consume("orders", group = "my-processor"):
  echo "Received: ", record.value
  record.commit()
```

## Producing Messages

### Simple Producer

```nim
let kafka = newKafkaClient()
let producer = kafka.producer("orders")

# Send single message
producer.send(Order(orderId: "123", amount: 99.99))

# Send with key for partitioning
producer.send(
  Order(orderId: "123", amount: 99.99),
  key = "customer-456"
)

# Send with headers
producer.send(
  Order(orderId: "123", amount: 99.99),
  key = "customer-456",
  headers = {"correlation-id": "abc-123"}.toTable
)
```

### Batch Producer

```nim
let producer = kafka.producer("orders")

# Send batch
producer.sendBatch(@[
  Order(orderId: "124", amount: 49.99),
  Order(orderId: "125", amount: 149.99),
  Order(orderId: "126", amount: 29.99)
])

# Send batch with keys
producer.sendBatch(@[
  Message[Order](key: some("cust-1"), value: Order(orderId: "124", amount: 49.99)),
  Message[Order](key: some("cust-2"), value: Order(orderId: "125", amount: 149.99))
])
```

### Async Producer

```nim
import asyncdispatch

let producer = kafka.asyncProducer("orders")

# Async send
let future = producer.send(Order(orderId: "123", amount: 99.99))

# With callback
future.addCallback proc(metadata: RecordMetadata) =
  echo "Sent to partition ", metadata.partition

# Or await
let metadata = await producer.send(order)
```

### Transactional Producer

```nim
kafka.transaction("orders"):
  tx.send(Order(orderId: "123", status: Created))
  tx.send(Order(orderId: "123", status: Validated))
  # Automatically commits on success, rolls back on exception
```

## Consuming Messages

### Basic Consumer with Iterator

```nim
let consumer = kafka.consumer("orders", group = "order-processor")

for record in consumer:
  echo "Topic: ", record.topic
  echo "Partition: ", record.partition
  echo "Offset: ", record.offset
  echo "Key: ", record.key
  echo "Value: ", record.value
  echo "Timestamp: ", record.timestamp
  echo "Headers: ", record.headers

  processOrder(record.value.to(Order))
  record.commit()
```

### Consumer with Functional Operations

```nim
import sequtils, sugar

let consumer = kafka.consumer("orders", group = "processor")

# Filter and map
consumer.toSeq
  .filter(r => r.value.to(Order).amount > 100)
  .map(r => r.value.to(Order))
  .apply(processHighValueOrder)

# Take specific number
for record in consumer.take(10):
  processOrder(record.value.to(Order))
  record.commit()
```

### Consumer with Configuration

```nim
let config = ConsumerConfig(
  offset: Earliest,
  autoCommit: true,
  maxPollRecords: 100,
  sessionTimeout: initDuration(seconds = 30)
)

for record in kafka.consume("orders", group = "processor", config = config):
  processOrder(record.value.to(Order))
  # Auto-committed
```

### Consumer from Timestamp

```nim
import times

let yesterday = now() - 1.days

let config = ConsumerConfig(
  offset: Timestamp(yesterday)
)

for record in kafka.consume("orders", group = "replay", config = config):
  echo "Replaying: ", record.value
```

### Batch Consumer

```nim
let config = BatchConfig(
  batchSize: 100,
  batchTimeout: initDuration(seconds = 5)
)

for batch in kafka.consumeBatch("orders", group = "batch-processor", config = config):
  for record in batch:
    processOrder(record.value.to(Order))
  batch.commit()
```

### Async Consumer

```nim
import asyncdispatch

let consumer = kafka.asyncConsumer("orders", group = "async-processor")

proc processRecords() {.async.} =
  while true:
    let record = await consumer.next()
    await processOrderAsync(record.value.to(Order))
    await record.commit()

waitFor processRecords()
```

### Parallel Consumer with Threads

```nim
import threadpool

let consumer = kafka.consumer("orders", group = "parallel-processor")
var channel: Channel[KafkaRecord]
channel.open()

# Start workers
for i in 0..<10:
  spawn:
    while true:
      let record = channel.recv()
      processOrder(record.value.to(Order))
      record.commit()

# Feed records to channel
for record in consumer:
  channel.send(record)
```

## Stream Processing

### Filter and Transform

```nim
kafka.stream("orders")
  .filter(proc(o: Order): bool = o.amount > 100)
  .map(proc(o: Order): PremiumOrder = PremiumOrder(order: o, tier: "premium"))
  .to("high-value-orders")
```

### Windowed Aggregations

```nim
kafka.stream("orders")
  .window(tumblingWindow(initDuration(minutes = 5)))
  .groupBy(proc(o: Order): string = o.customerId)
  .count()
  .forEach proc(key: string, window: WindowResult[int64]) =
    echo "Customer ", key, ": ", window.value, " orders in ", window.start, "-", window.endTime
```

### Joins

```nim
let orders = kafka.stream("orders")
let customers = kafka.stream("customers")

orders.join(customers,
  on = proc(o: Order, c: Customer): bool = o.customerId == c.id,
  window = initDuration(hours = 1)
).forEach proc(order: Order, customer: Customer) =
  echo "Order by ", customer.name
```

### Branching

```nim
kafka.stream("orders")
  .branch(@[
    (proc(o: Order): bool = o.region == "us", "us-orders"),
    (proc(o: Order): bool = o.region == "eu", "eu-orders"),
    (proc(o: Order): bool = true, "other-orders")
  ])
```

### Aggregations

```nim
kafka.stream("orders")
  .groupBy(proc(o: Order): string = o.customerId)
  .reduce(0.0, proc(total: float, o: Order): float = total + o.amount)
  .forEach proc(customerId: string, total: float) =
    echo "Customer ", customerId, " total: $", total
```

## Topic Administration

```nim
let admin = kafka.admin

# Create topic
admin.createTopic("orders", TopicConfig(
  partitions: 3,
  retentionMs: 7 * 24 * 60 * 60 * 1000 # 7 days
))

# List topics
for topic in admin.listTopics():
  echo topic.name, ": ", topic.partitions, " partitions"

# Describe topic
let info = admin.describeTopic("orders")
echo "Partitions: ", info.partitions
echo "Retention: ", info.retentionMs, "ms"

# Alter topic
admin.alterTopic("orders", TopicConfig(
  retentionMs: 30 * 24 * 60 * 60 * 1000 # 30 days
))

# Delete topic
admin.deleteTopic("old-events")
```

## Consumer Groups

```nim
let admin = kafka.admin

# List groups
for group in admin.listGroups():
  echo "Group: ", group.id, ", Members: ", group.memberCount

# Describe group
let info = admin.describeGroup("order-processor")
echo "State: ", info.state
echo "Members: ", info.members.len
echo "Total Lag: ", info.totalLag

# Reset offsets
admin.resetOffsets("order-processor", "orders", Earliest)

# Reset to timestamp
admin.resetOffsets("order-processor", "orders", Timestamp(yesterday))
```

## Error Handling

```nim
let producer = kafka.producer("orders")

try:
  producer.send(order)
except TopicNotFoundError:
  echo "Topic not found"
except MessageTooLargeError:
  echo "Message too large"
except TimeoutError:
  echo "Request timed out"
except KafkaError as e:
  echo "Kafka error: ", e.code, " - ", e.msg

  if e.retriable:
    # Safe to retry
    discard
```

### Exception Hierarchy

```nim
type
  KafkaError* = object of CatchableError
    code*: string
    retriable*: bool

  TopicNotFoundError* = object of KafkaError
  PartitionNotFoundError* = object of KafkaError
  MessageTooLargeError* = object of KafkaError
  NotLeaderError* = object of KafkaError
  OffsetOutOfRangeError* = object of KafkaError
  GroupCoordinatorError* = object of KafkaError
  RebalanceInProgressError* = object of KafkaError
  UnauthorizedError* = object of KafkaError
  QuotaExceededError* = object of KafkaError
  TimeoutError* = object of KafkaError
  DisconnectedError* = object of KafkaError
  SerializationError* = object of KafkaError
```

### Dead Letter Queue Pattern

```nim
let consumer = kafka.consumer("orders", group = "processor")
let dlqProducer = kafka.producer("orders-dlq")

for record in consumer:
  try:
    processOrder(record.value.to(Order))
  except ProcessingError as e:
    dlqProducer.send(
      DlqRecord(
        originalRecord: record.value,
        error: e.msg,
        timestamp: now()
      ),
      headers: {
        "original-topic": record.topic,
        "original-partition": $record.partition,
        "original-offset": $record.offset
      }.toTable
    )
  record.commit()
```

### Retry with Exponential Backoff

```nim
import random, os

proc withRetry[T](operation: proc(): T, maxAttempts = 3, baseDelay = 1000): T =
  var attempt = 0
  while true:
    inc attempt
    try:
      return operation()
    except KafkaError as e:
      if not e.retriable or attempt >= maxAttempts:
        raise e
      let delay = baseDelay * (1 shl (attempt - 1))
      let jitter = rand(delay div 10)
      sleep(delay + jitter)

let producer = kafka.producer("orders")
withRetry proc(): RecordMetadata =
  producer.send(order)
```

## Configuration

### Environment Variables

```bash
export KAFKA_DO_URL=https://kafka.do
export KAFKA_DO_API_KEY=your-api-key
```

### Client Configuration

```nim
let config = KafkaConfig(
  url: "https://kafka.do",
  apiKey: "your-api-key",
  timeout: initDuration(seconds = 30),
  retries: 3
)

let kafka = newKafkaClient(config)
```

### Producer Configuration

```nim
let config = ProducerConfig(
  batchSize: 16384,
  lingerMs: 5,
  compression: Gzip,
  acks: All,
  retries: 3,
  retryBackoffMs: 100
)

let producer = kafka.producer("orders", config)
```

### Consumer Configuration

```nim
let config = ConsumerConfig(
  offset: Latest,
  autoCommit: false,
  fetchMinBytes: 1,
  fetchMaxWaitMs: 500,
  maxPollRecords: 500,
  sessionTimeout: initDuration(seconds = 30),
  heartbeatInterval: initDuration(seconds = 3)
)

for record in kafka.consume("orders", group = "processor", config = config):
  # ...
  discard
```

## Testing

### Mock Client

```nim
import unittest
import kafkado/testing

suite "OrderProcessor":
  test "processes all orders":
    let kafka = newMockKafka()

    # Seed test data
    kafka.seed("orders", @[
      Order(orderId: "123", amount: 99.99),
      Order(orderId: "124", amount: 149.99)
    ])

    # Process
    var processed: seq[Order]
    for record in kafka.consumer("orders", group = "test").take(2):
      processed.add(record.value.to(Order))
      record.commit()

    check processed.len == 2
    check processed[0].orderId == "123"

  test "sends messages":
    let kafka = newMockKafka()
    let producer = kafka.producer("orders")

    producer.send(Order(orderId: "125", amount: 99.99))

    let messages = kafka.getMessages("orders")
    check messages.len == 1
    check messages[0].to(Order).orderId == "125"
```

### Integration Testing

```nim
import unittest, os, uuids

suite "Integration":
  var kafka: KafkaClient
  var topic: string

  setup:
    kafka = newKafkaClient(KafkaConfig(
      url: getEnv("TEST_KAFKA_URL"),
      apiKey: getEnv("TEST_KAFKA_API_KEY")
    ))
    topic = "test-orders-" & $genUUID()

  teardown:
    try:
      kafka.admin.deleteTopic(topic)
    except:
      discard

  test "end to end produces and consumes":
    # Produce
    kafka.producer(topic).send(Order(orderId: "123", amount: 99.99))

    # Consume
    for record in kafka.consumer(topic, group = "test").take(1):
      check record.value.to(Order).orderId == "123"
```

## API Reference

### KafkaClient

```nim
type KafkaClient* = ref object

proc newKafkaClient*(config: KafkaConfig = defaultConfig()): KafkaClient

proc producer*[T](client: KafkaClient, topic: string, config: ProducerConfig = defaultProducerConfig()): Producer[T]
proc asyncProducer*[T](client: KafkaClient, topic: string, config: ProducerConfig = defaultProducerConfig()): AsyncProducer[T]
proc consumer*(client: KafkaClient, topic: string, group: string, config: ConsumerConfig = defaultConsumerConfig()): Consumer
proc asyncConsumer*(client: KafkaClient, topic: string, group: string, config: ConsumerConfig = defaultConsumerConfig()): AsyncConsumer
proc consumeBatch*(client: KafkaClient, topic: string, group: string, config: BatchConfig): BatchConsumer
proc transaction*[T](client: KafkaClient, topic: string, body: proc(tx: Transaction[T]))
proc stream*[T](client: KafkaClient, topic: string): Stream[T]
proc admin*(client: KafkaClient): Admin
```

### Producer

```nim
type Producer*[T] = ref object

proc send*[T](producer: Producer[T], value: T, key: Option[string] = none(string), headers: Table[string, string] = initTable[string, string]()): RecordMetadata
proc sendBatch*[T](producer: Producer[T], values: seq[T]): seq[RecordMetadata]
proc sendBatch*[T](producer: Producer[T], messages: seq[Message[T]]): seq[RecordMetadata]
```

### KafkaRecord

```nim
type KafkaRecord* = ref object
  topic*: string
  partition*: int32
  offset*: int64
  key*: Option[string]
  value*: JsonNode
  timestamp*: DateTime
  headers*: Table[string, string]

proc commit*(record: KafkaRecord)
proc to*[T](record: KafkaRecord): T
```

### Stream

```nim
type Stream*[T] = ref object

proc filter*[T](stream: Stream[T], predicate: proc(x: T): bool): Stream[T]
proc map*[T, R](stream: Stream[T], mapper: proc(x: T): R): Stream[R]
proc flatMap*[T, R](stream: Stream[T], mapper: proc(x: T): seq[R]): Stream[R]
proc window*[T](stream: Stream[T], window: Window): WindowedStream[T]
proc groupBy*[T, K](stream: Stream[T], keySelector: proc(x: T): K): GroupedStream[K, T]
proc reduce*[T, S](stream: GroupedStream[K, T], initial: S, reducer: proc(acc: S, x: T): S): Stream[(K, S)]
proc count*[K, T](stream: GroupedStream[K, T]): Stream[(K, int64)]
proc branch*[T](stream: Stream[T], branches: seq[(proc(x: T): bool, string)])
proc join*[T, R](stream: Stream[T], other: Stream[R], on: proc(a: T, b: R): bool, window: Duration): Stream[(T, R)]
proc to*[T](stream: Stream[T], topic: string)
proc forEach*[T](stream: Stream[T], handler: proc(x: T))
```

## License

MIT
