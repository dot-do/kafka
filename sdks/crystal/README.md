# kafka-do

> Event Streaming for Crystal. Fibers. Channels. Zero Ops.

```crystal
kafka.produce("orders", {order_id: "123", amount: 99.99})
```

Ruby-like syntax. C-like performance. The Crystal way.

## Installation

```yaml
# shard.yml
dependencies:
  kafka-do:
    github: dot-do/kafka-crystal
    version: ~> 0.1.0
```

```bash
shards install
```

Requires Crystal 1.10+.

## Quick Start

```crystal
require "kafka-do"

record Order, order_id : String, amount : Float64

kafka = Kafka::Client.new

# Produce
kafka.produce("orders", Order.new("123", 99.99))

# Consume
kafka.consume("orders", group: "my-processor") do |record|
  puts "Received: #{record.value}"
  record.commit
end
```

## Producing Messages

### Simple Producer

```crystal
kafka = Kafka::Client.new
producer = kafka.producer("orders")

# Send single message
producer.send(Order.new("123", 99.99))

# Send with key for partitioning
producer.send(
  Order.new("123", 99.99),
  key: "customer-456"
)

# Send with headers
producer.send(
  Order.new("123", 99.99),
  key: "customer-456",
  headers: {"correlation-id" => "abc-123"}
)
```

### Batch Producer

```crystal
producer = kafka.producer("orders")

# Send batch
producer.send_batch([
  Order.new("124", 49.99),
  Order.new("125", 149.99),
  Order.new("126", 29.99),
])

# Send batch with keys
producer.send_batch([
  Kafka::Message.new(key: "cust-1", value: Order.new("124", 49.99)),
  Kafka::Message.new(key: "cust-2", value: Order.new("125", 149.99)),
])
```

### Async Producer with Fibers

```crystal
producer = kafka.producer("orders")

# Non-blocking send
spawn do
  begin
    producer.send(order)
  rescue ex
    puts "Failed: #{ex.message}"
  end
end

# Wait for all pending sends
Fiber.yield
```

### Transactional Producer

```crystal
kafka.transaction("orders") do |tx|
  tx.send(Order.new("123", Status::Created))
  tx.send(Order.new("123", Status::Validated))
  # Automatically commits on success, aborts on exception
end
```

## Consuming Messages

### Basic Consumer with Block

```crystal
kafka.consume("orders", group: "order-processor") do |record|
  puts "Topic: #{record.topic}"
  puts "Partition: #{record.partition}"
  puts "Offset: #{record.offset}"
  puts "Key: #{record.key}"
  puts "Value: #{record.value}"
  puts "Timestamp: #{record.timestamp}"
  puts "Headers: #{record.headers}"

  process_order(record.value.as(Order))
  record.commit
end
```

### Consumer with Iterator

```crystal
consumer = kafka.consumer("orders", group: "processor")

# Use each
consumer.each do |record|
  process_order(record.value.as(Order))
  record.commit
end

# Filter and map
consumer
  .select { |r| r.value.as(Order).amount > 100 }
  .map(&.value.as(Order))
  .each { |order| process_high_value_order(order) }

# Take specific number
consumer.first(10).each do |record|
  process_order(record.value.as(Order))
  record.commit
end
```

### Consumer with Configuration

```crystal
config = Kafka::ConsumerConfig.new(
  offset: Kafka::Offset::Earliest,
  auto_commit: true,
  max_poll_records: 100,
  session_timeout: 30.seconds
)

kafka.consume("orders", group: "processor", config: config) do |record|
  process_order(record.value.as(Order))
  # Auto-committed
end
```

### Consumer from Timestamp

```crystal
yesterday = Time.utc - 1.day

config = Kafka::ConsumerConfig.new(
  offset: Kafka::Offset::Timestamp.new(yesterday)
)

kafka.consume("orders", group: "replay", config: config) do |record|
  puts "Replaying: #{record.value}"
end
```

### Batch Consumer

```crystal
config = Kafka::BatchConfig.new(
  batch_size: 100,
  batch_timeout: 5.seconds
)

kafka.consume_batch("orders", group: "batch-processor", config: config) do |batch|
  batch.each do |record|
    process_order(record.value.as(Order))
  end
  batch.commit
end
```

### Parallel Consumer with Channels

```crystal
consumer = kafka.consumer("orders", group: "parallel-processor")
channel = Channel(Kafka::Record).new(100)

# Start workers
10.times do
  spawn do
    loop do
      record = channel.receive
      process_order(record.value.as(Order))
      record.commit
    end
  end
end

# Feed records to channel
consumer.each do |record|
  channel.send(record)
end
```

## Stream Processing

### Filter and Transform

```crystal
kafka.stream("orders")
  .filter { |order| order.amount > 100 }
  .map { |order| PremiumOrder.new(order, "premium") }
  .to("high-value-orders")
```

### Windowed Aggregations

```crystal
kafka.stream("orders")
  .window(Kafka::Window.tumbling(5.minutes))
  .group_by(&.customer_id)
  .count
  .each do |key, window|
    puts "Customer #{key}: #{window.value} orders in #{window.start}-#{window.end}"
  end
```

### Joins

```crystal
orders = kafka.stream("orders")
customers = kafka.stream("customers")

orders.join(customers,
  on: ->(order : Order, customer : Customer) { order.customer_id == customer.id },
  window: 1.hour
).each do |order, customer|
  puts "Order by #{customer.name}"
end
```

### Branching

```crystal
kafka.stream("orders")
  .branch([
    {->(o : Order) { o.region == "us" }, "us-orders"},
    {->(o : Order) { o.region == "eu" }, "eu-orders"},
    {->(o : Order) { true }, "other-orders"},
  ])
```

### Aggregations

```crystal
kafka.stream("orders")
  .group_by(&.customer_id)
  .reduce(0.0) { |total, order| total + order.amount }
  .each do |customer_id, total|
    puts "Customer #{customer_id} total: $#{total}"
  end
```

## Topic Administration

```crystal
admin = kafka.admin

# Create topic
admin.create_topic("orders", Kafka::TopicConfig.new(
  partitions: 3,
  retention_ms: 7.days.total_milliseconds.to_i64
))

# List topics
admin.list_topics.each do |topic|
  puts "#{topic.name}: #{topic.partitions} partitions"
end

# Describe topic
info = admin.describe_topic("orders")
puts "Partitions: #{info.partitions}"
puts "Retention: #{info.retention_ms}ms"

# Alter topic
admin.alter_topic("orders", Kafka::TopicConfig.new(
  retention_ms: 30.days.total_milliseconds.to_i64
))

# Delete topic
admin.delete_topic("old-events")
```

## Consumer Groups

```crystal
admin = kafka.admin

# List groups
admin.list_groups.each do |group|
  puts "Group: #{group.id}, Members: #{group.member_count}"
end

# Describe group
info = admin.describe_group("order-processor")
puts "State: #{info.state}"
puts "Members: #{info.members.size}"
puts "Total Lag: #{info.total_lag}"

# Reset offsets
admin.reset_offsets("order-processor", "orders", Kafka::Offset::Earliest)

# Reset to timestamp
admin.reset_offsets("order-processor", "orders", Kafka::Offset::Timestamp.new(yesterday))
```

## Error Handling

```crystal
producer = kafka.producer("orders")

begin
  producer.send(order)
rescue ex : Kafka::TopicNotFoundError
  puts "Topic not found"
rescue ex : Kafka::MessageTooLargeError
  puts "Message too large"
rescue ex : Kafka::TimeoutError
  puts "Request timed out"
rescue ex : Kafka::Error
  puts "Kafka error: #{ex.code} - #{ex.message}"

  if ex.retriable?
    # Safe to retry
  end
end
```

### Exception Hierarchy

```crystal
class Kafka::Error < Exception
  getter code : String
  getter retriable? : Bool
end

class Kafka::TopicNotFoundError < Kafka::Error
class Kafka::PartitionNotFoundError < Kafka::Error
class Kafka::MessageTooLargeError < Kafka::Error
class Kafka::NotLeaderError < Kafka::Error
class Kafka::OffsetOutOfRangeError < Kafka::Error
class Kafka::GroupCoordinatorError < Kafka::Error
class Kafka::RebalanceInProgressError < Kafka::Error
class Kafka::UnauthorizedError < Kafka::Error
class Kafka::QuotaExceededError < Kafka::Error
class Kafka::TimeoutError < Kafka::Error
class Kafka::DisconnectedError < Kafka::Error
class Kafka::SerializationError < Kafka::Error
```

### Dead Letter Queue Pattern

```crystal
consumer = kafka.consumer("orders", group: "processor")
dlq_producer = kafka.producer("orders-dlq")

consumer.each do |record|
  begin
    process_order(record.value.as(Order))
  rescue ex : ProcessingError
    dlq_producer.send(
      DlqRecord.new(
        original_record: record.value,
        error: ex.message,
        timestamp: Time.utc
      ),
      headers: {
        "original-topic" => record.topic,
        "original-partition" => record.partition.to_s,
        "original-offset" => record.offset.to_s,
      }
    )
  end
  record.commit
end
```

### Retry with Exponential Backoff

```crystal
def with_retry(max_attempts = 3, base_delay = 1.second, &block)
  attempt = 0
  loop do
    attempt += 1
    begin
      return yield
    rescue ex : Kafka::Error
      raise ex unless ex.retriable? && attempt < max_attempts
      delay = base_delay * (2 ** (attempt - 1))
      jitter = Random.rand(delay.total_seconds / 10).seconds
      sleep(delay + jitter)
    end
  end
end

producer = kafka.producer("orders")
with_retry { producer.send(order) }
```

## Configuration

### Environment Variables

```bash
export KAFKA_DO_URL=https://kafka.do
export KAFKA_DO_API_KEY=your-api-key
```

### Client Configuration

```crystal
config = Kafka::Config.new(
  url: "https://kafka.do",
  api_key: "your-api-key",
  timeout: 30.seconds,
  retries: 3
)

kafka = Kafka::Client.new(config)
```

### Producer Configuration

```crystal
config = Kafka::ProducerConfig.new(
  batch_size: 16384,
  linger_ms: 5,
  compression: Kafka::Compression::Gzip,
  acks: Kafka::Acks::All,
  retries: 3,
  retry_backoff_ms: 100
)

producer = kafka.producer("orders", config)
```

### Consumer Configuration

```crystal
config = Kafka::ConsumerConfig.new(
  offset: Kafka::Offset::Latest,
  auto_commit: false,
  fetch_min_bytes: 1,
  fetch_max_wait_ms: 500,
  max_poll_records: 500,
  session_timeout: 30.seconds,
  heartbeat_interval: 3.seconds
)

kafka.consume("orders", group: "processor", config: config) do |record|
  # ...
end
```

## Testing

### Mock Client

```crystal
require "kafka-do/testing"

describe "OrderProcessor" do
  it "processes all orders" do
    kafka = Kafka::MockClient.new

    # Seed test data
    kafka.seed("orders", [
      Order.new("123", 99.99),
      Order.new("124", 149.99),
    ])

    # Process
    processed = [] of Order
    kafka.consumer("orders", group: "test").first(2).each do |record|
      processed << record.value.as(Order)
      record.commit
    end

    processed.size.should eq 2
    processed.first.order_id.should eq "123"
  end

  it "sends messages" do
    kafka = Kafka::MockClient.new
    producer = kafka.producer("orders")

    producer.send(Order.new("125", 99.99))

    messages = kafka.get_messages("orders")
    messages.size.should eq 1
    messages.first.as(Order).order_id.should eq "125"
  end
end
```

### Integration Testing

```crystal
describe "Integration" do
  kafka : Kafka::Client?
  topic : String?

  before_all do
    kafka = Kafka::Client.new(Kafka::Config.new(
      url: ENV["TEST_KAFKA_URL"],
      api_key: ENV["TEST_KAFKA_API_KEY"]
    ))
    topic = "test-orders-#{UUID.random}"
  end

  after_all do
    kafka.try(&.admin.delete_topic(topic.not_nil!)) rescue nil
  end

  it "produces and consumes end to end" do
    k = kafka.not_nil!
    t = topic.not_nil!

    # Produce
    k.producer(t).send(Order.new("123", 99.99))

    # Consume
    record = k.consumer(t, group: "test").first
    record.value.as(Order).order_id.should eq "123"
  end
end
```

## API Reference

### Kafka::Client

```crystal
class Kafka::Client
  def initialize(config : Config = Config.default)

  def producer(topic : String, config : ProducerConfig = ProducerConfig.default) : Producer
  def consumer(topic : String, group : String, config : ConsumerConfig = ConsumerConfig.default) : Consumer
  def consume(topic : String, group : String, config : ConsumerConfig = ConsumerConfig.default, &block : Record -> Nil) : Nil
  def consume_batch(topic : String, group : String, config : BatchConfig, &block : Batch -> Nil) : Nil
  def transaction(topic : String, &block : Transaction -> Nil) : Nil
  def stream(topic : String) : Stream
  def admin : Admin
end
```

### Kafka::Producer

```crystal
class Kafka::Producer(T)
  def send(value : T, key : String? = nil, headers : Hash(String, String) = {} of String => String) : RecordMetadata
  def send_batch(values : Array(T)) : Array(RecordMetadata)
  def send_batch(messages : Array(Message(T))) : Array(RecordMetadata)
end
```

### Kafka::Record

```crystal
class Kafka::Record
  getter topic : String
  getter partition : Int32
  getter offset : Int64
  getter key : String?
  getter value : JSON::Any
  getter timestamp : Time
  getter headers : Hash(String, String)

  def commit : Nil
end
```

### Kafka::Stream

```crystal
class Kafka::Stream(T)
  def filter(&block : T -> Bool) : Stream(T)
  def map(&block : T -> R) : Stream(R) forall R
  def flat_map(&block : T -> Array(R)) : Stream(R) forall R
  def window(window : Window) : WindowedStream(T)
  def group_by(&block : T -> K) : GroupedStream(K, T) forall K
  def reduce(initial : S, &block : S, T -> S) : Stream(Tuple(K, S)) forall S
  def count : Stream(Tuple(K, Int64))
  def branch(branches : Array(Tuple(Proc(T, Bool), String))) : Nil
  def join(other : Stream(R), *, on : Proc(T, R, Bool), window : Time::Span) : Stream(Tuple(T, R)) forall R
  def to(topic : String) : Nil
  def each(&block : T -> Nil) : Nil
end
```

## License

MIT
