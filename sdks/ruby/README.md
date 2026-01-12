# kafka.do

> Event Streaming for Ruby. Blocks. Enumerables. Zero Ops.

```ruby
kafka = Kafka.new(['broker1:9092'])

kafka.producer.tap do |producer|
  producer.produce({ order_id: '123', amount: 99.99 }, topic: 'orders')
  producer.deliver_messages
end
```

Blocks for resource management. Enumerables for iteration. The Ruby way.

## Installation

```ruby
# Gemfile
gem 'kafka.do'
```

```bash
bundle install
```

Or install directly:

```bash
gem install kafka.do
```

Requires Ruby 2.6+.

## Quick Start

```ruby
require 'kafka_do'

kafka = Kafka.new(['broker1:9092'])

# Produce
producer = kafka.producer
producer.produce({ order_id: '123', amount: 99.99 }, topic: 'orders')
producer.deliver_messages

# Consume
consumer = kafka.consumer(group_id: 'my-processor')
consumer.subscribe('orders')
consumer.each_message do |message|
  puts "Received: #{message.value}"
end
```

## Producing Messages

### Simple Producer

```ruby
kafka = Kafka.new(['broker1:9092'])
producer = kafka.producer

# Send single message
producer.produce('Hello!', topic: 'orders')

# Send with key for partitioning
producer.produce({ order_id: '123', amount: 99.99 }, topic: 'orders', key: 'customer-456')

# Send with headers
producer.produce(
  { order_id: '123' },
  topic: 'orders',
  key: 'customer-456',
  headers: { 'correlation-id' => 'abc-123' }
)

# Deliver all buffered messages
producer.deliver_messages
```

### Batch Producer

```ruby
producer = kafka.producer

# Send multiple messages
producer.produce({ order_id: '124', amount: 49.99 }, topic: 'orders')
producer.produce({ order_id: '125', amount: 149.99 }, topic: 'orders')
producer.produce({ order_id: '126', amount: 29.99 }, topic: 'orders')

# Deliver all at once
producer.deliver_messages
```

### Producer with Options

```ruby
producer = kafka.producer(
  compression_codec: :gzip,
  required_acks: -1,  # Wait for all replicas
  max_buffer_size: 1000
)

producer.produce(order, topic: 'orders')
producer.deliver_messages

# Shutdown gracefully (delivers remaining messages)
producer.shutdown
```

## Consuming Messages

### Basic Consumer with Block

```ruby
consumer = kafka.consumer(group_id: 'order-processor')
consumer.subscribe('orders')

consumer.each_message do |message|
  puts "Topic: #{message.topic}"
  puts "Partition: #{message.partition}"
  puts "Offset: #{message.offset}"
  puts "Key: #{message.key}"
  puts "Value: #{message.value}"
  puts "Timestamp: #{message.timestamp}"
  puts "Headers: #{message.headers}"

  process_order(message.value)
end
```

### Consumer with Enumerable Methods

```ruby
consumer = kafka.consumer(group_id: 'processor')
consumer.subscribe('orders')

# Take specific number using Enumerator
consumer.each_message.take(10).each do |message|
  process_order(message.value)
end
```

### Consumer with Options

```ruby
consumer = kafka.consumer(
  group_id: 'processor',
  auto_offset_reset: :earliest,
  enable_auto_commit: true,
  session_timeout: 60,
  heartbeat_interval: 15
)

consumer.subscribe('orders', start_from_beginning: true)

consumer.each_message do |message|
  process_order(message.value)
end
```

### Batch Consumer

```ruby
consumer = KafkaDo::BatchConsumer.new(kafka, group_id: 'batch-processor')
consumer.subscribe('orders')

consumer.each_batch(batch_size: 100, batch_timeout: 5.0) do |batch|
  batch.each do |message|
    process_order(message.value)
  end
end
```

### Manual Offset Management

```ruby
consumer = kafka.consumer(group_id: 'manual', enable_auto_commit: false)
consumer.subscribe('orders')

consumer.each_message do |message|
  begin
    process_order(message.value)
    consumer.commit_offsets(message)
  rescue ProcessingError => e
    # Handle error without committing
    log_error(e)
  end
end
```

### Seek to Position

```ruby
consumer = kafka.consumer(group_id: 'replay')
consumer.subscribe('orders')

# Seek to specific offset
tp = KafkaDo::TopicPartition.new('orders', 0)
consumer.seek(tp, 1000)

# Or seek to beginning/end
consumer.seek_to_beginning(tp)
consumer.seek_to_end(tp)
```

### Pause and Resume

```ruby
tp = KafkaDo::TopicPartition.new('orders', 0)

# Pause consumption
consumer.pause(tp)

# Resume consumption
consumer.resume(tp)

# Check paused partitions
consumer.paused  # => [TopicPartition]
```

## Stream Processing

### Filter and Transform

```ruby
kafka.stream('orders')
  .filter { |msg| msg.value['amount'] > 100 }
  .map { |msg| msg.value.merge(tier: 'premium') }
  .to('high-value-orders')
```

### Peek for Side Effects

```ruby
kafka.stream('orders')
  .peek { |msg| logger.info("Processing: #{msg.key}") }
  .filter { |msg| msg.value['status'] == 'completed' }
  .to('completed-orders')
```

### Transform Keys

```ruby
kafka.stream('orders')
  .map_key { |msg| msg.value['customer_id'] }
  .to('orders-by-customer')
```

### Flat Map (One-to-Many)

```ruby
kafka.stream('orders')
  .flat_map { |msg| msg.value['items'] }
  .to('order-items')
```

### Branching

```ruby
high_value, low_value = kafka.stream('orders').branch(
  ->(msg) { msg.value['amount'] > 100 },
  ->(msg) { msg.value['amount'] <= 100 }
)

high_value.to('high-value-orders')
low_value.to('low-value-orders')
```

### Aggregations

```ruby
kafka.stream('orders')
  .group_by { |msg| msg.value['customer_id'] }
  .count
  .each { |customer_id, count| puts "#{customer_id}: #{count} orders" }
```

### Merge Streams

```ruby
stream_a = kafka.stream('topic-a')
stream_b = kafka.stream('topic-b')

stream_a.merge(stream_b)
  .filter { |msg| msg.value['active'] }
  .to('merged-topic')
```

## Topic Administration

```ruby
admin = kafka.admin

# Create topic
admin.create_topic('orders',
  num_partitions: 3,
  replication_factor: 2,
  config: { 'retention.ms' => '604800000' }
)

# Create multiple topics
admin.create_topics([
  { name: 'orders', num_partitions: 3 },
  { name: 'payments', num_partitions: 3 }
])

# List topics
admin.list_topics.each do |topic|
  puts topic
end

# Describe topic
metadata = admin.describe_topic('orders')
puts "Name: #{metadata.name}"
puts "Partitions: #{metadata.partitions}"
puts "Replication Factor: #{metadata.replication_factor}"
puts "Config: #{metadata.config}"

# Describe partitions
admin.describe_partitions('orders').each do |p|
  puts "Partition #{p.partition}: leader=#{p.leader}, replicas=#{p.replicas}"
end

# Get partition numbers
admin.partitions_for('orders')  # => [0, 1, 2]

# Alter topic config
admin.alter_topic_config('orders', { 'retention.ms' => '2592000000' })

# Create additional partitions
admin.create_partitions('orders', 6)  # Increase to 6 partitions

# Delete topic
admin.delete_topic('old-events')
```

## Consumer Groups

```ruby
admin = kafka.admin

# List groups
admin.list_consumer_groups.each do |group_id|
  puts "Group: #{group_id}"
end

# Describe group
metadata = admin.describe_consumer_group('order-processor')
puts "State: #{metadata.state}"
puts "Members: #{metadata.members}"
puts "Coordinator: #{metadata.coordinator}"

# Fetch committed offsets
offsets = admin.fetch_offsets('order-processor')
offsets.each do |tp, offset|
  puts "#{tp}: #{offset}"
end

# Reset offsets to earliest
admin.reset_offsets('order-processor', 'orders')

# Reset offsets to latest
admin.reset_offsets_to_latest('order-processor', 'orders')

# Set specific offsets
admin.set_offsets('order-processor', 'orders', { 0 => 1000, 1 => 2000 })

# List topic offsets (earliest/latest)
admin.list_topic_offsets('orders').each do |p|
  puts "Partition #{p[:partition]}: #{p[:low]} - #{p[:high]}"
end

# Delete consumer group
admin.delete_consumer_group('old-processor')
```

## Error Handling

```ruby
begin
  producer = kafka.producer
  producer.produce(order, topic: 'orders')
  producer.deliver_messages
rescue KafkaDo::TopicNotFoundError => e
  puts "Topic not found: #{e.topic}"
rescue KafkaDo::TopicAlreadyExistsError => e
  puts "Topic already exists: #{e.topic}"
rescue KafkaDo::MessageTooLargeError
  puts 'Message too large'
rescue KafkaDo::TimeoutError
  puts 'Request timed out'
rescue KafkaDo::Error => e
  puts "Kafka error: #{e.code} - #{e.message}"

  if e.retriable?
    retry
  end
end
```

### Exception Hierarchy

```ruby
KafkaDo::Error                    # Base class
  KafkaDo::ConnectionError        # Connection issues (retriable)
  KafkaDo::TimeoutError           # Timeouts (retriable)
  KafkaDo::ProducerError          # Producer errors
  KafkaDo::ConsumerError          # Consumer errors
  KafkaDo::AdminError             # Admin errors
  KafkaDo::TopicNotFoundError     # Topic doesn't exist
  KafkaDo::TopicAlreadyExistsError # Topic already exists
  KafkaDo::PartitionNotFoundError # Partition doesn't exist
  KafkaDo::MessageTooLargeError   # Message size exceeded
  KafkaDo::OffsetOutOfRangeError  # Invalid offset (retriable)
  KafkaDo::GroupCoordinatorError  # Group coordinator issues (retriable)
  KafkaDo::RebalanceInProgressError # Rebalance in progress (retriable)
  KafkaDo::NotLeaderError         # Not leader for partition (retriable)
  KafkaDo::UnauthorizedError      # Authorization failed
  KafkaDo::QuotaExceededError     # Quota exceeded (retriable)
  KafkaDo::DisconnectedError      # Disconnected (retriable)
  KafkaDo::SerializationError     # Serialization failed
  KafkaDo::ClosedError            # Client is closed
```

## Testing

### Using the Mock Client

```ruby
require 'kafka_do/testing'

RSpec.configure do |config|
  config.before(:each) do
    KafkaDo::Testing.reset!
  end
end
```

### Testing Producers

```ruby
require 'kafka_do/testing'

RSpec.describe OrderPublisher do
  before { KafkaDo::Testing.reset! }

  it 'publishes order to Kafka' do
    kafka = KafkaDo::Testing.client

    publisher = OrderPublisher.new(kafka)
    publisher.publish(order_id: '123', amount: 99.99)

    messages = KafkaDo::Testing.messages('orders')
    expect(messages.size).to eq(1)
    expect(messages.first.value).to include('order_id' => '123')
  end
end
```

### Testing Consumers

```ruby
require 'kafka_do/testing'

RSpec.describe OrderProcessor do
  before { KafkaDo::Testing.reset! }

  it 'processes orders' do
    # Seed test data
    KafkaDo::Testing.produce_messages('orders', [
      { value: '{"order_id":"123","amount":99.99}', key: 'cust-1' },
      { value: '{"order_id":"124","amount":49.99}', key: 'cust-2' }
    ])

    kafka = KafkaDo::Testing.client
    consumer = kafka.consumer(group_id: 'test')
    consumer.subscribe('orders')

    processed = []
    consumer.poll.each do |message|
      processed << message.value
    end

    expect(processed.size).to eq(2)
  end
end
```

### Verifying RPC Calls

```ruby
require 'kafka_do/testing'

RSpec.describe TopicManager do
  before { KafkaDo::Testing.reset! }

  it 'creates topic with correct config' do
    KafkaDo::Testing.create_topic('existing-topic')

    kafka = KafkaDo::Testing.client
    admin = kafka.admin

    admin.create_topic('new-topic', num_partitions: 3)

    # Verify the call was made
    expect(KafkaDo::Testing.called?('kafka.admin.createTopics')).to be true

    call = KafkaDo::Testing.last_call('kafka.admin.createTopics')
    expect(call[:args].first[:topics].first[:numPartitions]).to eq(3)
  end
end
```

## Configuration

### Environment Variables

```bash
export KAFKA_DO_URL=https://kafka.do
export KAFKA_DO_API_KEY=your-api-key
```

### Client Configuration

```ruby
kafka = Kafka.new(
  ['broker1:9092', 'broker2:9092'],
  client_id: 'my-app',
  connection_url: 'https://kafka.do',
  timeout: 30,
  api_key: 'your-api-key'
)
```

### Producer Configuration

```ruby
producer = kafka.producer(
  compression_codec: :gzip,   # :none, :gzip, :snappy, :lz4, :zstd
  required_acks: -1,          # 0, 1, or -1 (all)
  max_buffer_size: 1000,
  max_buffer_bytesize: 10_000_000
)
```

### Consumer Configuration

```ruby
consumer = kafka.consumer(
  group_id: 'my-group',
  offset_commit_interval: 10,
  offset_commit_threshold: 0,
  heartbeat_interval: 10,
  session_timeout: 30,
  auto_offset_reset: :latest,   # :earliest or :latest
  enable_auto_commit: true
)
```

## API Reference

### KafkaDo::Kafka

```ruby
class KafkaDo::Kafka
  # Constants
  COMPRESSION_NONE, COMPRESSION_GZIP, COMPRESSION_SNAPPY, COMPRESSION_LZ4, COMPRESSION_ZSTD
  OFFSET_EARLIEST, OFFSET_LATEST
  ACKS_NONE, ACKS_LEADER, ACKS_ALL

  def initialize(brokers = [], client_id: nil, connection_url: 'https://kafka.do', timeout: 30, api_key: nil)
  def producer(**options)
  def consumer(group_id:, **options)
  def admin
  def stream(topic, group_id: nil)
  def create_topic(topic, num_partitions: 1, replication_factor: 1, config: {})
  def delete_topic(topic)
  def topics
  def partitions_for(topic)
  def close
  def closed?
end
```

### KafkaDo::Producer

```ruby
class KafkaDo::Producer
  def produce(value, topic:, key: nil, partition: nil, partition_key: nil, headers: nil, create_time: nil)
  def deliver_messages
  def clear_buffer
  def buffer_empty?
  def shutdown
  def close
  def closed?
end
```

### KafkaDo::Consumer

```ruby
class KafkaDo::Consumer
  include Enumerable

  def subscribe(*topics, start_from_beginning: false)
  def unsubscribe
  def assign(*topic_partitions)
  def assignment
  def each_message(min_bytes: 1, max_bytes: 1_048_576, max_wait_time: 5, &block)
  def poll(timeout_ms: 1000, max_records: 500)
  def commit_offsets(message = nil)
  def seek(topic_partition, offset)
  def seek_to_beginning(*topic_partitions)
  def seek_to_end(*topic_partitions)
  def position(topic_partition)
  def committed(*topic_partitions)
  def pause(*topic_partitions)
  def resume(*topic_partitions)
  def paused
  def stop
  def close
  def closed?
end
```

### KafkaDo::Stream

```ruby
class KafkaDo::Stream
  include Enumerable

  def map(&block)
  def flat_map(&block)
  def filter(&block)
  def filter_not(&block)
  def peek(&block)
  def map_key(&block)
  def group_by(&block)
  def group_by_key
  def merge(other_stream)
  def branch(*predicates)
  def to(topic, key: nil, &block)
  def for_each(&block)
  def each(&block)
  def start(&block)
  def stop
  def stopped?
end
```

### KafkaDo::Message

```ruby
class KafkaDo::Message
  attr_reader :topic, :partition, :offset, :key, :value, :timestamp, :headers

  def topic_partition
end
```

### KafkaDo::TopicPartition

```ruby
class KafkaDo::TopicPartition
  attr_reader :topic, :partition

  def initialize(topic, partition)
  def to_h
end
```

### KafkaDo::RecordMetadata

```ruby
class KafkaDo::RecordMetadata
  attr_reader :topic, :partition, :offset, :timestamp, :serialized_key_size, :serialized_value_size

  def topic_partition
end
```

## License

MIT
