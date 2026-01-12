# kafka-do

> Event Streaming for Ruby. Blocks. Enumerables. Zero Ops.

```ruby
kafka.producer('orders') do |producer|
  producer.send(order_id: '123', amount: 99.99)
end
```

Blocks for resource management. Enumerables for iteration. The Ruby way.

## Installation

```ruby
# Gemfile
gem 'kafka-do'
```

```bash
bundle install
```

Or install directly:

```bash
gem install kafka-do
```

Requires Ruby 3.1+.

## Quick Start

```ruby
require 'kafka-do'

kafka = Kafka::Client.new

# Produce
kafka.producer('orders') do |producer|
  producer.send(order_id: '123', amount: 99.99)
end

# Consume
kafka.consumer('orders', group: 'my-processor').each do |record|
  puts "Received: #{record.value}"
  record.commit
end
```

## Producing Messages

### Simple Producer

```ruby
kafka = Kafka::Client.new

kafka.producer('orders') do |producer|
  # Send single message
  producer.send(order_id: '123', amount: 99.99)

  # Send with key for partitioning
  producer.send({ order_id: '123', amount: 99.99 }, key: 'customer-456')

  # Send with headers
  producer.send(
    { order_id: '123' },
    key: 'customer-456',
    headers: { 'correlation-id' => 'abc-123' }
  )
end
```

### Batch Producer

```ruby
kafka.producer('orders') do |producer|
  # Send batch
  producer.send_batch([
    { order_id: '124', amount: 49.99 },
    { order_id: '125', amount: 149.99 },
    { order_id: '126', amount: 29.99 }
  ])

  # Send batch with keys
  producer.send_batch([
    { key: 'cust-1', value: { order_id: '124' } },
    { key: 'cust-2', value: { order_id: '125' } }
  ])
end
```

### Async Producer

```ruby
producer = kafka.async_producer('orders')

# Non-blocking send
producer.send(order_id: '123', amount: 99.99)
producer.send(order_id: '124', amount: 49.99)

# Force delivery of buffered messages
producer.deliver

# With delivery callbacks
producer.send(order, &:delivery_report)

producer.on_delivery do |report|
  if report.error?
    puts "Delivery failed: #{report.error}"
  else
    puts "Delivered to partition #{report.partition} offset #{report.offset}"
  end
end
```

### Transactional Producer

```ruby
kafka.transaction('orders') do |tx|
  tx.send(order_id: '123', status: 'created')
  tx.send(order_id: '123', status: 'validated')
  # Automatically commits on success, aborts on exception
end
```

## Consuming Messages

### Basic Consumer with Block

```ruby
kafka.consumer('orders', group: 'order-processor').each do |record|
  puts "Topic: #{record.topic}"
  puts "Partition: #{record.partition}"
  puts "Offset: #{record.offset}"
  puts "Key: #{record.key}"
  puts "Value: #{record.value}"
  puts "Timestamp: #{record.timestamp}"
  puts "Headers: #{record.headers}"

  process_order(record.value)
  record.commit
end
```

### Consumer with Enumerable Methods

```ruby
consumer = kafka.consumer('orders', group: 'processor')

# Select and process
consumer
  .select { |r| r.value[:amount] > 100 }
  .map(&:value)
  .each { |order| process_high_value_order(order) }

# Take specific number
consumer.take(10).each do |record|
  process_order(record.value)
  record.commit
end
```

### Consumer with Options

```ruby
consumer = kafka.consumer('orders', group: 'processor',
  offset: :earliest,
  auto_commit: true,
  max_poll_records: 100,
  session_timeout: 30_000
)

consumer.each do |record|
  process_order(record.value)
  # Auto-committed
end
```

### Consumer from Timestamp

```ruby
yesterday = Time.now - 86_400

consumer = kafka.consumer('orders', group: 'replay',
  offset: yesterday
)

consumer.each do |record|
  puts "Replaying: #{record.value}"
end
```

### Batch Consumer

```ruby
consumer = kafka.batch_consumer('orders', group: 'batch-processor',
  batch_size: 100,
  batch_timeout: 5.0
)

consumer.each_batch do |batch|
  batch.each do |record|
    process_order(record.value)
  end
  batch.commit
end
```

### Parallel Consumer with Thread Pool

```ruby
require 'concurrent'

pool = Concurrent::FixedThreadPool.new(10)
consumer = kafka.consumer('orders', group: 'parallel-processor')

consumer.each do |record|
  pool.post do
    process_order(record.value)
    record.commit
  end
end

pool.shutdown
pool.wait_for_termination
```

### Consumer with Fibers (Async)

```ruby
require 'async'
require 'kafka-do/async'

Async do
  kafka = Kafka::AsyncClient.new

  kafka.consumer('orders', group: 'async-processor').each do |record|
    Async do
      process_order(record.value)
      record.commit
    end
  end
end
```

## Stream Processing

### Filter and Transform

```ruby
kafka.stream('orders')
  .filter { |order| order[:amount] > 100 }
  .map { |order| order.merge(tier: 'premium') }
  .to('high-value-orders')
```

### Windowed Aggregations

```ruby
kafka.stream('orders')
  .window(tumbling: '5m')
  .group_by { |order| order[:customer_id] }
  .count
  .each do |key, window|
    puts "Customer #{key}: #{window.value} orders in #{window.start}-#{window.end}"
  end
```

### Joins

```ruby
orders = kafka.stream('orders')
customers = kafka.stream('customers')

orders.join(customers,
  on: ->(order, customer) { order[:customer_id] == customer[:id] },
  window: '1h'
).each do |order, customer|
  puts "Order by #{customer[:name]}"
end
```

### Branching

```ruby
kafka.stream('orders').branch(
  [->(o) { o[:region] == 'us' }, 'us-orders'],
  [->(o) { o[:region] == 'eu' }, 'eu-orders'],
  [->(_) { true }, 'other-orders']
)
```

### Aggregations

```ruby
kafka.stream('orders')
  .group_by { |order| order[:customer_id] }
  .reduce(0) { |total, order| total + order[:amount] }
  .each do |customer_id, total|
    puts "Customer #{customer_id} total: $#{total}"
  end
```

## Topic Administration

```ruby
admin = kafka.admin

# Create topic
admin.create_topic('orders',
  partitions: 3,
  retention_ms: 7 * 24 * 60 * 60 * 1000 # 7 days
)

# List topics
admin.list_topics.each do |topic|
  puts "#{topic.name}: #{topic.partitions} partitions"
end

# Describe topic
info = admin.describe_topic('orders')
puts "Partitions: #{info.partitions}"
puts "Retention: #{info.retention_ms}ms"

# Alter topic
admin.alter_topic('orders',
  retention_ms: 30 * 24 * 60 * 60 * 1000 # 30 days
)

# Delete topic
admin.delete_topic('old-events')
```

## Consumer Groups

```ruby
admin = kafka.admin

# List groups
admin.list_groups.each do |group|
  puts "Group: #{group.id}, Members: #{group.member_count}"
end

# Describe group
info = admin.describe_group('order-processor')
puts "State: #{info.state}"
puts "Members: #{info.members}"
puts "Total Lag: #{info.total_lag}"

# Reset offsets
admin.reset_offsets('order-processor', 'orders', :earliest)

# Reset to timestamp
admin.reset_offsets('order-processor', 'orders', yesterday)
```

## Error Handling

```ruby
begin
  kafka.producer('orders') do |producer|
    producer.send(order)
  end
rescue Kafka::TopicNotFoundError
  puts 'Topic not found'
rescue Kafka::MessageTooLargeError
  puts 'Message too large'
rescue Kafka::TimeoutError
  puts 'Request timed out'
rescue Kafka::Error => e
  puts "Kafka error: #{e.code} - #{e.message}"

  if e.retriable?
    retry
  end
end
```

### Exception Hierarchy

```ruby
Kafka::Error # Base class
  Kafka::TopicNotFoundError
  Kafka::PartitionNotFoundError
  Kafka::MessageTooLargeError
  Kafka::NotLeaderError
  Kafka::OffsetOutOfRangeError
  Kafka::GroupCoordinatorError
  Kafka::RebalanceInProgressError
  Kafka::UnauthorizedError
  Kafka::QuotaExceededError
  Kafka::TimeoutError
  Kafka::DisconnectedError
  Kafka::SerializationError
```

### Dead Letter Queue Pattern

```ruby
consumer = kafka.consumer('orders', group: 'processor')
dlq_producer = kafka.producer('orders-dlq')

consumer.each do |record|
  begin
    process_order(record.value)
  rescue ProcessingError => e
    dlq_producer.send({
      original_record: record.value,
      error: e.message,
      timestamp: Time.now.iso8601
    }, headers: {
      'original-topic' => record.topic,
      'original-partition' => record.partition.to_s,
      'original-offset' => record.offset.to_s
    })
  end
  record.commit
end
```

### Retry with Exponential Backoff

```ruby
def with_retry(max_attempts: 3, base_delay: 1)
  attempts = 0
  begin
    attempts += 1
    yield
  rescue Kafka::Error => e
    raise unless e.retriable? && attempts < max_attempts

    delay = base_delay * (2 ** (attempts - 1))
    sleep(delay + rand(0.0..delay * 0.1))
    retry
  end
end

kafka.producer('orders') do |producer|
  with_retry do
    producer.send(order)
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
kafka = Kafka::Client.new(
  url: 'https://kafka.do',
  api_key: 'your-api-key',
  timeout: 30,
  retries: 3
)
```

### Producer Configuration

```ruby
kafka.producer('orders',
  batch_size: 16_384,
  linger_ms: 5,
  compression: :gzip,
  acks: :all,
  retries: 3,
  retry_backoff_ms: 100
) do |producer|
  producer.send(order)
end
```

### Consumer Configuration

```ruby
kafka.consumer('orders', group: 'processor',
  offset: :latest,
  auto_commit: false,
  fetch_min_bytes: 1,
  fetch_max_wait_ms: 500,
  max_poll_records: 500,
  session_timeout: 30_000,
  heartbeat_interval: 3_000
)
```

## Rails Integration

### Configuration

```ruby
# config/initializers/kafka.rb
Rails.application.config.kafka = Kafka::Client.new(
  url: Rails.application.credentials.kafka[:url],
  api_key: Rails.application.credentials.kafka[:api_key]
)
```

### Model Callbacks

```ruby
class Order < ApplicationRecord
  after_create :publish_to_kafka

  private

  def publish_to_kafka
    Rails.application.config.kafka.producer('orders') do |producer|
      producer.send(as_kafka_message)
    end
  end

  def as_kafka_message
    {
      id: id,
      customer_id: customer_id,
      amount: amount.to_f,
      created_at: created_at.iso8601
    }
  end
end
```

### Background Job Consumer

```ruby
# app/jobs/kafka_consumer_job.rb
class KafkaConsumerJob < ApplicationJob
  queue_as :kafka

  def perform
    kafka = Rails.application.config.kafka

    kafka.consumer('orders', group: 'rails-processor').each do |record|
      ProcessOrderJob.perform_later(record.value)
      record.commit
    end
  end
end
```

## Testing

### Mock Client

```ruby
require 'kafka-do/testing'

RSpec.describe OrderProcessor do
  let(:kafka) { Kafka::MockClient.new }

  it 'processes all orders' do
    # Seed test data
    kafka.seed('orders', [
      { order_id: '123', amount: 99.99 },
      { order_id: '124', amount: 149.99 }
    ])

    # Process
    processed = []
    kafka.consumer('orders', group: 'test').take(2).each do |record|
      processed << record.value
      record.commit
    end

    expect(processed.size).to eq(2)
    expect(processed.first[:order_id]).to eq('123')
  end

  it 'sends messages' do
    kafka.producer('orders') do |producer|
      producer.send(order_id: '125', amount: 99.99)
    end

    messages = kafka.messages('orders')
    expect(messages.size).to eq(1)
    expect(messages.first[:order_id]).to eq('125')
  end
end
```

### Integration Testing

```ruby
RSpec.describe 'Kafka Integration', :integration do
  let(:kafka) do
    Kafka::Client.new(
      url: ENV['TEST_KAFKA_URL'],
      api_key: ENV['TEST_KAFKA_API_KEY']
    )
  end

  let(:topic) { "test-orders-#{SecureRandom.uuid}" }

  after do
    kafka.admin.delete_topic(topic) rescue nil
  end

  it 'produces and consumes messages' do
    # Produce
    kafka.producer(topic) do |producer|
      producer.send(order_id: '123', amount: 99.99)
    end

    # Consume
    record = kafka.consumer(topic, group: 'test').first
    expect(record.value[:order_id]).to eq('123')
  end
end
```

## API Reference

### Kafka::Client

```ruby
class Kafka::Client
  def initialize(url: nil, api_key: nil, timeout: 30, retries: 3)
  def producer(topic, **options, &block)
  def async_producer(topic, **options)
  def consumer(topic, group:, **options)
  def batch_consumer(topic, group:, **options)
  def transaction(topic, &block)
  def stream(topic)
  def admin
  def close
end
```

### Kafka::Producer

```ruby
class Kafka::Producer
  def send(value, key: nil, headers: nil, partition: nil)
  def send_batch(records)
  def close
end
```

### Kafka::Consumer

```ruby
class Kafka::Consumer
  include Enumerable

  def each(&block)
  def take(n)
  def close
end
```

### Kafka::Record

```ruby
class Kafka::Record
  attr_reader :topic, :partition, :offset, :key, :value, :timestamp, :headers

  def commit
  def commit_async(&callback)
end
```

### Kafka::Stream

```ruby
class Kafka::Stream
  def filter(&block)
  def map(&block)
  def flat_map(&block)
  def window(tumbling: nil, sliding: nil, session: nil)
  def group_by(&block)
  def reduce(initial, &block)
  def count
  def branch(*conditions)
  def join(other, on:, window:)
  def to(topic)
  def each(&block)
end
```

## License

MIT
