# kafka_do

> Event Streaming for Elixir. Processes. Streams. Zero Ops.

```elixir
KafkaDo.produce("orders", %{order_id: "123", amount: 99.99})
```

GenServers. Pattern matching. The Elixir way.

## Installation

```elixir
# mix.exs
def deps do
  [
    {:kafka_do, "~> 0.1.0"}
  ]
end
```

```bash
mix deps.get
```

Requires Elixir 1.15+ and OTP 26+.

## Quick Start

```elixir
defmodule Order do
  defstruct [:order_id, :amount]
end

# Produce
KafkaDo.produce("orders", %Order{order_id: "123", amount: 99.99})

# Consume
KafkaDo.consume("orders", group: "my-processor", fn record ->
  IO.puts("Received: #{inspect(record.value)}")
  :ok
end)
```

## Application Configuration

```elixir
# config/config.exs
config :kafka_do,
  url: System.get_env("KAFKA_DO_URL", "https://kafka.do"),
  api_key: System.get_env("KAFKA_DO_API_KEY")
```

## Producing Messages

### Simple Producer

```elixir
# Send single message
KafkaDo.produce("orders", %{order_id: "123", amount: 99.99})

# Send with key for partitioning
KafkaDo.produce("orders", %{order_id: "123", amount: 99.99},
  key: "customer-456"
)

# Send with headers
KafkaDo.produce("orders", %{order_id: "123"},
  key: "customer-456",
  headers: %{"correlation-id" => "abc-123"}
)
```

### Batch Producer

```elixir
# Send batch
KafkaDo.produce_batch("orders", [
  %{order_id: "124", amount: 49.99},
  %{order_id: "125", amount: 149.99},
  %{order_id: "126", amount: 29.99}
])

# Send batch with keys
KafkaDo.produce_batch("orders", [
  %{key: "cust-1", value: %{order_id: "124"}},
  %{key: "cust-2", value: %{order_id: "125"}}
])
```

### Async Producer

```elixir
# Non-blocking send (returns immediately)
KafkaDo.produce_async("orders", %{order_id: "123"})

# With callback
KafkaDo.produce_async("orders", %{order_id: "123"},
  on_delivery: fn
    {:ok, metadata} -> IO.puts("Delivered to partition #{metadata.partition}")
    {:error, reason} -> IO.puts("Failed: #{inspect(reason)}")
  end
)
```

### Transactional Producer

```elixir
KafkaDo.transaction("orders", fn tx ->
  KafkaDo.Transaction.produce(tx, %{order_id: "123", status: :created})
  KafkaDo.Transaction.produce(tx, %{order_id: "123", status: :validated})
  # Automatically commits on :ok, aborts on error
  :ok
end)
```

## Consuming Messages

### Basic Consumer with Callback

```elixir
KafkaDo.consume("orders", group: "order-processor", fn record ->
  IO.puts("Topic: #{record.topic}")
  IO.puts("Partition: #{record.partition}")
  IO.puts("Offset: #{record.offset}")
  IO.puts("Key: #{record.key}")
  IO.puts("Value: #{inspect(record.value)}")
  IO.puts("Timestamp: #{record.timestamp}")
  IO.puts("Headers: #{inspect(record.headers)}")

  process_order(record.value)
  :ok  # Return :ok to commit
end)
```

### Consumer GenServer

```elixir
defmodule OrderConsumer do
  use KafkaDo.Consumer,
    topic: "orders",
    group: "order-processor"

  def handle_record(record, state) do
    case process_order(record.value) do
      :ok ->
        {:commit, state}
      {:error, reason} ->
        Logger.error("Processing failed: #{inspect(reason)}")
        {:skip, state}  # Skip and continue
    end
  end

  def handle_batch(records, state) do
    Enum.each(records, &process_order(&1.value))
    {:commit_all, state}
  end
end

# Start in supervision tree
children = [
  OrderConsumer
]
```

### Consumer with Stream

```elixir
"orders"
|> KafkaDo.stream(group: "processor")
|> Stream.filter(fn r -> r.value.amount > 100 end)
|> Stream.map(fn r -> r.value end)
|> Stream.each(&process_high_value_order/1)
|> Stream.run()

# Take specific number
"orders"
|> KafkaDo.stream(group: "processor")
|> Stream.take(10)
|> Enum.each(fn record ->
  process_order(record.value)
  KafkaDo.commit(record)
end)
```

### Consumer with Configuration

```elixir
config = %KafkaDo.ConsumerConfig{
  offset: :earliest,
  auto_commit: true,
  max_poll_records: 100,
  session_timeout: 30_000
}

KafkaDo.consume("orders", group: "processor", config: config, fn record ->
  process_order(record.value)
  :ok  # Auto-committed
end)
```

### Consumer from Timestamp

```elixir
yesterday = DateTime.utc_now() |> DateTime.add(-86400, :second)

config = %KafkaDo.ConsumerConfig{
  offset: {:timestamp, yesterday}
}

KafkaDo.consume("orders", group: "replay", config: config, fn record ->
  IO.puts("Replaying: #{inspect(record.value)}")
  :ok
end)
```

### Batch Consumer

```elixir
config = %KafkaDo.BatchConsumerConfig{
  batch_size: 100,
  batch_timeout: 5_000
}

KafkaDo.consume_batch("orders", group: "batch-processor", config: config, fn batch ->
  Enum.each(batch, fn record ->
    process_order(record.value)
  end)
  :ok  # Commits all
end)
```

### Parallel Consumer with Task

```elixir
defmodule ParallelConsumer do
  use KafkaDo.Consumer,
    topic: "orders",
    group: "parallel-processor"

  def handle_record(record, state) do
    Task.Supervisor.start_child(MyApp.TaskSupervisor, fn ->
      process_order(record.value)
      KafkaDo.commit(record)
    end)
    {:skip, state}  # Don't auto-commit, task will commit
  end
end
```

### Consumer with Broadway

```elixir
defmodule OrdersBroadway do
  use Broadway

  def start_link(_opts) do
    Broadway.start_link(__MODULE__,
      name: __MODULE__,
      producer: [
        module: {KafkaDo.Broadway.Producer, [
          topic: "orders",
          group: "broadway-processor"
        ]}
      ],
      processors: [
        default: [concurrency: 10]
      ],
      batchers: [
        default: [batch_size: 100, batch_timeout: 1000]
      ]
    )
  end

  def handle_message(_, message, _) do
    order = message.data
    process_order(order)
    message
  end

  def handle_batch(_, messages, _, _) do
    messages
  end
end
```

## Stream Processing

### Filter and Transform

```elixir
"orders"
|> KafkaDo.stream(group: "stream-processor")
|> Stream.filter(fn r -> r.value.amount > 100 end)
|> Stream.map(fn r -> Map.put(r.value, :tier, "premium") end)
|> KafkaDo.to("high-value-orders")
```

### Windowed Aggregations

```elixir
"orders"
|> KafkaDo.stream(group: "window-processor")
|> KafkaDo.Stream.window(tumbling: :timer.minutes(5))
|> KafkaDo.Stream.group_by(fn r -> r.value.customer_id end)
|> KafkaDo.Stream.count()
|> Stream.each(fn {key, window} ->
  IO.puts("Customer #{key}: #{window.value} orders in #{window.start}-#{window.end}")
end)
|> Stream.run()
```

### Joins

```elixir
orders = KafkaDo.stream("orders", group: "join-orders")
customers = KafkaDo.stream("customers", group: "join-customers")

KafkaDo.Stream.join(orders, customers,
  on: fn order, customer -> order.value.customer_id == customer.value.id end,
  window: :timer.hours(1)
)
|> Stream.each(fn {order, customer} ->
  IO.puts("Order by #{customer.value.name}")
end)
|> Stream.run()
```

### Branching

```elixir
"orders"
|> KafkaDo.stream(group: "branch-processor")
|> KafkaDo.Stream.branch([
  {fn r -> r.value.region == "us" end, "us-orders"},
  {fn r -> r.value.region == "eu" end, "eu-orders"},
  {fn _ -> true end, "other-orders"}
])
```

### Aggregations

```elixir
"orders"
|> KafkaDo.stream(group: "agg-processor")
|> KafkaDo.Stream.group_by(fn r -> r.value.customer_id end)
|> KafkaDo.Stream.reduce(0, fn r, total -> total + r.value.amount end)
|> Stream.each(fn {customer_id, total} ->
  IO.puts("Customer #{customer_id} total: $#{total}")
end)
|> Stream.run()
```

## Topic Administration

```elixir
# Create topic
KafkaDo.Admin.create_topic("orders",
  partitions: 3,
  retention_ms: 7 * 24 * 60 * 60 * 1000  # 7 days
)

# List topics
KafkaDo.Admin.list_topics()
|> Enum.each(fn topic ->
  IO.puts("#{topic.name}: #{topic.partitions} partitions")
end)

# Describe topic
info = KafkaDo.Admin.describe_topic("orders")
IO.puts("Partitions: #{info.partitions}")
IO.puts("Retention: #{info.retention_ms}ms")

# Alter topic
KafkaDo.Admin.alter_topic("orders",
  retention_ms: 30 * 24 * 60 * 60 * 1000  # 30 days
)

# Delete topic
KafkaDo.Admin.delete_topic("old-events")
```

## Consumer Groups

```elixir
# List groups
KafkaDo.Admin.list_groups()
|> Enum.each(fn group ->
  IO.puts("Group: #{group.id}, Members: #{group.member_count}")
end)

# Describe group
info = KafkaDo.Admin.describe_group("order-processor")
IO.puts("State: #{info.state}")
IO.puts("Members: #{length(info.members)}")
IO.puts("Total Lag: #{info.total_lag}")

# Reset offsets
KafkaDo.Admin.reset_offsets("order-processor", "orders", :earliest)

# Reset to timestamp
KafkaDo.Admin.reset_offsets("order-processor", "orders", {:timestamp, yesterday})
```

## Error Handling

```elixir
case KafkaDo.produce("orders", order) do
  {:ok, metadata} ->
    IO.puts("Sent to partition #{metadata.partition}")

  {:error, %KafkaDo.Error.TopicNotFound{}} ->
    IO.puts("Topic not found")

  {:error, %KafkaDo.Error.MessageTooLarge{}} ->
    IO.puts("Message too large")

  {:error, %KafkaDo.Error.Timeout{}} ->
    IO.puts("Request timed out")

  {:error, %KafkaDo.Error{} = e} when e.retriable? ->
    # Safe to retry
    retry_with_backoff(fn -> KafkaDo.produce("orders", order) end)

  {:error, %KafkaDo.Error{code: code, message: message}} ->
    IO.puts("Kafka error: #{code} - #{message}")
end
```

### Error Types

```elixir
defmodule KafkaDo.Error do
  defexception [:code, :message, :retriable?]
end

defmodule KafkaDo.Error.TopicNotFound do
  defexception [:topic]
end

defmodule KafkaDo.Error.PartitionNotFound do
  defexception [:topic, :partition]
end

defmodule KafkaDo.Error.MessageTooLarge do
  defexception [:size, :max_size]
end

defmodule KafkaDo.Error.NotLeader do
  defexception [:topic, :partition]
end

defmodule KafkaDo.Error.OffsetOutOfRange do
  defexception [:offset, :topic]
end

defmodule KafkaDo.Error.GroupCoordinator do
  defexception [:message]
end

defmodule KafkaDo.Error.RebalanceInProgress do
  defexception []
end

defmodule KafkaDo.Error.Unauthorized do
  defexception [:message]
end

defmodule KafkaDo.Error.QuotaExceeded do
  defexception []
end

defmodule KafkaDo.Error.Timeout do
  defexception []
end

defmodule KafkaDo.Error.Disconnected do
  defexception []
end

defmodule KafkaDo.Error.Serialization do
  defexception [:reason]
end
```

### Dead Letter Queue Pattern

```elixir
defmodule OrderConsumer do
  use KafkaDo.Consumer,
    topic: "orders",
    group: "processor"

  def handle_record(record, state) do
    case process_order(record.value) do
      :ok ->
        {:commit, state}

      {:error, reason} ->
        KafkaDo.produce("orders-dlq", %{
          original_record: record.value,
          error: inspect(reason),
          timestamp: DateTime.utc_now()
        }, headers: %{
          "original-topic" => record.topic,
          "original-partition" => to_string(record.partition),
          "original-offset" => to_string(record.offset)
        })
        {:commit, state}  # Commit to avoid reprocessing
    end
  end
end
```

### Retry with Exponential Backoff

```elixir
defmodule Retry do
  def with_backoff(fun, opts \\ []) do
    max_attempts = Keyword.get(opts, :max_attempts, 3)
    base_delay = Keyword.get(opts, :base_delay, 1_000)
    do_retry(fun, 1, max_attempts, base_delay)
  end

  defp do_retry(fun, attempt, max_attempts, base_delay) do
    case fun.() do
      {:ok, result} ->
        {:ok, result}

      {:error, %KafkaDo.Error{retriable?: true}} when attempt < max_attempts ->
        delay = base_delay * :math.pow(2, attempt - 1) |> round()
        jitter = :rand.uniform(div(delay, 10))
        Process.sleep(delay + jitter)
        do_retry(fun, attempt + 1, max_attempts, base_delay)

      {:error, _} = error ->
        error
    end
  end
end

Retry.with_backoff(fn -> KafkaDo.produce("orders", order) end)
```

## Configuration

### Environment Variables

```bash
export KAFKA_DO_URL=https://kafka.do
export KAFKA_DO_API_KEY=your-api-key
```

### Application Configuration

```elixir
# config/config.exs
config :kafka_do,
  url: "https://kafka.do",
  api_key: "your-api-key",
  timeout: 30_000,
  retries: 3

# Producer config
config :kafka_do, :producer,
  batch_size: 16_384,
  linger_ms: 5,
  compression: :gzip,
  acks: :all,
  retries: 3,
  retry_backoff_ms: 100

# Consumer config
config :kafka_do, :consumer,
  offset: :latest,
  auto_commit: false,
  fetch_min_bytes: 1,
  fetch_max_wait_ms: 500,
  max_poll_records: 500,
  session_timeout: 30_000,
  heartbeat_interval: 3_000
```

## Testing

### Mock Client

```elixir
defmodule OrderConsumerTest do
  use ExUnit.Case

  setup do
    {:ok, kafka} = KafkaDo.Mock.start_link()
    {:ok, kafka: kafka}
  end

  test "processes all orders", %{kafka: kafka} do
    # Seed test data
    KafkaDo.Mock.seed(kafka, "orders", [
      %{order_id: "123", amount: 99.99},
      %{order_id: "124", amount: 149.99}
    ])

    # Process
    processed =
      kafka
      |> KafkaDo.Mock.stream("orders", group: "test")
      |> Enum.take(2)
      |> Enum.map(& &1.value)

    assert length(processed) == 2
    assert hd(processed).order_id == "123"
  end

  test "sends messages", %{kafka: kafka} do
    KafkaDo.Mock.produce(kafka, "orders", %{order_id: "125"})

    messages = KafkaDo.Mock.get_messages(kafka, "orders")
    assert length(messages) == 1
    assert hd(messages).order_id == "125"
  end
end
```

### Integration Testing

```elixir
defmodule IntegrationTest do
  use ExUnit.Case

  @moduletag :integration

  setup_all do
    Application.put_env(:kafka_do, :url, System.get_env("TEST_KAFKA_URL"))
    Application.put_env(:kafka_do, :api_key, System.get_env("TEST_KAFKA_API_KEY"))

    topic = "test-orders-#{:crypto.strong_rand_bytes(8) |> Base.encode16()}"

    on_exit(fn ->
      KafkaDo.Admin.delete_topic(topic)
    end)

    {:ok, topic: topic}
  end

  test "end to end produces and consumes", %{topic: topic} do
    # Produce
    {:ok, _} = KafkaDo.produce(topic, %{order_id: "123"})

    # Consume
    record =
      topic
      |> KafkaDo.stream(group: "test")
      |> Enum.take(1)
      |> hd()

    assert record.value.order_id == "123"
  end
end
```

## API Reference

### KafkaDo

```elixir
# Producing
@spec produce(topic :: String.t(), value :: term(), opts :: keyword()) ::
  {:ok, RecordMetadata.t()} | {:error, Error.t()}

@spec produce_async(topic :: String.t(), value :: term(), opts :: keyword()) :: :ok

@spec produce_batch(topic :: String.t(), values :: [term()]) ::
  {:ok, [RecordMetadata.t()]} | {:error, Error.t()}

@spec transaction(topic :: String.t(), (Transaction.t() -> :ok | {:error, term()})) ::
  :ok | {:error, Error.t()}

# Consuming
@spec consume(topic :: String.t(), opts :: keyword(), handler :: (Record.t() -> :ok | {:error, term()})) ::
  :ok | {:error, Error.t()}

@spec stream(topic :: String.t(), opts :: keyword()) :: Enumerable.t()
```

### KafkaDo.Record

```elixir
defmodule KafkaDo.Record do
  @type t :: %__MODULE__{
    topic: String.t(),
    partition: non_neg_integer(),
    offset: non_neg_integer(),
    key: String.t() | nil,
    value: term(),
    timestamp: DateTime.t(),
    headers: %{String.t() => String.t()}
  }
end

@spec commit(record :: Record.t()) :: :ok | {:error, Error.t()}
```

### KafkaDo.Stream

```elixir
@spec filter(stream :: Enumerable.t(), predicate :: (term() -> boolean())) :: Enumerable.t()
@spec map(stream :: Enumerable.t(), mapper :: (term() -> term())) :: Enumerable.t()
@spec window(stream :: Enumerable.t(), opts :: keyword()) :: Enumerable.t()
@spec group_by(stream :: Enumerable.t(), key_fn :: (term() -> term())) :: Enumerable.t()
@spec reduce(stream :: Enumerable.t(), acc :: term(), reducer :: (term(), term() -> term())) :: Enumerable.t()
@spec count(stream :: Enumerable.t()) :: Enumerable.t()
@spec branch(stream :: Enumerable.t(), branches :: [{(term() -> boolean()), String.t()}]) :: :ok
@spec join(stream1 :: Enumerable.t(), stream2 :: Enumerable.t(), opts :: keyword()) :: Enumerable.t()
@spec to(stream :: Enumerable.t(), topic :: String.t()) :: :ok
```

## License

MIT
