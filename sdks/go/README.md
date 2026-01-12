# kafka-do

> Event Streaming for Go. Context-first. Errors as Values. Zero Ops.

```go
producer := kafka.NewProducer("orders")
err := producer.Send(ctx, kafka.Message{Value: order})
```

Context for cancellation. Value and error. The Go way.

## Installation

```bash
go get go.dotdo.dev/kafka
```

Requires Go 1.21+.

## Quick Start

```go
package main

import (
    "context"
    "log"

    "go.dotdo.dev/kafka"
)

func main() {
    ctx := context.Background()

    // Create client
    client, err := kafka.NewClient(ctx)
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close()

    // Produce
    producer := client.Producer("orders")
    err = producer.Send(ctx, kafka.Message{
        Value: map[string]any{"order_id": "123", "amount": 99.99},
    })
    if err != nil {
        log.Fatal(err)
    }

    // Consume
    consumer := client.Consumer("orders", "my-processor")
    for msg := range consumer.Messages(ctx) {
        log.Printf("Received: %v", msg.Value)
        if err := msg.Commit(ctx); err != nil {
            log.Printf("Commit failed: %v", err)
        }
    }
}
```

## Producing Messages

### Simple Producer

```go
client, _ := kafka.NewClient(ctx)
producer := client.Producer("orders")

// Send single message
err := producer.Send(ctx, kafka.Message{
    Value: map[string]any{"order_id": "123", "amount": 99.99},
})

// Send with key for partitioning
err = producer.Send(ctx, kafka.Message{
    Key:   "customer-456",
    Value: map[string]any{"order_id": "123", "amount": 99.99},
})

// Send with headers
err = producer.Send(ctx, kafka.Message{
    Value: map[string]any{"order_id": "123"},
    Headers: map[string]string{
        "correlation-id": "abc-123",
    },
})
```

### Typed Producer

```go
type Order struct {
    OrderID    string  `json:"order_id"`
    CustomerID string  `json:"customer_id"`
    Amount     float64 `json:"amount"`
}

// Use generics for type safety
producer := kafka.NewTypedProducer[Order](client, "orders")

err := producer.Send(ctx, Order{
    OrderID:    "123",
    CustomerID: "cust-456",
    Amount:     99.99,
})
```

### Batch Producer

```go
producer := client.Producer("orders")

// Send batch for high throughput
err := producer.SendBatch(ctx, []kafka.Message{
    {Value: map[string]any{"order_id": "124", "amount": 49.99}},
    {Value: map[string]any{"order_id": "125", "amount": 149.99}},
    {Value: map[string]any{"order_id": "126", "amount": 29.99}},
})
```

### Async Producer

```go
producer := client.AsyncProducer("orders")

// Non-blocking send
producer.Send(ctx, kafka.Message{
    Value: map[string]any{"order_id": "123"},
})

// Check delivery reports
for report := range producer.Reports() {
    if report.Err != nil {
        log.Printf("Failed to deliver: %v", report.Err)
    } else {
        log.Printf("Delivered to partition %d offset %d",
            report.Partition, report.Offset)
    }
}
```

## Consuming Messages

### Basic Consumer

```go
consumer := client.Consumer("orders", "order-processor")

// Iterate over messages
for msg := range consumer.Messages(ctx) {
    log.Printf("Topic: %s", msg.Topic)
    log.Printf("Partition: %d", msg.Partition)
    log.Printf("Offset: %d", msg.Offset)
    log.Printf("Key: %s", msg.Key)
    log.Printf("Value: %v", msg.Value)
    log.Printf("Timestamp: %v", msg.Timestamp)
    log.Printf("Headers: %v", msg.Headers)

    // Process the message
    if err := processOrder(msg.Value); err != nil {
        log.Printf("Processing failed: %v", err)
        continue
    }

    // Commit offset
    if err := msg.Commit(ctx); err != nil {
        log.Printf("Commit failed: %v", err)
    }
}
```

### Typed Consumer

```go
type Order struct {
    OrderID string  `json:"order_id"`
    Amount  float64 `json:"amount"`
}

consumer := kafka.NewTypedConsumer[Order](client, "orders", "processor")

for msg := range consumer.Messages(ctx) {
    order := msg.Value // Type is Order, not any
    log.Printf("Order %s: $%.2f", order.OrderID, order.Amount)
    msg.Commit(ctx)
}
```

### Consumer with Options

```go
consumer := client.Consumer("orders", "processor",
    kafka.WithOffset(kafka.OffsetEarliest),
    kafka.WithAutoCommit(true),
    kafka.WithMaxPollRecords(100),
    kafka.WithSessionTimeout(30*time.Second),
)

for msg := range consumer.Messages(ctx) {
    processOrder(msg.Value)
    // Auto-committed
}
```

### Consumer from Timestamp

```go
yesterday := time.Now().Add(-24 * time.Hour)

consumer := client.Consumer("orders", "replay-processor",
    kafka.WithOffsetTime(yesterday),
)

for msg := range consumer.Messages(ctx) {
    log.Printf("Replaying: %v", msg.Value)
}
```

### Batch Consumer

```go
consumer := client.Consumer("orders", "batch-processor",
    kafka.WithBatchSize(100),
    kafka.WithBatchTimeout(5*time.Second),
)

for batch := range consumer.Batches(ctx) {
    // Process batch
    for _, msg := range batch.Messages {
        processOrder(msg.Value)
    }

    // Commit all at once
    if err := batch.Commit(ctx); err != nil {
        log.Printf("Batch commit failed: %v", err)
    }
}
```

### Parallel Consumer

```go
import "golang.org/x/sync/errgroup"

consumer := client.Consumer("orders", "parallel-processor")

g, ctx := errgroup.WithContext(ctx)
const workers = 10

// Create buffered channel
msgs := make(chan *kafka.Message, 100)

// Spawn workers
for i := 0; i < workers; i++ {
    g.Go(func() error {
        for msg := range msgs {
            if err := processOrder(msg.Value); err != nil {
                return err
            }
            msg.Commit(ctx)
        }
        return nil
    })
}

// Feed messages to workers
g.Go(func() error {
    defer close(msgs)
    for msg := range consumer.Messages(ctx) {
        select {
        case msgs <- msg:
        case <-ctx.Done():
            return ctx.Err()
        }
    }
    return nil
})

if err := g.Wait(); err != nil {
    log.Fatal(err)
}
```

## Stream Processing

### Filter and Transform

```go
stream := kafka.NewStream(client, "orders")

// Build pipeline
filtered := stream.
    Filter(func(msg *kafka.Message) bool {
        order := msg.Value.(map[string]any)
        return order["amount"].(float64) > 100
    }).
    Map(func(msg *kafka.Message) any {
        order := msg.Value.(map[string]any)
        order["tier"] = "premium"
        return order
    })

// Materialize to topic
if err := filtered.To(ctx, "high-value-orders"); err != nil {
    log.Fatal(err)
}
```

### Typed Stream Processing

```go
type Order struct {
    OrderID string  `json:"order_id"`
    Amount  float64 `json:"amount"`
    Tier    string  `json:"tier,omitempty"`
}

type PremiumOrder struct {
    Order
    Tier string `json:"tier"`
}

stream := kafka.NewTypedStream[Order](client, "orders")

premium := stream.
    Filter(func(order Order) bool {
        return order.Amount > 100
    }).
    Map(func(order Order) PremiumOrder {
        return PremiumOrder{Order: order, Tier: "premium"}
    })

premium.To(ctx, "high-value-orders")
```

### Windowed Aggregations

```go
stream := kafka.NewStream(client, "orders")

// Count orders per customer in 5-minute tumbling windows
counts := stream.
    Window(kafka.TumblingWindow(5 * time.Minute)).
    GroupBy(func(msg *kafka.Message) string {
        return msg.Value.(map[string]any)["customer_id"].(string)
    }).
    Count()

for window := range counts.Results(ctx) {
    log.Printf("Customer %s: %d orders in window %v-%v",
        window.Key, window.Value, window.Start, window.End)
}
```

### Joins

```go
orders := kafka.NewStream(client, "orders")
customers := kafka.NewStream(client, "customers")

// Join orders with customers
joined := orders.Join(customers,
    kafka.JoinOn(func(order, customer any) bool {
        o := order.(map[string]any)
        c := customer.(map[string]any)
        return o["customer_id"] == c["id"]
    }),
    kafka.JoinWindow(time.Hour),
)

for result := range joined.Results(ctx) {
    order, customer := result.Left, result.Right
    log.Printf("Order by %s", customer.(map[string]any)["name"])
}
```

### Branching

```go
stream := kafka.NewStream(client, "orders")

// Route to different topics
stream.Branch(ctx,
    kafka.Branch{
        Predicate: func(msg *kafka.Message) bool {
            return msg.Value.(map[string]any)["region"] == "us"
        },
        Topic: "us-orders",
    },
    kafka.Branch{
        Predicate: func(msg *kafka.Message) bool {
            return msg.Value.(map[string]any)["region"] == "eu"
        },
        Topic: "eu-orders",
    },
    kafka.Branch{
        Predicate: func(_ *kafka.Message) bool { return true },
        Topic:     "other-orders",
    },
)
```

## Topic Administration

```go
admin := client.Admin()

// Create topic
err := admin.CreateTopic(ctx, "orders", kafka.TopicConfig{
    Partitions:  3,
    RetentionMs: 7 * 24 * 60 * 60 * 1000, // 7 days
})

// List topics
topics, err := admin.ListTopics(ctx)
for _, topic := range topics {
    log.Printf("%s: %d partitions", topic.Name, topic.Partitions)
}

// Describe topic
info, err := admin.DescribeTopic(ctx, "orders")
log.Printf("Partitions: %d", info.Partitions)
log.Printf("Retention: %dms", info.RetentionMs)

// Alter topic
err = admin.AlterTopic(ctx, "orders", kafka.TopicConfig{
    RetentionMs: 30 * 24 * 60 * 60 * 1000, // 30 days
})

// Delete topic
err = admin.DeleteTopic(ctx, "old-events")
```

## Consumer Groups

```go
admin := client.Admin()

// List groups
groups, err := admin.ListGroups(ctx)
for _, group := range groups {
    log.Printf("Group: %s, Members: %d", group.ID, group.MemberCount)
}

// Describe group
info, err := admin.DescribeGroup(ctx, "order-processor")
log.Printf("State: %s", info.State)
log.Printf("Members: %d", len(info.Members))
log.Printf("Total Lag: %d", info.TotalLag)

// Reset offsets to earliest
err = admin.ResetOffsets(ctx, "order-processor", "orders", kafka.OffsetEarliest)

// Reset offsets to timestamp
err = admin.ResetOffsetsToTime(ctx, "order-processor", "orders", yesterday)
```

## Error Handling

```go
import "errors"

producer := client.Producer("orders")

err := producer.Send(ctx, kafka.Message{Value: order})
if err != nil {
    var kafkaErr *kafka.Error
    if errors.As(err, &kafkaErr) {
        switch kafkaErr.Code {
        case kafka.ErrTopicNotFound:
            log.Printf("Topic not found")
        case kafka.ErrMessageTooLarge:
            log.Printf("Message too large")
        case kafka.ErrNotLeader:
            // Retriable
            log.Printf("Not leader, retrying...")
        default:
            log.Printf("Kafka error: %s - %s", kafkaErr.Code, kafkaErr.Message)
        }
        if kafkaErr.Retriable {
            // Safe to retry
        }
    }

    if errors.Is(err, context.DeadlineExceeded) {
        log.Printf("Request timed out")
    }

    if errors.Is(err, context.Canceled) {
        log.Printf("Request canceled")
    }
}
```

### Error Constants

```go
const (
    ErrTopicNotFound     = "topic_not_found"
    ErrPartitionNotFound = "partition_not_found"
    ErrMessageTooLarge   = "message_too_large"
    ErrNotLeader         = "not_leader"
    ErrOffsetOutOfRange  = "offset_out_of_range"
    ErrGroupCoordinator  = "group_coordinator"
    ErrRebalanceInProgress = "rebalance_in_progress"
    ErrUnauthorized      = "unauthorized"
    ErrQuotaExceeded     = "quota_exceeded"
)

var (
    ErrClosed       = errors.New("kafka: client closed")
    ErrDisconnected = errors.New("kafka: disconnected")
)
```

### Dead Letter Queue

```go
consumer := client.Consumer("orders", "processor")
dlqProducer := client.Producer("orders-dlq")

for msg := range consumer.Messages(ctx) {
    if err := processOrder(msg.Value); err != nil {
        // Send to DLQ
        dlqProducer.Send(ctx, kafka.Message{
            Value: map[string]any{
                "original":  msg.Value,
                "error":     err.Error(),
                "timestamp": time.Now(),
            },
            Headers: map[string]string{
                "original-topic":     msg.Topic,
                "original-partition": strconv.Itoa(msg.Partition),
                "original-offset":    strconv.FormatInt(msg.Offset, 10),
            },
        })
    }
    msg.Commit(ctx)
}
```

## Configuration

### Environment Variables

```bash
export KAFKA_DO_URL="https://kafka.do"
export KAFKA_DO_API_KEY="your-api-key"
```

### Client Options

```go
client, err := kafka.NewClient(ctx,
    kafka.WithURL("https://kafka.do"),
    kafka.WithAPIKey("your-api-key"),
    kafka.WithTimeout(30*time.Second),
    kafka.WithRetries(3),
    kafka.WithLogger(slog.Default()),
)
```

### Producer Options

```go
producer := client.Producer("orders",
    kafka.WithBatchSize(16384),
    kafka.WithLingerMs(5),
    kafka.WithCompression(kafka.CompressionGzip),
    kafka.WithAcks(kafka.AcksAll),
    kafka.WithProducerRetries(3),
)
```

### Consumer Options

```go
consumer := client.Consumer("orders", "processor",
    kafka.WithOffset(kafka.OffsetLatest),
    kafka.WithAutoCommit(false),
    kafka.WithFetchMinBytes(1),
    kafka.WithFetchMaxWaitMs(500),
    kafka.WithMaxPollRecords(500),
    kafka.WithSessionTimeout(30*time.Second),
    kafka.WithHeartbeatInterval(3*time.Second),
)
```

## Testing

### Mock Client

```go
import (
    "testing"
    "go.dotdo.dev/kafka/mock"
)

func TestOrderProcessing(t *testing.T) {
    client := mock.NewClient()

    // Seed test data
    client.Seed("orders", []kafka.Message{
        {Value: map[string]any{"order_id": "123", "amount": 99.99}},
        {Value: map[string]any{"order_id": "124", "amount": 149.99}},
    })

    // Process
    consumer := client.Consumer("orders", "test")
    ctx, cancel := context.WithTimeout(context.Background(), time.Second)
    defer cancel()

    count := 0
    for msg := range consumer.Messages(ctx) {
        count++
        msg.Commit(ctx)
        if count == 2 {
            break
        }
    }

    if count != 2 {
        t.Errorf("expected 2 messages, got %d", count)
    }
}

func TestProducer(t *testing.T) {
    client := mock.NewClient()
    producer := client.Producer("orders")

    producer.Send(context.Background(), kafka.Message{
        Value: map[string]any{"order_id": "125"},
    })

    // Verify
    messages := client.GetMessages("orders")
    if len(messages) != 1 {
        t.Errorf("expected 1 message, got %d", len(messages))
    }
}
```

### Integration Testing

```go
func TestIntegration(t *testing.T) {
    if testing.Short() {
        t.Skip("skipping integration test")
    }

    ctx := context.Background()
    client, err := kafka.NewClient(ctx,
        kafka.WithURL(os.Getenv("TEST_KAFKA_URL")),
        kafka.WithAPIKey(os.Getenv("TEST_KAFKA_API_KEY")),
    )
    if err != nil {
        t.Fatal(err)
    }
    defer client.Close()

    topic := fmt.Sprintf("test-orders-%s", uuid.New())
    t.Cleanup(func() {
        client.Admin().DeleteTopic(ctx, topic)
    })

    // Produce
    producer := client.Producer(topic)
    err = producer.Send(ctx, kafka.Message{
        Value: map[string]any{"order_id": "123"},
    })
    if err != nil {
        t.Fatal(err)
    }

    // Consume
    consumer := client.Consumer(topic, "test")
    for msg := range consumer.Messages(ctx) {
        order := msg.Value.(map[string]any)
        if order["order_id"] != "123" {
            t.Errorf("unexpected order_id: %v", order["order_id"])
        }
        break
    }
}
```

## API Reference

### Client

```go
type Client struct{}

func NewClient(ctx context.Context, opts ...Option) (*Client, error)
func (c *Client) Close() error
func (c *Client) Producer(topic string, opts ...ProducerOption) *Producer
func (c *Client) AsyncProducer(topic string, opts ...ProducerOption) *AsyncProducer
func (c *Client) Consumer(topic string, group string, opts ...ConsumerOption) *Consumer
func (c *Client) Admin() *Admin
```

### Producer

```go
type Producer struct{}

func (p *Producer) Send(ctx context.Context, msg Message) error
func (p *Producer) SendBatch(ctx context.Context, msgs []Message) error

type AsyncProducer struct{}

func (p *AsyncProducer) Send(ctx context.Context, msg Message)
func (p *AsyncProducer) Reports() <-chan DeliveryReport
func (p *AsyncProducer) Close() error
```

### Consumer

```go
type Consumer struct{}

func (c *Consumer) Messages(ctx context.Context) <-chan *Message
func (c *Consumer) Batches(ctx context.Context) <-chan *Batch
func (c *Consumer) Close() error
```

### Message

```go
type Message struct {
    Topic     string
    Partition int
    Offset    int64
    Key       string
    Value     any
    Timestamp time.Time
    Headers   map[string]string
}

func (m *Message) Commit(ctx context.Context) error
```

### Stream

```go
type Stream[T any] struct{}

func NewStream(client *Client, topic string) *Stream[any]
func NewTypedStream[T any](client *Client, topic string) *Stream[T]

func (s *Stream[T]) Filter(fn func(T) bool) *Stream[T]
func (s *Stream[T]) Map(fn func(T) R) *Stream[R]
func (s *Stream[T]) FlatMap(fn func(T) []R) *Stream[R]
func (s *Stream[T]) Window(w Window) *WindowedStream[T]
func (s *Stream[T]) GroupBy(fn func(T) K) *GroupedStream[K, T]
func (s *Stream[T]) Branch(ctx context.Context, branches ...Branch) error
func (s *Stream[T]) Join(other *Stream[R], opts ...JoinOption) *JoinedStream[T, R]
func (s *Stream[T]) To(ctx context.Context, topic string) error
func (s *Stream[T]) Results(ctx context.Context) <-chan T
```

## License

MIT
