# Kafka.Do

> Event Streaming for .NET. Async/Await. LINQ. Zero Ops.

```csharp
var producer = kafka.Producer<Order>("orders");
await producer.SendAsync(new Order("123", 99.99m));
```

Async patterns. Generics. The .NET way.

## Installation

```bash
dotnet add package DotDo.Kafka
```

Requires .NET 8.0+.

## Quick Start

```csharp
using DotDo.Kafka;

var kafka = new KafkaClient();

// Produce
var producer = kafka.Producer<Order>("orders");
await producer.SendAsync(new Order("123", 99.99m));

// Consume
await foreach (var record in kafka.Consumer<Order>("orders", "my-processor"))
{
    Console.WriteLine($"Received: {record.Value}");
    await record.CommitAsync();
}

record Order(string OrderId, decimal Amount);
```

## Producing Messages

### Simple Producer

```csharp
var kafka = new KafkaClient();
var producer = kafka.Producer<Order>("orders");

// Send single message
await producer.SendAsync(new Order("123", 99.99m));

// Send with key for partitioning
await producer.SendAsync("customer-456", new Order("123", 99.99m));

// Send with headers
await producer.SendAsync(new Message<Order>
{
    Key = "customer-456",
    Value = new Order("123", 99.99m),
    Headers = new Dictionary<string, string>
    {
        ["correlation-id"] = "abc-123"
    }
});
```

### Batch Producer

```csharp
var producer = kafka.Producer<Order>("orders");

// Send batch
await producer.SendBatchAsync(new[]
{
    new Order("124", 49.99m),
    new Order("125", 149.99m),
    new Order("126", 29.99m)
});

// Send batch with keys
await producer.SendBatchAsync(new[]
{
    new Message<Order> { Key = "cust-1", Value = new Order("124", 49.99m) },
    new Message<Order> { Key = "cust-2", Value = new Order("125", 149.99m) }
});
```

### Fire-and-Forget Producer

```csharp
var producer = kafka.Producer<Order>("orders");

// Non-blocking send
_ = producer.SendAsync(order); // Fire and forget

// Or with logging on failure
producer.SendAsync(order).ContinueWith(t =>
{
    if (t.IsFaulted)
        logger.LogError(t.Exception, "Failed to send message");
});
```

### Transactional Producer

```csharp
await using var transaction = await kafka.BeginTransactionAsync<Order>("orders");

await transaction.SendAsync(new Order("123", 99.99m));
await transaction.SendAsync(new Order("124", 49.99m));

await transaction.CommitAsync();
// Or: await transaction.AbortAsync();
```

## Consuming Messages

### Basic Consumer with IAsyncEnumerable

```csharp
var consumer = kafka.Consumer<Order>("orders", "order-processor");

await foreach (var record in consumer)
{
    Console.WriteLine($"Topic: {record.Topic}");
    Console.WriteLine($"Partition: {record.Partition}");
    Console.WriteLine($"Offset: {record.Offset}");
    Console.WriteLine($"Key: {record.Key}");
    Console.WriteLine($"Value: {record.Value}");
    Console.WriteLine($"Timestamp: {record.Timestamp}");
    Console.WriteLine($"Headers: {string.Join(", ", record.Headers)}");

    await ProcessOrderAsync(record.Value);
    await record.CommitAsync();
}
```

### Consumer with LINQ

```csharp
var consumer = kafka.Consumer<Order>("orders", "processor");

await foreach (var order in consumer
    .Where(r => r.Value.Amount > 100)
    .Select(r => r.Value))
{
    await ProcessHighValueOrderAsync(order);
}
```

### Consumer with Cancellation

```csharp
using var cts = new CancellationTokenSource();

// Cancel after 1 hour
cts.CancelAfter(TimeSpan.FromHours(1));

try
{
    await foreach (var record in kafka.Consumer<Order>("orders", "processor")
        .WithCancellation(cts.Token))
    {
        await ProcessAsync(record.Value);
        await record.CommitAsync();
    }
}
catch (OperationCanceledException)
{
    Console.WriteLine("Consumer stopped");
}
```

### Consumer with Options

```csharp
var options = new ConsumerOptions
{
    Offset = Offset.Earliest,
    AutoCommit = true,
    MaxPollRecords = 100,
    SessionTimeout = TimeSpan.FromSeconds(30)
};

await foreach (var record in kafka.Consumer<Order>("orders", "processor", options))
{
    await ProcessAsync(record.Value);
    // Auto-committed
}
```

### Consumer from Timestamp

```csharp
var yesterday = DateTimeOffset.UtcNow.AddDays(-1);

var options = new ConsumerOptions
{
    Offset = Offset.FromTimestamp(yesterday)
};

await foreach (var record in kafka.Consumer<Order>("orders", "replay", options))
{
    Console.WriteLine($"Replaying: {record.Value}");
}
```

### Batch Consumer

```csharp
var options = new BatchConsumerOptions
{
    BatchSize = 100,
    BatchTimeout = TimeSpan.FromSeconds(5)
};

var consumer = kafka.BatchConsumer<Order>("orders", "batch-processor", options);

await foreach (var batch in consumer)
{
    foreach (var record in batch)
    {
        await ProcessAsync(record.Value);
    }
    await batch.CommitAsync();
}
```

### Parallel Consumer with Channels

```csharp
using System.Threading.Channels;

var channel = Channel.CreateBounded<KafkaRecord<Order>>(100);
var consumer = kafka.Consumer<Order>("orders", "parallel-processor");

// Start workers
var workers = Enumerable.Range(0, 10)
    .Select(_ => Task.Run(async () =>
    {
        await foreach (var record in channel.Reader.ReadAllAsync())
        {
            await ProcessAsync(record.Value);
            await record.CommitAsync();
        }
    }))
    .ToArray();

// Feed records to workers
await foreach (var record in consumer)
{
    await channel.Writer.WriteAsync(record);
}

channel.Writer.Complete();
await Task.WhenAll(workers);
```

### Hosted Service Consumer

```csharp
public class OrderConsumerService : BackgroundService
{
    private readonly IKafkaClient _kafka;
    private readonly ILogger<OrderConsumerService> _logger;

    public OrderConsumerService(IKafkaClient kafka, ILogger<OrderConsumerService> logger)
    {
        _kafka = kafka;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await foreach (var record in _kafka.Consumer<Order>("orders", "processor")
            .WithCancellation(stoppingToken))
        {
            try
            {
                await ProcessOrderAsync(record.Value);
                await record.CommitAsync();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error processing order {OrderId}", record.Value.OrderId);
            }
        }
    }
}
```

## Stream Processing

### Filter and Transform

```csharp
using DotDo.Kafka.Streams;

var stream = kafka.Stream<Order>("orders");

await stream
    .Where(order => order.Amount > 100)
    .Select(order => new PremiumOrder(order, "premium"))
    .ToAsync("high-value-orders");
```

### Windowed Aggregations

```csharp
await kafka.Stream<Order>("orders")
    .Window(Window.Tumbling(TimeSpan.FromMinutes(5)))
    .GroupBy(order => order.CustomerId)
    .Count()
    .ForEachAsync((key, window) =>
    {
        Console.WriteLine($"Customer {key}: {window.Value} orders in {window.Start}-{window.End}");
    });
```

### Joins

```csharp
var orders = kafka.Stream<Order>("orders");
var customers = kafka.Stream<Customer>("customers");

await orders
    .Join(
        customers,
        (order, customer) => order.CustomerId == customer.Id,
        TimeSpan.FromHours(1))
    .ForEachAsync((order, customer) =>
    {
        Console.WriteLine($"Order by {customer.Name}");
    });
```

### Branching

```csharp
await kafka.Stream<Order>("orders")
    .Branch(
        (order => order.Region == "us", "us-orders"),
        (order => order.Region == "eu", "eu-orders"),
        (_ => true, "other-orders")
    );
```

### Aggregations

```csharp
await kafka.Stream<Order>("orders")
    .GroupBy(order => order.CustomerId)
    .Aggregate(
        seed: 0m,
        (total, order) => total + order.Amount)
    .ForEachAsync((customerId, total) =>
    {
        Console.WriteLine($"Customer {customerId} total: {total:C}");
    });
```

## Topic Administration

```csharp
var admin = kafka.Admin;

// Create topic
await admin.CreateTopicAsync("orders", new TopicConfig
{
    Partitions = 3,
    RetentionMs = (long)TimeSpan.FromDays(7).TotalMilliseconds
});

// List topics
await foreach (var topic in admin.ListTopicsAsync())
{
    Console.WriteLine($"{topic.Name}: {topic.Partitions} partitions");
}

// Describe topic
var info = await admin.DescribeTopicAsync("orders");
Console.WriteLine($"Partitions: {info.Partitions}");
Console.WriteLine($"Retention: {info.RetentionMs}ms");

// Alter topic
await admin.AlterTopicAsync("orders", new TopicConfig
{
    RetentionMs = (long)TimeSpan.FromDays(30).TotalMilliseconds
});

// Delete topic
await admin.DeleteTopicAsync("old-events");
```

## Consumer Groups

```csharp
var admin = kafka.Admin;

// List groups
await foreach (var group in admin.ListGroupsAsync())
{
    Console.WriteLine($"Group: {group.Id}, Members: {group.MemberCount}");
}

// Describe group
var info = await admin.DescribeGroupAsync("order-processor");
Console.WriteLine($"State: {info.State}");
Console.WriteLine($"Members: {info.Members.Count}");
Console.WriteLine($"Total Lag: {info.TotalLag}");

// Reset offsets
await admin.ResetOffsetsAsync("order-processor", "orders", Offset.Earliest);

// Reset to timestamp
await admin.ResetOffsetsAsync("order-processor", "orders", Offset.FromTimestamp(yesterday));
```

## Error Handling

```csharp
using DotDo.Kafka.Exceptions;

var producer = kafka.Producer<Order>("orders");

try
{
    await producer.SendAsync(order);
}
catch (TopicNotFoundException)
{
    Console.Error.WriteLine("Topic not found");
}
catch (MessageTooLargeException)
{
    Console.Error.WriteLine("Message too large");
}
catch (TimeoutException)
{
    Console.Error.WriteLine("Request timed out");
}
catch (KafkaException ex) when (ex.IsRetriable)
{
    // Safe to retry
    await RetryAsync(() => producer.SendAsync(order));
}
catch (KafkaException ex)
{
    Console.Error.WriteLine($"Kafka error: {ex.Code} - {ex.Message}");
}
```

### Exception Hierarchy

```csharp
KafkaException (base)
├── TopicNotFoundException
├── PartitionNotFoundException
├── MessageTooLargeException
├── NotLeaderException
├── OffsetOutOfRangeException
├── GroupCoordinatorException
├── RebalanceInProgressException
├── UnauthorizedException
├── QuotaExceededException
├── TimeoutException
├── DisconnectedException
└── SerializationException
```

### Dead Letter Queue with Polly

```csharp
using Polly;

var consumer = kafka.Consumer<Order>("orders", "processor");
var dlqProducer = kafka.Producer<DlqRecord>("orders-dlq");

var retryPolicy = Policy
    .Handle<ProcessingException>()
    .RetryAsync(3);

await foreach (var record in consumer)
{
    try
    {
        await retryPolicy.ExecuteAsync(() => ProcessOrderAsync(record.Value));
    }
    catch (ProcessingException ex)
    {
        await dlqProducer.SendAsync(new Message<DlqRecord>
        {
            Value = new DlqRecord(record.Value, ex.Message, DateTimeOffset.UtcNow),
            Headers = new Dictionary<string, string>
            {
                ["original-topic"] = record.Topic,
                ["original-partition"] = record.Partition.ToString(),
                ["original-offset"] = record.Offset.ToString()
            }
        });
    }
    await record.CommitAsync();
}
```

## Dependency Injection

```csharp
// Program.cs
builder.Services.AddKafka(options =>
{
    options.Url = "https://kafka.do";
    options.ApiKey = builder.Configuration["Kafka:ApiKey"];
});

// Or with configuration section
builder.Services.AddKafka(builder.Configuration.GetSection("Kafka"));
```

```json
// appsettings.json
{
  "Kafka": {
    "Url": "https://kafka.do",
    "ApiKey": "your-api-key",
    "Timeout": "00:00:30",
    "Retries": 3
  }
}
```

```csharp
// Usage in services
public class OrderService
{
    private readonly IKafkaClient _kafka;

    public OrderService(IKafkaClient kafka)
    {
        _kafka = kafka;
    }

    public async Task PublishOrderAsync(Order order)
    {
        var producer = _kafka.Producer<Order>("orders");
        await producer.SendAsync(order);
    }
}
```

## Configuration

### Environment Variables

```bash
export KAFKA_DO_URL=https://kafka.do
export KAFKA_DO_API_KEY=your-api-key
```

### Client Configuration

```csharp
var config = new KafkaConfig
{
    Url = "https://kafka.do",
    ApiKey = "your-api-key",
    Timeout = TimeSpan.FromSeconds(30),
    Retries = 3
};

var kafka = new KafkaClient(config);
```

### Producer Configuration

```csharp
var options = new ProducerOptions
{
    BatchSize = 16384,
    LingerMs = 5,
    Compression = Compression.Gzip,
    Acks = Acks.All,
    Retries = 3,
    RetryBackoffMs = 100
};

var producer = kafka.Producer<Order>("orders", options);
```

### Consumer Configuration

```csharp
var options = new ConsumerOptions
{
    Offset = Offset.Latest,
    AutoCommit = false,
    FetchMinBytes = 1,
    FetchMaxWaitMs = 500,
    MaxPollRecords = 500,
    SessionTimeout = TimeSpan.FromSeconds(30),
    HeartbeatInterval = TimeSpan.FromSeconds(3)
};

var consumer = kafka.Consumer<Order>("orders", "processor", options);
```

## Testing

### Mock Client

```csharp
using DotDo.Kafka.Testing;

public class OrderProcessorTests
{
    [Fact]
    public async Task ProcessOrders_ProcessesAllMessages()
    {
        var kafka = new MockKafkaClient();

        // Seed test data
        kafka.Seed("orders", new[]
        {
            new Order("123", 99.99m),
            new Order("124", 149.99m)
        });

        // Process
        var processed = new List<Order>();
        var consumer = kafka.Consumer<Order>("orders", "test");

        await foreach (var record in consumer.Take(2))
        {
            processed.Add(record.Value);
            await record.CommitAsync();
        }

        Assert.Equal(2, processed.Count);
        Assert.Equal("123", processed[0].OrderId);
    }

    [Fact]
    public async Task Producer_SendsMessage()
    {
        var kafka = new MockKafkaClient();
        var producer = kafka.Producer<Order>("orders");

        await producer.SendAsync(new Order("125", 99.99m));

        var messages = kafka.GetMessages<Order>("orders");
        Assert.Single(messages);
        Assert.Equal("125", messages[0].OrderId);
    }
}
```

### Integration Testing

```csharp
public class IntegrationTests : IAsyncLifetime
{
    private IKafkaClient _kafka;
    private string _topic;

    public async Task InitializeAsync()
    {
        _kafka = new KafkaClient(new KafkaConfig
        {
            Url = Environment.GetEnvironmentVariable("TEST_KAFKA_URL"),
            ApiKey = Environment.GetEnvironmentVariable("TEST_KAFKA_API_KEY")
        });
        _topic = $"test-orders-{Guid.NewGuid()}";
    }

    public async Task DisposeAsync()
    {
        await _kafka.Admin.DeleteTopicAsync(_topic);
        _kafka.Dispose();
    }

    [Fact]
    public async Task EndToEnd_ProducesAndConsumes()
    {
        // Produce
        var producer = _kafka.Producer<Order>(_topic);
        await producer.SendAsync(new Order("123", 99.99m));

        // Consume
        var consumer = _kafka.Consumer<Order>(_topic, "test");
        await foreach (var record in consumer.Take(1))
        {
            Assert.Equal("123", record.Value.OrderId);
        }
    }
}
```

## API Reference

### IKafkaClient

```csharp
public interface IKafkaClient : IDisposable
{
    IProducer<T> Producer<T>(string topic, ProducerOptions? options = null);
    IAsyncEnumerable<KafkaRecord<T>> Consumer<T>(string topic, string group, ConsumerOptions? options = null);
    IAsyncEnumerable<Batch<T>> BatchConsumer<T>(string topic, string group, BatchConsumerOptions? options = null);
    Task<ITransaction<T>> BeginTransactionAsync<T>(string topic);
    IStream<T> Stream<T>(string topic);
    IAdmin Admin { get; }
}
```

### IProducer

```csharp
public interface IProducer<T>
{
    Task<RecordMetadata> SendAsync(T value, CancellationToken ct = default);
    Task<RecordMetadata> SendAsync(string key, T value, CancellationToken ct = default);
    Task<RecordMetadata> SendAsync(Message<T> message, CancellationToken ct = default);
    Task<IReadOnlyList<RecordMetadata>> SendBatchAsync(IEnumerable<T> values, CancellationToken ct = default);
    Task<IReadOnlyList<RecordMetadata>> SendBatchAsync(IEnumerable<Message<T>> messages, CancellationToken ct = default);
}
```

### KafkaRecord

```csharp
public sealed class KafkaRecord<T>
{
    public string Topic { get; }
    public int Partition { get; }
    public long Offset { get; }
    public string? Key { get; }
    public T Value { get; }
    public DateTimeOffset Timestamp { get; }
    public IReadOnlyDictionary<string, string> Headers { get; }

    public Task CommitAsync(CancellationToken ct = default);
}
```

### IStream

```csharp
public interface IStream<T>
{
    IStream<T> Where(Func<T, bool> predicate);
    IStream<TResult> Select<TResult>(Func<T, TResult> selector);
    IStream<TResult> SelectMany<TResult>(Func<T, IEnumerable<TResult>> selector);
    IWindowedStream<T> Window(Window window);
    IGroupedStream<TKey, T> GroupBy<TKey>(Func<T, TKey> keySelector);
    Task BranchAsync(params (Func<T, bool> predicate, string topic)[] branches);
    IJoinedStream<T, TOther> Join<TOther>(IStream<TOther> other, Func<T, TOther, bool> condition, TimeSpan window);
    Task ToAsync(string topic, CancellationToken ct = default);
    Task ForEachAsync(Func<T, Task> handler, CancellationToken ct = default);
}
```

## License

MIT
