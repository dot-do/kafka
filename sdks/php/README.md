# kafka-do

> Event Streaming for PHP. Iterators. Closures. Zero Ops.

```php
$kafka->producer('orders')->send(['order_id' => '123', 'amount' => 99.99]);
```

Generators for memory efficiency. Closures for callbacks. The PHP way.

## Installation

```bash
composer require dotdo/kafka
```

Requires PHP 8.2+.

## Quick Start

```php
<?php

use DotDo\Kafka\Kafka;

$kafka = new Kafka();

// Produce
$kafka->producer('orders')->send([
    'order_id' => '123',
    'amount' => 99.99
]);

// Consume
foreach ($kafka->consumer('orders', group: 'my-processor') as $record) {
    echo "Received: " . json_encode($record->value) . "\n";
    $record->commit();
}
```

## Producing Messages

### Simple Producer

```php
$kafka = new Kafka();
$producer = $kafka->producer('orders');

// Send single message
$producer->send(['order_id' => '123', 'amount' => 99.99]);

// Send with key for partitioning
$producer->send(
    value: ['order_id' => '123', 'amount' => 99.99],
    key: 'customer-456'
);

// Send with headers
$producer->send(
    value: ['order_id' => '123'],
    key: 'customer-456',
    headers: ['correlation-id' => 'abc-123']
);
```

### Batch Producer

```php
$producer = $kafka->producer('orders');

// Send batch
$producer->sendBatch([
    ['order_id' => '124', 'amount' => 49.99],
    ['order_id' => '125', 'amount' => 149.99],
    ['order_id' => '126', 'amount' => 29.99]
]);

// Send batch with keys
$producer->sendBatch([
    ['key' => 'cust-1', 'value' => ['order_id' => '124']],
    ['key' => 'cust-2', 'value' => ['order_id' => '125']]
]);
```

### Async Producer (with Promises)

```php
use function React\Async\await;

$producer = $kafka->asyncProducer('orders');

// Returns a promise
$promise = $producer->send(['order_id' => '123']);

$promise->then(
    fn($metadata) => echo "Sent to partition {$metadata->partition}\n",
    fn($error) => echo "Failed: {$error->getMessage()}\n"
);

// Or await
$metadata = await($producer->send(['order_id' => '123']));
```

### Transactional Producer

```php
$kafka->transaction('orders', function ($tx) {
    $tx->send(['order_id' => '123', 'status' => 'created']);
    $tx->send(['order_id' => '123', 'status' => 'validated']);
    // Automatically commits on success, aborts on exception
});
```

## Consuming Messages

### Basic Consumer with Generator

```php
$consumer = $kafka->consumer('orders', group: 'order-processor');

foreach ($consumer as $record) {
    echo "Topic: {$record->topic}\n";
    echo "Partition: {$record->partition}\n";
    echo "Offset: {$record->offset}\n";
    echo "Key: {$record->key}\n";
    echo "Value: " . json_encode($record->value) . "\n";
    echo "Timestamp: {$record->timestamp->format('c')}\n";
    echo "Headers: " . json_encode($record->headers) . "\n";

    processOrder($record->value);
    $record->commit();
}
```

### Consumer with Functional Processing

```php
use function DotDo\Kafka\{filter, map, take};

$consumer = $kafka->consumer('orders', group: 'processor');

// Filter and process
$highValue = filter($consumer, fn($r) => $r->value['amount'] > 100);
foreach ($highValue as $record) {
    processHighValueOrder($record->value);
    $record->commit();
}

// Take specific number
foreach (take($consumer, 10) as $record) {
    processOrder($record->value);
    $record->commit();
}
```

### Consumer with Options

```php
$consumer = $kafka->consumer('orders', group: 'processor', options: [
    'offset' => 'earliest',
    'auto_commit' => true,
    'max_poll_records' => 100,
    'session_timeout_ms' => 30000
]);

foreach ($consumer as $record) {
    processOrder($record->value);
    // Auto-committed
}
```

### Consumer from Timestamp

```php
$yesterday = new DateTimeImmutable('-1 day');

$consumer = $kafka->consumer('orders', group: 'replay', options: [
    'offset' => $yesterday
]);

foreach ($consumer as $record) {
    echo "Replaying: " . json_encode($record->value) . "\n";
}
```

### Batch Consumer

```php
$consumer = $kafka->batchConsumer('orders', group: 'batch-processor', options: [
    'batch_size' => 100,
    'batch_timeout' => 5.0
]);

foreach ($consumer as $batch) {
    foreach ($batch as $record) {
        processOrder($record->value);
    }
    $batch->commit();
}
```

### Parallel Consumer with Fibers

```php
use Fiber;

$consumer = $kafka->consumer('orders', group: 'parallel-processor');
$fibers = [];
$maxConcurrency = 10;

foreach ($consumer as $record) {
    // Start new fiber for processing
    $fiber = new Fiber(function () use ($record) {
        processOrder($record->value);
        $record->commit();
    });
    $fiber->start();
    $fibers[] = $fiber;

    // Clean up completed fibers
    $fibers = array_filter($fibers, fn($f) => !$f->isTerminated());

    // Wait if at max concurrency
    while (count($fibers) >= $maxConcurrency) {
        array_shift($fibers)?->resume();
    }
}
```

### Consumer with ReactPHP

```php
use React\EventLoop\Loop;
use DotDo\Kafka\React\ReactKafka;

$kafka = new ReactKafka(Loop::get());

$kafka->consumer('orders', group: 'reactive-processor')
    ->filter(fn($r) => $r->value['amount'] > 100)
    ->subscribe(
        onNext: function ($record) {
            processOrder($record->value);
            $record->commit();
        },
        onError: fn($e) => echo "Error: {$e->getMessage()}\n"
    );

Loop::get()->run();
```

## Stream Processing

### Filter and Transform

```php
$kafka->stream('orders')
    ->filter(fn($order) => $order['amount'] > 100)
    ->map(fn($order) => array_merge($order, ['tier' => 'premium']))
    ->to('high-value-orders');
```

### Windowed Aggregations

```php
$kafka->stream('orders')
    ->window(tumbling: '5m')
    ->groupBy(fn($order) => $order['customer_id'])
    ->count()
    ->each(function ($key, $window) {
        echo "Customer {$key}: {$window->value} orders in {$window->start}-{$window->end}\n";
    });
```

### Joins

```php
$orders = $kafka->stream('orders');
$customers = $kafka->stream('customers');

$orders->join($customers,
    on: fn($order, $customer) => $order['customer_id'] === $customer['id'],
    window: '1h'
)->each(function ($order, $customer) {
    echo "Order by {$customer['name']}\n";
});
```

### Branching

```php
$kafka->stream('orders')->branch([
    [fn($o) => $o['region'] === 'us', 'us-orders'],
    [fn($o) => $o['region'] === 'eu', 'eu-orders'],
    [fn($o) => true, 'other-orders']
]);
```

### Aggregations

```php
$kafka->stream('orders')
    ->groupBy(fn($order) => $order['customer_id'])
    ->reduce(0, fn($total, $order) => $total + $order['amount'])
    ->each(function ($customerId, $total) {
        echo "Customer {$customerId} total: \${$total}\n";
    });
```

## Topic Administration

```php
$admin = $kafka->admin();

// Create topic
$admin->createTopic('orders', [
    'partitions' => 3,
    'retention_ms' => 7 * 24 * 60 * 60 * 1000 // 7 days
]);

// List topics
foreach ($admin->listTopics() as $topic) {
    echo "{$topic->name}: {$topic->partitions} partitions\n";
}

// Describe topic
$info = $admin->describeTopic('orders');
echo "Partitions: {$info->partitions}\n";
echo "Retention: {$info->retentionMs}ms\n";

// Alter topic
$admin->alterTopic('orders', [
    'retention_ms' => 30 * 24 * 60 * 60 * 1000 // 30 days
]);

// Delete topic
$admin->deleteTopic('old-events');
```

## Consumer Groups

```php
$admin = $kafka->admin();

// List groups
foreach ($admin->listGroups() as $group) {
    echo "Group: {$group->id}, Members: {$group->memberCount}\n";
}

// Describe group
$info = $admin->describeGroup('order-processor');
echo "State: {$info->state}\n";
echo "Members: " . count($info->members) . "\n";
echo "Total Lag: {$info->totalLag}\n";

// Reset offsets
$admin->resetOffsets('order-processor', 'orders', 'earliest');

// Reset to timestamp
$admin->resetOffsets('order-processor', 'orders', $yesterday);
```

## Error Handling

```php
use DotDo\Kafka\Exception\{
    KafkaException,
    TopicNotFoundException,
    MessageTooLargeException,
    TimeoutException
};

try {
    $kafka->producer('orders')->send($order);
} catch (TopicNotFoundException $e) {
    echo "Topic not found\n";
} catch (MessageTooLargeException $e) {
    echo "Message too large\n";
} catch (TimeoutException $e) {
    echo "Request timed out\n";
} catch (KafkaException $e) {
    echo "Kafka error: {$e->getCode()} - {$e->getMessage()}\n";

    if ($e->isRetriable()) {
        // Safe to retry
    }
}
```

### Exception Hierarchy

```php
KafkaException // Base class
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

### Dead Letter Queue Pattern

```php
$consumer = $kafka->consumer('orders', group: 'processor');
$dlqProducer = $kafka->producer('orders-dlq');

foreach ($consumer as $record) {
    try {
        processOrder($record->value);
    } catch (ProcessingException $e) {
        $dlqProducer->send(
            value: [
                'original_record' => $record->value,
                'error' => $e->getMessage(),
                'timestamp' => (new DateTimeImmutable())->format('c')
            ],
            headers: [
                'original-topic' => $record->topic,
                'original-partition' => (string) $record->partition,
                'original-offset' => (string) $record->offset
            ]
        );
    }
    $record->commit();
}
```

### Retry with Exponential Backoff

```php
function withRetry(callable $fn, int $maxAttempts = 3, float $baseDelay = 1.0): mixed
{
    $attempts = 0;
    while (true) {
        try {
            $attempts++;
            return $fn();
        } catch (KafkaException $e) {
            if (!$e->isRetriable() || $attempts >= $maxAttempts) {
                throw $e;
            }
            $delay = $baseDelay * pow(2, $attempts - 1);
            usleep((int) ($delay * 1_000_000 + random_int(0, (int) ($delay * 100_000))));
        }
    }
}

withRetry(fn() => $kafka->producer('orders')->send($order));
```

## Configuration

### Environment Variables

```bash
export KAFKA_DO_URL=https://kafka.do
export KAFKA_DO_API_KEY=your-api-key
```

### Client Configuration

```php
$kafka = new Kafka([
    'url' => 'https://kafka.do',
    'api_key' => 'your-api-key',
    'timeout' => 30,
    'retries' => 3
]);
```

### Producer Configuration

```php
$producer = $kafka->producer('orders', options: [
    'batch_size' => 16384,
    'linger_ms' => 5,
    'compression' => 'gzip',
    'acks' => 'all',
    'retries' => 3,
    'retry_backoff_ms' => 100
]);
```

### Consumer Configuration

```php
$consumer = $kafka->consumer('orders', group: 'processor', options: [
    'offset' => 'latest',
    'auto_commit' => false,
    'fetch_min_bytes' => 1,
    'fetch_max_wait_ms' => 500,
    'max_poll_records' => 500,
    'session_timeout_ms' => 30000,
    'heartbeat_interval_ms' => 3000
]);
```

## Laravel Integration

### Service Provider

```php
// config/kafka.php
return [
    'url' => env('KAFKA_DO_URL', 'https://kafka.do'),
    'api_key' => env('KAFKA_DO_API_KEY'),
    'timeout' => 30,
    'retries' => 3,
];

// app/Providers/KafkaServiceProvider.php
class KafkaServiceProvider extends ServiceProvider
{
    public function register(): void
    {
        $this->app->singleton(Kafka::class, fn($app) =>
            new Kafka($app['config']['kafka'])
        );
    }
}
```

### Facade

```php
use DotDo\Kafka\Facades\Kafka;

// Produce
Kafka::producer('orders')->send($order);

// Consume
foreach (Kafka::consumer('orders', group: 'processor') as $record) {
    $record->commit();
}
```

### Event Publishing

```php
// app/Events/OrderCreated.php
class OrderCreated implements ShouldBroadcastToKafka
{
    public function __construct(public Order $order) {}

    public function broadcastOn(): string
    {
        return 'orders';
    }

    public function broadcastWith(): array
    {
        return $this->order->toArray();
    }
}

// Usage
event(new OrderCreated($order));
```

### Queue Worker

```php
// app/Console/Commands/KafkaConsumer.php
class KafkaConsumer extends Command
{
    protected $signature = 'kafka:consume {topic} {group}';

    public function handle(Kafka $kafka): void
    {
        $consumer = $kafka->consumer(
            $this->argument('topic'),
            group: $this->argument('group')
        );

        foreach ($consumer as $record) {
            dispatch(new ProcessKafkaRecord($record->value));
            $record->commit();
        }
    }
}
```

## Testing

### Mock Client

```php
use DotDo\Kafka\Testing\MockKafka;
use PHPUnit\Framework\TestCase;

class OrderProcessorTest extends TestCase
{
    public function testProcessesAllOrders(): void
    {
        $kafka = new MockKafka();

        // Seed test data
        $kafka->seed('orders', [
            ['order_id' => '123', 'amount' => 99.99],
            ['order_id' => '124', 'amount' => 149.99]
        ]);

        // Process
        $processed = [];
        foreach (take($kafka->consumer('orders', group: 'test'), 2) as $record) {
            $processed[] = $record->value;
            $record->commit();
        }

        $this->assertCount(2, $processed);
        $this->assertEquals('123', $processed[0]['order_id']);
    }

    public function testSendsMessages(): void
    {
        $kafka = new MockKafka();
        $kafka->producer('orders')->send(['order_id' => '125']);

        $messages = $kafka->getMessages('orders');
        $this->assertCount(1, $messages);
        $this->assertEquals('125', $messages[0]['order_id']);
    }
}
```

### Integration Testing

```php
class IntegrationTest extends TestCase
{
    private Kafka $kafka;
    private string $topic;

    protected function setUp(): void
    {
        $this->kafka = new Kafka([
            'url' => getenv('TEST_KAFKA_URL'),
            'api_key' => getenv('TEST_KAFKA_API_KEY')
        ]);
        $this->topic = 'test-orders-' . bin2hex(random_bytes(8));
    }

    protected function tearDown(): void
    {
        try {
            $this->kafka->admin()->deleteTopic($this->topic);
        } catch (Exception) {}
    }

    public function testEndToEnd(): void
    {
        // Produce
        $this->kafka->producer($this->topic)->send(['order_id' => '123']);

        // Consume
        $consumer = $this->kafka->consumer($this->topic, group: 'test');
        foreach ($consumer as $record) {
            $this->assertEquals('123', $record->value['order_id']);
            break;
        }
    }
}
```

## API Reference

### Kafka

```php
class Kafka
{
    public function __construct(array $config = []);
    public function producer(string $topic, array $options = []): Producer;
    public function asyncProducer(string $topic, array $options = []): AsyncProducer;
    public function consumer(string $topic, string $group, array $options = []): Generator;
    public function batchConsumer(string $topic, string $group, array $options = []): Generator;
    public function transaction(string $topic, callable $callback): void;
    public function stream(string $topic): Stream;
    public function admin(): Admin;
}
```

### Producer

```php
class Producer
{
    public function send(
        array $value,
        ?string $key = null,
        array $headers = [],
        ?int $partition = null
    ): RecordMetadata;

    public function sendBatch(array $records): array;
}
```

### Record

```php
class Record
{
    public readonly string $topic;
    public readonly int $partition;
    public readonly int $offset;
    public readonly ?string $key;
    public readonly array $value;
    public readonly DateTimeImmutable $timestamp;
    public readonly array $headers;

    public function commit(): void;
    public function commitAsync(): Promise;
}
```

### Stream

```php
class Stream
{
    public function filter(callable $predicate): self;
    public function map(callable $mapper): self;
    public function flatMap(callable $mapper): self;
    public function window(string $tumbling = null, string $sliding = null): WindowedStream;
    public function groupBy(callable $keySelector): GroupedStream;
    public function branch(array $conditions): void;
    public function join(Stream $other, callable $on, string $window): JoinedStream;
    public function to(string $topic): void;
    public function each(callable $handler): void;
}
```

## License

MIT
