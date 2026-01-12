# kafka-do

> Event Streaming for Java. Fluent APIs. CompletableFuture. Zero Ops.

```java
var producer = kafka.producer("orders");
producer.send(new Order("123", 99.99)).get();
```

Builder patterns. Generics. The Java way.

## Installation

### Maven

```xml
<dependency>
    <groupId>com.dotdo</groupId>
    <artifactId>kafka</artifactId>
    <version>0.1.0</version>
</dependency>
```

### Gradle

```groovy
implementation 'com.dotdo:kafka:0.1.0'
```

Requires Java 17+.

## Quick Start

```java
import com.dotdo.kafka.*;

public class QuickStart {
    public static void main(String[] args) {
        try (var kafka = Kafka.create()) {
            // Produce
            var producer = kafka.producer("orders", Order.class);
            producer.send(new Order("123", 99.99)).get();

            // Consume
            var consumer = kafka.consumer("orders", "my-processor", Order.class);
            consumer.forEach(record -> {
                System.out.println("Received: " + record.value());
                record.commit();
            });
        }
    }
}

record Order(String orderId, double amount) {}
```

## Producing Messages

### Simple Producer

```java
var kafka = Kafka.create();
var producer = kafka.producer("orders", Order.class);

// Send single message (async)
CompletableFuture<RecordMetadata> future = producer.send(new Order("123", 99.99));
future.get(); // Block for result

// Send with key for partitioning
producer.send("customer-456", new Order("123", 99.99)).get();

// Send with headers
producer.send(Message.<Order>builder()
    .value(new Order("123", 99.99))
    .key("customer-456")
    .header("correlation-id", "abc-123")
    .build()
).get();
```

### Batch Producer

```java
var producer = kafka.producer("orders", Order.class);

// Send batch
producer.sendBatch(List.of(
    new Order("124", 49.99),
    new Order("125", 149.99),
    new Order("126", 29.99)
)).get();

// Send batch with keys
producer.sendBatch(List.of(
    Message.<Order>builder().key("cust-1").value(new Order("124", 49.99)).build(),
    Message.<Order>builder().key("cust-2").value(new Order("125", 149.99)).build()
)).get();
```

### Async Producer with Callbacks

```java
var producer = kafka.producer("orders", Order.class);

producer.send(new Order("123", 99.99))
    .thenAccept(metadata -> {
        System.out.println("Sent to partition " + metadata.partition() +
            " offset " + metadata.offset());
    })
    .exceptionally(e -> {
        System.err.println("Failed: " + e.getMessage());
        return null;
    });
```

### Transactional Producer

```java
try (var transaction = kafka.transaction("orders", Order.class)) {
    transaction.send(new Order("123", 99.99));
    transaction.send(new Order("124", 49.99));
    transaction.commit(); // Or abort() on error
}
```

## Consuming Messages

### Basic Consumer

```java
var consumer = kafka.consumer("orders", "order-processor", Order.class);

consumer.forEach(record -> {
    System.out.println("Topic: " + record.topic());
    System.out.println("Partition: " + record.partition());
    System.out.println("Offset: " + record.offset());
    System.out.println("Key: " + record.key());
    System.out.println("Value: " + record.value());
    System.out.println("Timestamp: " + record.timestamp());
    System.out.println("Headers: " + record.headers());

    processOrder(record.value());
    record.commit();
});
```

### Consumer with Stream API

```java
var consumer = kafka.consumer("orders", "processor", Order.class);

consumer.stream()
    .filter(r -> r.value().amount() > 100)
    .map(Record::value)
    .forEach(this::processHighValueOrder);
```

### Consumer with Configuration

```java
var config = ConsumerConfig.builder()
    .offset(Offset.EARLIEST)
    .autoCommit(true)
    .maxPollRecords(100)
    .sessionTimeout(Duration.ofSeconds(30))
    .build();

var consumer = kafka.consumer("orders", "processor", Order.class, config);

consumer.forEach(record -> {
    processOrder(record.value());
    // Auto-committed
});
```

### Consumer from Timestamp

```java
var yesterday = Instant.now().minus(Duration.ofDays(1));

var config = ConsumerConfig.builder()
    .offset(Offset.timestamp(yesterday))
    .build();

var consumer = kafka.consumer("orders", "replay", Order.class, config);

consumer.forEach(record -> {
    System.out.println("Replaying: " + record.value());
});
```

### Batch Consumer

```java
var config = ConsumerConfig.builder()
    .batchSize(100)
    .batchTimeout(Duration.ofSeconds(5))
    .build();

var consumer = kafka.batchConsumer("orders", "batch-processor", Order.class, config);

consumer.forEachBatch(batch -> {
    batch.records().forEach(record -> {
        processOrder(record.value());
    });
    batch.commit();
});
```

### Parallel Consumer with ExecutorService

```java
var executor = Executors.newFixedThreadPool(10);
var consumer = kafka.consumer("orders", "parallel-processor", Order.class);

consumer.forEach(record -> {
    executor.submit(() -> {
        processOrder(record.value());
        record.commit();
    });
});
```

### Consumer with Reactive Streams (Project Reactor)

```java
import reactor.core.publisher.Flux;

Flux<Record<Order>> flux = kafka.reactiveConsumer("orders", "reactive-processor", Order.class);

flux.filter(r -> r.value().amount() > 100)
    .flatMap(r -> processAsync(r.value()).thenReturn(r))
    .doOnNext(Record::commit)
    .subscribe();
```

## Stream Processing

### Filter and Transform

```java
import com.dotdo.kafka.streams.*;

var stream = kafka.stream("orders", Order.class);

stream.filter(order -> order.amount() > 100)
    .map(order -> new PremiumOrder(order, "premium"))
    .to("high-value-orders");
```

### Windowed Aggregations

```java
import com.dotdo.kafka.streams.*;

kafka.stream("orders", Order.class)
    .window(Windows.tumbling(Duration.ofMinutes(5)))
    .groupBy(Order::customerId)
    .count()
    .forEach((key, window) -> {
        System.out.printf("Customer %s: %d orders in %s-%s%n",
            key, window.value(), window.start(), window.end());
    });
```

### Joins

```java
var orders = kafka.stream("orders", Order.class);
var customers = kafka.stream("customers", Customer.class);

orders.join(customers,
        (order, customer) -> order.customerId().equals(customer.id()),
        Duration.ofHours(1))
    .forEach((order, customer) -> {
        System.out.println("Order by " + customer.name());
    });
```

### Branching

```java
kafka.stream("orders", Order.class)
    .branch(
        Branch.when(o -> o.region().equals("us"), "us-orders"),
        Branch.when(o -> o.region().equals("eu"), "eu-orders"),
        Branch.otherwise("other-orders")
    );
```

### Aggregations

```java
kafka.stream("orders", Order.class)
    .groupBy(Order::customerId)
    .aggregate(
        () -> 0.0,  // Initial value
        (key, order, total) -> total + order.amount()  // Aggregator
    )
    .forEach((customerId, total) -> {
        System.out.printf("Customer %s total: $%.2f%n", customerId, total);
    });
```

## Topic Administration

```java
var admin = kafka.admin();

// Create topic
admin.createTopic("orders", TopicConfig.builder()
    .partitions(3)
    .retentionMs(Duration.ofDays(7).toMillis())
    .build()
).get();

// List topics
admin.listTopics().get().forEach(topic -> {
    System.out.println(topic.name() + ": " + topic.partitions() + " partitions");
});

// Describe topic
var info = admin.describeTopic("orders").get();
System.out.println("Partitions: " + info.partitions());
System.out.println("Retention: " + info.retentionMs() + "ms");

// Alter topic
admin.alterTopic("orders", TopicConfig.builder()
    .retentionMs(Duration.ofDays(30).toMillis())
    .build()
).get();

// Delete topic
admin.deleteTopic("old-events").get();
```

## Consumer Groups

```java
var admin = kafka.admin();

// List groups
admin.listGroups().get().forEach(group -> {
    System.out.println("Group: " + group.id() + ", Members: " + group.memberCount());
});

// Describe group
var info = admin.describeGroup("order-processor").get();
System.out.println("State: " + info.state());
System.out.println("Members: " + info.members());
System.out.println("Total Lag: " + info.totalLag());

// Reset offsets
admin.resetOffsets("order-processor", "orders", Offset.EARLIEST).get();

// Reset to timestamp
admin.resetOffsets("order-processor", "orders", Offset.timestamp(yesterday)).get();
```

## Error Handling

```java
import com.dotdo.kafka.exceptions.*;

var producer = kafka.producer("orders", Order.class);

try {
    producer.send(order).get();
} catch (ExecutionException e) {
    var cause = e.getCause();

    if (cause instanceof TopicNotFoundException) {
        System.err.println("Topic not found");
    } else if (cause instanceof MessageTooLargeException) {
        System.err.println("Message too large");
    } else if (cause instanceof TimeoutException) {
        System.err.println("Request timed out");
    } else if (cause instanceof KafkaException ke) {
        System.err.println("Kafka error: " + ke.code() + " - " + ke.getMessage());

        if (ke.isRetriable()) {
            // Safe to retry
        }
    }
}
```

### Exception Hierarchy

```java
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

### Dead Letter Queue Pattern

```java
var consumer = kafka.consumer("orders", "processor", Order.class);
var dlqProducer = kafka.producer("orders-dlq", DlqRecord.class);

consumer.forEach(record -> {
    try {
        processOrder(record.value());
    } catch (ProcessingException e) {
        dlqProducer.send(Message.<DlqRecord>builder()
            .value(new DlqRecord(record.value(), e.getMessage(), Instant.now()))
            .header("original-topic", record.topic())
            .header("original-partition", String.valueOf(record.partition()))
            .header("original-offset", String.valueOf(record.offset()))
            .build()
        );
    }
    record.commit();
});
```

### Retry with Resilience4j

```java
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;

var retryConfig = RetryConfig.custom()
    .maxAttempts(3)
    .waitDuration(Duration.ofMillis(500))
    .retryOnException(e -> e instanceof KafkaException ke && ke.isRetriable())
    .build();

var retry = Retry.of("kafka-producer", retryConfig);

var producer = kafka.producer("orders", Order.class);

Retry.decorateRunnable(retry, () -> {
    producer.send(order).get();
}).run();
```

## Configuration

### Environment Variables

```bash
export KAFKA_DO_URL=https://kafka.do
export KAFKA_DO_API_KEY=your-api-key
```

### Programmatic Configuration

```java
var config = KafkaConfig.builder()
    .url("https://kafka.do")
    .apiKey("your-api-key")
    .timeout(Duration.ofSeconds(30))
    .retries(3)
    .build();

var kafka = Kafka.create(config);
```

### Producer Configuration

```java
var config = ProducerConfig.builder()
    .batchSize(16384)
    .lingerMs(5)
    .compression(Compression.GZIP)
    .acks(Acks.ALL)
    .retries(3)
    .retryBackoffMs(100)
    .build();

var producer = kafka.producer("orders", Order.class, config);
```

### Consumer Configuration

```java
var config = ConsumerConfig.builder()
    .offset(Offset.LATEST)
    .autoCommit(false)
    .fetchMinBytes(1)
    .fetchMaxWaitMs(500)
    .maxPollRecords(500)
    .sessionTimeout(Duration.ofSeconds(30))
    .heartbeatInterval(Duration.ofSeconds(3))
    .build();

var consumer = kafka.consumer("orders", "processor", Order.class, config);
```

## Testing

### Mock Client

```java
import com.dotdo.kafka.testing.MockKafka;

class OrderProcessorTest {
    private MockKafka kafka;

    @BeforeEach
    void setUp() {
        kafka = new MockKafka();
    }

    @Test
    void testOrderProcessing() {
        // Seed test data
        kafka.seed("orders", List.of(
            new Order("123", 99.99),
            new Order("124", 149.99)
        ));

        // Process
        var consumer = kafka.consumer("orders", "test", Order.class);
        var processed = new ArrayList<Order>();

        consumer.forEach(record -> {
            processed.add(record.value());
            record.commit();
            if (processed.size() == 2) {
                consumer.close();
            }
        });

        assertEquals(2, processed.size());
        assertEquals("123", processed.get(0).orderId());
    }

    @Test
    void testProducer() {
        var producer = kafka.producer("orders", Order.class);
        producer.send(new Order("125", 99.99)).join();

        var messages = kafka.getMessages("orders", Order.class);
        assertEquals(1, messages.size());
        assertEquals("125", messages.get(0).orderId());
    }
}
```

### Integration Testing with TestContainers

```java
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
class IntegrationTest {
    @Test
    void testEndToEnd() {
        var kafka = Kafka.create(KafkaConfig.builder()
            .url(System.getenv("TEST_KAFKA_URL"))
            .apiKey(System.getenv("TEST_KAFKA_API_KEY"))
            .build());

        var topic = "test-orders-" + UUID.randomUUID();

        try {
            // Produce
            var producer = kafka.producer(topic, Order.class);
            producer.send(new Order("123", 99.99)).get();

            // Consume
            var consumer = kafka.consumer(topic, "test", Order.class);
            var record = consumer.poll(Duration.ofSeconds(10)).iterator().next();

            assertEquals("123", record.value().orderId());
        } finally {
            kafka.admin().deleteTopic(topic).join();
        }
    }
}
```

## API Reference

### Kafka

```java
public class Kafka implements AutoCloseable {
    public static Kafka create();
    public static Kafka create(KafkaConfig config);

    public <T> Producer<T> producer(String topic, Class<T> type);
    public <T> Producer<T> producer(String topic, Class<T> type, ProducerConfig config);

    public <T> Consumer<T> consumer(String topic, String group, Class<T> type);
    public <T> Consumer<T> consumer(String topic, String group, Class<T> type, ConsumerConfig config);
    public <T> BatchConsumer<T> batchConsumer(String topic, String group, Class<T> type, ConsumerConfig config);
    public <T> Flux<Record<T>> reactiveConsumer(String topic, String group, Class<T> type);

    public <T> Transaction<T> transaction(String topic, Class<T> type);
    public <T> Stream<T> stream(String topic, Class<T> type);

    public Admin admin();

    @Override
    public void close();
}
```

### Producer

```java
public interface Producer<T> {
    CompletableFuture<RecordMetadata> send(T value);
    CompletableFuture<RecordMetadata> send(String key, T value);
    CompletableFuture<RecordMetadata> send(Message<T> message);
    CompletableFuture<List<RecordMetadata>> sendBatch(List<T> values);
    CompletableFuture<List<RecordMetadata>> sendBatch(List<Message<T>> messages);
}
```

### Consumer

```java
public interface Consumer<T> extends AutoCloseable {
    void forEach(java.util.function.Consumer<Record<T>> handler);
    java.util.stream.Stream<Record<T>> stream();
    Iterable<Record<T>> poll(Duration timeout);
}
```

### Record

```java
public interface Record<T> {
    String topic();
    int partition();
    long offset();
    String key();
    T value();
    Instant timestamp();
    Map<String, String> headers();
    void commit();
    CompletableFuture<Void> commitAsync();
}
```

### Stream

```java
public interface Stream<T> {
    Stream<T> filter(Predicate<T> predicate);
    <R> Stream<R> map(Function<T, R> mapper);
    <R> Stream<R> flatMap(Function<T, java.util.stream.Stream<R>> mapper);
    WindowedStream<T> window(Window window);
    GroupedStream<K, T> groupBy(Function<T, K> keySelector);
    void branch(Branch<T>... branches);
    <R> JoinedStream<T, R> join(Stream<R> other, BiPredicate<T, R> condition, Duration window);
    void to(String topic);
    void forEach(java.util.function.Consumer<T> handler);
}
```

## License

MIT
