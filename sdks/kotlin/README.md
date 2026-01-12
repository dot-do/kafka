# kafka-do

> Event Streaming for Kotlin. Coroutines. Flow. Zero Ops.

```kotlin
val producer = kafka.producer<Order>("orders")
producer.send(Order("123", 99.99))
```

Structured concurrency. Extension functions. The Kotlin way.

## Installation

### Gradle (Kotlin DSL)

```kotlin
dependencies {
    implementation("com.dotdo:kafka:0.1.0")
}
```

### Gradle (Groovy)

```groovy
implementation 'com.dotdo:kafka:0.1.0'
```

Requires Kotlin 1.9+ and JDK 17+.

## Quick Start

```kotlin
import com.dotdo.kafka.*
import kotlinx.serialization.Serializable

@Serializable
data class Order(val orderId: String, val amount: Double)

suspend fun main() {
    val kafka = Kafka()

    // Produce
    val producer = kafka.producer<Order>("orders")
    producer.send(Order("123", 99.99))

    // Consume
    kafka.consumer<Order>("orders", "my-processor").collect { record ->
        println("Received: ${record.value}")
        record.commit()
    }
}
```

## Producing Messages

### Simple Producer

```kotlin
val kafka = Kafka()
val producer = kafka.producer<Order>("orders")

// Send single message
producer.send(Order("123", 99.99))

// Send with key for partitioning
producer.send(
    value = Order("123", 99.99),
    key = "customer-456"
)

// Send with headers
producer.send(
    value = Order("123", 99.99),
    key = "customer-456",
    headers = mapOf("correlation-id" to "abc-123")
)
```

### Batch Producer

```kotlin
val producer = kafka.producer<Order>("orders")

// Send batch
producer.sendBatch(listOf(
    Order("124", 49.99),
    Order("125", 149.99),
    Order("126", 29.99)
))

// Send batch with keys
producer.sendBatch(listOf(
    Message(key = "cust-1", value = Order("124", 49.99)),
    Message(key = "cust-2", value = Order("125", 149.99))
))
```

### Fire-and-Forget with Launch

```kotlin
val producer = kafka.producer<Order>("orders")

// Non-blocking send
scope.launch {
    try {
        producer.send(order)
    } catch (e: KafkaException) {
        println("Failed: ${e.message}")
    }
}
```

### Transactional Producer

```kotlin
kafka.transaction<Order>("orders") { tx ->
    tx.send(Order("123", status = Status.CREATED))
    tx.send(Order("123", status = Status.VALIDATED))
    // Automatically commits on success, aborts on exception
}
```

## Consuming Messages

### Basic Consumer with Flow

```kotlin
val consumer = kafka.consumer<Order>("orders", "order-processor")

consumer.collect { record ->
    println("Topic: ${record.topic}")
    println("Partition: ${record.partition}")
    println("Offset: ${record.offset}")
    println("Key: ${record.key}")
    println("Value: ${record.value}")
    println("Timestamp: ${record.timestamp}")
    println("Headers: ${record.headers}")

    processOrder(record.value)
    record.commit()
}
```

### Consumer with Flow Operators

```kotlin
val consumer = kafka.consumer<Order>("orders", "processor")

// Filter and map
consumer
    .filter { it.value.amount > 100 }
    .map { it.value }
    .collect { order ->
        processHighValueOrder(order)
    }

// Take specific number
consumer.take(10).collect { record ->
    processOrder(record.value)
    record.commit()
}
```

### Consumer with Configuration

```kotlin
val config = ConsumerConfig(
    offset = Offset.EARLIEST,
    autoCommit = true,
    maxPollRecords = 100,
    sessionTimeout = 30.seconds
)

val consumer = kafka.consumer<Order>("orders", "processor", config)

consumer.collect { record ->
    processOrder(record.value)
    // Auto-committed
}
```

### Consumer from Timestamp

```kotlin
import kotlin.time.Duration.Companion.days

val yesterday = Clock.System.now() - 1.days

val config = ConsumerConfig(offset = Offset.Timestamp(yesterday))
val consumer = kafka.consumer<Order>("orders", "replay", config)

consumer.collect { record ->
    println("Replaying: ${record.value}")
}
```

### Batch Consumer

```kotlin
val config = BatchConsumerConfig(
    batchSize = 100,
    batchTimeout = 5.seconds
)

val consumer = kafka.batchConsumer<Order>("orders", "batch-processor", config)

consumer.collect { batch ->
    batch.records.forEach { record ->
        processOrder(record.value)
    }
    batch.commit()
}
```

### Parallel Consumer with Coroutines

```kotlin
val consumer = kafka.consumer<Order>("orders", "parallel-processor")
val semaphore = Semaphore(10) // Limit concurrency

consumer.collect { record ->
    semaphore.withPermit {
        launch {
            processOrder(record.value)
            record.commit()
        }
    }
}
```

### Consumer with Channel

```kotlin
val consumer = kafka.consumer<Order>("orders", "channel-processor")
val channel = Channel<KafkaRecord<Order>>(100)

// Start workers
repeat(10) {
    launch {
        for (record in channel) {
            processOrder(record.value)
            record.commit()
        }
    }
}

// Feed records to channel
consumer.collect { record ->
    channel.send(record)
}
```

## Stream Processing

### Filter and Transform

```kotlin
kafka.stream<Order>("orders")
    .filter { it.amount > 100 }
    .map { order -> PremiumOrder(order, "premium") }
    .to("high-value-orders")
```

### Windowed Aggregations

```kotlin
kafka.stream<Order>("orders")
    .window(Window.tumbling(5.minutes))
    .groupBy { it.customerId }
    .count()
    .collect { key, window ->
        println("Customer $key: ${window.value} orders in ${window.start}-${window.end}")
    }
```

### Joins

```kotlin
val orders = kafka.stream<Order>("orders")
val customers = kafka.stream<Customer>("customers")

orders.join(
    customers,
    on = { order, customer -> order.customerId == customer.id },
    window = 1.hours
).collect { order, customer ->
    println("Order by ${customer.name}")
}
```

### Branching

```kotlin
kafka.stream<Order>("orders")
    .branch(
        { it.region == "us" } to "us-orders",
        { it.region == "eu" } to "eu-orders",
        { true } to "other-orders"
    )
```

### Aggregations

```kotlin
kafka.stream<Order>("orders")
    .groupBy { it.customerId }
    .reduce(0.0) { total, order -> total + order.amount }
    .collect { customerId, total ->
        println("Customer $customerId total: $$total")
    }
```

## Topic Administration

```kotlin
val admin = kafka.admin

// Create topic
admin.createTopic("orders", TopicConfig(
    partitions = 3,
    retentionMs = 7.days.inWholeMilliseconds
))

// List topics
admin.listTopics().forEach { topic ->
    println("${topic.name}: ${topic.partitions} partitions")
}

// Describe topic
val info = admin.describeTopic("orders")
println("Partitions: ${info.partitions}")
println("Retention: ${info.retentionMs}ms")

// Alter topic
admin.alterTopic("orders", TopicConfig(
    retentionMs = 30.days.inWholeMilliseconds
))

// Delete topic
admin.deleteTopic("old-events")
```

## Consumer Groups

```kotlin
val admin = kafka.admin

// List groups
admin.listGroups().forEach { group ->
    println("Group: ${group.id}, Members: ${group.memberCount}")
}

// Describe group
val info = admin.describeGroup("order-processor")
println("State: ${info.state}")
println("Members: ${info.members.size}")
println("Total Lag: ${info.totalLag}")

// Reset offsets
admin.resetOffsets("order-processor", "orders", Offset.EARLIEST)

// Reset to timestamp
admin.resetOffsets("order-processor", "orders", Offset.Timestamp(yesterday))
```

## Error Handling

```kotlin
val producer = kafka.producer<Order>("orders")

try {
    producer.send(order)
} catch (e: TopicNotFoundException) {
    println("Topic not found")
} catch (e: MessageTooLargeException) {
    println("Message too large")
} catch (e: TimeoutException) {
    println("Request timed out")
} catch (e: KafkaException) {
    println("Kafka error: ${e.code} - ${e.message}")

    if (e.isRetriable) {
        // Safe to retry
    }
}
```

### Exception Hierarchy

```kotlin
sealed class KafkaException : Exception() {
    abstract val isRetriable: Boolean
}

class TopicNotFoundException : KafkaException()
class PartitionNotFoundException : KafkaException()
class MessageTooLargeException : KafkaException()
class NotLeaderException : KafkaException()
class OffsetOutOfRangeException : KafkaException()
class GroupCoordinatorException : KafkaException()
class RebalanceInProgressException : KafkaException()
class UnauthorizedException : KafkaException()
class QuotaExceededException : KafkaException()
class TimeoutException : KafkaException()
class DisconnectedException : KafkaException()
class SerializationException : KafkaException()
```

### Dead Letter Queue Pattern

```kotlin
val consumer = kafka.consumer<Order>("orders", "processor")
val dlqProducer = kafka.producer<DlqRecord>("orders-dlq")

consumer.collect { record ->
    try {
        processOrder(record.value)
    } catch (e: ProcessingException) {
        dlqProducer.send(
            value = DlqRecord(
                originalRecord = record.value,
                error = e.message ?: "Unknown error",
                timestamp = Clock.System.now()
            ),
            headers = mapOf(
                "original-topic" to record.topic,
                "original-partition" to record.partition.toString(),
                "original-offset" to record.offset.toString()
            )
        )
    }
    record.commit()
}
```

### Retry with Exponential Backoff

```kotlin
suspend fun <T> withRetry(
    maxAttempts: Int = 3,
    baseDelay: Duration = 1.seconds,
    block: suspend () -> T
): T {
    var attempt = 0
    while (true) {
        try {
            attempt++
            return block()
        } catch (e: KafkaException) {
            if (!e.isRetriable || attempt >= maxAttempts) throw e
            val delay = baseDelay * (1 shl (attempt - 1))
            delay(delay + Random.nextLong(0, delay.inWholeMilliseconds / 10).milliseconds)
        }
    }
}

val producer = kafka.producer<Order>("orders")
withRetry {
    producer.send(order)
}
```

## Configuration

### Environment Variables

```bash
export KAFKA_DO_URL=https://kafka.do
export KAFKA_DO_API_KEY=your-api-key
```

### Client Configuration

```kotlin
val config = KafkaConfig(
    url = "https://kafka.do",
    apiKey = "your-api-key",
    timeout = 30.seconds,
    retries = 3
)

val kafka = Kafka(config)
```

### Producer Configuration

```kotlin
val config = ProducerConfig(
    batchSize = 16384,
    lingerMs = 5,
    compression = Compression.GZIP,
    acks = Acks.ALL,
    retries = 3,
    retryBackoffMs = 100
)

val producer = kafka.producer<Order>("orders", config)
```

### Consumer Configuration

```kotlin
val config = ConsumerConfig(
    offset = Offset.LATEST,
    autoCommit = false,
    fetchMinBytes = 1,
    fetchMaxWaitMs = 500,
    maxPollRecords = 500,
    sessionTimeout = 30.seconds,
    heartbeatInterval = 3.seconds
)

val consumer = kafka.consumer<Order>("orders", "processor", config)
```

## Ktor Integration

```kotlin
import io.ktor.server.application.*
import io.ktor.server.routing.*

fun Application.configureKafka() {
    val kafka = Kafka(
        KafkaConfig(
            url = environment.config.property("kafka.url").getString(),
            apiKey = environment.config.property("kafka.apiKey").getString()
        )
    )

    // Start consumer in background
    launch {
        kafka.consumer<Order>("orders", "ktor-processor").collect { record ->
            // Process...
            record.commit()
        }
    }

    routing {
        post("/orders") {
            val order = call.receive<Order>()
            kafka.producer<Order>("orders").send(order)
            call.respond(HttpStatusCode.Created)
        }
    }
}
```

## Spring Boot Integration

```kotlin
@Configuration
class KafkaConfiguration {
    @Bean
    fun kafka(
        @Value("\${kafka.url}") url: String,
        @Value("\${kafka.api-key}") apiKey: String
    ): Kafka = Kafka(KafkaConfig(url, apiKey))
}

@Service
class OrderService(private val kafka: Kafka) {
    private val producer = kafka.producer<Order>("orders")

    suspend fun createOrder(order: Order) {
        producer.send(order)
    }
}

@Component
class OrderConsumer(private val kafka: Kafka) {
    private val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())

    @PostConstruct
    fun start() {
        scope.launch {
            kafka.consumer<Order>("orders", "spring-processor").collect { record ->
                processOrder(record.value)
                record.commit()
            }
        }
    }

    @PreDestroy
    fun stop() {
        scope.cancel()
    }
}
```

## Testing

### Mock Client

```kotlin
import com.dotdo.kafka.testing.MockKafka
import kotlin.test.*

class OrderProcessorTest {
    @Test
    fun `processes all orders`() = runTest {
        val kafka = MockKafka()

        // Seed test data
        kafka.seed("orders", listOf(
            Order("123", 99.99),
            Order("124", 149.99)
        ))

        // Process
        val processed = mutableListOf<Order>()
        kafka.consumer<Order>("orders", "test")
            .take(2)
            .collect { record ->
                processed.add(record.value)
                record.commit()
            }

        assertEquals(2, processed.size)
        assertEquals("123", processed[0].orderId)
    }

    @Test
    fun `sends messages`() = runTest {
        val kafka = MockKafka()
        val producer = kafka.producer<Order>("orders")

        producer.send(Order("125", 99.99))

        val messages = kafka.getMessages<Order>("orders")
        assertEquals(1, messages.size)
        assertEquals("125", messages[0].orderId)
    }
}
```

### Integration Testing

```kotlin
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class IntegrationTest {
    private lateinit var kafka: Kafka
    private lateinit var topic: String

    @BeforeAll
    fun setUp() {
        kafka = Kafka(KafkaConfig(
            url = System.getenv("TEST_KAFKA_URL"),
            apiKey = System.getenv("TEST_KAFKA_API_KEY")
        ))
        topic = "test-orders-${UUID.randomUUID()}"
    }

    @AfterAll
    fun tearDown() = runBlocking {
        kotlin.runCatching { kafka.admin.deleteTopic(topic) }
    }

    @Test
    fun `end to end produces and consumes`() = runTest {
        // Produce
        val producer = kafka.producer<Order>(topic)
        producer.send(Order("123", 99.99))

        // Consume
        kafka.consumer<Order>(topic, "test").take(1).collect { record ->
            assertEquals("123", record.value.orderId)
        }
    }
}
```

## API Reference

### Kafka

```kotlin
class Kafka(config: KafkaConfig = KafkaConfig.default) {
    inline fun <reified T : Any> producer(
        topic: String,
        config: ProducerConfig = ProducerConfig.default
    ): Producer<T>

    inline fun <reified T : Any> consumer(
        topic: String,
        group: String,
        config: ConsumerConfig = ConsumerConfig.default
    ): Flow<KafkaRecord<T>>

    inline fun <reified T : Any> batchConsumer(
        topic: String,
        group: String,
        config: BatchConsumerConfig
    ): Flow<Batch<T>>

    suspend inline fun <reified T : Any> transaction(
        topic: String,
        block: suspend (Transaction<T>) -> Unit
    )

    inline fun <reified T : Any> stream(topic: String): KafkaStream<T>

    val admin: Admin
}
```

### Producer

```kotlin
interface Producer<T> {
    suspend fun send(
        value: T,
        key: String? = null,
        headers: Map<String, String> = emptyMap(),
        partition: Int? = null
    ): RecordMetadata

    suspend fun sendBatch(values: List<T>): List<RecordMetadata>
    suspend fun sendBatch(messages: List<Message<T>>): List<RecordMetadata>
}
```

### KafkaRecord

```kotlin
data class KafkaRecord<T>(
    val topic: String,
    val partition: Int,
    val offset: Long,
    val key: String?,
    val value: T,
    val timestamp: Instant,
    val headers: Map<String, String>
) {
    suspend fun commit()
    suspend fun commitAsync()
}
```

### KafkaStream

```kotlin
interface KafkaStream<T> {
    fun filter(predicate: (T) -> Boolean): KafkaStream<T>
    fun <R> map(transform: (T) -> R): KafkaStream<R>
    fun <R> flatMap(transform: (T) -> List<R>): KafkaStream<R>
    fun window(window: Window): WindowedStream<T>
    fun <K> groupBy(keySelector: (T) -> K): GroupedStream<K, T>
    fun branch(vararg branches: Pair<(T) -> Boolean, String>)
    fun <R> join(other: KafkaStream<R>, on: (T, R) -> Boolean, window: Duration): JoinedStream<T, R>
    suspend fun to(topic: String)
    suspend fun collect(handler: suspend (T) -> Unit)
}
```

## License

MIT
