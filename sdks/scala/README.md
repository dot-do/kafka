# kafka-do

> Event Streaming for Scala. Functional. Type-Safe. Zero Ops.

```scala
val producer = kafka.producer[Order]("orders")
producer.send(Order("123", 99.99))
```

Immutable data. Higher-order functions. The Scala way.

## Installation

### SBT

```scala
libraryDependencies += "com.dotdo" %% "kafka" % "0.1.0"
```

### Mill

```scala
ivy"com.dotdo::kafka:0.1.0"
```

Requires Scala 3.3+ and JDK 17+.

## Quick Start

```scala
import com.dotdo.kafka.*
import scala.concurrent.ExecutionContext.Implicits.global

case class Order(orderId: String, amount: Double) derives Codec

@main def main(): Unit =
  val kafka = Kafka()

  // Produce
  val producer = kafka.producer[Order]("orders")
  producer.send(Order("123", 99.99))

  // Consume
  kafka.consumer[Order]("orders", "my-processor").foreach: record =>
    println(s"Received: ${record.value}")
    record.commit()
```

## Producing Messages

### Simple Producer

```scala
val kafka = Kafka()
val producer = kafka.producer[Order]("orders")

// Send single message
producer.send(Order("123", 99.99))

// Send with key for partitioning
producer.send(
  value = Order("123", 99.99),
  key = Some("customer-456")
)

// Send with headers
producer.send(
  value = Order("123", 99.99),
  key = Some("customer-456"),
  headers = Map("correlation-id" -> "abc-123")
)
```

### Batch Producer

```scala
val producer = kafka.producer[Order]("orders")

// Send batch
producer.sendBatch(List(
  Order("124", 49.99),
  Order("125", 149.99),
  Order("126", 29.99)
))

// Send batch with keys
producer.sendBatch(List(
  Message(key = Some("cust-1"), value = Order("124", 49.99)),
  Message(key = Some("cust-2"), value = Order("125", 149.99))
))
```

### Async Producer with Future

```scala
import scala.concurrent.Future

val producer = kafka.producer[Order]("orders")

// Returns Future
val future: Future[RecordMetadata] = producer.sendAsync(order)

future.onComplete:
  case Success(metadata) =>
    println(s"Sent to partition ${metadata.partition}")
  case Failure(e) =>
    println(s"Failed: ${e.getMessage}")

// Or with for-comprehension
for
  metadata <- producer.sendAsync(order)
yield println(s"Delivered to offset ${metadata.offset}")
```

### Transactional Producer

```scala
kafka.transaction[Order]("orders"): tx =>
  tx.send(Order("123", Status.Created))
  tx.send(Order("123", Status.Validated))
  // Automatically commits on success, aborts on exception
```

## Consuming Messages

### Basic Consumer with Iterator

```scala
val consumer = kafka.consumer[Order]("orders", "order-processor")

consumer.foreach: record =>
  println(s"Topic: ${record.topic}")
  println(s"Partition: ${record.partition}")
  println(s"Offset: ${record.offset}")
  println(s"Key: ${record.key}")
  println(s"Value: ${record.value}")
  println(s"Timestamp: ${record.timestamp}")
  println(s"Headers: ${record.headers}")

  processOrder(record.value)
  record.commit()
```

### Consumer with Functional Operations

```scala
val consumer = kafka.consumer[Order]("orders", "processor")

// Filter and map
consumer
  .filter(_.value.amount > 100)
  .map(_.value)
  .foreach(processHighValueOrder)

// Take specific number
consumer
  .take(10)
  .foreach: record =>
    processOrder(record.value)
    record.commit()

// Collect to list
val orders: List[Order] = consumer
  .take(100)
  .map(_.value)
  .toList
```

### Consumer with Configuration

```scala
val config = ConsumerConfig(
  offset = Offset.Earliest,
  autoCommit = true,
  maxPollRecords = 100,
  sessionTimeout = 30.seconds
)

val consumer = kafka.consumer[Order]("orders", "processor", config)

consumer.foreach: record =>
  processOrder(record.value)
  // Auto-committed
```

### Consumer from Timestamp

```scala
import java.time.Instant
import scala.concurrent.duration.*

val yesterday = Instant.now.minusSeconds(86400)

val config = ConsumerConfig(offset = Offset.Timestamp(yesterday))
val consumer = kafka.consumer[Order]("orders", "replay", config)

consumer.foreach: record =>
  println(s"Replaying: ${record.value}")
```

### Batch Consumer

```scala
val config = BatchConsumerConfig(
  batchSize = 100,
  batchTimeout = 5.seconds
)

val consumer = kafka.batchConsumer[Order]("orders", "batch-processor", config)

consumer.foreach: batch =>
  batch.records.foreach: record =>
    processOrder(record.value)
  batch.commit()
```

### Parallel Consumer with Cats Effect

```scala
import cats.effect.*
import cats.effect.std.Queue
import cats.implicits.*

def consumeParallel(kafka: Kafka): IO[Unit] =
  for
    queue <- Queue.bounded[IO, KafkaRecord[Order]](100)
    consumer = kafka.consumer[Order]("orders", "parallel-processor")

    // Start workers
    workers = (1 to 10).toList.parTraverse_ { _ =>
      queue.take.flatMap { record =>
        processOrder(record.value) *> record.commitIO
      }.foreverM
    }

    // Feed records to queue
    producer = consumer.foreach(record => queue.offer(record).unsafeRunSync())

    _ <- workers.start
    _ <- IO(producer)
  yield ()
```

### Consumer with ZIO

```scala
import zio.*
import zio.stream.*

val consumer: ZStream[Kafka, Throwable, KafkaRecord[Order]] =
  ZStream.serviceWithStream[Kafka](_.consumerStream[Order]("orders", "zio-processor"))

val program: ZIO[Kafka, Throwable, Unit] =
  consumer
    .filter(_.value.amount > 100)
    .mapZIO(record => processOrder(record.value).as(record))
    .tap(_.commit)
    .runDrain
```

## Stream Processing

### Filter and Transform

```scala
kafka.stream[Order]("orders")
  .filter(_.amount > 100)
  .map(order => PremiumOrder(order, "premium"))
  .to("high-value-orders")
```

### Windowed Aggregations

```scala
kafka.stream[Order]("orders")
  .window(Window.Tumbling(5.minutes))
  .groupBy(_.customerId)
  .count
  .foreach: (key, window) =>
    println(s"Customer $key: ${window.value} orders in ${window.start}-${window.end}")
```

### Joins

```scala
val orders = kafka.stream[Order]("orders")
val customers = kafka.stream[Customer]("customers")

orders
  .join(customers)(
    on = (order, customer) => order.customerId == customer.id,
    window = 1.hour
  )
  .foreach: (order, customer) =>
    println(s"Order by ${customer.name}")
```

### Branching

```scala
kafka.stream[Order]("orders")
  .branch(
    (_.region == "us") -> "us-orders",
    (_.region == "eu") -> "eu-orders",
    ((_: Order) => true) -> "other-orders"
  )
```

### Aggregations

```scala
kafka.stream[Order]("orders")
  .groupBy(_.customerId)
  .fold(0.0)((total, order) => total + order.amount)
  .foreach: (customerId, total) =>
    println(s"Customer $customerId total: $$$total")
```

## Topic Administration

```scala
val admin = kafka.admin

// Create topic
admin.createTopic("orders", TopicConfig(
  partitions = 3,
  retentionMs = 7.days.toMillis
))

// List topics
admin.listTopics().foreach: topic =>
  println(s"${topic.name}: ${topic.partitions} partitions")

// Describe topic
val info = admin.describeTopic("orders")
println(s"Partitions: ${info.partitions}")
println(s"Retention: ${info.retentionMs}ms")

// Alter topic
admin.alterTopic("orders", TopicConfig(
  retentionMs = 30.days.toMillis
))

// Delete topic
admin.deleteTopic("old-events")
```

## Consumer Groups

```scala
val admin = kafka.admin

// List groups
admin.listGroups().foreach: group =>
  println(s"Group: ${group.id}, Members: ${group.memberCount}")

// Describe group
val info = admin.describeGroup("order-processor")
println(s"State: ${info.state}")
println(s"Members: ${info.members.size}")
println(s"Total Lag: ${info.totalLag}")

// Reset offsets
admin.resetOffsets("order-processor", "orders", Offset.Earliest)

// Reset to timestamp
admin.resetOffsets("order-processor", "orders", Offset.Timestamp(yesterday))
```

## Error Handling

```scala
import scala.util.{Try, Success, Failure}

val producer = kafka.producer[Order]("orders")

Try(producer.send(order)) match
  case Success(metadata) =>
    println(s"Sent to partition ${metadata.partition}")
  case Failure(e: TopicNotFoundException) =>
    println("Topic not found")
  case Failure(e: MessageTooLargeException) =>
    println("Message too large")
  case Failure(e: TimeoutException) =>
    println("Request timed out")
  case Failure(e: KafkaException) if e.isRetriable =>
    // Safe to retry
    retryWithBackoff(producer.send(order))
  case Failure(e: KafkaException) =>
    println(s"Kafka error: ${e.code} - ${e.message}")
  case Failure(e) =>
    println(s"Unexpected error: ${e.getMessage}")
```

### Exception Hierarchy

```scala
sealed trait KafkaException extends Exception:
  def isRetriable: Boolean

case class TopicNotFoundException(topic: String) extends KafkaException
case class PartitionNotFoundException(topic: String, partition: Int) extends KafkaException
case class MessageTooLargeException(size: Long, maxSize: Long) extends KafkaException
case class NotLeaderException(topic: String, partition: Int) extends KafkaException
case class OffsetOutOfRangeException(offset: Long, topic: String) extends KafkaException
case class GroupCoordinatorException(message: String) extends KafkaException
case class RebalanceInProgressException() extends KafkaException
case class UnauthorizedException(message: String) extends KafkaException
case class QuotaExceededException() extends KafkaException
case class TimeoutException() extends KafkaException
case class DisconnectedException() extends KafkaException
case class SerializationException(cause: Throwable) extends KafkaException
```

### Dead Letter Queue Pattern

```scala
val consumer = kafka.consumer[Order]("orders", "processor")
val dlqProducer = kafka.producer[DlqRecord]("orders-dlq")

consumer.foreach: record =>
  Try(processOrder(record.value)) match
    case Success(_) => ()
    case Failure(e: ProcessingException) =>
      dlqProducer.send(
        DlqRecord(
          originalRecord = record.value,
          error = e.getMessage,
          timestamp = Instant.now
        ),
        headers = Map(
          "original-topic" -> record.topic,
          "original-partition" -> record.partition.toString,
          "original-offset" -> record.offset.toString
        )
      )
  record.commit()
```

### Retry with Exponential Backoff

```scala
def withRetry[T](
  maxAttempts: Int = 3,
  baseDelay: FiniteDuration = 1.second
)(block: => T): T =
  @annotation.tailrec
  def loop(attempt: Int): T =
    Try(block) match
      case Success(result) => result
      case Failure(e: KafkaException) if e.isRetriable && attempt < maxAttempts =>
        val delay = baseDelay * math.pow(2, attempt - 1).toLong
        Thread.sleep(delay.toMillis + scala.util.Random.nextLong(delay.toMillis / 10))
        loop(attempt + 1)
      case Failure(e) => throw e
  loop(1)

val producer = kafka.producer[Order]("orders")
withRetry()(producer.send(order))
```

## Configuration

### Environment Variables

```bash
export KAFKA_DO_URL=https://kafka.do
export KAFKA_DO_API_KEY=your-api-key
```

### Client Configuration

```scala
val config = KafkaConfig(
  url = "https://kafka.do",
  apiKey = "your-api-key",
  timeout = 30.seconds,
  retries = 3
)

val kafka = Kafka(config)
```

### Producer Configuration

```scala
val config = ProducerConfig(
  batchSize = 16384,
  lingerMs = 5,
  compression = Compression.Gzip,
  acks = Acks.All,
  retries = 3,
  retryBackoffMs = 100
)

val producer = kafka.producer[Order]("orders", config)
```

### Consumer Configuration

```scala
val config = ConsumerConfig(
  offset = Offset.Latest,
  autoCommit = false,
  fetchMinBytes = 1,
  fetchMaxWaitMs = 500,
  maxPollRecords = 500,
  sessionTimeout = 30.seconds,
  heartbeatInterval = 3.seconds
)

val consumer = kafka.consumer[Order]("orders", "processor", config)
```

## Testing

### Mock Client

```scala
import com.dotdo.kafka.testing.MockKafka
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class OrderProcessorSpec extends AnyFlatSpec with Matchers:

  "OrderProcessor" should "process all orders" in:
    val kafka = MockKafka()

    // Seed test data
    kafka.seed("orders", List(
      Order("123", 99.99),
      Order("124", 149.99)
    ))

    // Process
    val processed = kafka.consumer[Order]("orders", "test")
      .take(2)
      .map(_.value)
      .toList

    processed should have size 2
    processed.head.orderId shouldBe "123"

  it should "send messages" in:
    val kafka = MockKafka()
    val producer = kafka.producer[Order]("orders")

    producer.send(Order("125", 99.99))

    val messages = kafka.getMessages[Order]("orders")
    messages should have size 1
    messages.head.orderId shouldBe "125"
```

### Integration Testing

```scala
import org.scalatest.{BeforeAndAfterAll, Suite}

trait KafkaIntegrationSpec extends BeforeAndAfterAll { self: Suite =>
  var kafka: Kafka = _
  var topic: String = _

  override def beforeAll(): Unit =
    super.beforeAll()
    kafka = Kafka(KafkaConfig(
      url = sys.env("TEST_KAFKA_URL"),
      apiKey = sys.env("TEST_KAFKA_API_KEY")
    ))
    topic = s"test-orders-${java.util.UUID.randomUUID}"

  override def afterAll(): Unit =
    scala.util.Try(kafka.admin.deleteTopic(topic))
    super.afterAll()
}

class IntegrationSpec extends AnyFlatSpec with Matchers with KafkaIntegrationSpec:

  "Kafka" should "produce and consume messages" in:
    // Produce
    val producer = kafka.producer[Order](topic)
    producer.send(Order("123", 99.99))

    // Consume
    val record = kafka.consumer[Order](topic, "test").take(1).next()
    record.value.orderId shouldBe "123"
```

## API Reference

### Kafka

```scala
class Kafka(config: KafkaConfig = KafkaConfig.default):
  def producer[T: Codec](topic: String, config: ProducerConfig = ProducerConfig.default): Producer[T]
  def consumer[T: Codec](topic: String, group: String, config: ConsumerConfig = ConsumerConfig.default): Iterator[KafkaRecord[T]]
  def batchConsumer[T: Codec](topic: String, group: String, config: BatchConsumerConfig): Iterator[Batch[T]]
  def transaction[T: Codec](topic: String)(block: Transaction[T] => Unit): Unit
  def stream[T: Codec](topic: String): KafkaStream[T]
  def admin: Admin
```

### Producer

```scala
trait Producer[T]:
  def send(value: T, key: Option[String] = None, headers: Map[String, String] = Map.empty): RecordMetadata
  def sendAsync(value: T, key: Option[String] = None, headers: Map[String, String] = Map.empty): Future[RecordMetadata]
  def sendBatch(values: List[T]): List[RecordMetadata]
  def sendBatch(messages: List[Message[T]]): List[RecordMetadata]
```

### KafkaRecord

```scala
case class KafkaRecord[T](
  topic: String,
  partition: Int,
  offset: Long,
  key: Option[String],
  value: T,
  timestamp: Instant,
  headers: Map[String, String]
):
  def commit(): Unit
  def commitAsync(): Future[Unit]
```

### KafkaStream

```scala
trait KafkaStream[T]:
  def filter(predicate: T => Boolean): KafkaStream[T]
  def map[R: Codec](transform: T => R): KafkaStream[R]
  def flatMap[R: Codec](transform: T => List[R]): KafkaStream[R]
  def window(window: Window): WindowedStream[T]
  def groupBy[K](keySelector: T => K): GroupedStream[K, T]
  def branch(branches: ((T => Boolean), String)*): Unit
  def join[R: Codec](other: KafkaStream[R])(on: (T, R) => Boolean, window: FiniteDuration): JoinedStream[T, R]
  def to(topic: String): Unit
  def foreach(handler: T => Unit): Unit
```

## License

MIT
