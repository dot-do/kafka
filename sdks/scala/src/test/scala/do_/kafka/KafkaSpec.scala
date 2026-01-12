package do_.kafka

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterEach
import io.circe.*
import io.circe.generic.semiauto.*

class KafkaSpec extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  case class Order(orderId: String, amount: Double)

  given Encoder[Order] = deriveEncoder[Order]
  given Decoder[Order] = deriveDecoder[Order]
  given Codec[Order] = Codec.fromCirce[Order]

  var kafka: Kafka = _

  override def beforeEach(): Unit =
    kafka = Kafka()

  override def afterEach(): Unit =
    kafka.close()

  // ============ Producer Tests ============

  "Producer" should "send a message successfully" in:
    val producer = kafka.producer[Order]("orders")
    val result = producer.send(Order("123", 99.99))

    result.isRight shouldBe true
    result.toOption.get.topic shouldBe "orders"
    result.toOption.get.offset shouldBe 0L

  it should "send a message with key" in:
    val producer = kafka.producer[Order]("orders")
    val result = producer.send(Order("123", 99.99), key = Some("customer-1"))

    result.isRight shouldBe true

  it should "send a message with headers" in:
    val producer = kafka.producer[Order]("orders")
    val result = producer.send(
      Order("123", 99.99),
      headers = Map("correlation-id" -> "abc-123")
    )

    result.isRight shouldBe true

  it should "send a batch of messages" in:
    val producer = kafka.producer[Order]("orders")
    val results = producer.sendBatch(List(
      Order("1", 10.0),
      Order("2", 20.0),
      Order("3", 30.0)
    ))

    results.size shouldBe 3
    results.forall(_.isRight) shouldBe true

  it should "send a batch of messages with keys" in:
    val producer = kafka.producer[Order]("orders")
    val results = producer.sendBatch(List(
      Message(Order("1", 10.0), key = Some("cust-1")),
      Message(Order("2", 20.0), key = Some("cust-2"))
    ))

    results.size shouldBe 2
    results.forall(_.isRight) shouldBe true

  // ============ Consumer Tests ============

  "Consumer" should "consume messages" in:
    // Produce some messages first
    val producer = kafka.producer[Order]("test-orders")
    producer.send(Order("1", 10.0))
    producer.send(Order("2", 20.0))
    producer.send(Order("3", 30.0))

    // Consume
    val consumer = kafka.consumer[Order]("test-orders", "test-group")
    val records = consumer.take(3).toList

    records.size shouldBe 3
    records.map(_.value.orderId) shouldBe List("1", "2", "3")

  it should "filter records" in:
    val producer = kafka.producer[Order]("filter-test")
    producer.send(Order("1", 50.0))
    producer.send(Order("2", 150.0))
    producer.send(Order("3", 75.0))

    val consumer = kafka.consumer[Order]("filter-test", "filter-group")
    val filtered = consumer.filter(_.value.amount > 100).take(1).toList

    filtered.size shouldBe 1
    filtered.head.value.orderId shouldBe "2"

  it should "map over records" in:
    val producer = kafka.producer[Order]("map-test")
    producer.send(Order("1", 100.0))

    val consumer = kafka.consumer[Order]("map-test", "map-group")
    val amounts = consumer.take(1).map(_.value.amount).toList

    amounts shouldBe List(100.0)

  it should "commit offsets" in:
    val producer = kafka.producer[Order]("commit-test")
    producer.send(Order("1", 10.0))

    val consumer = kafka.consumer[Order]("commit-test", "commit-group")
    val record = consumer.next()

    record.commit() shouldBe Right(())

  // ============ Batch Consumer Tests ============

  "BatchConsumer" should "consume in batches" in:
    val producer = kafka.producer[Order]("batch-test")
    for i <- 1 to 10 do
      producer.send(Order(i.toString, i * 10.0))

    val config = BatchConsumerConfig(batchSize = 5)
    val consumer = kafka.batchConsumer[Order]("batch-test", "batch-group", config)
    val batch = consumer.next()

    batch.size shouldBe 5
    batch.nonEmpty shouldBe true

  it should "commit batch offsets" in:
    val producer = kafka.producer[Order]("batch-commit-test")
    for i <- 1 to 3 do
      producer.send(Order(i.toString, i * 10.0))

    val config = BatchConsumerConfig(batchSize = 3)
    val consumer = kafka.batchConsumer[Order]("batch-commit-test", "batch-commit-group", config)
    val batch = consumer.next()

    batch.commit() shouldBe Right(())

  // ============ Transaction Tests ============

  "Transaction" should "commit messages atomically" in:
    val result = kafka.transaction[Order]("tx-test") { tx =>
      tx.send(Order("1", 10.0))
      tx.send(Order("2", 20.0))
    }

    result.isRight shouldBe true
    result.toOption.get.size shouldBe 2

  // ============ Admin Tests ============

  "Admin" should "create a topic" in:
    val admin = kafka.admin
    val result = admin.createTopic("new-topic", TopicConfig(partitions = 3))

    result.isRight shouldBe true
    result.toOption.get.name shouldBe "new-topic"
    result.toOption.get.partitions shouldBe 3

  it should "list topics" in:
    val admin = kafka.admin
    admin.createTopic("topic-1")
    admin.createTopic("topic-2")

    val result = admin.listTopics()
    result.isRight shouldBe true
    result.toOption.get.map(_.name) should contain allOf ("topic-1", "topic-2")

  it should "describe a topic" in:
    val admin = kafka.admin
    admin.createTopic("describe-test", TopicConfig(partitions = 5))

    val result = admin.describeTopic("describe-test")
    result.isRight shouldBe true
    result.toOption.get.partitions shouldBe 5

  it should "delete a topic" in:
    val admin = kafka.admin
    admin.createTopic("delete-test")

    val result = admin.deleteTopic("delete-test")
    result.isRight shouldBe true

    admin.describeTopic("delete-test").isLeft shouldBe true

  it should "list consumer groups" in:
    // Create a consumer to register a group
    kafka.consumer[Order]("group-list-test", "test-consumer-group")

    val admin = kafka.admin
    val result = admin.listGroups()

    result.isRight shouldBe true

  it should "reset offsets" in:
    val producer = kafka.producer[Order]("reset-test")
    producer.send(Order("1", 10.0))
    producer.send(Order("2", 20.0))

    // Create consumer and consume
    val consumer = kafka.consumer[Order]("reset-test", "reset-group")
    consumer.take(2).toList

    // Reset offsets
    val admin = kafka.admin
    val result = admin.resetOffsets("reset-group", "reset-test", Offset.Earliest)

    result.isRight shouldBe true

  // ============ Error Handling Tests ============

  "Error handling" should "return TopicNotFoundError for non-existent topic" in:
    val admin = kafka.admin
    val result = admin.describeTopic("non-existent")

    result.isLeft shouldBe true
    result.left.toOption.get shouldBe a[TopicNotFoundError]

  it should "return error when creating duplicate topic" in:
    val admin = kafka.admin
    admin.createTopic("duplicate-test")

    val result = admin.createTopic("duplicate-test")
    result.isLeft shouldBe true

  // ============ Resource Management Tests ============

  "Kafka client" should "be closeable" in:
    val k = Kafka()
    k.isClosed shouldBe false

    k.close()
    k.isClosed shouldBe true

  it should "throw when used after close" in:
    val k = Kafka()
    k.close()

    an[IllegalStateException] should be thrownBy:
      k.producer[Order]("test")
