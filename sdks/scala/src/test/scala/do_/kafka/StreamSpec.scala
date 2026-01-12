package do_.kafka

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterEach
import io.circe.*
import io.circe.generic.semiauto.*
import scala.concurrent.duration.*

class StreamSpec extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  case class Order(orderId: String, amount: Double, region: String = "us")
  case class PremiumOrder(order: Order, tier: String)

  given Encoder[Order] = deriveEncoder[Order]
  given Decoder[Order] = deriveDecoder[Order]
  given Codec[Order] = Codec.fromCirce[Order]

  given Encoder[PremiumOrder] = deriveEncoder[PremiumOrder]
  given Decoder[PremiumOrder] = deriveDecoder[PremiumOrder]
  given Codec[PremiumOrder] = Codec.fromCirce[PremiumOrder]

  var kafka: Kafka = _

  override def beforeEach(): Unit =
    kafka = Kafka()

  override def afterEach(): Unit =
    kafka.close()

  "KafkaStream" should "filter records" in:
    val producer = kafka.producer[Order]("stream-filter-test")
    producer.send(Order("1", 50.0))
    producer.send(Order("2", 150.0))
    producer.send(Order("3", 75.0))

    val stream = kafka.stream[Order]("stream-filter-test")
      .filter(_.amount > 100)

    stream.getOperations.size shouldBe 1

  it should "map records" in:
    val producer = kafka.producer[Order]("stream-map-test")
    producer.send(Order("1", 100.0))

    val stream = kafka.stream[Order]("stream-map-test")
      .map(order => PremiumOrder(order, "gold"))

    stream.getOperations.size shouldBe 1

  it should "chain multiple operations" in:
    val producer = kafka.producer[Order]("stream-chain-test")
    producer.send(Order("1", 50.0))
    producer.send(Order("2", 150.0))

    val stream = kafka.stream[Order]("stream-chain-test")
      .filter(_.amount > 100)
      .map(order => PremiumOrder(order, "platinum"))

    stream.getOperations.size shouldBe 2

  it should "support windowing" in:
    val stream = kafka.stream[Order]("stream-window-test")
      .window(Window.Tumbling(5.minutes))

    // Windowed stream should be created
    stream shouldBe a[WindowedStream[?]]

  it should "support grouping" in:
    val stream = kafka.stream[Order]("stream-group-test")
      .groupBy(_.region)

    stream shouldBe a[GroupedStream[?, ?]]

  it should "support branching" in:
    val producer = kafka.producer[Order]("stream-branch-test")
    producer.send(Order("1", 100.0, "us"))
    producer.send(Order("2", 200.0, "eu"))

    // Branching should set up the branch operation
    val stream = kafka.stream[Order]("stream-branch-test")
    stream.branch(
      (_.region == "us") -> "us-orders",
      (_.region == "eu") -> "eu-orders"
    )

    // The branch operation should be added
    // Note: In a real implementation, this would route to different topics

  "WindowedStream" should "support groupBy" in:
    val windowed = kafka.stream[Order]("windowed-group-test")
      .window(Window.Tumbling(1.minute))
      .groupBy(_.region)

    windowed shouldBe a[WindowedGroupedStream[?, ?]]

  it should "support count" in:
    val windowed = kafka.stream[Order]("windowed-count-test")
      .window(Window.Hopping(1.minute, 30.seconds))

    val countStream = windowed.count
    countStream shouldBe a[KafkaStream[?]]

  "GroupedStream" should "support fold" in:
    val grouped = kafka.stream[Order]("grouped-fold-test")
      .groupBy(_.region)
      .fold(0.0)((total, order) => total + order.amount)

    grouped shouldBe a[KafkaStream[?]]

  it should "support count" in:
    val grouped = kafka.stream[Order]("grouped-count-test")
      .groupBy(_.region)
      .count

    grouped shouldBe a[KafkaStream[?]]

  "JoinedStream" should "be created from two streams" in:
    case class Customer(id: String, name: String)

    given Encoder[Customer] = deriveEncoder[Customer]
    given Decoder[Customer] = deriveDecoder[Customer]
    given Codec[Customer] = Codec.fromCirce[Customer]

    val orders = kafka.stream[Order]("join-orders")
    val customers = kafka.stream[Customer]("join-customers")

    val joined = orders.join(customers)(
      on = (order, customer) => order.orderId.startsWith(customer.id),
      window = 1.hour
    )

    joined shouldBe a[JoinedStream[?, ?]]
