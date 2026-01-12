package do_.kafka

import scala.concurrent.{ExecutionContext, Future}
import cats.effect.{IO, Resource}

/**
 * Main Kafka client - the entry point for all Kafka operations.
 *
 * Example usage:
 * {{{
 * import do_.kafka._
 *
 * case class Order(orderId: String, amount: Double) derives Codec
 *
 * val kafka = Kafka()
 *
 * // Produce messages
 * val producer = kafka.producer[Order]("orders")
 * producer.send(Order("123", 99.99))
 *
 * // Consume messages
 * kafka.consumer[Order]("orders", "my-processor").foreach { record =>
 *   println(s"Received: ${record.value}")
 *   record.commit()
 * }
 * }}}
 */
class Kafka private (
  val config: KafkaConfig,
  private val transport: RpcTransport
):

  private var closed = false

  /**
   * Creates a producer for a topic.
   *
   * @param topic The topic to produce to
   * @param config Producer configuration
   * @return A typed producer
   */
  def producer[T: Codec](topic: String, config: ProducerConfig = ProducerConfig.default): Producer[T] =
    ensureOpen()
    new Producer[T](transport, topic, config)

  /**
   * Creates a consumer for a topic.
   *
   * @param topic The topic to consume from
   * @param group The consumer group ID
   * @param config Consumer configuration
   * @return An iterator over records
   */
  def consumer[T: Codec](topic: String, group: String, config: ConsumerConfig = ConsumerConfig.default): Consumer[T] =
    ensureOpen()
    new Consumer[T](transport, topic, group, config)

  /**
   * Creates a batch consumer for a topic.
   *
   * @param topic The topic to consume from
   * @param group The consumer group ID
   * @param config Batch consumer configuration
   * @return An iterator over batches
   */
  def batchConsumer[T: Codec](topic: String, group: String, config: BatchConsumerConfig = BatchConsumerConfig.default): BatchConsumer[T] =
    ensureOpen()
    new BatchConsumer[T](transport, topic, group, config)

  /**
   * Executes a transaction.
   *
   * @param topic The topic to produce to
   * @param block The transaction block
   * @return Either an error or the list of record metadata
   */
  def transaction[T: Codec](topic: String)(block: Transaction[T] => Unit): Either[KafkaError, List[RecordMetadata]] =
    ensureOpen()
    val producer = this.producer[T](topic)
    val tx = new Transaction[T](producer, scala.collection.mutable.Buffer.empty)

    try
      block(tx)
      tx.commit()
    catch
      case e: Exception =>
        tx.abort()
        Left(GenericError(s"Transaction aborted: ${e.getMessage}"))

  /**
   * Creates a stream processor for a topic.
   *
   * @param topic The topic to stream from
   * @param config Consumer configuration
   * @return A Kafka stream
   */
  def stream[T: Codec](topic: String, config: ConsumerConfig = ConsumerConfig.default): KafkaStream[T] =
    ensureOpen()
    new KafkaStream[T](transport, topic, config)

  /**
   * Gets the admin client for topic and group management.
   */
  def admin: Admin =
    ensureOpen()
    new Admin(transport)

  /**
   * Checks if the client is closed.
   */
  def isClosed: Boolean = closed

  /**
   * Closes the client.
   */
  def close(): Unit =
    if !closed then
      closed = true
      transport.close()

  /**
   * Closes the client using Cats Effect IO.
   */
  def closeIO: IO[Unit] = IO(close())

  private def ensureOpen(): Unit =
    if closed then throw new IllegalStateException("Kafka client is closed")

  /**
   * Sets a custom transport (for testing).
   */
  private[kafka] def setTransport(t: RpcTransport): Unit =
    // This is a no-op for the public API, only used in testing

object Kafka:

  /**
   * Creates a Kafka client with default configuration.
   */
  def apply(): Kafka =
    apply(KafkaConfig.default)

  /**
   * Creates a Kafka client with custom configuration.
   */
  def apply(config: KafkaConfig): Kafka =
    new Kafka(config, new MockRpcTransport())

  /**
   * Creates a Kafka client from environment variables.
   */
  def fromEnv(): Kafka =
    apply(KafkaConfig.fromEnv)

  /**
   * Creates a resource-managed Kafka client.
   */
  def resource(config: KafkaConfig = KafkaConfig.default): Resource[IO, Kafka] =
    Resource.make(IO(apply(config)))(kafka => kafka.closeIO)

  /**
   * Creates a resource-managed client from environment.
   */
  def resourceFromEnv(): Resource[IO, Kafka] =
    Resource.make(IO(fromEnv()))(kafka => kafka.closeIO)
