package do_.kafka

import scala.concurrent.{ExecutionContext, Future}
import cats.effect.IO
import java.time.Instant

/**
 * A Kafka consumer that provides an Iterator interface over records.
 *
 * @tparam T The type of values to consume
 */
class Consumer[T] private[kafka] (
  private val transport: RpcTransport,
  val topic: String,
  val group: String,
  val config: ConsumerConfig
)(using codec: Codec[T]) extends Iterator[KafkaRecord[T]]:

  private var buffer: List[KafkaRecord[T]] = List.empty
  private var closed = false

  // Subscribe to the topic
  transport.call("subscribe", group, topic) match
    case Left(e) => throw new RuntimeException(e.message)
    case Right(_) => ()

  /**
   * Checks if there are more records available.
   */
  override def hasNext: Boolean =
    if closed then false
    else if buffer.nonEmpty then true
    else
      poll() match
        case Right(records) =>
          buffer = records
          buffer.nonEmpty
        case Left(_) => false

  /**
   * Returns the next record.
   */
  override def next(): KafkaRecord[T] =
    if buffer.isEmpty then
      poll() match
        case Right(records) => buffer = records
        case Left(e) => throw new RuntimeException(e.message)

    val record = buffer.head
    buffer = buffer.tail
    record

  /**
   * Polls for new records.
   */
  private def poll(): Either[KafkaError, List[KafkaRecord[T]]] =
    transport.call("poll", group, config.maxPollRecords).flatMap { result =>
      val records = result.asInstanceOf[List[Map[String, Any]]]
      val decoded = records.flatMap { r =>
        val bytes = r("value").asInstanceOf[Array[Byte]]
        codec.decode(bytes).toOption.map { value =>
          KafkaRecord(
            topic = r("topic").asInstanceOf[String],
            partition = r("partition").asInstanceOf[Int],
            offset = r("offset").asInstanceOf[Long],
            key = Option(r("key")).map(_.toString),
            value = value,
            timestamp = Instant.ofEpochMilli(r("timestamp").asInstanceOf[Long]),
            headers = r("headers").asInstanceOf[Map[String, String]],
            transport = transport,
            consumerGroup = group
          )
        }
      }
      Right(decoded)
    }

  /**
   * Takes a specific number of records.
   */
  override def take(n: Int): Iterator[KafkaRecord[T]] =
    new Iterator[KafkaRecord[T]]:
      private var count = 0
      private val underlying = Consumer.this

      override def hasNext: Boolean = count < n && underlying.hasNext
      override def next(): KafkaRecord[T] =
        count += 1
        underlying.next()

  /**
   * Filters records.
   */
  override def filter(p: KafkaRecord[T] => Boolean): Iterator[KafkaRecord[T]] =
    new Iterator[KafkaRecord[T]]:
      private var nextRecord: Option[KafkaRecord[T]] = None
      private val underlying = Consumer.this

      private def advance(): Unit =
        nextRecord = None
        while underlying.hasNext && nextRecord.isEmpty do
          val r = underlying.next()
          if p(r) then nextRecord = Some(r)

      override def hasNext: Boolean =
        if nextRecord.isDefined then true
        else
          advance()
          nextRecord.isDefined

      override def next(): KafkaRecord[T] =
        if nextRecord.isEmpty then advance()
        val r = nextRecord.getOrElse(throw new NoSuchElementException("No more records"))
        nextRecord = None
        r

  /**
   * Maps over records.
   */
  override def map[B](f: KafkaRecord[T] => B): Iterator[B] =
    new Iterator[B]:
      private val underlying = Consumer.this
      override def hasNext: Boolean = underlying.hasNext
      override def next(): B = f(underlying.next())

  /**
   * Closes the consumer.
   */
  def close(): Unit =
    closed = true

/**
 * A batch consumer that returns records in batches.
 */
class BatchConsumer[T] private[kafka] (
  private val transport: RpcTransport,
  val topic: String,
  val group: String,
  val config: BatchConsumerConfig
)(using codec: Codec[T]) extends Iterator[Batch[T]]:

  private val consumer = new Consumer[T](transport, topic, group, ConsumerConfig(
    offset = config.offset,
    autoCommit = config.autoCommit,
    maxPollRecords = config.batchSize
  ))

  private var closed = false

  /**
   * Checks if there are more batches available.
   */
  override def hasNext: Boolean = !closed && consumer.hasNext

  /**
   * Returns the next batch.
   */
  override def next(): Batch[T] =
    val records = consumer.take(config.batchSize).toList
    Batch(records, transport, group)

  /**
   * Closes the batch consumer.
   */
  def close(): Unit =
    closed = true
    consumer.close()
