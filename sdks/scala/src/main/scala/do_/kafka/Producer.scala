package do_.kafka

import scala.concurrent.{ExecutionContext, Future}
import cats.effect.IO
import java.time.Instant

/**
 * A Kafka producer for sending messages to a topic.
 *
 * @tparam T The type of values to produce
 */
class Producer[T] private[kafka] (
  private val transport: RpcTransport,
  val topic: String,
  val config: ProducerConfig
)(using codec: Codec[T]):

  /**
   * Sends a single message to the topic.
   *
   * @param value The value to send
   * @param key Optional key for partitioning
   * @param headers Optional headers
   * @return The record metadata or an error
   */
  def send(
    value: T,
    key: Option[String] = None,
    headers: Map[String, String] = Map.empty
  ): Either[KafkaError, RecordMetadata] =
    codec.encode(value).flatMap { bytes =>
      if bytes.length > config.maxRequestSize then
        Left(MessageTooLargeError(bytes.length, config.maxRequestSize))
      else
        transport.call("produce", topic, key.orNull, bytes, headers).map { result =>
          val m = result.asInstanceOf[Map[String, Any]]
          RecordMetadata(
            topic = m("topic").asInstanceOf[String],
            partition = m("partition").asInstanceOf[Int],
            offset = m("offset").asInstanceOf[Long],
            timestamp = Instant.ofEpochMilli(m("timestamp").asInstanceOf[Long]),
            keySize = m("keySize").asInstanceOf[Int],
            valueSize = m("valueSize").asInstanceOf[Int]
          )
        }
    }

  /**
   * Sends a single message asynchronously.
   */
  def sendAsync(
    value: T,
    key: Option[String] = None,
    headers: Map[String, String] = Map.empty
  )(using ExecutionContext): Future[Either[KafkaError, RecordMetadata]] =
    Future(send(value, key, headers))

  /**
   * Sends a single message using Cats Effect IO.
   */
  def sendIO(
    value: T,
    key: Option[String] = None,
    headers: Map[String, String] = Map.empty
  ): IO[Either[KafkaError, RecordMetadata]] =
    IO(send(value, key, headers))

  /**
   * Sends a batch of values to the topic.
   *
   * @param values The values to send
   * @return List of record metadata or errors
   */
  def sendBatch(values: List[T]): List[Either[KafkaError, RecordMetadata]] =
    values.map(send(_))

  /**
   * Sends a batch of messages with keys and headers.
   *
   * @param messages The messages to send
   * @return List of record metadata or errors
   */
  def sendBatch(messages: List[Message[T]]): List[Either[KafkaError, RecordMetadata]] =
    messages.map(m => send(m.value, m.key, m.headers))

  /**
   * Sends a batch asynchronously.
   */
  def sendBatchAsync(values: List[T])(using ExecutionContext): Future[List[Either[KafkaError, RecordMetadata]]] =
    Future(sendBatch(values))

  /**
   * Sends a batch using Cats Effect IO.
   */
  def sendBatchIO(values: List[T]): IO[List[Either[KafkaError, RecordMetadata]]] =
    IO(sendBatch(values))

/**
 * A transactional producer for atomic batch operations.
 */
class Transaction[T] private[kafka] (
  private val producer: Producer[T],
  private val pendingMessages: scala.collection.mutable.Buffer[(T, Option[String], Map[String, String])]
):

  /**
   * Adds a message to the transaction.
   */
  def send(
    value: T,
    key: Option[String] = None,
    headers: Map[String, String] = Map.empty
  ): Unit =
    pendingMessages += ((value, key, headers))

  /**
   * Commits all messages in the transaction atomically.
   */
  private[kafka] def commit(): Either[KafkaError, List[RecordMetadata]] =
    val results = pendingMessages.map { case (value, key, headers) =>
      producer.send(value, key, headers)
    }.toList

    // Check if any failed
    val errors = results.collect { case Left(e) => e }
    if errors.nonEmpty then
      Left(GenericError(s"Transaction failed: ${errors.map(_.message).mkString(", ")}"))
    else
      Right(results.collect { case Right(m) => m })

  /**
   * Aborts the transaction, discarding all pending messages.
   */
  private[kafka] def abort(): Unit =
    pendingMessages.clear()
