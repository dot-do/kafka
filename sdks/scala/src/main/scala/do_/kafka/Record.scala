package do_.kafka

import java.time.Instant
import scala.concurrent.{ExecutionContext, Future}
import cats.effect.IO

/**
 * A Kafka record received from a consumer.
 *
 * @param topic The topic the record was consumed from
 * @param partition The partition within the topic
 * @param offset The offset of this record in the partition
 * @param key The record key (if any)
 * @param value The deserialized record value
 * @param timestamp The timestamp of the record
 * @param headers The record headers
 */
case class KafkaRecord[T](
  topic: String,
  partition: Int,
  offset: Long,
  key: Option[String],
  value: T,
  timestamp: Instant,
  headers: Map[String, String],
  private val transport: RpcTransport,
  private val consumerGroup: String
):

  /**
   * Commits this record's offset synchronously.
   */
  def commit(): Either[KafkaError, Unit] =
    transport.call("commit", consumerGroup, topic, partition, offset).map(_ => ())

  /**
   * Commits this record's offset asynchronously.
   */
  def commitAsync()(using ExecutionContext): Future[Either[KafkaError, Unit]] =
    Future(commit())

  /**
   * Commits this record's offset using Cats Effect IO.
   */
  def commitIO: IO[Either[KafkaError, Unit]] =
    IO(commit())

  /**
   * Returns a copy without the transport (for serialization).
   */
  def toSerializable: SerializableRecord[T] =
    SerializableRecord(topic, partition, offset, key, value, timestamp, headers)

/**
 * Serializable version of KafkaRecord without transport.
 */
case class SerializableRecord[T](
  topic: String,
  partition: Int,
  offset: Long,
  key: Option[String],
  value: T,
  timestamp: Instant,
  headers: Map[String, String]
)

/**
 * Metadata returned after producing a message.
 */
case class RecordMetadata(
  topic: String,
  partition: Int,
  offset: Long,
  timestamp: Instant,
  keySize: Int,
  valueSize: Int
)

/**
 * A message to be sent with optional key and headers.
 */
case class Message[T](
  value: T,
  key: Option[String] = None,
  headers: Map[String, String] = Map.empty
)

/**
 * A batch of records for batch consumption.
 */
case class Batch[T](
  records: List[KafkaRecord[T]],
  private val transport: RpcTransport,
  private val consumerGroup: String
):

  /**
   * Commits all records in this batch.
   */
  def commit(): Either[KafkaError, Unit] =
    if records.isEmpty then Right(())
    else
      // Commit the highest offset for each partition
      val maxOffsets = records.groupBy(r => (r.topic, r.partition))
        .map { case ((topic, partition), recs) =>
          (topic, partition, recs.maxBy(_.offset).offset)
        }
        .toList

      // Commit each partition
      maxOffsets.foldLeft[Either[KafkaError, Unit]](Right(())) { case (acc, (topic, partition, offset)) =>
        acc.flatMap(_ => transport.call("commit", consumerGroup, topic, partition, offset).map(_ => ()))
      }

  /**
   * Commits all records asynchronously.
   */
  def commitAsync()(using ExecutionContext): Future[Either[KafkaError, Unit]] =
    Future(commit())

  /**
   * Commits all records using Cats Effect IO.
   */
  def commitIO: IO[Either[KafkaError, Unit]] =
    IO(commit())

  /**
   * Number of records in this batch.
   */
  def size: Int = records.size

  /**
   * Whether this batch is empty.
   */
  def isEmpty: Boolean = records.isEmpty

  /**
   * Whether this batch is non-empty.
   */
  def nonEmpty: Boolean = records.nonEmpty
