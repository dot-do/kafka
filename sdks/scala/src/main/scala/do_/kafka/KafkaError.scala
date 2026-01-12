package do_.kafka

import scala.util.control.NoStackTrace

/**
 * Sealed trait representing all Kafka errors.
 * Use pattern matching to handle specific error types.
 */
sealed trait KafkaError extends NoStackTrace:
  def message: String
  def isRetriable: Boolean
  override def getMessage: String = message

object KafkaError:
  /** Create a generic KafkaError */
  def apply(msg: String): KafkaError = GenericError(msg)

/**
 * Generic Kafka error.
 */
final case class GenericError(
  message: String,
  isRetriable: Boolean = false
) extends KafkaError

/**
 * Connection-related errors.
 */
final case class ConnectionError(
  message: String,
  host: Option[String] = None,
  isRetriable: Boolean = true
) extends KafkaError

/**
 * Authentication failures.
 */
final case class AuthenticationError(
  message: String,
  isRetriable: Boolean = false
) extends KafkaError

/**
 * Authorization/permission errors.
 */
final case class AuthorizationError(
  message: String,
  isRetriable: Boolean = false
) extends KafkaError

/**
 * Topic not found error.
 */
final case class TopicNotFoundError(
  topic: String,
  message: String = "",
  isRetriable: Boolean = false
) extends KafkaError:
  override def getMessage: String =
    if message.isEmpty then s"Topic not found: $topic"
    else message

/**
 * Partition not found error.
 */
final case class PartitionNotFoundError(
  topic: String,
  partition: Int,
  isRetriable: Boolean = false
) extends KafkaError:
  def message: String = s"Partition $partition not found for topic $topic"

/**
 * Message too large error.
 */
final case class MessageTooLargeError(
  size: Long,
  maxSize: Long,
  isRetriable: Boolean = false
) extends KafkaError:
  def message: String = s"Message size $size exceeds maximum $maxSize"

/**
 * Not leader for partition error.
 */
final case class NotLeaderError(
  topic: String,
  partition: Int,
  isRetriable: Boolean = true
) extends KafkaError:
  def message: String = s"Not leader for partition $partition of topic $topic"

/**
 * Offset out of range error.
 */
final case class OffsetOutOfRangeError(
  offset: Long,
  topic: String,
  isRetriable: Boolean = false
) extends KafkaError:
  def message: String = s"Offset $offset out of range for topic $topic"

/**
 * Group coordinator error.
 */
final case class GroupCoordinatorError(
  message: String,
  isRetriable: Boolean = true
) extends KafkaError

/**
 * Rebalance in progress error.
 */
final case class RebalanceInProgressError(
  isRetriable: Boolean = true
) extends KafkaError:
  def message: String = "Consumer group rebalance in progress"

/**
 * Quota exceeded error.
 */
final case class QuotaExceededError(
  isRetriable: Boolean = true
) extends KafkaError:
  def message: String = "Quota exceeded"

/**
 * Timeout error.
 */
final case class TimeoutError(
  operation: String,
  timeoutMs: Long = 0,
  isRetriable: Boolean = true
) extends KafkaError:
  def message: String = s"Operation '$operation' timed out after ${timeoutMs}ms"

/**
 * Disconnected error.
 */
final case class DisconnectedError(
  isRetriable: Boolean = true
) extends KafkaError:
  def message: String = "Disconnected from broker"

/**
 * Serialization error.
 */
final case class SerializationError(
  message: String,
  cause: Option[Throwable] = None,
  isRetriable: Boolean = false
) extends KafkaError
