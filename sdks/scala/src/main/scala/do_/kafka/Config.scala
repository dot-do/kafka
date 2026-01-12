package do_.kafka

import scala.concurrent.duration.*
import java.time.Instant

/**
 * Kafka client configuration.
 */
case class KafkaConfig(
  url: String = sys.env.getOrElse("KAFKA_DO_URL", "https://kafka.do"),
  apiKey: Option[String] = sys.env.get("KAFKA_DO_API_KEY"),
  timeout: FiniteDuration = 30.seconds,
  retries: Int = 3,
  retryBackoff: FiniteDuration = 100.millis
)

object KafkaConfig:
  val default: KafkaConfig = KafkaConfig()

  def fromEnv: KafkaConfig = KafkaConfig(
    url = sys.env.getOrElse("KAFKA_DO_URL", "https://kafka.do"),
    apiKey = sys.env.get("KAFKA_DO_API_KEY")
  )

/**
 * Producer configuration.
 */
case class ProducerConfig(
  batchSize: Int = 16384,
  lingerMs: Int = 5,
  compression: Compression = Compression.None,
  acks: Acks = Acks.All,
  retries: Int = 3,
  retryBackoffMs: Int = 100,
  maxRequestSize: Int = 1048576
)

object ProducerConfig:
  val default: ProducerConfig = ProducerConfig()

/**
 * Compression type for producer messages.
 */
enum Compression:
  case None, Gzip, Snappy, Lz4, Zstd

/**
 * Acknowledgment setting for producers.
 */
enum Acks:
  case None     // acks=0
  case Leader   // acks=1
  case All      // acks=all/-1

object Acks:
  def fromInt(n: Int): Acks = n match
    case 0 => Acks.None
    case 1 => Acks.Leader
    case _ => Acks.All

/**
 * Consumer configuration.
 */
case class ConsumerConfig(
  offset: Offset = Offset.Latest,
  autoCommit: Boolean = false,
  fetchMinBytes: Int = 1,
  fetchMaxWaitMs: Int = 500,
  maxPollRecords: Int = 500,
  sessionTimeout: FiniteDuration = 30.seconds,
  heartbeatInterval: FiniteDuration = 3.seconds
)

object ConsumerConfig:
  val default: ConsumerConfig = ConsumerConfig()

/**
 * Offset position for consumers.
 */
enum Offset:
  case Earliest
  case Latest
  case Timestamp(instant: Instant)
  case Specific(offset: Long)

/**
 * Batch consumer configuration.
 */
case class BatchConsumerConfig(
  batchSize: Int = 100,
  batchTimeout: FiniteDuration = 5.seconds,
  offset: Offset = Offset.Latest,
  autoCommit: Boolean = false
)

object BatchConsumerConfig:
  val default: BatchConsumerConfig = BatchConsumerConfig()

/**
 * Topic configuration for administration.
 */
case class TopicConfig(
  partitions: Int = 1,
  replicationFactor: Short = 1,
  retentionMs: Long = 604800000L, // 7 days
  cleanupPolicy: CleanupPolicy = CleanupPolicy.Delete,
  compressionType: Compression = Compression.None
)

object TopicConfig:
  val default: TopicConfig = TopicConfig()

/**
 * Cleanup policy for topics.
 */
enum CleanupPolicy:
  case Delete
  case Compact
  case DeleteAndCompact

/**
 * Windowing configuration for stream processing.
 */
enum Window:
  case Tumbling(duration: FiniteDuration)
  case Hopping(duration: FiniteDuration, advance: FiniteDuration)
  case Session(inactivityGap: FiniteDuration)
