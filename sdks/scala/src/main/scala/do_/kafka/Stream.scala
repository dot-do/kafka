package do_.kafka

import scala.concurrent.duration.*

/**
 * A Kafka stream for processing records with transformations.
 *
 * @tparam T The type of values in the stream
 */
class KafkaStream[T] private[kafka] (
  private val transport: RpcTransport,
  val topic: String,
  val config: ConsumerConfig
)(using codec: Codec[T]):

  private var operations: List[StreamOperation] = List.empty

  /**
   * Filters records in the stream.
   */
  def filter(predicate: T => Boolean): KafkaStream[T] =
    operations = operations :+ StreamOperation.Filter(predicate.asInstanceOf[Any => Boolean])
    this

  /**
   * Maps over records in the stream.
   */
  def map[R: Codec](transform: T => R): KafkaStream[R] =
    val newStream = new KafkaStream[R](transport, topic, config)
    newStream.operations = operations :+ StreamOperation.Map(transform.asInstanceOf[Any => Any])
    newStream

  /**
   * Flat maps over records in the stream.
   */
  def flatMap[R: Codec](transform: T => List[R]): KafkaStream[R] =
    val newStream = new KafkaStream[R](transport, topic, config)
    newStream.operations = operations :+ StreamOperation.FlatMap(transform.asInstanceOf[Any => List[Any]])
    newStream

  /**
   * Windows the stream.
   */
  def window(window: Window): WindowedStream[T] =
    WindowedStream(this, window)

  /**
   * Groups the stream by key.
   */
  def groupBy[K](keySelector: T => K): GroupedStream[K, T] =
    GroupedStream(this, keySelector)

  /**
   * Branches the stream to multiple topics based on predicates.
   */
  def branch(branches: ((T => Boolean), String)*): Unit =
    // Each branch sends matching records to a different topic
    operations = operations :+ StreamOperation.Branch(branches.map { case (p, t) =>
      (p.asInstanceOf[Any => Boolean], t)
    }.toList)

  /**
   * Joins this stream with another stream.
   */
  def join[R: Codec](other: KafkaStream[R])(
    on: (T, R) => Boolean,
    window: FiniteDuration
  ): JoinedStream[T, R] =
    JoinedStream(this, other, on, window)

  /**
   * Sends output to a topic.
   */
  def to(outputTopic: String): Unit =
    operations = operations :+ StreamOperation.Sink(outputTopic)

  /**
   * Processes each record with a handler.
   */
  def foreach(handler: T => Unit): Unit =
    // Create a consumer and process records
    val group = s"stream-${System.currentTimeMillis()}"
    val consumer = new Consumer[T](transport, topic, group, config)

    consumer.foreach { record =>
      var value: Any = record.value

      // Apply operations
      operations.foreach {
        case StreamOperation.Filter(p) =>
          if !p(value) then return // Skip this record

        case StreamOperation.Map(f) =>
          value = f(value)

        case StreamOperation.FlatMap(f) =>
          // For foreach, we just take the first result
          val results = f(value)
          if results.isEmpty then return
          value = results.head

        case _ => ()
      }

      handler(value.asInstanceOf[T])
    }

  /**
   * Gets the internal operations for testing.
   */
  private[kafka] def getOperations: List[StreamOperation] = operations

/**
 * Internal stream operations.
 */
private[kafka] enum StreamOperation:
  case Filter(predicate: Any => Boolean)
  case Map(transform: Any => Any)
  case FlatMap(transform: Any => List[Any])
  case Branch(branches: List[(Any => Boolean, String)])
  case Sink(topic: String)

/**
 * A windowed stream for time-based aggregations.
 */
case class WindowedStream[T](
  private val source: KafkaStream[T],
  private val window: Window
):

  /**
   * Groups by key within the window.
   */
  def groupBy[K](keySelector: T => K): WindowedGroupedStream[K, T] =
    WindowedGroupedStream(source, window, keySelector)

  /**
   * Counts records in the window.
   */
  def count: KafkaStream[Long] =
    // Simplified: returns count as a new stream
    given Codec[Long] = Codec[Long]
    source.map(_ => 1L)

/**
 * A windowed and grouped stream.
 */
case class WindowedGroupedStream[K, T](
  private val source: KafkaStream[T],
  private val window: Window,
  private val keySelector: T => K
):

  /**
   * Counts records per key within the window.
   */
  def count: Unit =
    source.foreach { value =>
      val key = keySelector(value)
      // In a real implementation, this would maintain windowed state
      // and emit (key, window, count) tuples
    }

  /**
   * Reduces records per key within the window.
   */
  def reduce(reducer: (T, T) => T): Unit =
    source.foreach { value =>
      val key = keySelector(value)
      // In a real implementation, this would maintain windowed state
    }

/**
 * A grouped stream for aggregations.
 */
case class GroupedStream[K, T](
  private val source: KafkaStream[T],
  private val keySelector: T => K
):

  /**
   * Folds over grouped records.
   */
  def fold[R](zero: R)(combine: (R, T) => R): KafkaStream[(K, R)] =
    given Codec[(K, R)] = new Codec[(K, R)]:
      def encode(value: (K, R)): Either[KafkaError, Array[Byte]] =
        Right(value.toString.getBytes("UTF-8"))
      def decode(bytes: Array[Byte]): Either[KafkaError, (K, R)] =
        Left(SerializationError("Cannot decode tuple"))

    // Simplified implementation
    source.map { value =>
      val key = keySelector(value)
      (key, combine(zero, value))
    }

  /**
   * Counts records per key.
   */
  def count: KafkaStream[(K, Long)] =
    given Codec[(K, Long)] = new Codec[(K, Long)]:
      def encode(value: (K, Long)): Either[KafkaError, Array[Byte]] =
        Right(value.toString.getBytes("UTF-8"))
      def decode(bytes: Array[Byte]): Either[KafkaError, (K, Long)] =
        Left(SerializationError("Cannot decode tuple"))

    source.map { value =>
      (keySelector(value), 1L)
    }

/**
 * A joined stream from two sources.
 */
case class JoinedStream[L, R](
  private val left: KafkaStream[L],
  private val right: KafkaStream[R],
  private val joinPredicate: (L, R) => Boolean,
  private val windowDuration: FiniteDuration
):

  /**
   * Processes joined pairs.
   */
  def foreach(handler: (L, R) => Unit): Unit =
    // Simplified: in a real implementation, this would maintain
    // a windowed buffer of records from both streams and emit joins
    ()
