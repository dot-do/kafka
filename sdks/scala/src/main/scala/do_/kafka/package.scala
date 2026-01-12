package do_

/**
 * Kafka SDK for Scala.
 *
 * This package provides a functional, type-safe Kafka client with:
 * - Producer and consumer APIs with Iterator support
 * - Stream processing with functional transformations
 * - Cats Effect IO integration
 * - Automatic codec derivation for case classes
 *
 * Example usage:
 * {{{
 * import do_.kafka._
 *
 * case class Order(orderId: String, amount: Double) derives Codec
 *
 * // Create client
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
 *
 * // Stream processing
 * kafka.stream[Order]("orders")
 *   .filter(_.amount > 100)
 *   .map(order => PremiumOrder(order, "premium"))
 *   .to("premium-orders")
 *
 * // Close client
 * kafka.close()
 * }}}
 */
package object kafka:

  // Re-export commonly used types
  type KafkaError = do_.kafka.KafkaError
  type Codec[T] = do_.kafka.Codec[T]
  type KafkaRecord[T] = do_.kafka.KafkaRecord[T]
  type RecordMetadata = do_.kafka.RecordMetadata
  type Message[T] = do_.kafka.Message[T]
  type Batch[T] = do_.kafka.Batch[T]

  // Re-export companion objects
  val Codec = do_.kafka.Codec
  val Kafka = do_.kafka.Kafka

  // Re-export configuration types
  type KafkaConfig = do_.kafka.KafkaConfig
  type ProducerConfig = do_.kafka.ProducerConfig
  type ConsumerConfig = do_.kafka.ConsumerConfig
  type BatchConsumerConfig = do_.kafka.BatchConsumerConfig
  type TopicConfig = do_.kafka.TopicConfig

  val KafkaConfig = do_.kafka.KafkaConfig
  val ProducerConfig = do_.kafka.ProducerConfig
  val ConsumerConfig = do_.kafka.ConsumerConfig
  val BatchConsumerConfig = do_.kafka.BatchConsumerConfig
  val TopicConfig = do_.kafka.TopicConfig

  // Re-export enums
  type Offset = do_.kafka.Offset
  type Compression = do_.kafka.Compression
  type Acks = do_.kafka.Acks
  type Window = do_.kafka.Window

  val Offset = do_.kafka.Offset
  val Compression = do_.kafka.Compression
  val Acks = do_.kafka.Acks
  val Window = do_.kafka.Window

  // Re-export error types
  type ConnectionError = do_.kafka.ConnectionError
  type AuthenticationError = do_.kafka.AuthenticationError
  type TopicNotFoundError = do_.kafka.TopicNotFoundError
  type MessageTooLargeError = do_.kafka.MessageTooLargeError
  type TimeoutError = do_.kafka.TimeoutError
  type SerializationError = do_.kafka.SerializationError

  // Re-export admin types
  type TopicInfo = do_.kafka.TopicInfo
  type GroupInfo = do_.kafka.GroupInfo
  type DetailedGroupInfo = do_.kafka.DetailedGroupInfo

  /**
   * Extension methods for working with Either[KafkaError, T].
   */
  extension [T](result: Either[KafkaError, T])
    /**
     * Converts to Option, discarding error information.
     */
    def toOption: Option[T] = result.toOption

    /**
     * Gets the value or throws the error.
     */
    def getOrThrow: T = result match
      case Right(v) => v
      case Left(e) => throw e

    /**
     * Maps over the success value.
     */
    def mapValue[U](f: T => U): Either[KafkaError, U] = result.map(f)

    /**
     * Flat maps over the success value.
     */
    def flatMapValue[U](f: T => Either[KafkaError, U]): Either[KafkaError, U] = result.flatMap(f)

    /**
     * Recovers from retriable errors.
     */
    def recoverRetriable(f: KafkaError => T): Either[KafkaError, T] =
      result.left.flatMap { e =>
        if e.isRetriable then Right(f(e))
        else Left(e)
      }

    /**
     * Taps into the result for side effects.
     */
    def tap(f: T => Unit): Either[KafkaError, T] =
      result.foreach(f)
      result

    /**
     * Taps into errors for side effects.
     */
    def tapError(f: KafkaError => Unit): Either[KafkaError, T] =
      result.left.foreach(f)
      result

  /**
   * Extension methods for Option to convert to Either[KafkaError, T].
   */
  extension [T](opt: Option[T])
    /**
     * Converts Option to Either with a custom error.
     */
    def toKafkaError(error: => KafkaError): Either[KafkaError, T] =
      opt.toRight(error)

    /**
     * Converts Option to Either with a topic not found error.
     */
    def orTopicNotFound(topic: String): Either[KafkaError, T] =
      opt.toRight(TopicNotFoundError(topic))
