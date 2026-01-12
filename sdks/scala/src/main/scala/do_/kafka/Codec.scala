package do_.kafka

import io.circe.*
import io.circe.generic.semiauto.*
import io.circe.syntax.*
import io.circe.parser.*
import scala.deriving.Mirror

/**
 * Type class for encoding and decoding Kafka messages.
 * Provides type-safe serialization for case classes.
 */
trait Codec[T]:
  /**
   * Encodes a value to bytes.
   */
  def encode(value: T): Either[KafkaError, Array[Byte]]

  /**
   * Decodes bytes to a value.
   */
  def decode(bytes: Array[Byte]): Either[KafkaError, T]

object Codec:

  /**
   * Creates a codec from Circe encoder/decoder.
   */
  def fromCirce[T](using encoder: Encoder[T], decoder: Decoder[T]): Codec[T] =
    new Codec[T]:
      def encode(value: T): Either[KafkaError, Array[Byte]] =
        Right(encoder(value).noSpaces.getBytes("UTF-8"))

      def decode(bytes: Array[Byte]): Either[KafkaError, T] =
        parse(new String(bytes, "UTF-8")) match
          case Left(e) => Left(SerializationError(s"Failed to parse JSON: ${e.message}"))
          case Right(json) =>
            decoder.decodeJson(json) match
              case Left(e) => Left(SerializationError(e.message))
              case Right(v) => Right(v)

  /**
   * Derive codec for case classes automatically using Circe.
   */
  inline def derived[T](using m: Mirror.ProductOf[T], e: Encoder[T], d: Decoder[T]): Codec[T] =
    fromCirce[T]

  /**
   * Summon an existing codec.
   */
  def apply[T](using codec: Codec[T]): Codec[T] = codec

  // String codec
  given Codec[String] with
    def encode(value: String): Either[KafkaError, Array[Byte]] =
      Right(value.getBytes("UTF-8"))

    def decode(bytes: Array[Byte]): Either[KafkaError, String] =
      Right(new String(bytes, "UTF-8"))

  // Byte array codec (identity)
  given Codec[Array[Byte]] with
    def encode(value: Array[Byte]): Either[KafkaError, Array[Byte]] =
      Right(value)

    def decode(bytes: Array[Byte]): Either[KafkaError, Array[Byte]] =
      Right(bytes)

  // Int codec
  given Codec[Int] with
    def encode(value: Int): Either[KafkaError, Array[Byte]] =
      Right(value.toString.getBytes("UTF-8"))

    def decode(bytes: Array[Byte]): Either[KafkaError, Int] =
      try Right(new String(bytes, "UTF-8").toInt)
      catch case e: NumberFormatException =>
        Left(SerializationError(s"Failed to decode Int: ${e.getMessage}"))

  // Long codec
  given Codec[Long] with
    def encode(value: Long): Either[KafkaError, Array[Byte]] =
      Right(value.toString.getBytes("UTF-8"))

    def decode(bytes: Array[Byte]): Either[KafkaError, Long] =
      try Right(new String(bytes, "UTF-8").toLong)
      catch case e: NumberFormatException =>
        Left(SerializationError(s"Failed to decode Long: ${e.getMessage}"))
