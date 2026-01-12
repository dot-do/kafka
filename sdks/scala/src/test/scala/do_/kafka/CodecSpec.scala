package do_.kafka

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import io.circe.*
import io.circe.generic.semiauto.*

class CodecSpec extends AnyFlatSpec with Matchers:

  case class TestMessage(id: String, value: Int)

  given Encoder[TestMessage] = deriveEncoder[TestMessage]
  given Decoder[TestMessage] = deriveDecoder[TestMessage]

  "Codec" should "encode and decode String" in:
    val codec = summon[Codec[String]]
    val original = "Hello, World!"

    val encoded = codec.encode(original)
    encoded.isRight shouldBe true

    val decoded = codec.decode(encoded.toOption.get)
    decoded shouldBe Right(original)

  it should "encode and decode Int" in:
    val codec = summon[Codec[Int]]
    val original = 42

    val encoded = codec.encode(original)
    encoded.isRight shouldBe true

    val decoded = codec.decode(encoded.toOption.get)
    decoded shouldBe Right(original)

  it should "encode and decode Long" in:
    val codec = summon[Codec[Long]]
    val original = 123456789L

    val encoded = codec.encode(original)
    encoded.isRight shouldBe true

    val decoded = codec.decode(encoded.toOption.get)
    decoded shouldBe Right(original)

  it should "encode and decode Array[Byte]" in:
    val codec = summon[Codec[Array[Byte]]]
    val original = Array[Byte](1, 2, 3, 4, 5)

    val encoded = codec.encode(original)
    encoded.isRight shouldBe true

    val decoded = codec.decode(encoded.toOption.get)
    decoded.isRight shouldBe true
    decoded.toOption.get shouldBe original

  it should "encode and decode case classes using fromCirce" in:
    val codec = Codec.fromCirce[TestMessage]
    val original = TestMessage("test-1", 100)

    val encoded = codec.encode(original)
    encoded.isRight shouldBe true

    val decoded = codec.decode(encoded.toOption.get)
    decoded shouldBe Right(original)

  it should "return error for invalid Int decode" in:
    val codec = summon[Codec[Int]]
    val invalidBytes = "not-a-number".getBytes("UTF-8")

    val decoded = codec.decode(invalidBytes)
    decoded.isLeft shouldBe true
    decoded.left.toOption.get shouldBe a[SerializationError]

  it should "return error for invalid JSON decode" in:
    val codec = Codec.fromCirce[TestMessage]
    val invalidBytes = "not-json".getBytes("UTF-8")

    val decoded = codec.decode(invalidBytes)
    decoded.isLeft shouldBe true
    decoded.left.toOption.get shouldBe a[SerializationError]

  it should "return error for wrong JSON structure" in:
    val codec = Codec.fromCirce[TestMessage]
    val wrongStructure = """{"wrong": "fields"}""".getBytes("UTF-8")

    val decoded = codec.decode(wrongStructure)
    decoded.isLeft shouldBe true
