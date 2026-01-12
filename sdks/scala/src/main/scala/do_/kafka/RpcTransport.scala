package do_.kafka

import scala.concurrent.{Future, ExecutionContext}
import io.circe.*
import io.circe.syntax.*
import java.time.Instant

/**
 * Transport layer for RPC communication with kafka.do.
 */
trait RpcTransport:

  /**
   * Sends a synchronous RPC call.
   */
  def call(method: String, params: Any*): Either[KafkaError, Any]

  /**
   * Sends an asynchronous RPC call.
   */
  def callAsync(method: String, params: Any*)(using ExecutionContext): Future[Either[KafkaError, Any]]

  /**
   * Closes the transport.
   */
  def close(): Unit

/**
 * Mock RPC transport for testing purposes.
 */
class MockRpcTransport extends RpcTransport:
  import scala.collection.mutable

  private case class TopicPartition(topic: String, partition: Int)
  private case class StoredRecord(
    key: Option[String],
    value: Array[Byte],
    headers: Map[String, String],
    timestamp: Instant,
    offset: Long
  )

  private val topics = mutable.Map[String, Int]() // topic -> partition count
  private val records = mutable.Map[TopicPartition, mutable.Buffer[StoredRecord]]()
  private val offsets = mutable.Map[(String, TopicPartition), Long]() // (group, tp) -> offset
  private val groups = mutable.Map[String, mutable.Set[String]]() // group -> topics
  private var closed = false
  private var nextOffset = mutable.Map[TopicPartition, Long]().withDefaultValue(0L)

  def isClosed: Boolean = closed

  override def call(method: String, params: Any*): Either[KafkaError, Any] =
    if closed then
      Left(ConnectionError("Transport is closed"))
    else
      method match
        case "createTopic" if params.nonEmpty =>
          val topic = params(0).toString
          val partitions = if params.length > 1 then params(1).asInstanceOf[Int] else 1
          if topics.contains(topic) then
            Left(GenericError(s"Topic $topic already exists"))
          else
            topics(topic) = partitions
            for p <- 0 until partitions do
              records(TopicPartition(topic, p)) = mutable.Buffer.empty
            Right(Map("topic" -> topic, "partitions" -> partitions))

        case "deleteTopic" if params.nonEmpty =>
          val topic = params(0).toString
          if !topics.contains(topic) then
            Left(TopicNotFoundError(topic))
          else
            val partitions = topics(topic)
            topics.remove(topic)
            for p <- 0 until partitions do
              records.remove(TopicPartition(topic, p))
            Right(Map("ok" -> true))

        case "listTopics" =>
          Right(topics.map { case (name, partitions) =>
            Map("name" -> name, "partitions" -> partitions)
          }.toList)

        case "describeTopic" if params.nonEmpty =>
          val topic = params(0).toString
          topics.get(topic) match
            case Some(partitions) =>
              Right(Map(
                "name" -> topic,
                "partitions" -> partitions,
                "retentionMs" -> 604800000L
              ))
            case None =>
              Left(TopicNotFoundError(topic))

        case "produce" if params.length >= 4 =>
          val topic = params(0).toString
          val key = Option(params(1)).map(_.toString)
          val value = params(2).asInstanceOf[Array[Byte]]
          val headers = if params.length > 3 then params(3).asInstanceOf[Map[String, String]] else Map.empty[String, String]

          if !topics.contains(topic) then
            // Auto-create topic with 1 partition
            topics(topic) = 1
            records(TopicPartition(topic, 0)) = mutable.Buffer.empty

          // Determine partition (simple hash)
          val partition = key.map(k => Math.abs(k.hashCode) % topics(topic)).getOrElse(0)
          val tp = TopicPartition(topic, partition)

          val offset = nextOffset(tp)
          nextOffset(tp) = offset + 1

          val record = StoredRecord(key, value, headers, Instant.now(), offset)
          records.getOrElseUpdate(tp, mutable.Buffer.empty) += record

          Right(Map(
            "topic" -> topic,
            "partition" -> partition,
            "offset" -> offset,
            "timestamp" -> record.timestamp.toEpochMilli,
            "keySize" -> key.map(_.getBytes("UTF-8").length).getOrElse(0),
            "valueSize" -> value.length
          ))

        case "subscribe" if params.length >= 2 =>
          val group = params(0).toString
          val topic = params(1).toString

          if !topics.contains(topic) then
            Left(TopicNotFoundError(topic))
          else
            groups.getOrElseUpdate(group, mutable.Set.empty) += topic
            // Initialize offsets to 0 for all partitions if not set
            for p <- 0 until topics(topic) do
              val tp = TopicPartition(topic, p)
              offsets.getOrElseUpdate((group, tp), 0L)
            Right(Map("ok" -> true))

        case "poll" if params.length >= 2 =>
          val group = params(0).toString
          val maxRecords = params(1).asInstanceOf[Int]

          val subscribedTopics = groups.getOrElse(group, mutable.Set.empty)
          val result = mutable.Buffer[Map[String, Any]]()

          for
            topic <- subscribedTopics
            partitions = topics.getOrElse(topic, 0)
            partition <- 0 until partitions
          do
            val tp = TopicPartition(topic, partition)
            val currentOffset = offsets.getOrElse((group, tp), 0L)
            val partitionRecords = records.getOrElse(tp, mutable.Buffer.empty)

            val toFetch = partitionRecords.slice(currentOffset.toInt, currentOffset.toInt + maxRecords)
            for (record, idx) <- toFetch.zipWithIndex do
              result += Map(
                "topic" -> topic,
                "partition" -> partition,
                "offset" -> (currentOffset + idx),
                "key" -> record.key.orNull,
                "value" -> record.value,
                "timestamp" -> record.timestamp.toEpochMilli,
                "headers" -> record.headers
              )

          Right(result.toList)

        case "commit" if params.length >= 4 =>
          val group = params(0).toString
          val topic = params(1).toString
          val partition = params(2).asInstanceOf[Int]
          val offset = params(3).asInstanceOf[Long]

          val tp = TopicPartition(topic, partition)
          offsets((group, tp)) = offset + 1 // Commit offset + 1 (next offset to fetch)
          Right(Map("ok" -> true))

        case "listGroups" =>
          Right(groups.map { case (id, topics) =>
            Map("id" -> id, "memberCount" -> 1, "topics" -> topics.toList)
          }.toList)

        case "describeGroup" if params.nonEmpty =>
          val group = params(0).toString
          groups.get(group) match
            case Some(topics) =>
              Right(Map(
                "id" -> group,
                "state" -> "Stable",
                "members" -> List(Map("id" -> "member-1")),
                "totalLag" -> 0L
              ))
            case None =>
              Left(GenericError(s"Consumer group not found: $group"))

        case "resetOffsets" if params.length >= 3 =>
          val group = params(0).toString
          val topic = params(1).toString
          val offset = params(2) match
            case "earliest" => 0L
            case "latest" =>
              val maxOffset = (0 until topics.getOrElse(topic, 0))
                .map(p => records.get(TopicPartition(topic, p)).map(_.size.toLong).getOrElse(0L))
                .maxOption.getOrElse(0L)
              maxOffset
            case o: Long => o
            case _ => 0L

          for p <- 0 until topics.getOrElse(topic, 0) do
            offsets((group, TopicPartition(topic, p))) = offset

          Right(Map("ok" -> true))

        case _ =>
          Left(GenericError(s"Unknown method: $method"))

  override def callAsync(method: String, params: Any*)(using ec: ExecutionContext): Future[Either[KafkaError, Any]] =
    Future(call(method, params*))

  override def close(): Unit =
    closed = true

  // Helper methods for testing

  def seed[T](topic: String, values: List[T])(using codec: Codec[T]): Unit =
    if !topics.contains(topic) then
      topics(topic) = 1
      records(TopicPartition(topic, 0)) = mutable.Buffer.empty

    values.foreach { value =>
      codec.encode(value) match
        case Right(bytes) =>
          call("produce", topic, null, bytes, Map.empty[String, String])
        case Left(_) => ()
    }

  def getMessages[T](topic: String)(using codec: Codec[T]): List[T] =
    val partitions = topics.getOrElse(topic, 0)
    (0 until partitions).flatMap { p =>
      records.getOrElse(TopicPartition(topic, p), mutable.Buffer.empty)
        .flatMap(r => codec.decode(r.value).toOption)
    }.toList
