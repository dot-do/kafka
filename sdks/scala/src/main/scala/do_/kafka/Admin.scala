package do_.kafka

import scala.concurrent.{ExecutionContext, Future}
import cats.effect.IO

/**
 * Kafka administrative operations for managing topics and consumer groups.
 */
class Admin private[kafka] (
  private val transport: RpcTransport
):

  // ============ Topic Operations ============

  /**
   * Creates a new topic.
   *
   * @param name The topic name
   * @param config The topic configuration
   * @return Either an error or the created topic info
   */
  def createTopic(name: String, config: TopicConfig = TopicConfig.default): Either[KafkaError, TopicInfo] =
    transport.call("createTopic", name, config.partitions).map { result =>
      val m = result.asInstanceOf[Map[String, Any]]
      TopicInfo(
        name = m("topic").asInstanceOf[String],
        partitions = m("partitions").asInstanceOf[Int],
        retentionMs = config.retentionMs
      )
    }

  /**
   * Creates a topic asynchronously.
   */
  def createTopicAsync(name: String, config: TopicConfig = TopicConfig.default)(using ExecutionContext): Future[Either[KafkaError, TopicInfo]] =
    Future(createTopic(name, config))

  /**
   * Creates a topic using Cats Effect IO.
   */
  def createTopicIO(name: String, config: TopicConfig = TopicConfig.default): IO[Either[KafkaError, TopicInfo]] =
    IO(createTopic(name, config))

  /**
   * Deletes a topic.
   *
   * @param name The topic name
   * @return Either an error or unit
   */
  def deleteTopic(name: String): Either[KafkaError, Unit] =
    transport.call("deleteTopic", name).map(_ => ())

  /**
   * Deletes a topic asynchronously.
   */
  def deleteTopicAsync(name: String)(using ExecutionContext): Future[Either[KafkaError, Unit]] =
    Future(deleteTopic(name))

  /**
   * Deletes a topic using Cats Effect IO.
   */
  def deleteTopicIO(name: String): IO[Either[KafkaError, Unit]] =
    IO(deleteTopic(name))

  /**
   * Lists all topics.
   *
   * @return Either an error or list of topic info
   */
  def listTopics(): Either[KafkaError, List[TopicInfo]] =
    transport.call("listTopics").map { result =>
      result.asInstanceOf[List[Map[String, Any]]].map { m =>
        TopicInfo(
          name = m("name").asInstanceOf[String],
          partitions = m("partitions").asInstanceOf[Int],
          retentionMs = 0L
        )
      }
    }

  /**
   * Lists topics asynchronously.
   */
  def listTopicsAsync()(using ExecutionContext): Future[Either[KafkaError, List[TopicInfo]]] =
    Future(listTopics())

  /**
   * Lists topics using Cats Effect IO.
   */
  def listTopicsIO(): IO[Either[KafkaError, List[TopicInfo]]] =
    IO(listTopics())

  /**
   * Describes a topic.
   *
   * @param name The topic name
   * @return Either an error or detailed topic info
   */
  def describeTopic(name: String): Either[KafkaError, TopicInfo] =
    transport.call("describeTopic", name).map { result =>
      val m = result.asInstanceOf[Map[String, Any]]
      TopicInfo(
        name = m("name").asInstanceOf[String],
        partitions = m("partitions").asInstanceOf[Int],
        retentionMs = m("retentionMs").asInstanceOf[Long]
      )
    }

  /**
   * Describes a topic asynchronously.
   */
  def describeTopicAsync(name: String)(using ExecutionContext): Future[Either[KafkaError, TopicInfo]] =
    Future(describeTopic(name))

  /**
   * Describes a topic using Cats Effect IO.
   */
  def describeTopicIO(name: String): IO[Either[KafkaError, TopicInfo]] =
    IO(describeTopic(name))

  /**
   * Alters topic configuration.
   *
   * @param name The topic name
   * @param config The new configuration
   * @return Either an error or unit
   */
  def alterTopic(name: String, config: TopicConfig): Either[KafkaError, Unit] =
    transport.call("alterTopic", name, config).map(_ => ())

  // ============ Consumer Group Operations ============

  /**
   * Lists all consumer groups.
   *
   * @return Either an error or list of group info
   */
  def listGroups(): Either[KafkaError, List[GroupInfo]] =
    transport.call("listGroups").map { result =>
      result.asInstanceOf[List[Map[String, Any]]].map { m =>
        GroupInfo(
          id = m("id").asInstanceOf[String],
          memberCount = m("memberCount").asInstanceOf[Int],
          state = "Unknown"
        )
      }
    }

  /**
   * Lists groups asynchronously.
   */
  def listGroupsAsync()(using ExecutionContext): Future[Either[KafkaError, List[GroupInfo]]] =
    Future(listGroups())

  /**
   * Lists groups using Cats Effect IO.
   */
  def listGroupsIO(): IO[Either[KafkaError, List[GroupInfo]]] =
    IO(listGroups())

  /**
   * Describes a consumer group.
   *
   * @param groupId The group ID
   * @return Either an error or detailed group info
   */
  def describeGroup(groupId: String): Either[KafkaError, DetailedGroupInfo] =
    transport.call("describeGroup", groupId).map { result =>
      val m = result.asInstanceOf[Map[String, Any]]
      DetailedGroupInfo(
        id = m("id").asInstanceOf[String],
        state = m("state").asInstanceOf[String],
        members = m("members").asInstanceOf[List[Map[String, Any]]].map { mem =>
          GroupMember(id = mem("id").asInstanceOf[String])
        },
        totalLag = m("totalLag").asInstanceOf[Long]
      )
    }

  /**
   * Describes a group asynchronously.
   */
  def describeGroupAsync(groupId: String)(using ExecutionContext): Future[Either[KafkaError, DetailedGroupInfo]] =
    Future(describeGroup(groupId))

  /**
   * Describes a group using Cats Effect IO.
   */
  def describeGroupIO(groupId: String): IO[Either[KafkaError, DetailedGroupInfo]] =
    IO(describeGroup(groupId))

  /**
   * Resets consumer group offsets.
   *
   * @param groupId The group ID
   * @param topic The topic
   * @param offset The offset to reset to
   * @return Either an error or unit
   */
  def resetOffsets(groupId: String, topic: String, offset: Offset): Either[KafkaError, Unit] =
    val offsetValue = offset match
      case Offset.Earliest => "earliest"
      case Offset.Latest => "latest"
      case Offset.Specific(o) => o
      case Offset.Timestamp(t) => t.toEpochMilli

    transport.call("resetOffsets", groupId, topic, offsetValue).map(_ => ())

  /**
   * Resets offsets asynchronously.
   */
  def resetOffsetsAsync(groupId: String, topic: String, offset: Offset)(using ExecutionContext): Future[Either[KafkaError, Unit]] =
    Future(resetOffsets(groupId, topic, offset))

  /**
   * Resets offsets using Cats Effect IO.
   */
  def resetOffsetsIO(groupId: String, topic: String, offset: Offset): IO[Either[KafkaError, Unit]] =
    IO(resetOffsets(groupId, topic, offset))

/**
 * Topic information.
 */
case class TopicInfo(
  name: String,
  partitions: Int,
  retentionMs: Long
)

/**
 * Consumer group information.
 */
case class GroupInfo(
  id: String,
  memberCount: Int,
  state: String
)

/**
 * Detailed consumer group information.
 */
case class DetailedGroupInfo(
  id: String,
  state: String,
  members: List[GroupMember],
  totalLag: Long
)

/**
 * Consumer group member information.
 */
case class GroupMember(
  id: String
)
