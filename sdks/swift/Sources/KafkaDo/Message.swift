import Foundation

// MARK: - KafkaRecord

/// A record consumed from a Kafka topic.
public struct KafkaRecord<T: Decodable & Sendable>: Sendable {
    /// The topic the record was consumed from.
    public let topic: String

    /// The partition the record was consumed from.
    public let partition: Int

    /// The offset of the record within the partition.
    public let offset: Int64

    /// The key of the record, if any.
    public let key: String?

    /// The deserialized value of the record.
    public let value: T

    /// The timestamp when the record was produced.
    public let timestamp: Date

    /// Headers associated with the record.
    public let headers: [String: String]

    /// Internal commit handler.
    internal let commitHandler: @Sendable () async throws -> Void

    /// Creates a new KafkaRecord.
    public init(
        topic: String,
        partition: Int,
        offset: Int64,
        key: String?,
        value: T,
        timestamp: Date,
        headers: [String: String],
        commitHandler: @escaping @Sendable () async throws -> Void
    ) {
        self.topic = topic
        self.partition = partition
        self.offset = offset
        self.key = key
        self.value = value
        self.timestamp = timestamp
        self.headers = headers
        self.commitHandler = commitHandler
    }

    /// Commits the offset for this record.
    public func commit() async throws {
        try await commitHandler()
    }
}

// MARK: - Message

/// A message to be produced to a Kafka topic.
public struct Message<T: Encodable & Sendable>: Sendable {
    /// The key for partitioning.
    public let key: String?

    /// The value to be sent.
    public let value: T

    /// Headers to include with the message.
    public let headers: [String: String]

    /// Creates a new message with only a value.
    public init(value: T) {
        self.key = nil
        self.value = value
        self.headers = [:]
    }

    /// Creates a new message with a key and value.
    public init(key: String?, value: T) {
        self.key = key
        self.value = value
        self.headers = [:]
    }

    /// Creates a new message with key, value, and headers.
    public init(key: String?, value: T, headers: [String: String]) {
        self.key = key
        self.value = value
        self.headers = headers
    }
}

// MARK: - RecordMetadata

/// Metadata returned after successfully producing a record.
public struct RecordMetadata: Sendable, Equatable {
    /// The topic the record was produced to.
    public let topic: String

    /// The partition the record was produced to.
    public let partition: Int

    /// The offset of the record within the partition.
    public let offset: Int64

    /// The timestamp of the record.
    public let timestamp: Date

    /// Creates new record metadata.
    public init(topic: String, partition: Int, offset: Int64, timestamp: Date) {
        self.topic = topic
        self.partition = partition
        self.offset = offset
        self.timestamp = timestamp
    }
}

// MARK: - TopicInfo

/// Information about a Kafka topic.
public struct TopicInfo: Sendable, Equatable {
    /// The name of the topic.
    public let name: String

    /// The number of partitions.
    public let partitions: Int

    /// The replication factor.
    public let replicationFactor: Int

    /// Retention time in milliseconds.
    public let retentionMs: Int64

    /// Creates new topic info.
    public init(name: String, partitions: Int, replicationFactor: Int, retentionMs: Int64) {
        self.name = name
        self.partitions = partitions
        self.replicationFactor = replicationFactor
        self.retentionMs = retentionMs
    }
}

// MARK: - TopicConfig

/// Configuration for creating or altering a topic.
public struct TopicConfig: Sendable, Equatable {
    /// The number of partitions.
    public let partitions: Int?

    /// The replication factor.
    public let replicationFactor: Int?

    /// Retention time in milliseconds.
    public let retentionMs: Int64?

    /// Default configuration.
    public static let `default` = TopicConfig()

    /// Creates a new topic configuration.
    public init(partitions: Int? = nil, replicationFactor: Int? = nil, retentionMs: Int64? = nil) {
        self.partitions = partitions
        self.replicationFactor = replicationFactor
        self.retentionMs = retentionMs
    }
}

// MARK: - ConsumerGroupInfo

/// Information about a consumer group.
public struct ConsumerGroupInfo: Sendable, Equatable {
    /// The group ID.
    public let id: String

    /// The state of the group.
    public let state: ConsumerGroupState

    /// The number of members in the group.
    public let memberCount: Int

    /// The members of the group.
    public let members: [ConsumerGroupMember]

    /// The total lag across all partitions.
    public let totalLag: Int64

    /// Creates new consumer group info.
    public init(id: String, state: ConsumerGroupState, memberCount: Int, members: [ConsumerGroupMember], totalLag: Int64) {
        self.id = id
        self.state = state
        self.memberCount = memberCount
        self.members = members
        self.totalLag = totalLag
    }
}

/// The state of a consumer group.
public enum ConsumerGroupState: String, Sendable {
    case unknown = "Unknown"
    case preparingRebalance = "PreparingRebalance"
    case completingRebalance = "CompletingRebalance"
    case stable = "Stable"
    case dead = "Dead"
    case empty = "Empty"
}

/// Information about a consumer group member.
public struct ConsumerGroupMember: Sendable, Equatable {
    /// The member ID.
    public let id: String

    /// The client ID.
    public let clientId: String

    /// The host the member is running on.
    public let host: String

    /// The partitions assigned to this member.
    public let assignments: [PartitionAssignment]

    /// Creates new member info.
    public init(id: String, clientId: String, host: String, assignments: [PartitionAssignment]) {
        self.id = id
        self.clientId = clientId
        self.host = host
        self.assignments = assignments
    }
}

/// A partition assignment for a consumer group member.
public struct PartitionAssignment: Sendable, Equatable {
    /// The topic name.
    public let topic: String

    /// The partition number.
    public let partition: Int

    /// Creates a new partition assignment.
    public init(topic: String, partition: Int) {
        self.topic = topic
        self.partition = partition
    }
}

// MARK: - Offset Position

/// The starting offset position for a consumer.
public enum OffsetPosition: Sendable, Equatable {
    /// Start from the earliest available offset.
    case earliest

    /// Start from the latest offset.
    case latest

    /// Start from a specific offset.
    case offset(Int64)

    /// Start from a specific timestamp.
    case timestamp(Date)
}

// MARK: - Raw Message

/// A raw message with binary data, used internally.
public struct RawMessage: Sendable {
    public let topic: String
    public let partition: Int
    public let offset: Int64
    public let key: Data?
    public let value: Data
    public let timestamp: Date
    public let headers: [String: String]

    public init(
        topic: String,
        partition: Int,
        offset: Int64,
        key: Data?,
        value: Data,
        timestamp: Date,
        headers: [String: String]
    ) {
        self.topic = topic
        self.partition = partition
        self.offset = offset
        self.key = key
        self.value = value
        self.timestamp = timestamp
        self.headers = headers
    }
}
