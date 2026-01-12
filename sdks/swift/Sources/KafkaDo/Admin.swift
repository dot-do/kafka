import Foundation

// MARK: - Admin

/// Kafka admin client for topic and consumer group management.
public struct Admin: Sendable {
    /// The Kafka client.
    private let kafka: Kafka

    /// Creates a new admin client.
    internal init(kafka: Kafka) {
        self.kafka = kafka
    }

    // MARK: - Topic Management

    /// Creates a new topic.
    /// - Parameters:
    ///   - name: The name of the topic to create.
    ///   - config: Optional configuration for the topic.
    public func createTopic(_ name: String, config: TopicConfig = .default) async throws {
        try await kafka.rpcClient.createTopic(name, config: config)
    }

    /// Deletes a topic.
    /// - Parameter name: The name of the topic to delete.
    public func deleteTopic(_ name: String) async throws {
        try await kafka.rpcClient.deleteTopic(name)
    }

    /// Lists all topics.
    /// - Returns: An array of topic information.
    public func listTopics() async throws -> [TopicInfo] {
        return try await kafka.rpcClient.listTopics()
    }

    /// Describes a topic.
    /// - Parameter name: The name of the topic to describe.
    /// - Returns: Information about the topic.
    public func describeTopic(_ name: String) async throws -> TopicInfo {
        return try await kafka.rpcClient.describeTopic(name)
    }

    /// Alters a topic's configuration.
    /// - Parameters:
    ///   - name: The name of the topic to alter.
    ///   - config: The new configuration for the topic.
    public func alterTopic(_ name: String, config: TopicConfig) async throws {
        try await kafka.rpcClient.alterTopic(name, config: config)
    }

    /// Checks if a topic exists.
    /// - Parameter name: The name of the topic to check.
    /// - Returns: `true` if the topic exists, `false` otherwise.
    public func topicExists(_ name: String) async throws -> Bool {
        do {
            _ = try await describeTopic(name)
            return true
        } catch KafkaError.topicNotFound {
            return false
        }
    }

    // MARK: - Consumer Group Management

    /// Lists all consumer groups.
    /// - Returns: An array of consumer group information.
    public func listGroups() async throws -> [ConsumerGroupInfo] {
        return try await kafka.rpcClient.listGroups()
    }

    /// Describes a consumer group.
    /// - Parameter groupId: The ID of the group to describe.
    /// - Returns: Information about the consumer group.
    public func describeGroup(_ groupId: String) async throws -> ConsumerGroupInfo {
        return try await kafka.rpcClient.describeGroup(groupId)
    }

    /// Resets offsets for a consumer group.
    /// - Parameters:
    ///   - groupId: The ID of the consumer group.
    ///   - topic: The topic to reset offsets for.
    ///   - to: The position to reset to.
    public func resetOffsets(_ groupId: String, topic: String, to position: OffsetPosition) async throws {
        try await kafka.rpcClient.resetOffsets(groupId, topic: topic, to: position)
    }

    /// Gets the lag for a consumer group on a topic.
    /// - Parameters:
    ///   - groupId: The ID of the consumer group.
    ///   - topic: The topic to get lag for.
    /// - Returns: The total lag across all partitions.
    public func getLag(_ groupId: String, topic: String) async throws -> Int64 {
        let groupInfo = try await describeGroup(groupId)
        return groupInfo.totalLag
    }
}

// MARK: - Partition Management

extension Admin {
    /// Gets partition information for a topic.
    /// - Parameter topic: The topic to get partitions for.
    /// - Returns: The number of partitions.
    public func getPartitionCount(_ topic: String) async throws -> Int {
        let info = try await describeTopic(topic)
        return info.partitions
    }

    /// Increases the number of partitions for a topic.
    /// - Parameters:
    ///   - topic: The topic to modify.
    ///   - newCount: The new partition count (must be greater than current).
    public func increasePartitions(_ topic: String, to newCount: Int) async throws {
        let currentCount = try await getPartitionCount(topic)
        guard newCount > currentCount else {
            throw KafkaError.invalidConfiguration(
                "New partition count (\(newCount)) must be greater than current (\(currentCount))"
            )
        }
        try await alterTopic(topic, config: TopicConfig(partitions: newCount))
    }
}

// MARK: - Convenience Methods

extension Admin {
    /// Creates a topic if it doesn't exist.
    /// - Parameters:
    ///   - name: The name of the topic to create.
    ///   - config: Optional configuration for the topic.
    /// - Returns: `true` if the topic was created, `false` if it already existed.
    @discardableResult
    public func createTopicIfNotExists(_ name: String, config: TopicConfig = .default) async throws -> Bool {
        if try await topicExists(name) {
            return false
        }
        try await createTopic(name, config: config)
        return true
    }

    /// Deletes a topic if it exists.
    /// - Parameter name: The name of the topic to delete.
    /// - Returns: `true` if the topic was deleted, `false` if it didn't exist.
    @discardableResult
    public func deleteTopicIfExists(_ name: String) async throws -> Bool {
        do {
            try await deleteTopic(name)
            return true
        } catch KafkaError.topicNotFound {
            return false
        }
    }
}
