import Foundation
@testable import KafkaDo

// MARK: - MockRPCClient

/// A mock RPC client for testing purposes.
public actor MockRPCClient: RPCClientProtocol {
    /// Stored messages by topic.
    public var messages: [String: [RawMessage]] = [:]

    /// Topics and their configurations.
    public var topics: [String: TopicConfig] = [:]

    /// Consumer groups.
    public var groups: [String: MockConsumerGroup] = [:]

    /// Subscriptions by group ID.
    public var subscriptions: [String: [String]] = [:]

    /// Committed offsets by group-topic-partition.
    public var committedOffsets: [String: Int64] = [:]

    /// Error to throw on next operation.
    public var nextError: KafkaError?

    /// Count of operations by type.
    public var operationCounts: [String: Int] = [:]

    public init() {}

    // MARK: - Test Helpers

    /// Seeds a topic with messages.
    public func seed<T: Encodable>(_ topic: String, with values: [T]) throws {
        let encoder = JSONEncoder()
        encoder.dateEncodingStrategy = .iso8601

        var rawMessages: [RawMessage] = []
        for (index, value) in values.enumerated() {
            let data: Data
            if let d = value as? Data {
                data = d
            } else if let s = value as? String {
                data = s.data(using: .utf8) ?? Data()
            } else {
                data = try encoder.encode(value)
            }

            let message = RawMessage(
                topic: topic,
                partition: 0,
                offset: Int64(index),
                key: nil,
                value: data,
                timestamp: Date(),
                headers: [:]
            )
            rawMessages.append(message)
        }

        messages[topic, default: []].append(contentsOf: rawMessages)
    }

    /// Gets messages from a topic, decoded to the specified type.
    public func getMessages<T: Decodable>(_ topic: String, as type: T.Type) throws -> [T] {
        guard let rawMessages = messages[topic] else {
            return []
        }

        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601

        return try rawMessages.map { raw in
            if type == Data.self {
                return raw.value as! T
            } else if type == String.self {
                return String(data: raw.value, encoding: .utf8) as! T
            } else {
                return try decoder.decode(T.self, from: raw.value)
            }
        }
    }

    /// Clears all stored data.
    public func reset() {
        messages = [:]
        topics = [:]
        groups = [:]
        subscriptions = [:]
        committedOffsets = [:]
        nextError = nil
        operationCounts = [:]
    }

    /// Records an operation.
    private func recordOperation(_ name: String) {
        operationCounts[name, default: 0] += 1
    }

    /// Throws the next error if set.
    private func throwIfError() throws {
        if let error = nextError {
            nextError = nil
            throw error
        }
    }

    // MARK: - RPCClientProtocol Implementation

    public func produce(topic: String, messages produceMessages: [ProduceMessage]) async throws -> [RecordMetadata] {
        recordOperation("produce")
        try throwIfError()

        let baseOffset = Int64(self.messages[topic]?.count ?? 0)
        var results: [RecordMetadata] = []

        for (index, msg) in produceMessages.enumerated() {
            let offset = baseOffset + Int64(index)
            let timestamp = Date()

            let raw = RawMessage(
                topic: topic,
                partition: 0,
                offset: offset,
                key: msg.key,
                value: msg.value,
                timestamp: timestamp,
                headers: msg.headers
            )

            self.messages[topic, default: []].append(raw)

            results.append(RecordMetadata(
                topic: topic,
                partition: 0,
                offset: offset,
                timestamp: timestamp
            ))
        }

        return results
    }

    public func fetch(topic: String, groupId: String, config: ConsumerConfig) async throws -> [RawMessage] {
        recordOperation("fetch")
        try throwIfError()

        let offsetKey = "\(groupId)-\(topic)-0"
        let currentOffset = committedOffsets[offsetKey] ?? 0

        guard let topicMessages = messages[topic] else {
            return []
        }

        let availableMessages = topicMessages.filter { $0.offset >= currentOffset }
        let limit = min(config.maxPollRecords, availableMessages.count)

        return Array(availableMessages.prefix(limit))
    }

    public func commit(topic: String, groupId: String, partition: Int, offset: Int64) async throws {
        recordOperation("commit")
        try throwIfError()

        let key = "\(groupId)-\(topic)-\(partition)"
        committedOffsets[key] = offset
    }

    public func subscribe(topics topicList: [String], groupId: String) async throws {
        recordOperation("subscribe")
        try throwIfError()

        subscriptions[groupId] = topicList

        // Ensure group exists
        if groups[groupId] == nil {
            groups[groupId] = MockConsumerGroup(id: groupId)
        }
    }

    public func createTopic(_ name: String, config: TopicConfig) async throws {
        recordOperation("createTopic")
        try throwIfError()

        if topics[name] != nil {
            throw KafkaError.unknownError("Topic already exists")
        }

        topics[name] = config
    }

    public func deleteTopic(_ name: String) async throws {
        recordOperation("deleteTopic")
        try throwIfError()

        guard topics[name] != nil else {
            throw KafkaError.topicNotFound(name)
        }

        topics.removeValue(forKey: name)
        messages.removeValue(forKey: name)
    }

    public func listTopics() async throws -> [TopicInfo] {
        recordOperation("listTopics")
        try throwIfError()

        return topics.map { name, config in
            TopicInfo(
                name: name,
                partitions: config.partitions ?? 1,
                replicationFactor: config.replicationFactor ?? 1,
                retentionMs: config.retentionMs ?? 604800000
            )
        }
    }

    public func describeTopic(_ name: String) async throws -> TopicInfo {
        recordOperation("describeTopic")
        try throwIfError()

        guard let config = topics[name] else {
            throw KafkaError.topicNotFound(name)
        }

        return TopicInfo(
            name: name,
            partitions: config.partitions ?? 1,
            replicationFactor: config.replicationFactor ?? 1,
            retentionMs: config.retentionMs ?? 604800000
        )
    }

    public func alterTopic(_ name: String, config: TopicConfig) async throws {
        recordOperation("alterTopic")
        try throwIfError()

        guard topics[name] != nil else {
            throw KafkaError.topicNotFound(name)
        }

        topics[name] = config
    }

    public func listGroups() async throws -> [ConsumerGroupInfo] {
        recordOperation("listGroups")
        try throwIfError()

        return groups.values.map { group in
            ConsumerGroupInfo(
                id: group.id,
                state: group.state,
                memberCount: group.members.count,
                members: group.members,
                totalLag: group.totalLag
            )
        }
    }

    public func describeGroup(_ groupId: String) async throws -> ConsumerGroupInfo {
        recordOperation("describeGroup")
        try throwIfError()

        guard let group = groups[groupId] else {
            throw KafkaError.groupCoordinatorError("Group not found: \(groupId)")
        }

        return ConsumerGroupInfo(
            id: group.id,
            state: group.state,
            memberCount: group.members.count,
            members: group.members,
            totalLag: group.totalLag
        )
    }

    public func resetOffsets(_ groupId: String, topic: String, to position: OffsetPosition) async throws {
        recordOperation("resetOffsets")
        try throwIfError()

        let key = "\(groupId)-\(topic)-0"

        switch position {
        case .earliest:
            committedOffsets[key] = 0
        case .latest:
            committedOffsets[key] = Int64(messages[topic]?.count ?? 0)
        case .offset(let offset):
            committedOffsets[key] = offset
        case .timestamp:
            // For testing, just reset to 0
            committedOffsets[key] = 0
        }
    }
}

// MARK: - Mock Consumer Group

/// A mock consumer group for testing.
public struct MockConsumerGroup {
    public var id: String
    public var state: ConsumerGroupState = .stable
    public var members: [ConsumerGroupMember] = []
    public var totalLag: Int64 = 0

    public init(id: String) {
        self.id = id
    }
}

// MARK: - MockKafkaClient

/// A convenience wrapper for testing Kafka applications.
///
/// Provides an easy way to seed test data and verify produced messages:
///
/// ```swift
/// let kafka = MockKafkaClient()
///
/// // Seed test data
/// kafka.seed("orders", with: [
///     Order(orderId: "123", amount: 99.99),
///     Order(orderId: "124", amount: 149.99)
/// ])
///
/// // Process messages
/// for try await record in kafka.consumer(for: "orders", group: "test").prefix(2) {
///     processed.append(record.value)
/// }
///
/// // Verify produced messages
/// let messages = kafka.getMessages("orders", as: Order.self)
/// ```
public final class MockKafkaClient: @unchecked Sendable {
    /// The underlying mock RPC client.
    public let mockRPC: MockRPCClient

    /// The underlying Kafka client.
    public let kafka: Kafka

    /// Creates a new mock Kafka client.
    public init(brokers: [String] = ["mock-broker:9092"], config: KafkaConfig = .default) {
        self.mockRPC = MockRPCClient()
        self.kafka = Kafka(brokers: brokers, config: config, rpcClient: mockRPC)
    }

    // MARK: - Test Data Helpers

    /// Seeds a topic with test values.
    /// - Parameters:
    ///   - topic: The topic to seed.
    ///   - values: The values to seed the topic with.
    public func seed<T: Encodable>(_ topic: String, with values: [T]) {
        Task {
            try await mockRPC.seed(topic, with: values)
        }
    }

    /// Seeds a topic with test values (async version).
    /// - Parameters:
    ///   - topic: The topic to seed.
    ///   - values: The values to seed the topic with.
    public func seedAsync<T: Encodable>(_ topic: String, with values: [T]) async throws {
        try await mockRPC.seed(topic, with: values)
    }

    /// Gets all messages from a topic.
    /// - Parameters:
    ///   - topic: The topic to get messages from.
    ///   - type: The type to decode messages as.
    /// - Returns: An array of decoded messages.
    public func getMessages<T: Decodable>(_ topic: String, as type: T.Type) -> [T] {
        // Synchronous wrapper - for test assertions
        var result: [T] = []
        let semaphore = DispatchSemaphore(value: 0)
        Task {
            do {
                result = try await mockRPC.getMessages(topic, as: type)
            } catch {
                // Ignore errors in sync wrapper
            }
            semaphore.signal()
        }
        semaphore.wait()
        return result
    }

    /// Gets all messages from a topic (async version).
    /// - Parameters:
    ///   - topic: The topic to get messages from.
    ///   - type: The type to decode messages as.
    /// - Returns: An array of decoded messages.
    public func getMessagesAsync<T: Decodable>(_ topic: String, as type: T.Type) async throws -> [T] {
        try await mockRPC.getMessages(topic, as: type)
    }

    /// Clears all stored data.
    public func reset() {
        Task {
            await mockRPC.reset()
        }
    }

    /// Clears all stored data (async version).
    public func resetAsync() async {
        await mockRPC.reset()
    }

    /// Sets the next error to be thrown.
    /// - Parameter error: The error to throw on the next operation.
    public func setNextError(_ error: KafkaError) {
        Task {
            await mockRPC.setNextError(error)
        }
    }

    /// Sets the next error to be thrown (async version).
    /// - Parameter error: The error to throw on the next operation.
    public func setNextErrorAsync(_ error: KafkaError) async {
        await mockRPC.setNextError(error)
    }

    // MARK: - Producer Methods

    /// Creates a producer for the specified topic.
    public func producer<T: Encodable & Sendable>(for topic: String, config: ProducerConfig = .default) -> TopicProducer<T> {
        kafka.producer(for: topic, config: config)
    }

    /// Creates a typed producer.
    public func producer<T: Encodable & Sendable>(config: ProducerConfig = .default) -> Producer<T> {
        kafka.producer(config: config)
    }

    // MARK: - Consumer Methods

    /// Creates a consumer for the specified topic and group.
    public func consumer<T: Decodable & Sendable>(for topic: String, group: String, config: ConsumerConfig = .default) -> TopicConsumer<T> {
        kafka.consumer(for: topic, group: group, config: config)
    }

    /// Creates a consumer with the specified group ID.
    public func consumer<T: Decodable & Sendable>(groupId: String, config: ConsumerConfig = .default) -> Consumer<T> {
        kafka.consumer(groupId: groupId, config: config)
    }

    // MARK: - Admin Methods

    /// Returns the admin client.
    public var admin: Admin {
        kafka.admin()
    }

    // MARK: - Stream Methods

    /// Creates a stream from a topic.
    public func stream<T: Decodable & Sendable>(from topic: String) -> Stream<T> {
        kafka.stream(from: topic)
    }

    // MARK: - Transaction Methods

    /// Executes a transaction on a topic.
    public func transaction<T: Encodable & Sendable>(
        topic: String,
        operation: @Sendable (Transaction<T>) async throws -> Void
    ) async throws {
        try await kafka.transaction(topic: topic, operation: operation)
    }
}

// MARK: - MockRPCClient Extension for setNextError

extension MockRPCClient {
    /// Sets the next error to be thrown (nonisolated wrapper).
    public nonisolated func setNextError(_ error: KafkaError) async {
        await setNextErrorInternal(error)
    }

    /// Internal method to set the next error.
    private func setNextErrorInternal(_ error: KafkaError) {
        self.nextError = error
    }
}
