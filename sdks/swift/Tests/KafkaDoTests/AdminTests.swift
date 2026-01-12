import XCTest
@testable import KafkaDo

final class AdminTests: XCTestCase {

    var mockRPC: MockRPCClient!
    var kafka: Kafka!
    var admin: Admin!

    override func setUp() async throws {
        mockRPC = MockRPCClient()
        kafka = Kafka(brokers: ["broker:9092"], rpcClient: mockRPC)
        admin = kafka.admin()
    }

    // MARK: - Topic Creation Tests

    func testCreateTopic() async throws {
        try await admin.createTopic("new-topic")

        let topics = await mockRPC.topics
        XCTAssertNotNil(topics["new-topic"])
    }

    func testCreateTopicWithConfig() async throws {
        let config = TopicConfig(
            partitions: 3,
            replicationFactor: 2,
            retentionMs: 86400000
        )

        try await admin.createTopic("configured-topic", config: config)

        let topics = await mockRPC.topics
        XCTAssertNotNil(topics["configured-topic"])

        let info = try await admin.describeTopic("configured-topic")
        XCTAssertEqual(info.partitions, 3)
        XCTAssertEqual(info.replicationFactor, 2)
        XCTAssertEqual(info.retentionMs, 86400000)
    }

    func testCreateTopicIfNotExists() async throws {
        // Create first time
        let created1 = try await admin.createTopicIfNotExists("test-topic")
        XCTAssertTrue(created1)

        // Try to create again
        let created2 = try await admin.createTopicIfNotExists("test-topic")
        XCTAssertFalse(created2)
    }

    // MARK: - Topic Deletion Tests

    func testDeleteTopic() async throws {
        try await admin.createTopic("to-delete")
        XCTAssertTrue(try await admin.topicExists("to-delete"))

        try await admin.deleteTopic("to-delete")
        XCTAssertFalse(try await admin.topicExists("to-delete"))
    }

    func testDeleteNonExistentTopic() async throws {
        do {
            try await admin.deleteTopic("non-existent")
            XCTFail("Expected error to be thrown")
        } catch let error as KafkaError {
            if case .topicNotFound = error {
                // Expected
            } else {
                XCTFail("Unexpected error: \(error)")
            }
        }
    }

    func testDeleteTopicIfExists() async throws {
        try await admin.createTopic("to-delete")

        let deleted1 = try await admin.deleteTopicIfExists("to-delete")
        XCTAssertTrue(deleted1)

        let deleted2 = try await admin.deleteTopicIfExists("to-delete")
        XCTAssertFalse(deleted2)
    }

    // MARK: - Topic Listing Tests

    func testListTopics() async throws {
        try await admin.createTopic("topic-1")
        try await admin.createTopic("topic-2")
        try await admin.createTopic("topic-3")

        let topics = try await admin.listTopics()
        XCTAssertEqual(topics.count, 3)

        let names = topics.map { $0.name }
        XCTAssertTrue(names.contains("topic-1"))
        XCTAssertTrue(names.contains("topic-2"))
        XCTAssertTrue(names.contains("topic-3"))
    }

    func testListTopicsEmpty() async throws {
        let topics = try await admin.listTopics()
        XCTAssertEqual(topics.count, 0)
    }

    // MARK: - Topic Description Tests

    func testDescribeTopic() async throws {
        let config = TopicConfig(partitions: 5, replicationFactor: 3, retentionMs: 604800000)
        try await admin.createTopic("my-topic", config: config)

        let info = try await admin.describeTopic("my-topic")
        XCTAssertEqual(info.name, "my-topic")
        XCTAssertEqual(info.partitions, 5)
        XCTAssertEqual(info.replicationFactor, 3)
        XCTAssertEqual(info.retentionMs, 604800000)
    }

    func testDescribeNonExistentTopic() async throws {
        do {
            _ = try await admin.describeTopic("non-existent")
            XCTFail("Expected error to be thrown")
        } catch let error as KafkaError {
            if case .topicNotFound = error {
                // Expected
            } else {
                XCTFail("Unexpected error: \(error)")
            }
        }
    }

    // MARK: - Topic Alteration Tests

    func testAlterTopic() async throws {
        try await admin.createTopic("my-topic", config: TopicConfig(partitions: 3, retentionMs: 3600000))

        try await admin.alterTopic("my-topic", config: TopicConfig(retentionMs: 86400000))

        let info = try await admin.describeTopic("my-topic")
        XCTAssertEqual(info.retentionMs, 86400000)
    }

    // MARK: - Topic Existence Tests

    func testTopicExists() async throws {
        XCTAssertFalse(try await admin.topicExists("test-topic"))

        try await admin.createTopic("test-topic")

        XCTAssertTrue(try await admin.topicExists("test-topic"))
    }

    // MARK: - Partition Tests

    func testGetPartitionCount() async throws {
        try await admin.createTopic("partitioned-topic", config: TopicConfig(partitions: 10))

        let count = try await admin.getPartitionCount("partitioned-topic")
        XCTAssertEqual(count, 10)
    }

    func testIncreasePartitions() async throws {
        try await admin.createTopic("expandable-topic", config: TopicConfig(partitions: 3))

        try await admin.increasePartitions("expandable-topic", to: 6)

        let count = try await admin.getPartitionCount("expandable-topic")
        XCTAssertEqual(count, 6)
    }

    func testIncreasePartitionsInvalidCount() async throws {
        try await admin.createTopic("expandable-topic", config: TopicConfig(partitions: 5))

        do {
            try await admin.increasePartitions("expandable-topic", to: 3)
            XCTFail("Expected error to be thrown")
        } catch let error as KafkaError {
            if case .invalidConfiguration = error {
                // Expected
            } else {
                XCTFail("Unexpected error: \(error)")
            }
        }
    }

    // MARK: - Consumer Group Tests

    func testListGroups() async throws {
        // Subscribe a consumer to create a group
        let consumer: Consumer<String> = kafka.consumer(groupId: "group-1")
        _ = try await consumer.subscribe(topics: ["topic"])

        let consumer2: Consumer<String> = kafka.consumer(groupId: "group-2")
        _ = try await consumer2.subscribe(topics: ["topic"])

        let groups = try await admin.listGroups()
        XCTAssertEqual(groups.count, 2)
    }

    func testDescribeGroup() async throws {
        // Create a group by subscribing
        let consumer: Consumer<String> = kafka.consumer(groupId: "my-group")
        _ = try await consumer.subscribe(topics: ["topic"])

        let info = try await admin.describeGroup("my-group")
        XCTAssertEqual(info.id, "my-group")
        XCTAssertEqual(info.state, .stable)
    }

    func testDescribeNonExistentGroup() async throws {
        do {
            _ = try await admin.describeGroup("non-existent-group")
            XCTFail("Expected error to be thrown")
        } catch let error as KafkaError {
            if case .groupCoordinatorError = error {
                // Expected
            } else {
                XCTFail("Unexpected error: \(error)")
            }
        }
    }

    // MARK: - Offset Reset Tests

    func testResetOffsetsToEarliest() async throws {
        // Seed some messages
        try await mockRPC.seed("test-topic", with: ["1", "2", "3"])

        // Create consumer and consume some messages
        let consumer: Consumer<String> = kafka.consumer(groupId: "reset-group")
        _ = try await consumer.subscribe(topics: ["test-topic"])

        // Manually set an offset
        await mockRPC.commit(topic: "test-topic", groupId: "reset-group", partition: 0, offset: 2)

        // Reset to earliest
        try await admin.resetOffsets("reset-group", topic: "test-topic", to: .earliest)

        let offset = await mockRPC.committedOffsets["reset-group-test-topic-0"]
        XCTAssertEqual(offset, 0)
    }

    func testResetOffsetsToLatest() async throws {
        try await mockRPC.seed("test-topic", with: ["1", "2", "3"])

        let consumer: Consumer<String> = kafka.consumer(groupId: "reset-group")
        _ = try await consumer.subscribe(topics: ["test-topic"])

        try await admin.resetOffsets("reset-group", topic: "test-topic", to: .latest)

        let offset = await mockRPC.committedOffsets["reset-group-test-topic-0"]
        XCTAssertEqual(offset, 3)
    }

    func testResetOffsetsToSpecificOffset() async throws {
        let consumer: Consumer<String> = kafka.consumer(groupId: "reset-group")
        _ = try await consumer.subscribe(topics: ["test-topic"])

        try await admin.resetOffsets("reset-group", topic: "test-topic", to: .offset(50))

        let offset = await mockRPC.committedOffsets["reset-group-test-topic-0"]
        XCTAssertEqual(offset, 50)
    }

    // MARK: - Lag Tests

    func testGetLag() async throws {
        // Create a mock group with lag
        var group = MockConsumerGroup(id: "lag-group")
        group.totalLag = 100
        await mockRPC.setGroup(group)

        let lag = try await admin.getLag("lag-group", topic: "test-topic")
        XCTAssertEqual(lag, 100)
    }
}

// Helper extension for tests
extension MockRPCClient {
    func setGroup(_ group: MockConsumerGroup) async {
        self.groups[group.id] = group
    }

    func commit(topic: String, groupId: String, partition: Int, offset: Int64) async {
        let key = "\(groupId)-\(topic)-\(partition)"
        self.committedOffsets[key] = offset
    }
}
