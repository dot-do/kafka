import XCTest
@testable import KafkaDo

final class ConsumerTests: XCTestCase {

    var mockRPC: MockRPCClient!
    var kafka: Kafka!

    override func setUp() async throws {
        mockRPC = MockRPCClient()
        kafka = Kafka(brokers: ["broker:9092"], rpcClient: mockRPC)
    }

    // MARK: - Basic Consumer Tests

    func testSubscribe() async throws {
        let consumer: Consumer<String> = kafka.consumer(groupId: "test-group")

        _ = try await consumer.subscribe(topics: ["test-topic"])

        let subscriptions = await mockRPC.subscriptions["test-group"]
        XCTAssertEqual(subscriptions, ["test-topic"])
    }

    func testSubscribeMultipleTopics() async throws {
        let consumer: Consumer<String> = kafka.consumer(groupId: "test-group")

        _ = try await consumer.subscribe(topics: ["topic-1", "topic-2", "topic-3"])

        let subscriptions = await mockRPC.subscriptions["test-group"]
        XCTAssertEqual(subscriptions?.count, 3)
    }

    // MARK: - Message Consumption Tests

    func testConsumeMessages() async throws {
        // Seed messages
        try await mockRPC.seed("test-topic", with: ["Message 1", "Message 2", "Message 3"])

        let consumer: Consumer<String> = kafka.consumer(groupId: "test-group")
        let subscribedConsumer = try await consumer.subscribe(topics: ["test-topic"])

        var receivedMessages: [String] = []
        var count = 0

        for try await record in subscribedConsumer.messages() {
            receivedMessages.append(record.value)
            count += 1
            if count >= 3 {
                break
            }
        }

        XCTAssertEqual(receivedMessages.count, 3)
        XCTAssertEqual(receivedMessages[0], "Message 1")
        XCTAssertEqual(receivedMessages[1], "Message 2")
        XCTAssertEqual(receivedMessages[2], "Message 3")
    }

    func testConsumeTypedMessages() async throws {
        struct Order: Codable, Equatable {
            let orderId: String
            let amount: Double
        }

        let orders = [
            Order(orderId: "1", amount: 10.0),
            Order(orderId: "2", amount: 20.0)
        ]

        try await mockRPC.seed("orders", with: orders)

        let consumer: Consumer<Order> = kafka.consumer(groupId: "order-processor")
        let subscribedConsumer = try await consumer.subscribe(topics: ["orders"])

        var receivedOrders: [Order] = []
        var count = 0

        for try await record in subscribedConsumer.messages() {
            receivedOrders.append(record.value)
            count += 1
            if count >= 2 {
                break
            }
        }

        XCTAssertEqual(receivedOrders, orders)
    }

    // MARK: - Record Properties Tests

    func testRecordProperties() async throws {
        try await mockRPC.seed("test-topic", with: ["Test message"])

        let consumer: Consumer<String> = kafka.consumer(groupId: "test-group")
        let subscribedConsumer = try await consumer.subscribe(topics: ["test-topic"])

        for try await record in subscribedConsumer.messages() {
            XCTAssertEqual(record.topic, "test-topic")
            XCTAssertEqual(record.partition, 0)
            XCTAssertEqual(record.offset, 0)
            XCTAssertNotNil(record.timestamp)
            break
        }
    }

    // MARK: - Commit Tests

    func testCommitOffset() async throws {
        try await mockRPC.seed("test-topic", with: ["Message 1", "Message 2"])

        let consumer: Consumer<String> = kafka.consumer(groupId: "test-group")
        let subscribedConsumer = try await consumer.subscribe(topics: ["test-topic"])

        var count = 0
        for try await record in subscribedConsumer.messages() {
            try await record.commit()
            count += 1
            if count >= 2 {
                break
            }
        }

        let committedOffset = await mockRPC.committedOffsets["test-group-test-topic-0"]
        XCTAssertEqual(committedOffset, 2) // Next offset after last consumed
    }

    func testAutoCommit() async throws {
        try await mockRPC.seed("test-topic", with: ["Message 1"])

        let config = ConsumerConfig(autoCommit: true)
        let consumer: Consumer<String> = kafka.consumer(groupId: "test-group", config: config)
        let subscribedConsumer = try await consumer.subscribe(topics: ["test-topic"])

        for try await _ in subscribedConsumer.messages() {
            break
        }

        let committedOffset = await mockRPC.committedOffsets["test-group-test-topic-0"]
        XCTAssertEqual(committedOffset, 1)
    }

    // MARK: - Consumer Config Tests

    func testConsumerConfig() {
        let config = ConsumerConfig(
            offset: .earliest,
            autoCommit: false,
            fetchMinBytes: 1024,
            fetchMaxWaitMs: 1000,
            maxPollRecords: 100,
            sessionTimeout: .seconds(60),
            heartbeatInterval: .seconds(5)
        )

        XCTAssertEqual(config.offset, .earliest)
        XCTAssertEqual(config.autoCommit, false)
        XCTAssertEqual(config.fetchMinBytes, 1024)
        XCTAssertEqual(config.fetchMaxWaitMs, 1000)
        XCTAssertEqual(config.maxPollRecords, 100)
        XCTAssertEqual(config.sessionTimeout, .seconds(60))
        XCTAssertEqual(config.heartbeatInterval, .seconds(5))
    }

    func testDefaultConsumerConfig() {
        let config = ConsumerConfig.default

        XCTAssertEqual(config.offset, .latest)
        XCTAssertEqual(config.autoCommit, false)
        XCTAssertEqual(config.maxPollRecords, 500)
    }

    // MARK: - Offset Position Tests

    func testOffsetPositions() {
        XCTAssertEqual(OffsetPosition.earliest, .earliest)
        XCTAssertEqual(OffsetPosition.latest, .latest)
        XCTAssertEqual(OffsetPosition.offset(100), .offset(100))

        let date = Date()
        XCTAssertEqual(OffsetPosition.timestamp(date), .timestamp(date))
    }
}

// MARK: - Batch Consumer Tests

final class BatchConsumerTests: XCTestCase {

    var mockRPC: MockRPCClient!
    var kafka: Kafka!

    override func setUp() async throws {
        mockRPC = MockRPCClient()
        kafka = Kafka(brokers: ["broker:9092"], rpcClient: mockRPC)
    }

    func testBatchConsumerConfig() {
        let config = BatchConsumerConfig(
            batchSize: 50,
            batchTimeout: .seconds(10)
        )

        XCTAssertEqual(config.batchSize, 50)
        XCTAssertEqual(config.batchTimeout, .seconds(10))
    }

    func testRecordBatchIteration() async throws {
        struct TestItem: Codable, Equatable {
            let id: Int
        }

        try await mockRPC.seed("test-topic", with: [
            TestItem(id: 1),
            TestItem(id: 2),
            TestItem(id: 3)
        ])

        let consumer: Consumer<TestItem> = kafka.consumer(groupId: "test-group")
        let subscribedConsumer = try await consumer.subscribe(topics: ["test-topic"])

        var allItems: [TestItem] = []
        var count = 0
        for try await record in subscribedConsumer.messages() {
            allItems.append(record.value)
            count += 1
            if count >= 3 {
                break
            }
        }

        XCTAssertEqual(allItems.count, 3)
        XCTAssertEqual(allItems[0].id, 1)
        XCTAssertEqual(allItems[1].id, 2)
        XCTAssertEqual(allItems[2].id, 3)
    }
}
