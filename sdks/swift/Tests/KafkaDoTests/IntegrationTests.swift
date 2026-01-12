import XCTest
@testable import KafkaDo

/// Integration tests that test the full flow of producer -> consumer.
final class IntegrationTests: XCTestCase {

    var mockRPC: MockRPCClient!
    var kafka: Kafka!

    override func setUp() async throws {
        mockRPC = MockRPCClient()
        kafka = Kafka(brokers: ["broker:9092"], rpcClient: mockRPC)
    }

    // MARK: - End-to-End Tests

    func testProduceAndConsume() async throws {
        struct Order: Codable, Equatable {
            let orderId: String
            let amount: Double
        }

        let topic = "orders"
        let groupId = "order-processor"

        // Create topic
        let admin = kafka.admin()
        try await admin.createTopic(topic, config: TopicConfig(partitions: 3))

        // Produce messages
        let producer: Producer<Order> = kafka.producer()
        let order1 = Order(orderId: "ORD-001", amount: 99.99)
        let order2 = Order(orderId: "ORD-002", amount: 149.99)

        _ = try await producer.send(topic: topic, value: order1, key: "customer-1")
        _ = try await producer.send(topic: topic, value: order2, key: "customer-2")

        // Consume messages
        let consumer: Consumer<Order> = kafka.consumer(groupId: groupId)
        let subscribedConsumer = try await consumer.subscribe(topics: [topic])

        var receivedOrders: [Order] = []
        var count = 0

        for try await record in subscribedConsumer.messages() {
            receivedOrders.append(record.value)
            try await record.commit()
            count += 1
            if count >= 2 {
                break
            }
        }

        XCTAssertEqual(receivedOrders.count, 2)
        XCTAssertEqual(receivedOrders[0], order1)
        XCTAssertEqual(receivedOrders[1], order2)
    }

    func testProduceBatchAndConsume() async throws {
        let topic = "events"
        let groupId = "event-processor"

        // Produce batch
        let producer: Producer<String> = kafka.producer()
        _ = try await producer.sendBatch(topic: topic, values: [
            "Event 1",
            "Event 2",
            "Event 3",
            "Event 4",
            "Event 5"
        ])

        // Consume all
        let consumer: Consumer<String> = kafka.consumer(groupId: groupId)
        let subscribedConsumer = try await consumer.subscribe(topics: [topic])

        var receivedEvents: [String] = []
        var count = 0

        for try await record in subscribedConsumer.messages() {
            receivedEvents.append(record.value)
            count += 1
            if count >= 5 {
                break
            }
        }

        XCTAssertEqual(receivedEvents.count, 5)
        XCTAssertEqual(receivedEvents, ["Event 1", "Event 2", "Event 3", "Event 4", "Event 5"])
    }

    func testMultipleConsumerGroups() async throws {
        let topic = "shared-events"

        // Produce messages
        let producer: Producer<String> = kafka.producer()
        _ = try await producer.sendBatch(topic: topic, values: ["Event A", "Event B"])

        // Consumer group 1
        let consumer1: Consumer<String> = kafka.consumer(groupId: "group-1")
        let sub1 = try await consumer1.subscribe(topics: [topic])

        // Consumer group 2
        let consumer2: Consumer<String> = kafka.consumer(groupId: "group-2")
        let sub2 = try await consumer2.subscribe(topics: [topic])

        // Both groups should receive all messages
        var group1Messages: [String] = []
        var count1 = 0
        for try await record in sub1.messages() {
            group1Messages.append(record.value)
            try await record.commit()
            count1 += 1
            if count1 >= 2 {
                break
            }
        }

        var group2Messages: [String] = []
        var count2 = 0
        for try await record in sub2.messages() {
            group2Messages.append(record.value)
            try await record.commit()
            count2 += 1
            if count2 >= 2 {
                break
            }
        }

        XCTAssertEqual(group1Messages, ["Event A", "Event B"])
        XCTAssertEqual(group2Messages, ["Event A", "Event B"])
    }

    func testReplayFromBeginning() async throws {
        let topic = "replayable"
        let groupId = "replay-processor"

        // Produce and consume initially
        let producer: Producer<String> = kafka.producer()
        _ = try await producer.sendBatch(topic: topic, values: ["1", "2", "3"])

        let consumer: Consumer<String> = kafka.consumer(groupId: groupId)
        let sub = try await consumer.subscribe(topics: [topic])

        // Consume all and commit
        var count = 0
        for try await record in sub.messages() {
            try await record.commit()
            count += 1
            if count >= 3 {
                break
            }
        }

        // Reset offsets to beginning
        let admin = kafka.admin()
        try await admin.resetOffsets(groupId, topic: topic, to: .earliest)

        // Should receive all messages again
        let consumer2: Consumer<String> = kafka.consumer(groupId: groupId)
        let sub2 = try await consumer2.subscribe(topics: [topic])

        var replayedMessages: [String] = []
        var replayCount = 0
        for try await record in sub2.messages() {
            replayedMessages.append(record.value)
            replayCount += 1
            if replayCount >= 3 {
                break
            }
        }

        XCTAssertEqual(replayedMessages, ["1", "2", "3"])
    }

    // MARK: - Topic Management Integration

    func testTopicLifecycle() async throws {
        let admin = kafka.admin()
        let topicName = "lifecycle-topic"

        // Create
        XCTAssertFalse(try await admin.topicExists(topicName))
        try await admin.createTopic(topicName, config: TopicConfig(partitions: 3))
        XCTAssertTrue(try await admin.topicExists(topicName))

        // Describe
        let info = try await admin.describeTopic(topicName)
        XCTAssertEqual(info.name, topicName)
        XCTAssertEqual(info.partitions, 3)

        // List
        let topics = try await admin.listTopics()
        XCTAssertTrue(topics.contains { $0.name == topicName })

        // Alter
        try await admin.alterTopic(topicName, config: TopicConfig(retentionMs: 86400000))
        let updatedInfo = try await admin.describeTopic(topicName)
        XCTAssertEqual(updatedInfo.retentionMs, 86400000)

        // Delete
        try await admin.deleteTopic(topicName)
        XCTAssertFalse(try await admin.topicExists(topicName))
    }

    // MARK: - Error Recovery Tests

    func testRecoveryAfterError() async throws {
        let topic = "recovery-test"
        let producer: Producer<String> = kafka.producer()

        // First send should fail
        await mockRPC.setNextError(.timeout)

        do {
            _ = try await producer.send(topic: topic, value: "First attempt")
            XCTFail("Expected error")
        } catch {
            // Expected
        }

        // Second send should succeed
        let metadata = try await producer.send(topic: topic, value: "Second attempt")
        XCTAssertEqual(metadata.topic, topic)

        let messages = try await mockRPC.getMessages(topic, as: String.self)
        XCTAssertEqual(messages.count, 1)
        XCTAssertEqual(messages[0], "Second attempt")
    }

    // MARK: - Complex Type Tests

    func testComplexTypes() async throws {
        struct Address: Codable, Equatable {
            let street: String
            let city: String
            let zip: String
        }

        struct Customer: Codable, Equatable {
            let id: String
            let name: String
            let address: Address
            let tags: [String]
        }

        let customer = Customer(
            id: "CUST-001",
            name: "John Doe",
            address: Address(street: "123 Main St", city: "Boston", zip: "02101"),
            tags: ["premium", "active"]
        )

        let topic = "customers"
        let producer: Producer<Customer> = kafka.producer()
        _ = try await producer.send(topic: topic, value: customer)

        let consumer: Consumer<Customer> = kafka.consumer(groupId: "customer-processor")
        let sub = try await consumer.subscribe(topics: [topic])

        for try await record in sub.messages() {
            XCTAssertEqual(record.value, customer)
            XCTAssertEqual(record.value.address.city, "Boston")
            XCTAssertEqual(record.value.tags, ["premium", "active"])
            break
        }
    }
}
