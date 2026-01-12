import XCTest
@testable import KafkaDo

final class StreamTests: XCTestCase {

    var mockRPC: MockRPCClient!
    var kafka: Kafka!

    override func setUp() async throws {
        mockRPC = MockRPCClient()
        kafka = Kafka(brokers: ["broker:9092"], rpcClient: mockRPC)
    }

    // MARK: - Window Type Tests

    func testTumblingWindow() {
        let window = Window.tumbling(.seconds(60))
        XCTAssertEqual(window, .tumbling(.seconds(60)))
    }

    func testSlidingWindow() {
        let window = Window.sliding(size: .seconds(60), slide: .seconds(10))
        XCTAssertEqual(window, .sliding(size: .seconds(60), slide: .seconds(10)))
    }

    func testSessionWindow() {
        let window = Window.session(gap: .seconds(300))
        XCTAssertEqual(window, .session(gap: .seconds(300)))
    }

    func testWindowConvenienceMethods() {
        let minutesWindow = Window.minutes(5)
        XCTAssertEqual(minutesWindow, .tumbling(.seconds(300)))

        let hoursWindow = Window.hours(1)
        XCTAssertEqual(hoursWindow, .tumbling(.seconds(3600)))
    }

    // MARK: - WindowedValue Tests

    func testWindowedValue() {
        let start = Date()
        let end = Date().addingTimeInterval(60)

        let windowed = WindowedValue(value: 42, start: start, end: end)

        XCTAssertEqual(windowed.value, 42)
        XCTAssertEqual(windowed.start, start)
        XCTAssertEqual(windowed.end, end)
    }

    // MARK: - Predicate Tests

    func testPredicate() {
        let isPositive = Predicate<Int> { $0 > 0 }

        XCTAssertTrue(isPositive.test(5))
        XCTAssertFalse(isPositive.test(-3))
        XCTAssertFalse(isPositive.test(0))
    }

    func testPredicateWithStruct() {
        struct Order: Sendable {
            let amount: Double
        }

        let isHighValue = Predicate<Order> { $0.amount > 100 }

        XCTAssertTrue(isHighValue.test(Order(amount: 150.0)))
        XCTAssertFalse(isHighValue.test(Order(amount: 50.0)))
    }

    // MARK: - Stream Creation Tests

    func testStreamCreation() {
        struct Event: Codable, Sendable {
            let id: String
        }

        let stream: Stream<Event> = kafka.stream(from: "events")
        // Stream should be created without error
        XCTAssertNotNil(stream)
    }

    // MARK: - Stream Filter Tests

    func testFilteredStreamCreation() {
        struct Order: Codable, Sendable {
            let amount: Double
        }

        let stream: Stream<Order> = kafka.stream(from: "orders")
        let filtered = stream.filter { $0.amount > 100 }

        // Filtered stream should be created without error
        XCTAssertNotNil(filtered)
    }

    // MARK: - Stream Map Tests

    func testMappedStreamCreation() {
        struct Order: Codable, Sendable {
            let orderId: String
            let amount: Double
        }

        struct Summary: Codable, Sendable {
            let orderId: String
            let total: String
        }

        let stream: Stream<Order> = kafka.stream(from: "orders")
        let mapped = stream.map { order in
            Summary(orderId: order.orderId, total: "$\(order.amount)")
        }

        XCTAssertNotNil(mapped)
    }

    // MARK: - Stream FlatMap Tests

    func testFlatMappedStreamCreation() {
        struct Order: Codable, Sendable {
            let items: [String]
        }

        struct Item: Codable, Sendable {
            let name: String
        }

        let stream: Stream<Order> = kafka.stream(from: "orders")
        let flatMapped = stream.flatMap { order in
            order.items.map { Item(name: $0) }
        }

        XCTAssertNotNil(flatMapped)
    }

    // MARK: - Stream GroupBy Tests

    func testGroupedStreamCreation() {
        struct Order: Codable, Sendable {
            let customerId: String
            let amount: Double
        }

        let stream: Stream<Order> = kafka.stream(from: "orders")
        let grouped = stream.groupBy(\.customerId)

        XCTAssertNotNil(grouped)
    }

    func testGroupedStreamCount() {
        struct Order: Codable, Sendable {
            let customerId: String
            let amount: Double
        }

        let stream: Stream<Order> = kafka.stream(from: "orders")
        let counted = stream.groupBy(\.customerId).count()

        XCTAssertNotNil(counted)
    }

    func testGroupedStreamReduce() {
        struct Order: Codable, Sendable {
            let customerId: String
            let amount: Double
        }

        let stream: Stream<Order> = kafka.stream(from: "orders")
        let totals = stream.groupBy(\.customerId).reduce(0.0) { total, order in
            total + order.amount
        }

        XCTAssertNotNil(totals)
    }

    // MARK: - Stream Window Tests

    func testWindowedStreamCreation() {
        struct Order: Codable, Sendable {
            let amount: Double
        }

        let stream: Stream<Order> = kafka.stream(from: "orders")
        let windowed = stream.window(.tumbling(.seconds(60)))

        XCTAssertNotNil(windowed)
    }

    func testWindowedGroupedStreamCreation() {
        struct Order: Codable, Sendable {
            let customerId: String
            let amount: Double
        }

        let stream: Stream<Order> = kafka.stream(from: "orders")
        let windowedGrouped = stream
            .window(.tumbling(.minutes(5)))
            .groupBy(\.customerId)

        XCTAssertNotNil(windowedGrouped)
    }

    func testWindowedAggregatedStreamCreation() {
        struct Order: Codable, Sendable {
            let customerId: String
            let amount: Double
        }

        let stream: Stream<Order> = kafka.stream(from: "orders")
        let windowedCount = stream
            .window(.tumbling(.minutes(5)))
            .groupBy(\.customerId)
            .count()

        XCTAssertNotNil(windowedCount)
    }

    // MARK: - Stream Join Tests

    func testJoinedStreamCreation() {
        struct Order: Codable, Sendable {
            let customerId: String
            let amount: Double
        }

        struct Customer: Codable, Sendable {
            let id: String
            let name: String
        }

        let orders: Stream<Order> = kafka.stream(from: "orders")
        let customers: Stream<Customer> = kafka.stream(from: "customers")

        let joined = orders.join(
            customers,
            on: { $0.customerId == $1.id },
            window: .hours(1)
        )

        XCTAssertNotNil(joined)
    }

    // MARK: - Chained Operations Tests

    func testFilterMapChain() {
        struct Order: Codable, Sendable {
            let orderId: String
            let amount: Double
        }

        struct HighValueOrder: Codable, Sendable {
            let orderId: String
            let tier: String
        }

        let stream: Stream<Order> = kafka.stream(from: "orders")
        let result = stream
            .filter { $0.amount > 100 }
            .map { order in
                HighValueOrder(orderId: order.orderId, tier: "premium")
            }

        XCTAssertNotNil(result)
    }
}

// MARK: - TopicProducer Tests

final class TopicProducerTests: XCTestCase {

    var mockRPC: MockRPCClient!
    var kafka: Kafka!

    override func setUp() async throws {
        mockRPC = MockRPCClient()
        kafka = Kafka(brokers: ["broker:9092"], rpcClient: mockRPC)
    }

    func testTopicProducerSend() async throws {
        struct Order: Codable, Equatable {
            let orderId: String
            let amount: Double
        }

        let producer: TopicProducer<Order> = kafka.producer(for: "orders")
        let order = Order(orderId: "123", amount: 99.99)

        let metadata = try await producer.send(order)

        XCTAssertEqual(metadata.topic, "orders")
        XCTAssertEqual(metadata.offset, 0)

        let messages = try await mockRPC.getMessages("orders", as: Order.self)
        XCTAssertEqual(messages.count, 1)
        XCTAssertEqual(messages[0], order)
    }

    func testTopicProducerSendWithKey() async throws {
        let producer: TopicProducer<String> = kafka.producer(for: "events")

        _ = try await producer.send("Event 1", key: "key-123")

        let rawMessages = await mockRPC.messages["events"]
        XCTAssertEqual(rawMessages?.count, 1)
        XCTAssertEqual(String(data: rawMessages![0].key!, encoding: .utf8), "key-123")
    }

    func testTopicProducerSendWithHeaders() async throws {
        let producer: TopicProducer<String> = kafka.producer(for: "events")

        _ = try await producer.send("Event 1", headers: ["source": "test"])

        let rawMessages = await mockRPC.messages["events"]
        XCTAssertEqual(rawMessages?.count, 1)
        XCTAssertEqual(rawMessages?[0].headers["source"], "test")
    }

    func testTopicProducerSendBatch() async throws {
        let producer: TopicProducer<String> = kafka.producer(for: "events")

        let results = try await producer.sendBatch(["Event 1", "Event 2", "Event 3"])

        XCTAssertEqual(results.count, 3)

        let messages = try await mockRPC.getMessages("events", as: String.self)
        XCTAssertEqual(messages.count, 3)
    }

    func testTopicProducerSendBatchMessages() async throws {
        struct Order: Codable, Equatable {
            let orderId: String
        }

        let producer: TopicProducer<Order> = kafka.producer(for: "orders")

        let messages = [
            Message(key: "cust-1", value: Order(orderId: "1")),
            Message(key: "cust-2", value: Order(orderId: "2"))
        ]

        let results = try await producer.sendBatch(messages)

        XCTAssertEqual(results.count, 2)

        let rawMessages = await mockRPC.messages["orders"]
        XCTAssertEqual(rawMessages?.count, 2)
        XCTAssertEqual(String(data: rawMessages![0].key!, encoding: .utf8), "cust-1")
        XCTAssertEqual(String(data: rawMessages![1].key!, encoding: .utf8), "cust-2")
    }
}

// MARK: - TopicConsumer Tests

final class TopicConsumerTests: XCTestCase {

    var mockRPC: MockRPCClient!
    var kafka: Kafka!

    override func setUp() async throws {
        mockRPC = MockRPCClient()
        kafka = Kafka(brokers: ["broker:9092"], rpcClient: mockRPC)
    }

    func testTopicConsumerConsumeMessages() async throws {
        // Seed messages
        try await mockRPC.seed("events", with: ["Event 1", "Event 2", "Event 3"])

        let consumer: TopicConsumer<String> = kafka.consumer(for: "events", group: "test-group")

        var received: [String] = []
        var count = 0

        for try await record in consumer {
            received.append(record.value)
            count += 1
            if count >= 3 {
                break
            }
        }

        XCTAssertEqual(received, ["Event 1", "Event 2", "Event 3"])
    }

    func testTopicConsumerTypedMessages() async throws {
        struct Order: Codable, Equatable {
            let orderId: String
            let amount: Double
        }

        let orders = [
            Order(orderId: "1", amount: 10.0),
            Order(orderId: "2", amount: 20.0)
        ]

        try await mockRPC.seed("orders", with: orders)

        let consumer: TopicConsumer<Order> = kafka.consumer(for: "orders", group: "order-processor")

        var received: [Order] = []
        var count = 0

        for try await record in consumer {
            received.append(record.value)
            count += 1
            if count >= 2 {
                break
            }
        }

        XCTAssertEqual(received, orders)
    }

    func testTopicConsumerWithCommit() async throws {
        try await mockRPC.seed("events", with: ["Event 1"])

        let consumer: TopicConsumer<String> = kafka.consumer(for: "events", group: "commit-group")

        for try await record in consumer {
            try await record.commit()
            break
        }

        let committedOffset = await mockRPC.committedOffsets["commit-group-events-0"]
        XCTAssertEqual(committedOffset, 1)
    }
}

// MARK: - MockKafkaClient Tests

final class MockKafkaClientTests: XCTestCase {

    func testMockKafkaClientCreation() {
        let mockKafka = MockKafkaClient()
        XCTAssertNotNil(mockKafka)
        XCTAssertNotNil(mockKafka.kafka)
        XCTAssertNotNil(mockKafka.mockRPC)
    }

    func testMockKafkaClientSeedAndGetMessages() async throws {
        struct Order: Codable, Equatable {
            let orderId: String
            let amount: Double
        }

        let mockKafka = MockKafkaClient()

        let orders = [
            Order(orderId: "123", amount: 99.99),
            Order(orderId: "124", amount: 149.99)
        ]

        try await mockKafka.seedAsync("orders", with: orders)

        let retrieved = try await mockKafka.getMessagesAsync("orders", as: Order.self)
        XCTAssertEqual(retrieved.count, 2)
        XCTAssertEqual(retrieved[0], orders[0])
        XCTAssertEqual(retrieved[1], orders[1])
    }

    func testMockKafkaClientProduceAndConsume() async throws {
        struct Order: Codable, Equatable {
            let orderId: String
            let amount: Double
        }

        let mockKafka = MockKafkaClient()

        // Produce
        let producer: TopicProducer<Order> = mockKafka.producer(for: "orders")
        let order = Order(orderId: "123", amount: 99.99)
        _ = try await producer.send(order)

        // Verify
        let messages = try await mockKafka.getMessagesAsync("orders", as: Order.self)
        XCTAssertEqual(messages.count, 1)
        XCTAssertEqual(messages[0], order)
    }

    func testMockKafkaClientReset() async throws {
        let mockKafka = MockKafkaClient()

        try await mockKafka.seedAsync("topic1", with: ["msg1"])
        try await mockKafka.seedAsync("topic2", with: ["msg2"])

        await mockKafka.resetAsync()

        let msgs1 = try await mockKafka.getMessagesAsync("topic1", as: String.self)
        let msgs2 = try await mockKafka.getMessagesAsync("topic2", as: String.self)

        XCTAssertEqual(msgs1.count, 0)
        XCTAssertEqual(msgs2.count, 0)
    }

    func testMockKafkaClientSetNextError() async throws {
        let mockKafka = MockKafkaClient()

        await mockKafka.setNextErrorAsync(.timeout)

        let producer: TopicProducer<String> = mockKafka.producer(for: "test")

        do {
            _ = try await producer.send("test")
            XCTFail("Expected error to be thrown")
        } catch let error as KafkaError {
            XCTAssertEqual(error, .timeout)
        }
    }

    func testMockKafkaClientAdmin() async throws {
        let mockKafka = MockKafkaClient()

        try await mockKafka.admin.createTopic("test-topic", config: TopicConfig(partitions: 3))

        let exists = try await mockKafka.admin.topicExists("test-topic")
        XCTAssertTrue(exists)

        let info = try await mockKafka.admin.describeTopic("test-topic")
        XCTAssertEqual(info.partitions, 3)
    }

    func testMockKafkaClientStream() {
        struct Event: Codable, Sendable {
            let id: String
        }

        let mockKafka = MockKafkaClient()

        let stream: Stream<Event> = mockKafka.stream(from: "events")
        XCTAssertNotNil(stream)
    }

    func testMockKafkaClientTransaction() async throws {
        let mockKafka = MockKafkaClient()

        try await mockKafka.transaction(topic: "orders") { (tx: Transaction<String>) in
            _ = try await tx.send("Order 1")
            _ = try await tx.send("Order 2")
        }

        let messages = try await mockKafka.getMessagesAsync("orders", as: String.self)
        XCTAssertEqual(messages.count, 2)
    }
}
