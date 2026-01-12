import XCTest
@testable import KafkaDo

final class ProducerTests: XCTestCase {

    var mockRPC: MockRPCClient!
    var kafka: Kafka!

    override func setUp() async throws {
        mockRPC = MockRPCClient()
        kafka = Kafka(brokers: ["broker:9092"], rpcClient: mockRPC)
    }

    // MARK: - Basic Send Tests

    func testSendStringMessage() async throws {
        let producer: Producer<String> = kafka.producer()

        let metadata = try await producer.send(topic: "test-topic", value: "Hello, Kafka!")

        XCTAssertEqual(metadata.topic, "test-topic")
        XCTAssertEqual(metadata.partition, 0)
        XCTAssertEqual(metadata.offset, 0)

        let messages = try await mockRPC.getMessages("test-topic", as: String.self)
        XCTAssertEqual(messages.count, 1)
        XCTAssertEqual(messages[0], "Hello, Kafka!")
    }

    func testSendDataMessage() async throws {
        let producer: Producer<Data> = kafka.producer()
        let data = "Test data".data(using: .utf8)!

        let metadata = try await producer.send(topic: "test-topic", value: data)

        XCTAssertEqual(metadata.topic, "test-topic")
        XCTAssertEqual(metadata.offset, 0)

        let messages = try await mockRPC.getMessages("test-topic", as: Data.self)
        XCTAssertEqual(messages.count, 1)
        XCTAssertEqual(String(data: messages[0], encoding: .utf8), "Test data")
    }

    func testSendCodableMessage() async throws {
        struct Order: Codable, Equatable {
            let orderId: String
            let amount: Double
        }

        let producer: Producer<Order> = kafka.producer()
        let order = Order(orderId: "123", amount: 99.99)

        let metadata = try await producer.send(topic: "orders", value: order)

        XCTAssertEqual(metadata.topic, "orders")
        XCTAssertEqual(metadata.offset, 0)

        let messages = try await mockRPC.getMessages("orders", as: Order.self)
        XCTAssertEqual(messages.count, 1)
        XCTAssertEqual(messages[0], order)
    }

    // MARK: - Send with Key Tests

    func testSendWithKey() async throws {
        let producer: Producer<String> = kafka.producer()

        _ = try await producer.send(
            topic: "test-topic",
            value: "Message with key",
            key: "customer-123"
        )

        let rawMessages = await mockRPC.messages["test-topic"]
        XCTAssertEqual(rawMessages?.count, 1)
        XCTAssertNotNil(rawMessages?[0].key)
        XCTAssertEqual(String(data: rawMessages![0].key!, encoding: .utf8), "customer-123")
    }

    // MARK: - Send with Headers Tests

    func testSendWithHeaders() async throws {
        let producer: Producer<String> = kafka.producer()

        _ = try await producer.send(
            topic: "test-topic",
            value: "Message with headers",
            headers: ["correlation-id": "abc-123", "source": "test"]
        )

        let rawMessages = await mockRPC.messages["test-topic"]
        XCTAssertEqual(rawMessages?.count, 1)
        XCTAssertEqual(rawMessages?[0].headers["correlation-id"], "abc-123")
        XCTAssertEqual(rawMessages?[0].headers["source"], "test")
    }

    // MARK: - Batch Send Tests

    func testSendBatchValues() async throws {
        let producer: Producer<String> = kafka.producer()

        let results = try await producer.sendBatch(
            topic: "test-topic",
            values: ["Message 1", "Message 2", "Message 3"]
        )

        XCTAssertEqual(results.count, 3)
        XCTAssertEqual(results[0].offset, 0)
        XCTAssertEqual(results[1].offset, 1)
        XCTAssertEqual(results[2].offset, 2)

        let messages = try await mockRPC.getMessages("test-topic", as: String.self)
        XCTAssertEqual(messages.count, 3)
    }

    func testSendBatchMessages() async throws {
        struct Order: Codable, Equatable {
            let orderId: String
            let amount: Double
        }

        let producer: Producer<Order> = kafka.producer()

        let messages = [
            Message(key: "cust-1", value: Order(orderId: "1", amount: 10.0)),
            Message(key: "cust-2", value: Order(orderId: "2", amount: 20.0))
        ]

        let results = try await producer.sendBatch(topic: "orders", messages: messages)

        XCTAssertEqual(results.count, 2)

        let rawMessages = await mockRPC.messages["orders"]
        XCTAssertEqual(rawMessages?.count, 2)
        XCTAssertEqual(String(data: rawMessages![0].key!, encoding: .utf8), "cust-1")
        XCTAssertEqual(String(data: rawMessages![1].key!, encoding: .utf8), "cust-2")
    }

    // MARK: - Multiple Sends Tests

    func testMultipleSendsIncrementOffset() async throws {
        let producer: Producer<String> = kafka.producer()

        let meta1 = try await producer.send(topic: "test-topic", value: "First")
        let meta2 = try await producer.send(topic: "test-topic", value: "Second")
        let meta3 = try await producer.send(topic: "test-topic", value: "Third")

        XCTAssertEqual(meta1.offset, 0)
        XCTAssertEqual(meta2.offset, 1)
        XCTAssertEqual(meta3.offset, 2)
    }

    // MARK: - Error Handling Tests

    func testSendWithError() async throws {
        await mockRPC.setNextError(.timeout)

        let producer: Producer<String> = kafka.producer()

        do {
            _ = try await producer.send(topic: "test-topic", value: "Test")
            XCTFail("Expected error to be thrown")
        } catch let error as KafkaError {
            XCTAssertEqual(error, .timeout)
        }
    }

    // MARK: - Transaction Tests

    func testTransaction() async throws {
        try await kafka.transaction(topic: "orders") { (tx: Transaction<String>) in
            _ = try await tx.send("Order 1")
            _ = try await tx.send("Order 2")
        }

        let messages = try await mockRPC.getMessages("orders", as: String.self)
        XCTAssertEqual(messages.count, 2)
    }

    func testTransactionBatch() async throws {
        try await kafka.transaction(topic: "orders") { (tx: Transaction<String>) in
            _ = try await tx.sendBatch(["Order 1", "Order 2", "Order 3"])
        }

        let messages = try await mockRPC.getMessages("orders", as: String.self)
        XCTAssertEqual(messages.count, 3)
    }
}

// Helper extension for tests
extension MockRPCClient {
    func setNextError(_ error: KafkaError) async {
        self.nextError = error
    }
}
