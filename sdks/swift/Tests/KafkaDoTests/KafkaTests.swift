import XCTest
@testable import KafkaDo

final class KafkaTests: XCTestCase {

    // MARK: - KafkaConfig Tests

    func testDefaultConfig() {
        let config = KafkaConfig.default
        XCTAssertEqual(config.url, "https://kafka.do")
        XCTAssertEqual(config.timeout, .seconds(30))
        XCTAssertEqual(config.retries, 3)
    }

    func testCustomConfig() {
        let config = KafkaConfig(
            url: "https://custom.kafka.do",
            apiKey: "test-key",
            timeout: .seconds(60),
            retries: 5
        )
        XCTAssertEqual(config.url, "https://custom.kafka.do")
        XCTAssertEqual(config.apiKey, "test-key")
        XCTAssertEqual(config.timeout, .seconds(60))
        XCTAssertEqual(config.retries, 5)
    }

    // MARK: - Kafka Client Tests

    func testClientCreation() async {
        let kafka = Kafka(brokers: ["broker1:9092", "broker2:9092"])
        XCTAssertEqual(kafka.brokers.count, 2)
        XCTAssertEqual(kafka.brokers[0], "broker1:9092")
    }

    func testClientWithMockRPC() async throws {
        let mockRPC = MockRPCClient()
        let kafka = Kafka(brokers: ["broker:9092"], rpcClient: mockRPC)

        let admin = kafka.admin()
        try await admin.createTopic("test-topic")

        let topics = try await admin.listTopics()
        XCTAssertEqual(topics.count, 1)
        XCTAssertEqual(topics[0].name, "test-topic")
    }

    // MARK: - KafkaError Tests

    func testErrorIsRetriable() {
        XCTAssertTrue(KafkaError.timeout.isRetriable)
        XCTAssertTrue(KafkaError.disconnected.isRetriable)
        XCTAssertTrue(KafkaError.rebalanceInProgress.isRetriable)
        XCTAssertTrue(KafkaError.notLeader(topic: "t", partition: 0).isRetriable)

        XCTAssertFalse(KafkaError.topicNotFound("t").isRetriable)
        XCTAssertFalse(KafkaError.unauthorized("reason").isRetriable)
        XCTAssertFalse(KafkaError.serializationError("reason").isRetriable)
    }

    func testErrorMessages() {
        XCTAssertEqual(
            KafkaError.topicNotFound("orders").message,
            "Topic 'orders' not found"
        )
        XCTAssertEqual(
            KafkaError.messageTooLarge(size: 2000000, maxSize: 1000000).message,
            "Message size 2000000 exceeds maximum allowed size 1000000"
        )
        XCTAssertEqual(
            KafkaError.timeout.message,
            "Request timed out"
        )
    }

    func testErrorCodes() {
        XCTAssertEqual(KafkaError.topicNotFound("t").code, 3)
        XCTAssertEqual(KafkaError.timeout.code, 7)
        XCTAssertEqual(KafkaError.unauthorized("reason").code, 31)
    }
}
