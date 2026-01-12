import XCTest
@testable import KafkaDo

final class MessageTests: XCTestCase {

    // MARK: - Message Tests

    func testMessageWithValue() {
        let message = Message(value: "Hello")
        XCTAssertNil(message.key)
        XCTAssertEqual(message.value, "Hello")
        XCTAssertTrue(message.headers.isEmpty)
    }

    func testMessageWithKeyAndValue() {
        let message = Message(key: "key-123", value: "Hello")
        XCTAssertEqual(message.key, "key-123")
        XCTAssertEqual(message.value, "Hello")
        XCTAssertTrue(message.headers.isEmpty)
    }

    func testMessageWithAllProperties() {
        let headers = ["header1": "value1", "header2": "value2"]
        let message = Message(key: "key-123", value: "Hello", headers: headers)
        XCTAssertEqual(message.key, "key-123")
        XCTAssertEqual(message.value, "Hello")
        XCTAssertEqual(message.headers, headers)
    }

    // MARK: - RecordMetadata Tests

    func testRecordMetadata() {
        let timestamp = Date()
        let metadata = RecordMetadata(
            topic: "test-topic",
            partition: 2,
            offset: 100,
            timestamp: timestamp
        )

        XCTAssertEqual(metadata.topic, "test-topic")
        XCTAssertEqual(metadata.partition, 2)
        XCTAssertEqual(metadata.offset, 100)
        XCTAssertEqual(metadata.timestamp, timestamp)
    }

    func testRecordMetadataEquatable() {
        let timestamp = Date()
        let meta1 = RecordMetadata(topic: "topic", partition: 0, offset: 10, timestamp: timestamp)
        let meta2 = RecordMetadata(topic: "topic", partition: 0, offset: 10, timestamp: timestamp)
        let meta3 = RecordMetadata(topic: "topic", partition: 0, offset: 11, timestamp: timestamp)

        XCTAssertEqual(meta1, meta2)
        XCTAssertNotEqual(meta1, meta3)
    }

    // MARK: - TopicInfo Tests

    func testTopicInfo() {
        let info = TopicInfo(
            name: "my-topic",
            partitions: 3,
            replicationFactor: 2,
            retentionMs: 604800000
        )

        XCTAssertEqual(info.name, "my-topic")
        XCTAssertEqual(info.partitions, 3)
        XCTAssertEqual(info.replicationFactor, 2)
        XCTAssertEqual(info.retentionMs, 604800000)
    }

    // MARK: - TopicConfig Tests

    func testTopicConfigDefaults() {
        let config = TopicConfig.default
        XCTAssertNil(config.partitions)
        XCTAssertNil(config.replicationFactor)
        XCTAssertNil(config.retentionMs)
    }

    func testTopicConfigCustom() {
        let config = TopicConfig(
            partitions: 5,
            replicationFactor: 3,
            retentionMs: 86400000
        )

        XCTAssertEqual(config.partitions, 5)
        XCTAssertEqual(config.replicationFactor, 3)
        XCTAssertEqual(config.retentionMs, 86400000)
    }

    // MARK: - ConsumerGroupInfo Tests

    func testConsumerGroupInfo() {
        let members = [
            ConsumerGroupMember(
                id: "member-1",
                clientId: "client-1",
                host: "host1",
                assignments: [PartitionAssignment(topic: "topic", partition: 0)]
            )
        ]

        let info = ConsumerGroupInfo(
            id: "my-group",
            state: .stable,
            memberCount: 1,
            members: members,
            totalLag: 50
        )

        XCTAssertEqual(info.id, "my-group")
        XCTAssertEqual(info.state, .stable)
        XCTAssertEqual(info.memberCount, 1)
        XCTAssertEqual(info.members.count, 1)
        XCTAssertEqual(info.totalLag, 50)
    }

    // MARK: - ConsumerGroupState Tests

    func testConsumerGroupStates() {
        XCTAssertEqual(ConsumerGroupState.unknown.rawValue, "Unknown")
        XCTAssertEqual(ConsumerGroupState.preparingRebalance.rawValue, "PreparingRebalance")
        XCTAssertEqual(ConsumerGroupState.completingRebalance.rawValue, "CompletingRebalance")
        XCTAssertEqual(ConsumerGroupState.stable.rawValue, "Stable")
        XCTAssertEqual(ConsumerGroupState.dead.rawValue, "Dead")
        XCTAssertEqual(ConsumerGroupState.empty.rawValue, "Empty")
    }

    // MARK: - ConsumerGroupMember Tests

    func testConsumerGroupMember() {
        let assignments = [
            PartitionAssignment(topic: "topic-1", partition: 0),
            PartitionAssignment(topic: "topic-1", partition: 1)
        ]

        let member = ConsumerGroupMember(
            id: "consumer-abc",
            clientId: "client-123",
            host: "192.168.1.1",
            assignments: assignments
        )

        XCTAssertEqual(member.id, "consumer-abc")
        XCTAssertEqual(member.clientId, "client-123")
        XCTAssertEqual(member.host, "192.168.1.1")
        XCTAssertEqual(member.assignments.count, 2)
    }

    // MARK: - PartitionAssignment Tests

    func testPartitionAssignment() {
        let assignment = PartitionAssignment(topic: "orders", partition: 5)
        XCTAssertEqual(assignment.topic, "orders")
        XCTAssertEqual(assignment.partition, 5)
    }

    // MARK: - OffsetPosition Tests

    func testOffsetPositionEarliest() {
        let position = OffsetPosition.earliest
        XCTAssertEqual(position, .earliest)
    }

    func testOffsetPositionLatest() {
        let position = OffsetPosition.latest
        XCTAssertEqual(position, .latest)
    }

    func testOffsetPositionOffset() {
        let position = OffsetPosition.offset(12345)
        if case .offset(let value) = position {
            XCTAssertEqual(value, 12345)
        } else {
            XCTFail("Expected offset position")
        }
    }

    func testOffsetPositionTimestamp() {
        let date = Date()
        let position = OffsetPosition.timestamp(date)
        if case .timestamp(let value) = position {
            XCTAssertEqual(value, date)
        } else {
            XCTFail("Expected timestamp position")
        }
    }

    // MARK: - RawMessage Tests

    func testRawMessage() {
        let timestamp = Date()
        let value = "test".data(using: .utf8)!
        let key = "key".data(using: .utf8)!

        let message = RawMessage(
            topic: "test-topic",
            partition: 1,
            offset: 42,
            key: key,
            value: value,
            timestamp: timestamp,
            headers: ["header": "value"]
        )

        XCTAssertEqual(message.topic, "test-topic")
        XCTAssertEqual(message.partition, 1)
        XCTAssertEqual(message.offset, 42)
        XCTAssertEqual(message.key, key)
        XCTAssertEqual(message.value, value)
        XCTAssertEqual(message.timestamp, timestamp)
        XCTAssertEqual(message.headers["header"], "value")
    }

    // MARK: - KafkaRecord Tests

    func testKafkaRecord() async throws {
        var committed = false
        let timestamp = Date()

        let record = KafkaRecord<String>(
            topic: "test-topic",
            partition: 0,
            offset: 10,
            key: "test-key",
            value: "test-value",
            timestamp: timestamp,
            headers: ["h1": "v1"],
            commitHandler: { committed = true }
        )

        XCTAssertEqual(record.topic, "test-topic")
        XCTAssertEqual(record.partition, 0)
        XCTAssertEqual(record.offset, 10)
        XCTAssertEqual(record.key, "test-key")
        XCTAssertEqual(record.value, "test-value")
        XCTAssertEqual(record.timestamp, timestamp)
        XCTAssertEqual(record.headers["h1"], "v1")

        XCTAssertFalse(committed)
        try await record.commit()
        XCTAssertTrue(committed)
    }
}
