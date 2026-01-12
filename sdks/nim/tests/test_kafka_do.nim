## Tests for Kafka SDK

import std/[unittest, json, options, tables, times]
import ../src/kafka_do

suite "KafkaConfig":
  test "creates with defaults":
    let config = newKafkaConfig()
    check config.url == "https://kafka.do"
    check config.timeout == DefaultTimeout
    check config.retries == DefaultRetries

  test "creates with custom URL":
    let config = newKafkaConfig("https://custom.kafka.do", "test-api-key")
    check config.url == "https://custom.kafka.do"
    check config.apiKey == "test-api-key"

  test "generates headers":
    let config = newKafkaConfig()
    config.apiKey = "test-key"
    let headers = config.headers
    check headers["Authorization"] == "Bearer test-key"
    check headers["Content-Type"] == "application/json"

suite "ProducerConfig":
  test "creates with defaults":
    let config = newProducerConfig()
    check config.batchSize == DefaultBatchSize
    check config.lingerMs == DefaultLingerMs
    check config.acks == -1  # All
    check config.retries == DefaultRetries

  test "creates with custom values":
    let config = newProducerConfig(
      batchSize = 32768,
      compression = 1,  # Gzip
      acks = 1  # Leader
    )
    check config.batchSize == 32768
    check config.compression == 1
    check config.acks == 1

suite "ConsumerConfig":
  test "creates with defaults":
    let config = newConsumerConfig()
    check config.offset == 1  # Latest
    check config.autoCommit == true
    check config.maxPollRecords == DefaultMaxPollRecords

  test "creates with custom values":
    let config = newConsumerConfig(
      offset = 0,  # Earliest
      autoCommit = false,
      maxPollRecords = 100
    )
    check config.offset == 0
    check config.autoCommit == false
    check config.maxPollRecords == 100

suite "TopicConfig":
  test "creates with defaults":
    let config = newTopicConfig()
    check config.partitions == 1
    check config.replicationFactor == 1
    check config.retentionMs == DefaultRetentionMs
    check config.cleanupPolicy == "delete"

suite "KafkaRecord":
  test "creates from JSON":
    let json = %*{
      "topic": "orders",
      "partition": 0,
      "offset": 42,
      "key": "customer-123",
      "value": {"order_id": "abc", "amount": 99.99},
      "timestamp": "2024-01-15T10:30:00Z",
      "headers": {"correlation-id": "xyz"}
    }

    let record = KafkaRecord.fromJson(json)
    check record.topic == "orders"
    check record.partition == 0
    check record.offset == 42
    check record.key.isSome
    check record.key.get == "customer-123"
    check record.value["order_id"].getStr == "abc"
    check record.headers["correlation-id"] == "xyz"

  test "handles missing optional fields":
    let json = %*{
      "topic": "orders",
      "partition": 0,
      "offset": 0,
      "value": nil
    }

    let record = KafkaRecord.fromJson(json)
    check record.key.isNone
    check record.headers.len == 0

suite "RecordMetadata":
  test "creates from JSON":
    let json = %*{
      "topic": "orders",
      "partition": 0,
      "offset": 42,
      "timestamp": "2024-01-15T10:30:00Z"
    }

    let metadata = RecordMetadata.fromJson(json)
    check metadata.topic == "orders"
    check metadata.partition == 0
    check metadata.offset == 42

suite "Message":
  test "creates with value only":
    let msg = newMessage(%*{"order_id": "123"})
    check msg.key.isNone
    check msg.headers.len == 0

  test "creates with key and headers":
    let msg = newMessage(
      %*{"order_id": "123"},
      key = some("customer-456"),
      headers = {"correlation-id": "abc"}.toTable
    )
    check msg.key.isSome
    check msg.key.get == "customer-456"
    check msg.headers["correlation-id"] == "abc"

suite "Batch":
  test "creates batch":
    let json = %*{
      "topic": "orders",
      "partition": 0,
      "offset": 0,
      "value": {"id": 1}
    }

    let records = @[
      KafkaRecord.fromJson(json),
      KafkaRecord.fromJson(json)
    ]

    let batch = newBatch(records)
    check batch.len == 2
    check not batch.isEmpty

  test "first and last":
    let json = %*{
      "topic": "orders",
      "partition": 0,
      "offset": 0,
      "value": {"id": 1}
    }

    let records = @[KafkaRecord.fromJson(json)]
    let batch = newBatch(records)

    check batch.first.isSome
    check batch.last.isSome

suite "TopicInfo":
  test "creates from JSON":
    let json = %*{
      "name": "orders",
      "partitions": 3,
      "replicationFactor": 2,
      "retentionMs": 604800000,
      "cleanupPolicy": "compact",
      "internal": false
    }

    let info = TopicInfo.fromJson(json)
    check info.name == "orders"
    check info.partitions == 3
    check info.replicationFactor == 2
    check info.cleanupPolicy == "compact"

suite "GroupInfo":
  test "creates from JSON":
    let json = %*{
      "id": "order-processor",
      "state": "Stable",
      "memberCount": 3,
      "totalLag": 100,
      "members": []
    }

    let info = GroupInfo.fromJson(json)
    check info.id == "order-processor"
    check info.state == "Stable"
    check info.memberCount == 3
    check info.totalLag == 100

suite "Window types":
  test "tumbling window":
    let w = newTumblingWindow(60000)
    check w.windowType == "tumbling"
    check w.durationMs == 60000

    let json = w.toJson()
    check json["type"].getStr == "tumbling"
    check json["durationMs"].getInt == 60000

  test "sliding window":
    let w = newSlidingWindow(60000, 10000)
    check w.windowType == "sliding"
    check w.durationMs == 60000
    check w.slideMs == 10000

  test "session window":
    let w = newSessionWindow(30000)
    check w.windowType == "session"
    check w.gapMs == 30000

  test "hopping window":
    let w = newHoppingWindow(60000, 15000)
    check w.windowType == "hopping"
    check w.durationMs == 60000
    check w.advanceMs == 15000

suite "Errors":
  test "creates KafkaError":
    let e = newKafkaError("TEST_CODE", "Test message")
    check e.code == "TEST_CODE"
    check e.msg == "Test message"
    check e.retriable == false

  test "creates retriable error":
    let e = newTimeoutError("Request timed out")
    check e.code == "TIMEOUT"
    check e.retriable == true

  test "creates TopicNotFoundError":
    let e = newTopicNotFoundError("orders")
    check e.topic == "orders"
    check e.code == "TOPIC_NOT_FOUND"

  test "creates ConnectionError":
    let e = newConnectionError("Connection failed", "https://kafka.do")
    check e.address == "https://kafka.do"
    check e.retriable == true

suite "Offset types":
  test "Offset enum":
    check Earliest.ord == 0
    check Latest.ord == 1

  test "TimestampOffset":
    let ts = getTime()
    let offset = newTimestampOffset(ts)
    check offset.timestamp == ts
