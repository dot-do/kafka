require "./spec_helper"

describe Kafka do
  describe "VERSION" do
    it "has a version" do
      Kafka::VERSION.should eq "0.1.0"
    end
  end

  describe Kafka::Offset do
    it "has Earliest and Latest" do
      Kafka::Offset::Earliest.value.should eq 0
      Kafka::Offset::Latest.value.should eq 1
    end
  end

  describe Kafka::Acks do
    it "has correct values" do
      Kafka::Acks::None.value.should eq 0
      Kafka::Acks::Leader.value.should eq 1
      Kafka::Acks::All.value.should eq -1
    end
  end

  describe Kafka::Compression do
    it "has compression types" do
      Kafka::Compression::None.value.should eq 0
      Kafka::Compression::Gzip.value.should eq 1
      Kafka::Compression::Snappy.value.should eq 2
      Kafka::Compression::Lz4.value.should eq 3
      Kafka::Compression::Zstd.value.should eq 4
    end
  end

  describe Kafka::TimestampOffset do
    it "stores a timestamp" do
      time = Time.utc(2024, 1, 15, 10, 30, 0)
      offset = Kafka::TimestampOffset.new(time)
      offset.timestamp.should eq time
    end
  end
end

describe Kafka::Config do
  it "has default values" do
    config = Kafka::Config.new
    config.url.should eq "https://kafka.do"
    config.timeout.should eq 30.seconds
    config.retries.should eq 3
  end

  it "generates headers with API key" do
    config = Kafka::Config.new
    config.api_key = "test-key"
    headers = config.headers
    headers["Authorization"].should eq "Bearer test-key"
    headers["Content-Type"].should eq "application/json"
  end
end

describe Kafka::ProducerConfig do
  it "has default values" do
    config = Kafka::ProducerConfig.default
    config.batch_size.should eq 16384
    config.linger_ms.should eq 5
    config.compression.should eq Kafka::Compression::None
    config.acks.should eq Kafka::Acks::All
    config.retries.should eq 3
    config.retry_backoff_ms.should eq 100
  end

  it "allows customization" do
    config = Kafka::ProducerConfig.new(
      batch_size: 32768,
      compression: Kafka::Compression::Gzip,
      acks: Kafka::Acks::Leader
    )
    config.batch_size.should eq 32768
    config.compression.should eq Kafka::Compression::Gzip
    config.acks.should eq Kafka::Acks::Leader
  end
end

describe Kafka::ConsumerConfig do
  it "has default values" do
    config = Kafka::ConsumerConfig.default
    config.auto_commit.should be_true
    config.max_poll_records.should eq 500
    config.session_timeout.should eq 30.seconds
    config.heartbeat_interval.should eq 3.seconds
  end

  it "allows timestamp offset" do
    time = Time.utc(2024, 1, 15)
    config = Kafka::ConsumerConfig.new(
      offset: Kafka::TimestampOffset.new(time)
    )

    case config.offset
    when Kafka::TimestampOffset
      config.offset.as(Kafka::TimestampOffset).timestamp.should eq time
    else
      fail "Expected TimestampOffset"
    end
  end
end

describe Kafka::TopicConfig do
  it "has default values" do
    config = Kafka::TopicConfig.new
    config.partitions.should eq 1
    config.replication_factor.should eq 1
    config.retention_ms.should eq 604800000_i64 # 7 days
    config.cleanup_policy.should eq "delete"
  end
end

describe Kafka::Record do
  it "creates from JSON" do
    json = JSON.parse(%({
      "topic": "orders",
      "partition": 0,
      "offset": 42,
      "key": "customer-123",
      "value": {"order_id": "abc", "amount": 99.99},
      "timestamp": "2024-01-15T10:30:00Z",
      "headers": {"correlation-id": "xyz"}
    }))

    record = Kafka::Record.from_json(json)
    record.topic.should eq "orders"
    record.partition.should eq 0
    record.offset.should eq 42_i64
    record.key.should eq "customer-123"
    record.value["order_id"].as_s.should eq "abc"
    record.value["amount"].as_f.should eq 99.99
    record.headers["correlation-id"].should eq "xyz"
  end

  it "handles missing optional fields" do
    json = JSON.parse(%({
      "topic": "orders",
      "partition": 0,
      "offset": 0,
      "value": null
    }))

    record = Kafka::Record.from_json(json)
    record.key.should be_nil
    record.headers.empty?.should be_true
  end
end

describe Kafka::RecordMetadata do
  it "creates from JSON" do
    json = JSON.parse(%({
      "topic": "orders",
      "partition": 0,
      "offset": 42,
      "timestamp": "2024-01-15T10:30:00Z"
    }))

    metadata = Kafka::RecordMetadata.from_json(json)
    metadata.topic.should eq "orders"
    metadata.partition.should eq 0
    metadata.offset.should eq 42_i64
  end
end

describe Kafka::Message do
  it "creates with value only" do
    value = JSON.parse(%({"order_id": "123"}))
    msg = Kafka::Message(JSON::Any).new(value)
    msg.value.should eq value
    msg.key.should be_nil
    msg.headers.empty?.should be_true
  end

  it "creates with key and headers" do
    value = JSON.parse(%({"order_id": "123"}))
    msg = Kafka::Message(JSON::Any).new(
      value,
      key: "customer-456",
      headers: {"correlation-id" => "abc"}
    )
    msg.key.should eq "customer-456"
    msg.headers["correlation-id"].should eq "abc"
  end
end

describe Kafka::Batch do
  it "manages a batch of records" do
    json = JSON.parse(%({
      "topic": "orders",
      "partition": 0,
      "offset": 0,
      "value": {"id": 1}
    }))

    records = [
      Kafka::Record.from_json(json),
      Kafka::Record.from_json(json),
    ]

    batch = Kafka::Batch.new(records)
    batch.size.should eq 2
    batch.empty?.should be_false
    batch.first.try(&.value["id"].as_i).should eq 1
  end
end
