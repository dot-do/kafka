module Kafka
  # Main client configuration
  class Config
    property url : String = "https://kafka.do"
    property api_key : String?
    property timeout : Time::Span = 30.seconds
    property retries : Int32 = 3

    def initialize
      @url = ENV["KAFKA_DO_URL"]? || "https://kafka.do"
      @api_key = ENV["KAFKA_DO_API_KEY"]?
    end

    def self.default : Config
      Config.new
    end

    # Get headers for API requests
    def headers : HTTP::Headers
      headers = HTTP::Headers.new
      headers["Content-Type"] = "application/json"
      if key = @api_key
        headers["Authorization"] = "Bearer #{key}"
      end
      headers
    end
  end

  # Producer configuration
  class ProducerConfig
    property batch_size : Int32 = 16384
    property linger_ms : Int32 = 5
    property compression : Compression = Compression::None
    property acks : Acks = Acks::All
    property retries : Int32 = 3
    property retry_backoff_ms : Int32 = 100

    def initialize(
      @batch_size : Int32 = 16384,
      @linger_ms : Int32 = 5,
      @compression : Compression = Compression::None,
      @acks : Acks = Acks::All,
      @retries : Int32 = 3,
      @retry_backoff_ms : Int32 = 100
    )
    end

    def self.default : ProducerConfig
      ProducerConfig.new
    end
  end

  # Consumer configuration
  class ConsumerConfig
    property offset : Offset | TimestampOffset = Offset::Latest
    property auto_commit : Bool = true
    property fetch_min_bytes : Int32 = 1
    property fetch_max_wait_ms : Int32 = 500
    property max_poll_records : Int32 = 500
    property session_timeout : Time::Span = 30.seconds
    property heartbeat_interval : Time::Span = 3.seconds

    def initialize(
      @offset : Offset | TimestampOffset = Offset::Latest,
      @auto_commit : Bool = true,
      @fetch_min_bytes : Int32 = 1,
      @fetch_max_wait_ms : Int32 = 500,
      @max_poll_records : Int32 = 500,
      @session_timeout : Time::Span = 30.seconds,
      @heartbeat_interval : Time::Span = 3.seconds
    )
    end

    def self.default : ConsumerConfig
      ConsumerConfig.new
    end
  end

  # Batch consumer configuration
  class BatchConfig
    property batch_size : Int32 = 100
    property batch_timeout : Time::Span = 5.seconds
    property auto_commit : Bool = true

    def initialize(
      @batch_size : Int32 = 100,
      @batch_timeout : Time::Span = 5.seconds,
      @auto_commit : Bool = true
    )
    end

    def self.default : BatchConfig
      BatchConfig.new
    end
  end

  # Topic configuration
  class TopicConfig
    property partitions : Int32 = 1
    property replication_factor : Int32 = 1
    property retention_ms : Int64 = 7 * 24 * 60 * 60 * 1000_i64 # 7 days
    property cleanup_policy : String = "delete"
    property compression_type : String = "producer"

    def initialize(
      @partitions : Int32 = 1,
      @replication_factor : Int32 = 1,
      @retention_ms : Int64 = 7 * 24 * 60 * 60 * 1000_i64,
      @cleanup_policy : String = "delete",
      @compression_type : String = "producer"
    )
    end
  end
end
