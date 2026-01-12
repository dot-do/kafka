# Kafka SDK for the .do platform
# Event Streaming for Crystal. Fibers. Channels. Zero Ops.
#
# Example:
#   require "kafka-do"
#
#   kafka = Kafka::Client.new
#   kafka.produce("orders", {order_id: "123", amount: 99.99})
#
#   # Consume messages
#   for record in kafka.consume("orders", group: "my-processor")
#     puts record.value
#     record.commit
#   end
#
#   # Stream processing
#   kafka.stream("orders")
#     .filter { |o| o["amount"].as_f > 100 }
#     .map { |o| o["customer_id"] }
#     .to("high-value-customers")

require "json"
require "http/client"
require "uri"

require "./kafka-do/config"
require "./kafka-do/errors"
require "./kafka-do/record"
require "./kafka-do/producer"
require "./kafka-do/consumer"
require "./kafka-do/admin"
require "./kafka-do/stream"
require "./kafka-do/client"

module Kafka
  VERSION = "0.1.0"

  # Offset positions for consumers
  enum Offset
    Earliest
    Latest
  end

  # Timestamp-based offset
  struct TimestampOffset
    getter timestamp : Time

    def initialize(@timestamp : Time)
    end
  end

  # Acknowledgment modes
  enum Acks
    None   =  0
    Leader =  1
    All    = -1
  end

  # Compression types
  enum Compression
    None
    Gzip
    Snappy
    Lz4
    Zstd
  end

  # Convenience method to create a client
  def self.client : Client
    Client.new
  end

  # Create a client with a specific URL
  def self.client(url : String, api_key : String? = nil) : Client
    config = Config.new
    config.url = url
    config.api_key = api_key
    Client.new(config)
  end
end
