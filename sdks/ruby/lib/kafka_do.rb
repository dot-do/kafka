# frozen_string_literal: true

require_relative 'kafka_do/version'
require_relative 'kafka_do/errors'
require_relative 'kafka_do/message'
require_relative 'kafka_do/record_metadata'
require_relative 'kafka_do/topic_partition'
require_relative 'kafka_do/producer'
require_relative 'kafka_do/consumer'
require_relative 'kafka_do/admin'
require_relative 'kafka_do/stream'
require_relative 'kafka_do/kafka'

# Kafka.do SDK for Ruby
#
# Event Streaming for Ruby. Blocks. Enumerables. Zero Ops.
#
# @example Basic usage following ruby-kafka API
#   kafka = Kafka.new(['broker1:9092'])
#
#   # Producer
#   producer = kafka.producer
#   producer.produce('Hello!', topic: 'my-topic')
#   producer.deliver_messages
#
#   # Consumer
#   consumer = kafka.consumer(group_id: 'my-group')
#   consumer.subscribe('my-topic')
#   consumer.each_message { |msg| puts msg.value }
#
#   # Admin
#   kafka.create_topic('new-topic', num_partitions: 3)
#
module KafkaDo
  class << self
    # Create a new Kafka client (convenience method)
    #
    # @param brokers [Array<String>] list of broker addresses
    # @param options [Hash] additional options
    # @return [Kafka] configured Kafka client
    def new(brokers = [], **options)
      Kafka.new(brokers, **options)
    end
  end
end

# Alias for ruby-kafka compatibility
Kafka = KafkaDo::Kafka unless defined?(Kafka)
