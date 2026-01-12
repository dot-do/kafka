# frozen_string_literal: true

require 'time'

module KafkaDo
  # Metadata about a successfully produced record
  #
  # @example Accessing record metadata
  #   metadata = producer.produce('Hello!', topic: 'my-topic')
  #   puts "Sent to partition #{metadata.partition} at offset #{metadata.offset}"
  #
  class RecordMetadata
    # @return [String] the topic the record was sent to
    attr_reader :topic

    # @return [Integer] the partition the record was sent to
    attr_reader :partition

    # @return [Integer] the offset of the record in the partition
    attr_reader :offset

    # @return [Time] the timestamp of the record
    attr_reader :timestamp

    # @return [Integer] the size of the serialized key
    attr_reader :serialized_key_size

    # @return [Integer] the size of the serialized value
    attr_reader :serialized_value_size

    # Create a new RecordMetadata
    #
    # @param topic [String] the topic name
    # @param partition [Integer] the partition number
    # @param offset [Integer] the offset
    # @param timestamp [Time] the timestamp
    # @param serialized_key_size [Integer] serialized key size
    # @param serialized_value_size [Integer] serialized value size
    def initialize(topic:, partition:, offset:, timestamp: nil,
                   serialized_key_size: 0, serialized_value_size: 0)
      @topic = topic
      @partition = partition
      @offset = offset
      @timestamp = timestamp || Time.now
      @serialized_key_size = serialized_key_size
      @serialized_value_size = serialized_value_size
    end

    # Create RecordMetadata from an RPC response hash
    #
    # @param data [Hash] the RPC response data
    # @return [RecordMetadata] the parsed metadata
    def self.from_rpc(data)
      timestamp = if data['timestamp']
                    Time.at(data['timestamp'] / 1000.0)
                  else
                    Time.now
                  end

      new(
        topic: data['topic'],
        partition: data['partition'].to_i,
        offset: data['offset'].to_i,
        timestamp: timestamp,
        serialized_key_size: data['serialized_key_size'].to_i,
        serialized_value_size: data['serialized_value_size'].to_i
      )
    end

    # Returns the TopicPartition for this record
    #
    # @return [TopicPartition]
    def topic_partition
      TopicPartition.new(topic, partition)
    end

    # Returns a string representation
    #
    # @return [String]
    def to_s
      "#<KafkaDo::RecordMetadata topic=#{topic} partition=#{partition} offset=#{offset}>"
    end

    # Returns a detailed inspection string
    #
    # @return [String]
    def inspect
      "#<KafkaDo::RecordMetadata topic=#{topic.inspect} partition=#{partition} " \
        "offset=#{offset} timestamp=#{timestamp}>"
    end
  end
end
