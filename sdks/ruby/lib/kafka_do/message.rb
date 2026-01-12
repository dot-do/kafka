# frozen_string_literal: true

require 'time'

module KafkaDo
  # Represents a Kafka message received from a topic
  #
  # @example Accessing message properties
  #   consumer.each_message do |message|
  #     puts "Topic: #{message.topic}"
  #     puts "Partition: #{message.partition}"
  #     puts "Offset: #{message.offset}"
  #     puts "Key: #{message.key}"
  #     puts "Value: #{message.value}"
  #     puts "Timestamp: #{message.timestamp}"
  #     puts "Headers: #{message.headers}"
  #   end
  #
  class Message
    # @return [String] the topic the message belongs to
    attr_reader :topic

    # @return [Integer] the partition number
    attr_reader :partition

    # @return [Integer] the message offset within the partition
    attr_reader :offset

    # @return [String, nil] the message key (may be nil)
    attr_reader :key

    # @return [String] the message value
    attr_reader :value

    # @return [Time] the message timestamp
    attr_reader :timestamp

    # @return [Hash<String, String>] optional message headers
    attr_reader :headers

    # Create a new Message
    #
    # @param topic [String] the topic name
    # @param partition [Integer] the partition number
    # @param offset [Integer] the offset
    # @param key [String, nil] the message key
    # @param value [String] the message value
    # @param timestamp [Time] the timestamp
    # @param headers [Hash] optional headers
    def initialize(topic:, partition:, offset:, value:, key: nil, timestamp: nil, headers: nil)
      @topic = topic
      @partition = partition
      @offset = offset
      @key = key
      @value = value
      @timestamp = timestamp || Time.now
      @headers = headers || {}
    end

    # Create a Message from an RPC response hash
    #
    # @param data [Hash] the RPC response data
    # @return [Message] the parsed message
    def self.from_rpc(data)
      timestamp = if data['timestamp']
                    Time.at(data['timestamp'] / 1000.0)
                  else
                    Time.now
                  end

      headers = {}
      if data['headers'].is_a?(Array)
        data['headers'].each do |header|
          key = header['key']
          value = header['value']
          headers[key] = decode_base64(value) if key && value
        end
      end

      new(
        topic: data['topic'],
        partition: data['partition'].to_i,
        offset: data['offset'].to_i,
        key: decode_base64(data['key']),
        value: decode_base64(data['value']),
        timestamp: timestamp,
        headers: headers
      )
    end

    # Returns the TopicPartition for this message
    #
    # @return [TopicPartition]
    def topic_partition
      TopicPartition.new(topic, partition)
    end

    # Returns a string representation
    #
    # @return [String]
    def to_s
      "#<KafkaDo::Message topic=#{topic} partition=#{partition} offset=#{offset}>"
    end

    # Returns a detailed inspection string
    #
    # @return [String]
    def inspect
      "#<KafkaDo::Message topic=#{topic.inspect} partition=#{partition} " \
        "offset=#{offset} key=#{key.inspect} value=#{value.inspect} " \
        "timestamp=#{timestamp} headers=#{headers.inspect}>"
    end

    # Equality check
    #
    # @param other [Message] the other message
    # @return [Boolean]
    def ==(other)
      return false unless other.is_a?(Message)

      topic == other.topic &&
        partition == other.partition &&
        offset == other.offset
    end
    alias eql? ==

    # Hash code for use in Hash keys
    #
    # @return [Integer]
    def hash
      [topic, partition, offset].hash
    end

    private

    def self.decode_base64(value)
      return nil if value.nil?
      return value unless value.is_a?(String)

      require 'base64'
      Base64.decode64(value)
    rescue StandardError
      value
    end

    private_class_method :decode_base64
  end
end
