# frozen_string_literal: true

require 'base64'
require 'json'

module KafkaDo
  # Kafka message producer
  #
  # Buffers messages and sends them in batches for efficiency.
  # Follows the ruby-kafka Producer API.
  #
  # @example Basic usage
  #   producer = kafka.producer
  #   producer.produce('Hello!', topic: 'my-topic')
  #   producer.produce('World!', topic: 'my-topic', key: 'greeting')
  #   producer.deliver_messages
  #
  # @example With block (auto-delivers)
  #   kafka.producer do |producer|
  #     producer.produce('Hello!', topic: 'my-topic')
  #   end
  #
  class Producer
    # @return [Kafka] the parent Kafka client
    attr_reader :kafka

    # @return [Integer] the number of buffered messages
    attr_reader :buffer_size

    # @return [Integer] the byte size of buffered messages
    attr_reader :buffer_bytesize

    # Create a new Producer
    #
    # @param kafka [Kafka] the parent Kafka client
    # @param compression_codec [Symbol] compression type
    # @param required_acks [Integer] number of acks required
    # @param max_buffer_size [Integer] maximum buffer size
    # @param max_buffer_bytesize [Integer] maximum buffer byte size
    def initialize(kafka, compression_codec: :none, required_acks: 1,
                   max_buffer_size: 1000, max_buffer_bytesize: 10_000_000, **_options)
      @kafka = kafka
      @compression_codec = compression_codec
      @required_acks = required_acks
      @max_buffer_size = max_buffer_size
      @max_buffer_bytesize = max_buffer_bytesize
      @buffer = []
      @buffer_size = 0
      @buffer_bytesize = 0
      @closed = false
      @mutex = Mutex.new
    end

    # Produce a message to a topic
    #
    # The message is buffered and will be sent when deliver_messages is called
    # or when the buffer is full.
    #
    # @param value [String, Hash] the message value (Hash will be JSON encoded)
    # @param topic [String] the target topic
    # @param key [String, nil] optional message key for partitioning
    # @param partition [Integer, nil] specific partition (auto-assigned if nil)
    # @param partition_key [String, nil] key used for partition calculation
    # @param headers [Hash, nil] optional message headers
    # @param create_time [Time, nil] optional message timestamp
    # @return [self] for method chaining
    #
    # @example Produce with value only
    #   producer.produce('Hello!', topic: 'greetings')
    #
    # @example Produce with key
    #   producer.produce('Hello!', topic: 'greetings', key: 'en')
    #
    # @example Produce with headers
    #   producer.produce('Hello!', topic: 'greetings',
    #     headers: { 'correlation-id' => 'abc-123' })
    #
    def produce(value, topic:, key: nil, partition: nil, partition_key: nil,
                headers: nil, create_time: nil)
      raise ClosedError, 'Producer is closed' if @closed

      # Encode value if it's a Hash
      encoded_value = value.is_a?(Hash) ? JSON.generate(value) : value.to_s
      encoded_key = key&.to_s

      message = {
        topic: topic,
        value: Base64.strict_encode64(encoded_value),
        partition: partition || -1
      }

      message[:key] = Base64.strict_encode64(encoded_key) if encoded_key
      message[:partition_key] = partition_key if partition_key
      message[:timestamp] = (create_time || Time.now).to_i * 1000

      if headers && !headers.empty?
        message[:headers] = headers.map do |k, v|
          { key: k.to_s, value: Base64.strict_encode64(v.to_s) }
        end
      end

      bytesize = encoded_value.bytesize + (encoded_key&.bytesize || 0)

      @mutex.synchronize do
        @buffer << message
        @buffer_size += 1
        @buffer_bytesize += bytesize

        # Auto-deliver if buffer is full
        if @buffer_size >= @max_buffer_size || @buffer_bytesize >= @max_buffer_bytesize
          deliver_messages_internal
        end
      end

      self
    end

    # Deliver all buffered messages
    #
    # Sends all buffered messages to Kafka and clears the buffer.
    #
    # @return [Array<RecordMetadata>] metadata for each delivered message
    # @raise [ProducerError] if delivery fails
    #
    # @example
    #   producer.produce('Hello!', topic: 'my-topic')
    #   producer.produce('World!', topic: 'my-topic')
    #   results = producer.deliver_messages
    #   results.each { |r| puts "Sent to partition #{r.partition}" }
    #
    def deliver_messages
      @mutex.synchronize { deliver_messages_internal }
    end

    # Clear the buffer without sending messages
    #
    # @return [void]
    def clear_buffer
      @mutex.synchronize do
        @buffer.clear
        @buffer_size = 0
        @buffer_bytesize = 0
      end
    end

    # Check if the buffer is empty
    #
    # @return [Boolean]
    def buffer_empty?
      @buffer_size.zero?
    end

    # Shutdown the producer, delivering any remaining messages
    #
    # @return [void]
    def shutdown
      deliver_messages unless buffer_empty?
      @closed = true
    end

    # Close the producer without delivering remaining messages
    #
    # @return [void]
    def close
      clear_buffer
      @closed = true
    end

    # Check if the producer is closed
    #
    # @return [Boolean]
    def closed?
      @closed
    end

    private

    def deliver_messages_internal
      return [] if @buffer.empty?

      messages = @buffer.dup
      @buffer.clear
      @buffer_size = 0
      @buffer_bytesize = 0

      # Group messages by topic for efficient batching
      by_topic = messages.group_by { |m| m[:topic] }

      results = []

      by_topic.each do |topic, topic_messages|
        batch_results = send_batch(topic, topic_messages)
        results.concat(batch_results)
      end

      results
    end

    def send_batch(topic, messages)
      client = kafka.rpc_client

      promise = client.call('kafka.producer.sendBatch', {
                              topic: topic,
                              messages: messages,
                              compression: @compression_codec.to_s,
                              acks: acks_value
                            })

      result = promise.await
      parse_batch_result(result, messages)
    rescue StandardError => e
      raise ProducerError.new("Failed to deliver messages: #{e.message}", cause: e)
    end

    def acks_value
      case @required_acks
      when :all, -1 then 'all'
      when 0 then '0'
      else '1'
      end
    end

    def parse_batch_result(result, messages)
      return [] unless result.is_a?(Array)

      result.map.with_index do |r, i|
        if r.is_a?(Hash) && r['error']
          raise ProducerError.new(r['error']['message'] || 'Send failed',
                                  code: r['error']['code'])
        end

        RecordMetadata.from_rpc(r || { 'topic' => messages[i][:topic] })
      end
    end
  end

  # Async producer for non-blocking message delivery
  #
  # @example
  #   producer = kafka.async_producer('orders')
  #   producer.produce('Hello!', topic: 'my-topic')
  #   producer.deliver
  #
  class AsyncProducer < Producer
    # Callback for delivery reports
    attr_accessor :on_delivery_callback

    def initialize(kafka, **options)
      super
      @pending_deliveries = []
      @on_delivery_callback = nil
    end

    # Set a callback for delivery reports
    #
    # @yield [RecordMetadata] called when a message is delivered
    def on_delivery(&block)
      @on_delivery_callback = block
    end

    # Trigger delivery of buffered messages (non-blocking)
    #
    # @return [void]
    def deliver
      Thread.new { deliver_messages }
    end
  end
end
