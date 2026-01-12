# frozen_string_literal: true

require 'securerandom'

module KafkaDo
  # Testing utilities for the Kafka.do SDK
  #
  # Provides mock implementations for testing Kafka-based applications
  # without connecting to a real broker.
  #
  # @example RSpec integration
  #   require 'kafka_do/testing'
  #
  #   RSpec.configure do |config|
  #     config.before(:each) do
  #       KafkaDo::Testing.reset!
  #     end
  #   end
  #
  # @example Using the test client
  #   kafka = KafkaDo::Testing.client
  #   producer = kafka.producer
  #   producer.produce('Hello!', topic: 'my-topic')
  #   producer.deliver_messages
  #
  #   # Verify messages were produced
  #   messages = KafkaDo::Testing.messages('my-topic')
  #   expect(messages.first.value).to eq('Hello!')
  #
  module Testing
    class << self
      # Get a pre-configured test Kafka client
      #
      # @return [Kafka] a Kafka client configured with the mock RPC
      def client(brokers: ['test-broker:9092'], **options)
        kafka = Kafka.new(brokers, **options)
        kafka.rpc_client = mock_rpc
        kafka
      end

      # Get the mock RPC client
      #
      # @return [MockRpcClient] the mock RPC client
      def mock_rpc
        @mock_rpc ||= MockRpcClient.new
      end

      # Reset all test state
      #
      # @return [void]
      def reset!
        @mock_rpc = nil
      end

      # Get all messages produced to a topic
      #
      # @param topic [String] the topic name
      # @return [Array<Message>] the messages
      def messages(topic)
        mock_rpc.messages(topic)
      end

      # Get all topics that have been used
      #
      # @return [Array<String>] topic names
      def topics
        mock_rpc.topics
      end

      # Produce test messages to a topic (for consumer testing)
      #
      # @param topic [String] the topic name
      # @param messages [Array<Hash, String>] messages to add
      # @return [void]
      def produce_messages(topic, messages)
        mock_rpc.add_test_messages(topic, messages)
      end

      # Create a test topic
      #
      # @param name [String] the topic name
      # @param partitions [Integer] number of partitions
      # @return [void]
      def create_topic(name, partitions: 1)
        mock_rpc.create_topic(name, partitions)
      end

      # Clear all messages from a topic
      #
      # @param topic [String] the topic name
      # @return [void]
      def clear_topic(topic)
        mock_rpc.clear_topic(topic)
      end

      # Get RPC call history
      #
      # @param method [String, nil] optional method name filter
      # @return [Array<Hash>] call history
      def calls(method = nil)
        mock_rpc.calls_for(method)
      end

      # Check if a specific RPC method was called
      #
      # @param method [String] the method name
      # @return [Boolean]
      def called?(method)
        !calls(method).empty?
      end

      # Get the last call for a method
      #
      # @param method [String] the method name
      # @return [Hash, nil] the call details
      def last_call(method)
        mock_rpc.last_call_for(method)
      end
    end

    # Mock RPC client for testing
    class MockRpcClient
      attr_reader :calls, :topic_messages, :topic_configs

      def initialize
        @calls = []
        @responses = {}
        @topic_messages = Hash.new { |h, k| h[k] = [] }
        @topic_configs = {}
        @consumer_positions = Hash.new { |h, k| h[k] = 0 }
        @closed = false
        @mutex = Mutex.new

        setup_default_responses
      end

      # Set up a mock response for a specific method
      #
      # @param method [String] the RPC method name
      # @param result [Object] the result to return
      # @param error [Exception] optional error to raise
      def mock_response(method, result = nil, error: nil)
        @responses[method] = { result: result, error: error }
      end

      # Call an RPC method
      #
      # @param method [String] the method name
      # @param args [Array] the arguments
      # @return [MockRpcPromise] the promise
      def call(method, *args)
        @calls << { method: method, args: args, timestamp: Time.now }

        # Handle built-in methods
        case method
        when 'kafka.producer.sendBatch'
          handle_send_batch(args.first)
        when 'kafka.consumer.subscribe'
          handle_subscribe(args.first)
        when 'kafka.consumer.poll'
          handle_poll(args.first)
        when 'kafka.consumer.commit'
          handle_commit(args.first)
        when 'kafka.admin.listTopics'
          handle_list_topics
        when 'kafka.admin.createTopics'
          handle_create_topics(args.first)
        when 'kafka.admin.deleteTopics'
          handle_delete_topics(args.first)
        when 'kafka.admin.describeTopics'
          handle_describe_topics(args.first)
        else
          # Use custom response or default
          response = @responses[method] || { result: nil }
          MockRpcPromise.new(response[:result], error: response[:error])
        end
      end

      # Close the client
      def close
        @closed = true
      end

      # Check if the client is closed
      def closed?
        @closed
      end

      # Find calls matching a method name
      def calls_for(method)
        return @calls if method.nil?

        @calls.select { |c| c[:method] == method }
      end

      # Get the last call for a method
      def last_call_for(method)
        calls_for(method).last
      end

      # Get all topics
      def topics
        @topic_messages.keys
      end

      # Get messages for a topic
      def messages(topic)
        @topic_messages[topic].dup
      end

      # Add test messages for consumer testing
      def add_test_messages(topic, messages)
        @mutex.synchronize do
          messages.each do |msg|
            @topic_messages[topic] << create_message(topic, msg)
          end
        end
      end

      # Create a topic
      def create_topic(name, partitions = 1)
        @topic_configs[name] = { partitions: partitions }
      end

      # Clear a topic's messages
      def clear_topic(topic)
        @mutex.synchronize do
          @topic_messages[topic].clear
        end
      end

      private

      def setup_default_responses
        # Set up sensible defaults for common operations
        @responses['kafka.consumer.subscribe'] = { result: { 'success' => true } }
        @responses['kafka.consumer.unsubscribe'] = { result: { 'success' => true } }
        @responses['kafka.consumer.assign'] = { result: { 'success' => true } }
        @responses['kafka.consumer.seek'] = { result: { 'success' => true } }
        @responses['kafka.consumer.seekToBeginning'] = { result: { 'success' => true } }
        @responses['kafka.consumer.seekToEnd'] = { result: { 'success' => true } }
        @responses['kafka.consumer.pause'] = { result: { 'success' => true } }
        @responses['kafka.consumer.resume'] = { result: { 'success' => true } }
        @responses['kafka.consumer.position'] = { result: 0 }
        @responses['kafka.consumer.committed'] = { result: [] }
      end

      def handle_send_batch(args)
        return MockRpcPromise.new([]) unless args

        topic = args[:topic]
        messages = args[:messages] || []

        results = messages.map.with_index do |msg, i|
          offset = @topic_messages[topic].size + i
          partition = msg[:partition] && msg[:partition] >= 0 ? msg[:partition] : 0

          # Store the message
          @topic_messages[topic] << Message.new(
            topic: topic,
            partition: partition,
            offset: offset,
            key: decode_base64(msg[:key]),
            value: decode_base64(msg[:value]),
            timestamp: Time.now,
            headers: parse_headers(msg[:headers])
          )

          { 'topic' => topic, 'partition' => partition, 'offset' => offset }
        end

        MockRpcPromise.new(results)
      end

      def handle_subscribe(args)
        topics = args[:topics] || []
        topics.each { |t| @topic_messages[t] ||= [] }
        MockRpcPromise.new({ 'success' => true })
      end

      def handle_poll(args)
        timeout_ms = args[:timeoutMs] || 1000
        max_records = args[:maxRecords] || 500

        messages = []
        @topic_messages.each do |topic, topic_msgs|
          position = @consumer_positions[topic]
          available = topic_msgs[position, max_records - messages.size] || []

          available.each do |msg|
            messages << serialize_message(msg)
            @consumer_positions[topic] += 1
          end

          break if messages.size >= max_records
        end

        MockRpcPromise.new(messages)
      end

      def handle_commit(_args)
        MockRpcPromise.new({ 'success' => true })
      end

      def handle_list_topics
        MockRpcPromise.new(@topic_messages.keys + @topic_configs.keys)
      end

      def handle_create_topics(args)
        topics = args[:topics] || []
        results = topics.map do |t|
          name = t[:name]
          if @topic_configs.key?(name)
            { 'name' => name, 'error' => { 'code' => 'TOPIC_ALREADY_EXISTS' } }
          else
            @topic_configs[name] = { partitions: t[:numPartitions] || 1 }
            { 'name' => name }
          end
        end
        MockRpcPromise.new(results)
      end

      def handle_delete_topics(args)
        topics = args[:topics] || []
        results = topics.map do |name|
          if @topic_configs.key?(name) || @topic_messages.key?(name)
            @topic_configs.delete(name)
            @topic_messages.delete(name)
            { 'name' => name }
          else
            { 'name' => name, 'error' => { 'code' => 'UNKNOWN_TOPIC_OR_PARTITION' } }
          end
        end
        MockRpcPromise.new(results)
      end

      def handle_describe_topics(args)
        topics = args[:topics] || []
        results = topics.map do |name|
          config = @topic_configs[name]
          if config
            {
              'name' => name,
              'partitions' => config[:partitions],
              'replicationFactor' => 1,
              'config' => {}
            }
          else
            { 'name' => name, 'error' => { 'code' => 'UNKNOWN_TOPIC_OR_PARTITION' } }
          end
        end
        MockRpcPromise.new(results)
      end

      def create_message(topic, msg)
        partition = 0
        offset = @topic_messages[topic].size

        case msg
        when Hash
          Message.new(
            topic: topic,
            partition: msg[:partition] || partition,
            offset: offset,
            key: msg[:key],
            value: msg[:value],
            timestamp: msg[:timestamp] || Time.now,
            headers: msg[:headers] || {}
          )
        else
          Message.new(
            topic: topic,
            partition: partition,
            offset: offset,
            value: msg.to_s,
            timestamp: Time.now
          )
        end
      end

      def serialize_message(msg)
        {
          'topic' => msg.topic,
          'partition' => msg.partition,
          'offset' => msg.offset,
          'key' => msg.key ? encode_base64(msg.key) : nil,
          'value' => encode_base64(msg.value),
          'timestamp' => (msg.timestamp.to_f * 1000).to_i,
          'headers' => msg.headers.map { |k, v| { 'key' => k, 'value' => encode_base64(v) } }
        }
      end

      def decode_base64(value)
        return nil if value.nil?

        require 'base64'
        Base64.decode64(value)
      rescue StandardError
        value
      end

      def encode_base64(value)
        return nil if value.nil?

        require 'base64'
        Base64.strict_encode64(value.to_s)
      end

      def parse_headers(headers)
        return {} unless headers.is_a?(Array)

        result = {}
        headers.each do |h|
          result[h[:key]] = decode_base64(h[:value]) if h[:key]
        end
        result
      end
    end

    # Mock RPC Promise for testing
    class MockRpcPromise
      def initialize(result = nil, error: nil)
        @result = result
        @error = error
      end

      def await
        raise @error if @error

        @result
      end
    end
  end
end
