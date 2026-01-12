# frozen_string_literal: true

module KafkaDo
  # Main Kafka client class
  #
  # Provides a ruby-kafka compatible API for interacting with Kafka brokers
  # through the kafka.do RPC service.
  #
  # @example Creating a Kafka client
  #   kafka = Kafka.new(['broker1:9092', 'broker2:9092'])
  #
  # @example With options
  #   kafka = Kafka.new(['broker1:9092'],
  #     client_id: 'my-app',
  #     connection_url: 'https://kafka.do'
  #   )
  #
  class Kafka
    # Compression types
    COMPRESSION_NONE = :none
    COMPRESSION_GZIP = :gzip
    COMPRESSION_SNAPPY = :snappy
    COMPRESSION_LZ4 = :lz4
    COMPRESSION_ZSTD = :zstd

    # Offset reset policies
    OFFSET_EARLIEST = :earliest
    OFFSET_LATEST = :latest

    # Acknowledgment levels
    ACKS_NONE = 0
    ACKS_LEADER = 1
    ACKS_ALL = -1

    # @return [Array<String>] the configured broker addresses
    attr_reader :brokers

    # @return [String, nil] the client ID
    attr_reader :client_id

    # @return [String] the connection URL for the kafka.do RPC service
    attr_reader :connection_url

    # Create a new Kafka client
    #
    # @param brokers [Array<String>] list of broker addresses (host:port)
    # @param client_id [String, nil] optional client identifier
    # @param connection_url [String] URL for the kafka.do service
    # @param timeout [Integer] connection timeout in seconds
    # @param api_key [String, nil] optional API key for authentication
    def initialize(brokers = [], client_id: nil, connection_url: 'https://kafka.do',
                   timeout: 30, api_key: nil)
      @brokers = Array(brokers)
      @client_id = client_id
      @connection_url = connection_url
      @timeout = timeout
      @api_key = api_key
      @rpc_client = nil
      @closed = false
      @mutex = Mutex.new
    end

    # Set the RPC client (primarily used for testing)
    #
    # @param client [Object] the RPC client instance
    def rpc_client=(client)
      @mutex.synchronize { @rpc_client = client }
    end

    # Get the RPC client, connecting if necessary
    #
    # @return [Object] the RPC client
    # @raise [ConnectionError] if connection fails
    def rpc_client
      @mutex.synchronize do
        raise ClosedError, 'Client is closed' if @closed

        @rpc_client ||= connect_rpc
      end
    end

    # Create a new producer
    #
    # @param compression_codec [Symbol] compression type (:none, :gzip, :snappy, :lz4, :zstd)
    # @param required_acks [Integer] number of acks required (0, 1, or -1 for all)
    # @param max_buffer_size [Integer] maximum buffer size in bytes
    # @param max_buffer_bytesize [Integer] maximum buffer byte size
    # @return [Producer] a new producer instance
    #
    # @example Creating a producer
    #   producer = kafka.producer
    #   producer.produce('Hello!', topic: 'my-topic')
    #   producer.deliver_messages
    #
    def producer(compression_codec: COMPRESSION_NONE,
                 required_acks: ACKS_LEADER,
                 max_buffer_size: 1000,
                 max_buffer_bytesize: 10_000_000,
                 **options)
      raise ClosedError, 'Client is closed' if @closed

      Producer.new(
        self,
        compression_codec: compression_codec,
        required_acks: required_acks,
        max_buffer_size: max_buffer_size,
        max_buffer_bytesize: max_buffer_bytesize,
        **options
      )
    end

    # Create a new consumer
    #
    # @param group_id [String] the consumer group ID
    # @param offset_commit_interval [Integer] offset commit interval in seconds
    # @param offset_commit_threshold [Integer] offset commit threshold
    # @param heartbeat_interval [Integer] heartbeat interval in seconds
    # @param session_timeout [Integer] session timeout in seconds
    # @return [Consumer] a new consumer instance
    #
    # @example Creating a consumer
    #   consumer = kafka.consumer(group_id: 'my-group')
    #   consumer.subscribe('my-topic')
    #   consumer.each_message { |msg| puts msg.value }
    #
    def consumer(group_id:,
                 offset_commit_interval: 10,
                 offset_commit_threshold: 0,
                 heartbeat_interval: 10,
                 session_timeout: 30,
                 **options)
      raise ClosedError, 'Client is closed' if @closed

      Consumer.new(
        self,
        group_id: group_id,
        offset_commit_interval: offset_commit_interval,
        offset_commit_threshold: offset_commit_threshold,
        heartbeat_interval: heartbeat_interval,
        session_timeout: session_timeout,
        **options
      )
    end

    # Create a new admin client
    #
    # @return [Admin] a new admin client instance
    def admin
      raise ClosedError, 'Client is closed' if @closed

      Admin.new(self)
    end

    # Create a new stream processor for a topic
    #
    # Provides a Kafka Streams-like DSL for building stream processing pipelines.
    #
    # @param topic [String] the source topic
    # @param group_id [String] optional consumer group ID
    # @return [Stream] a new stream processor
    #
    # @example Basic stream processing
    #   kafka.stream('orders')
    #     .filter { |msg| msg.value['status'] == 'completed' }
    #     .map { |msg| msg.value['total'] }
    #     .to('order-totals')
    #
    def stream(topic, group_id: nil)
      raise ClosedError, 'Client is closed' if @closed

      Stream.new(self, topic, group_id: group_id)
    end

    # Create a topic (convenience method delegating to Admin)
    #
    # @param topic [String] the topic name
    # @param num_partitions [Integer] number of partitions
    # @param replication_factor [Integer] replication factor
    # @param config [Hash] additional topic configuration
    # @return [void]
    #
    # @example Creating a topic
    #   kafka.create_topic('new-topic', num_partitions: 3, replication_factor: 2)
    #
    def create_topic(topic, num_partitions: 1, replication_factor: 1, config: {})
      admin.create_topic(topic,
                         num_partitions: num_partitions,
                         replication_factor: replication_factor,
                         config: config)
    end

    # Delete a topic (convenience method delegating to Admin)
    #
    # @param topic [String] the topic name
    # @return [void]
    def delete_topic(topic)
      admin.delete_topic(topic)
    end

    # List topics (convenience method delegating to Admin)
    #
    # @return [Array<String>] list of topic names
    def topics
      admin.list_topics
    end

    # Get partitions for a topic
    #
    # @param topic [String] the topic name
    # @return [Array<Integer>] list of partition numbers
    def partitions_for(topic)
      admin.partitions_for(topic)
    end

    # Close the Kafka client
    #
    # @return [void]
    def close
      @mutex.synchronize do
        return if @closed

        @rpc_client&.close rescue nil
        @rpc_client = nil
        @closed = true
      end
    end

    # Check if the client is closed
    #
    # @return [Boolean]
    def closed?
      @closed
    end

    private

    def connect_rpc
      # In production, this would connect via the RPC transport
      # For now, raise an error if no client is set
      raise ConnectionError, 'RPC client not configured. Set via kafka.rpc_client='
    end
  end
end
