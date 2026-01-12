# frozen_string_literal: true

module KafkaDo
  # Kafka message consumer
  #
  # Provides Enumerable-based message consumption following the ruby-kafka API.
  # Supports both each_message iteration and batch consumption.
  #
  # @example Basic usage with each_message
  #   consumer = kafka.consumer(group_id: 'my-group')
  #   consumer.subscribe('my-topic')
  #   consumer.each_message do |message|
  #     puts "Received: #{message.value}"
  #   end
  #
  # @example Using Enumerable methods
  #   consumer.subscribe('orders')
  #   high_value = consumer.take(100).select { |m| m.value['amount'] > 100 }
  #
  class Consumer
    include Enumerable

    # @return [Kafka] the parent Kafka client
    attr_reader :kafka

    # @return [String] the consumer group ID
    attr_reader :group_id

    # @return [Array<String>] subscribed topics
    attr_reader :subscriptions

    # Create a new Consumer
    #
    # @param kafka [Kafka] the parent Kafka client
    # @param group_id [String] the consumer group ID
    # @param offset_commit_interval [Integer] offset commit interval in seconds
    # @param offset_commit_threshold [Integer] offset commit threshold
    # @param heartbeat_interval [Integer] heartbeat interval in seconds
    # @param session_timeout [Integer] session timeout in seconds
    # @param auto_offset_reset [Symbol] offset reset policy (:earliest, :latest)
    # @param enable_auto_commit [Boolean] whether to auto-commit offsets
    def initialize(kafka, group_id:, offset_commit_interval: 10,
                   offset_commit_threshold: 0, heartbeat_interval: 10,
                   session_timeout: 30, auto_offset_reset: :latest,
                   enable_auto_commit: true, **_options)
      @kafka = kafka
      @group_id = group_id
      @offset_commit_interval = offset_commit_interval
      @offset_commit_threshold = offset_commit_threshold
      @heartbeat_interval = heartbeat_interval
      @session_timeout = session_timeout
      @auto_offset_reset = auto_offset_reset
      @enable_auto_commit = enable_auto_commit
      @subscriptions = []
      @assignments = []
      @paused = Set.new
      @closed = false
      @connected = false
      @mutex = Mutex.new
    end

    # Subscribe to one or more topics
    #
    # @param topics [String, Array<String>] topic(s) to subscribe to
    # @param start_from_beginning [Boolean] whether to start from earliest offset
    # @return [self]
    #
    # @example Subscribe to a single topic
    #   consumer.subscribe('my-topic')
    #
    # @example Subscribe to multiple topics
    #   consumer.subscribe('orders', 'payments', 'refunds')
    #
    def subscribe(*topics, start_from_beginning: false)
      raise ClosedError, 'Consumer is closed' if @closed

      flat_topics = topics.flatten.map(&:to_s)

      @mutex.synchronize do
        @subscriptions.concat(flat_topics).uniq!
      end

      # Send subscription to server
      client = kafka.rpc_client
      promise = client.call('kafka.consumer.subscribe', {
                              topics: @subscriptions,
                              groupId: @group_id,
                              autoOffsetReset: start_from_beginning ? 'earliest' : @auto_offset_reset.to_s
                            })
      promise.await

      @connected = true
      self
    end

    # Unsubscribe from all topics
    #
    # @return [self]
    def unsubscribe
      return self if @closed

      @mutex.synchronize { @subscriptions.clear }

      if @connected
        client = kafka.rpc_client
        promise = client.call('kafka.consumer.unsubscribe')
        promise.await rescue nil
      end

      self
    end

    # Manually assign specific partitions
    #
    # @param topic_partitions [Array<TopicPartition>] partitions to assign
    # @return [self]
    def assign(*topic_partitions)
      raise ClosedError, 'Consumer is closed' if @closed

      partitions = topic_partitions.flatten.map do |tp|
        tp.is_a?(TopicPartition) ? tp.to_h : tp
      end

      client = kafka.rpc_client
      promise = client.call('kafka.consumer.assign', {
                              partitions: partitions
                            })
      promise.await

      @mutex.synchronize do
        @assignments = topic_partitions.flatten
      end

      @connected = true
      self
    end

    # Get current partition assignments
    #
    # @return [Array<TopicPartition>]
    def assignment
      @assignments.dup
    end

    # Iterate over messages
    #
    # This is the primary method for consuming messages. It yields each message
    # as it arrives and handles offset commits automatically.
    #
    # @yield [Message] each message received
    # @return [Enumerator] if no block given
    #
    # @example Basic iteration
    #   consumer.each_message do |message|
    #     puts message.value
    #   end
    #
    # @example With Enumerator methods
    #   consumer.each_message.take(10).each { |m| process(m) }
    #
    def each_message(min_bytes: 1, max_bytes: 1_048_576, max_wait_time: 5, &block)
      return enum_for(:each_message, min_bytes: min_bytes, max_bytes: max_bytes, max_wait_time: max_wait_time) unless block_given?

      raise ClosedError, 'Consumer is closed' if @closed
      raise ConsumerError, 'No subscriptions' if @subscriptions.empty? && @assignments.empty?

      loop do
        break if @closed

        messages = poll(timeout_ms: max_wait_time * 1000, max_records: 100)
        messages.each do |message|
          break if @closed

          yield message

          # Auto-commit if enabled
          commit_offsets(message) if @enable_auto_commit
        end
      end
    end
    alias each each_message

    # Poll for messages
    #
    # @param timeout_ms [Integer] timeout in milliseconds
    # @param max_records [Integer] maximum number of records to return
    # @return [Array<Message>] received messages
    def poll(timeout_ms: 1000, max_records: 500)
      raise ClosedError, 'Consumer is closed' if @closed

      client = kafka.rpc_client
      promise = client.call('kafka.consumer.poll', {
                              timeoutMs: timeout_ms,
                              maxRecords: max_records
                            })

      result = promise.await
      parse_messages(result)
    rescue StandardError => e
      raise ConsumerError.new("Poll failed: #{e.message}", cause: e)
    end

    # Commit offsets for a specific message
    #
    # @param message [Message] the message to commit
    # @return [void]
    def commit_offsets(message = nil)
      return if @closed

      client = kafka.rpc_client

      args = if message
               {
                 offsets: [{
                   topic: message.topic,
                   partition: message.partition,
                   offset: message.offset + 1
                 }]
               }
             else
               {}
             end

      promise = client.call('kafka.consumer.commit', args)
      promise.await
    rescue StandardError => e
      raise ConsumerError.new("Commit failed: #{e.message}", cause: e)
    end

    # Seek to a specific offset
    #
    # @param topic_partition [TopicPartition] the partition to seek
    # @param offset [Integer] the offset to seek to
    # @return [void]
    def seek(topic_partition, offset)
      raise ClosedError, 'Consumer is closed' if @closed

      client = kafka.rpc_client
      promise = client.call('kafka.consumer.seek', {
                              topic: topic_partition.topic,
                              partition: topic_partition.partition,
                              offset: offset
                            })
      promise.await
    end

    # Seek to the beginning of partitions
    #
    # @param topic_partitions [Array<TopicPartition>] partitions to seek
    # @return [void]
    def seek_to_beginning(*topic_partitions)
      raise ClosedError, 'Consumer is closed' if @closed

      partitions = topic_partitions.flatten.map(&:to_h)

      client = kafka.rpc_client
      promise = client.call('kafka.consumer.seekToBeginning', {
                              partitions: partitions
                            })
      promise.await
    end

    # Seek to the end of partitions
    #
    # @param topic_partitions [Array<TopicPartition>] partitions to seek
    # @return [void]
    def seek_to_end(*topic_partitions)
      raise ClosedError, 'Consumer is closed' if @closed

      partitions = topic_partitions.flatten.map(&:to_h)

      client = kafka.rpc_client
      promise = client.call('kafka.consumer.seekToEnd', {
                              partitions: partitions
                            })
      promise.await
    end

    # Get the current position (offset) for a partition
    #
    # @param topic_partition [TopicPartition] the partition
    # @return [Integer] the current offset
    def position(topic_partition)
      raise ClosedError, 'Consumer is closed' if @closed

      client = kafka.rpc_client
      promise = client.call('kafka.consumer.position', {
                              topic: topic_partition.topic,
                              partition: topic_partition.partition
                            })

      result = promise.await
      result.to_i
    end

    # Get committed offsets for partitions
    #
    # @param topic_partitions [Array<TopicPartition>] partitions to query
    # @return [Hash<TopicPartition, Integer>] committed offsets
    def committed(*topic_partitions)
      raise ClosedError, 'Consumer is closed' if @closed

      partitions = topic_partitions.flatten.map(&:to_h)

      client = kafka.rpc_client
      promise = client.call('kafka.consumer.committed', {
                              partitions: partitions
                            })

      result = promise.await
      parse_committed_offsets(result)
    end

    # Pause consumption from partitions
    #
    # @param topic_partitions [Array<TopicPartition>] partitions to pause
    # @return [void]
    def pause(*topic_partitions)
      raise ClosedError, 'Consumer is closed' if @closed

      partitions = topic_partitions.flatten
      partitions.each { |tp| @paused.add(tp) }

      client = kafka.rpc_client
      promise = client.call('kafka.consumer.pause', {
                              partitions: partitions.map(&:to_h)
                            })
      promise.await
    end

    # Resume consumption from paused partitions
    #
    # @param topic_partitions [Array<TopicPartition>] partitions to resume
    # @return [void]
    def resume(*topic_partitions)
      raise ClosedError, 'Consumer is closed' if @closed

      partitions = topic_partitions.flatten
      partitions.each { |tp| @paused.delete(tp) }

      client = kafka.rpc_client
      promise = client.call('kafka.consumer.resume', {
                              partitions: partitions.map(&:to_h)
                            })
      promise.await
    end

    # Get paused partitions
    #
    # @return [Array<TopicPartition>]
    def paused
      @paused.to_a
    end

    # Stop the consumer
    #
    # @return [void]
    def stop
      @closed = true
    end

    # Close the consumer
    #
    # @return [void]
    def close
      return if @closed

      # Commit final offsets if auto-commit enabled
      commit_offsets if @enable_auto_commit && @connected

      @closed = true
      @connected = false
      @subscriptions.clear
      @assignments.clear
      @paused.clear
    end

    # Check if the consumer is closed
    #
    # @return [Boolean]
    def closed?
      @closed
    end

    private

    def parse_messages(result)
      return [] unless result.is_a?(Array)

      result.map { |data| Message.from_rpc(data) }
    end

    def parse_committed_offsets(result)
      return {} unless result.is_a?(Array)

      offsets = {}
      result.each do |data|
        next unless data.is_a?(Hash)

        tp = TopicPartition.new(data['topic'], data['partition'].to_i)
        offsets[tp] = data['offset'].to_i if data['offset']
      end
      offsets
    end
  end

  # Batch consumer for processing messages in groups
  class BatchConsumer < Consumer
    # Iterate over message batches
    #
    # @param batch_size [Integer] maximum batch size
    # @param batch_timeout [Float] timeout in seconds to wait for a full batch
    # @yield [Array<Message>] each batch of messages
    # @return [Enumerator] if no block given
    def each_batch(batch_size: 100, batch_timeout: 5.0, &block)
      return enum_for(:each_batch, batch_size: batch_size, batch_timeout: batch_timeout) unless block_given?

      raise ClosedError, 'Consumer is closed' if @closed

      batch = []
      batch_start = Time.now

      each_message do |message|
        batch << message

        if batch.size >= batch_size || (Time.now - batch_start) >= batch_timeout
          yield batch
          batch = []
          batch_start = Time.now
        end
      end

      # Yield remaining messages
      yield batch unless batch.empty?
    end
  end
end
