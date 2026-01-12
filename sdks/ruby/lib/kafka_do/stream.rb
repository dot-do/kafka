# frozen_string_literal: true

module KafkaDo
  # Stream processing DSL for Kafka Streams-like operations
  #
  # Provides a declarative API for building stream processing pipelines
  # with transformations like map, filter, and groupBy.
  #
  # @example Basic stream processing
  #   kafka = Kafka.new(['broker1:9092'])
  #
  #   stream = kafka.stream('orders')
  #     .filter { |msg| msg.value['status'] == 'completed' }
  #     .map { |msg| { total: msg.value['amount'], customer: msg.value['customer_id'] } }
  #     .to('completed-orders')
  #
  # @example Aggregation
  #   stream = kafka.stream('events')
  #     .group_by { |msg| msg.key }
  #     .count
  #     .to('event-counts')
  #
  class Stream
    include Enumerable

    # @return [Kafka] the parent Kafka client
    attr_reader :kafka

    # @return [String] the source topic
    attr_reader :source_topic

    # @return [Array<Proc>] the pipeline transformations
    attr_reader :transformations

    # Create a new Stream
    #
    # @param kafka [Kafka] the parent Kafka client
    # @param source_topic [String] the source topic name
    # @param group_id [String] consumer group ID for the stream
    def initialize(kafka, source_topic, group_id: nil)
      @kafka = kafka
      @source_topic = source_topic
      @group_id = group_id || "stream-#{source_topic}-#{SecureRandom.hex(4)}"
      @transformations = []
      @closed = false
    end

    # Apply a mapping transformation
    #
    # @yield [Message] the message to transform
    # @return [Stream] a new stream with the transformation applied
    #
    # @example Transform message values
    #   stream.map { |msg| msg.value.upcase }
    #
    def map(&block)
      raise ArgumentError, 'Block required for map' unless block_given?

      new_stream = dup_with_transformation
      new_stream.add_transformation(:map, block)
      new_stream
    end

    # Apply a flat-map transformation (one-to-many)
    #
    # @yield [Message] the message to transform
    # @yieldreturn [Array] array of output values
    # @return [Stream] a new stream with the transformation applied
    #
    # @example Split a message into multiple outputs
    #   stream.flat_map { |msg| msg.value['items'] }
    #
    def flat_map(&block)
      raise ArgumentError, 'Block required for flat_map' unless block_given?

      new_stream = dup_with_transformation
      new_stream.add_transformation(:flat_map, block)
      new_stream
    end

    # Apply a filter transformation
    #
    # @yield [Message] the message to filter
    # @yieldreturn [Boolean] true to keep the message
    # @return [Stream] a new stream with the filter applied
    #
    # @example Keep only high-value orders
    #   stream.filter { |msg| msg.value['amount'] > 100 }
    #
    def filter(&block)
      raise ArgumentError, 'Block required for filter' unless block_given?

      new_stream = dup_with_transformation
      new_stream.add_transformation(:filter, block)
      new_stream
    end
    alias select filter

    # Apply a filter-not transformation
    #
    # @yield [Message] the message to filter
    # @yieldreturn [Boolean] true to reject the message
    # @return [Stream] a new stream with the filter applied
    def filter_not(&block)
      filter { |msg| !block.call(msg) }
    end
    alias reject filter_not

    # Apply a peek transformation (for side effects without modifying the stream)
    #
    # @yield [Message] the message to peek at
    # @return [Stream] the same stream (unmodified)
    #
    # @example Log each message
    #   stream.peek { |msg| logger.info("Processing: #{msg.key}") }
    #
    def peek(&block)
      raise ArgumentError, 'Block required for peek' unless block_given?

      new_stream = dup_with_transformation
      new_stream.add_transformation(:peek, block)
      new_stream
    end
    alias tap peek

    # Transform message keys
    #
    # @yield [Message] the message
    # @yieldreturn the new key
    # @return [Stream] a new stream with transformed keys
    #
    # @example Extract customer ID as key
    #   stream.map_key { |msg| msg.value['customer_id'] }
    #
    def map_key(&block)
      raise ArgumentError, 'Block required for map_key' unless block_given?

      new_stream = dup_with_transformation
      new_stream.add_transformation(:map_key, block)
      new_stream
    end
    alias select_key map_key

    # Group messages by key for aggregation
    #
    # @yield [Message] optional custom key extractor
    # @return [GroupedStream] a grouped stream for aggregation
    #
    # @example Group by customer ID
    #   stream.group_by { |msg| msg.value['customer_id'] }
    #
    def group_by(&block)
      GroupedStream.new(self, &block)
    end

    # Group messages by key (default grouping)
    #
    # @return [GroupedStream] a grouped stream using message keys
    def group_by_key
      GroupedStream.new(self) { |msg| msg.key }
    end

    # Merge with another stream
    #
    # @param other_stream [Stream] the stream to merge with
    # @return [Stream] a new stream containing messages from both
    def merge(other_stream)
      MergedStream.new(self, other_stream)
    end

    # Write the stream to a destination topic
    #
    # @param topic [String] the destination topic
    # @param key [Proc] optional key transformer
    # @return [void]
    #
    # @example Write transformed messages
    #   stream.map { |msg| process(msg) }.to('processed-topic')
    #
    def to(topic, key: nil, &block)
      producer = kafka.producer

      each do |msg|
        output_key = key ? key.call(msg) : msg.key
        output_value = block ? block.call(msg) : msg

        producer.produce(
          output_value,
          topic: topic,
          key: output_key
        )
        producer.deliver_messages
      end
    end

    # Process the stream with a handler (similar to forEach)
    #
    # @yield [Message] each processed message
    # @return [void]
    #
    # @example Process each message
    #   stream.for_each { |msg| save_to_database(msg.value) }
    #
    def for_each(&block)
      each(&block)
    end
    alias process for_each

    # Branch the stream based on predicates
    #
    # @param predicates [Array<Proc>] predicates for each branch
    # @return [Array<Stream>] array of branched streams
    #
    # @example Branch orders by priority
    #   high, low = stream.branch(
    #     ->(m) { m.value['priority'] == 'high' },
    #     ->(m) { m.value['priority'] == 'low' }
    #   )
    #
    def branch(*predicates)
      predicates.map do |predicate|
        filter(&predicate)
      end
    end

    # Iterate over processed messages
    #
    # @yield [Object] each processed message/value
    # @return [Enumerator] if no block given
    def each(&block)
      return enum_for(:each) unless block_given?

      consumer = kafka.consumer(group_id: @group_id)
      consumer.subscribe(@source_topic)

      consumer.each_message do |msg|
        result = apply_transformations(msg)
        next if result.nil? # filtered out

        if result.is_a?(Array)
          result.each { |r| yield r }
        else
          yield result
        end
      end
    ensure
      consumer&.close
    end

    # Start processing the stream (non-blocking)
    #
    # @return [Thread] the processing thread
    def start(&block)
      Thread.new do
        block_given? ? for_each(&block) : for_each { |_| }
      end
    end

    # Stop the stream processing
    def stop
      @closed = true
    end

    # Check if stream processing is stopped
    def stopped?
      @closed
    end

    protected

    def add_transformation(type, block)
      @transformations << { type: type, block: block }
    end

    def dup_with_transformation
      new_stream = Stream.new(kafka, source_topic, group_id: @group_id)
      new_stream.instance_variable_set(:@transformations, @transformations.dup)
      new_stream
    end

    private

    def apply_transformations(msg)
      result = msg

      @transformations.each do |transformation|
        type = transformation[:type]
        block = transformation[:block]

        case type
        when :filter
          return nil unless block.call(result)
        when :map
          result = block.call(result)
        when :flat_map
          result = block.call(result)
          return result unless result.is_a?(Array)
        when :peek
          block.call(result)
        when :map_key
          if result.is_a?(Message)
            new_key = block.call(result)
            result = Message.new(
              topic: result.topic,
              partition: result.partition,
              offset: result.offset,
              key: new_key,
              value: result.value,
              timestamp: result.timestamp,
              headers: result.headers
            )
          end
        end
      end

      result
    end
  end

  # A grouped stream for aggregation operations
  class GroupedStream
    # @return [Stream] the source stream
    attr_reader :source_stream

    # Create a grouped stream
    #
    # @param source_stream [Stream] the source stream
    # @yield [Message] key extractor function
    def initialize(source_stream, &key_extractor)
      @source_stream = source_stream
      @key_extractor = key_extractor || ->(msg) { msg.key }
    end

    # Count messages per key
    #
    # @return [Table] a materialized table with counts
    def count
      aggregate({}) do |key, _value, aggregate|
        aggregate[key] = (aggregate[key] || 0) + 1
        aggregate
      end
    end

    # Reduce messages per key
    #
    # @yield [Object, Object] accumulator and new value
    # @return [Table] a materialized table with reduced values
    def reduce(initial = nil, &block)
      aggregate({}) do |key, value, aggregate|
        aggregate[key] = if aggregate.key?(key)
                           block.call(aggregate[key], value)
                         else
                           initial.nil? ? value : block.call(initial, value)
                         end
        aggregate
      end
    end

    # Custom aggregation
    #
    # @param initializer [Object] initial aggregate state
    # @yield [String, Object, Object] key, value, and aggregate state
    # @return [Table] a materialized table with aggregated values
    def aggregate(initializer = nil, &block)
      Table.new(@source_stream.kafka, initializer || {}, @key_extractor, block)
    end

    # Write aggregation results to a topic
    #
    # @param topic [String] destination topic
    # @return [Table] the materialized table
    def to(topic)
      table = count
      table.to_stream.to(topic)
      table
    end
  end

  # A merged stream from multiple sources
  class MergedStream < Stream
    def initialize(stream1, stream2)
      super(stream1.kafka, stream1.source_topic)
      @streams = [stream1, stream2]
    end

    def each(&block)
      return enum_for(:each) unless block_given?

      threads = @streams.map do |stream|
        Thread.new { stream.each(&block) }
      end

      threads.each(&:join)
    end
  end

  # A materialized table for stateful operations
  class Table
    include Enumerable

    # @return [Hash] the current state
    attr_reader :state

    def initialize(kafka, initial_state, key_extractor, aggregator)
      @kafka = kafka
      @state = initial_state.dup
      @key_extractor = key_extractor
      @aggregator = aggregator
      @mutex = Mutex.new
    end

    # Get a value by key
    #
    # @param key [Object] the key to look up
    # @return [Object] the value
    def [](key)
      @mutex.synchronize { @state[key] }
    end

    # Get all entries
    #
    # @return [Hash] the current state
    def all
      @mutex.synchronize { @state.dup }
    end

    # Iterate over all key-value pairs
    def each(&block)
      @mutex.synchronize { @state.each(&block) }
    end

    # Convert the table back to a stream
    #
    # @return [Stream] stream of key-value changes
    def to_stream
      # Return an enumerable that yields current state entries
      TableStream.new(@kafka, self)
    end
  end

  # A stream backed by a table
  class TableStream < Stream
    def initialize(kafka, table)
      super(kafka, nil)
      @table = table
    end

    def each(&block)
      return enum_for(:each) unless block_given?

      @table.each do |key, value|
        yield KeyValue.new(key, value)
      end
    end
  end

  # A simple key-value pair
  class KeyValue
    attr_reader :key, :value

    def initialize(key, value)
      @key = key
      @value = value
    end

    def to_a
      [key, value]
    end

    def to_h
      { key => value }
    end

    def ==(other)
      other.is_a?(KeyValue) && key == other.key && value == other.value
    end
    alias eql? ==

    def hash
      [key, value].hash
    end
  end
end
