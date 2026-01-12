module Kafka
  # Consumer for reading messages from Kafka topics
  class Consumer
    include Enumerable(Record)

    getter topic : String
    getter group : String
    getter config : ConsumerConfig
    @client : Client
    @subscribed : Bool = false
    @subscription_id : String?

    def initialize(@client : Client, @topic : String, @group : String, @config : ConsumerConfig = ConsumerConfig.default)
    end

    # Subscribe to the topic
    def subscribe : Nil
      return if @subscribed

      offset_config = case config.offset
                      when Offset::Earliest
                        {"type" => "earliest"}
                      when Offset::Latest
                        {"type" => "latest"}
                      when TimestampOffset
                        {"type" => "timestamp", "timestamp" => config.offset.as(TimestampOffset).timestamp.to_unix_ms}
                      else
                        {"type" => "latest"}
                      end

      response = @client.rpc_call("consumer.subscribe", {
        "topic"             => @topic,
        "group"             => @group,
        "offset"            => offset_config,
        "autoCommit"        => @config.auto_commit,
        "sessionTimeoutMs"  => @config.session_timeout.total_milliseconds.to_i,
        "heartbeatIntervalMs" => @config.heartbeat_interval.total_milliseconds.to_i,
      })

      @subscription_id = response["subscriptionId"]?.try(&.as_s)
      @subscribed = true
    end

    # Unsubscribe from the topic
    def unsubscribe : Nil
      return unless @subscribed

      if sub_id = @subscription_id
        @client.rpc_call("consumer.unsubscribe", {
          "subscriptionId" => sub_id,
        })
      end

      @subscribed = false
      @subscription_id = nil
    end

    # Poll for records
    def poll(timeout : Time::Span = 1.second) : Array(Record)
      subscribe unless @subscribed

      response = @client.rpc_call("consumer.poll", {
        "topic"          => @topic,
        "group"          => @group,
        "subscriptionId" => @subscription_id,
        "maxRecords"     => @config.max_poll_records,
        "timeoutMs"      => timeout.total_milliseconds.to_i,
      })

      records = response["records"]?.try(&.as_a) || [] of JSON::Any
      records.map { |r| Record.from_json(r, @client) }
    end

    # Iterator implementation for each
    def each(&)
      subscribe unless @subscribed

      loop do
        records = poll
        records.each do |record|
          yield record
        end
      end
    end

    # Take a specific number of records
    def take(n : Int32) : Array(Record)
      subscribe unless @subscribed

      result = [] of Record
      while result.size < n
        records = poll
        records.each do |record|
          result << record
          break if result.size >= n
        end
      end
      result
    end

    # Commit offset for a specific partition
    def commit(partition : Int32, offset : Int64) : Nil
      @client.commit_offset(@topic, partition, offset)
    end

    # Seek to a specific offset
    def seek(partition : Int32, offset : Int64) : Nil
      @client.rpc_call("consumer.seek", {
        "topic"          => @topic,
        "group"          => @group,
        "partition"      => partition,
        "offset"         => offset,
        "subscriptionId" => @subscription_id,
      })
    end

    # Seek to the beginning of a partition
    def seek_to_beginning(partition : Int32) : Nil
      @client.rpc_call("consumer.seekToBeginning", {
        "topic"          => @topic,
        "group"          => @group,
        "partition"      => partition,
        "subscriptionId" => @subscription_id,
      })
    end

    # Seek to the end of a partition
    def seek_to_end(partition : Int32) : Nil
      @client.rpc_call("consumer.seekToEnd", {
        "topic"          => @topic,
        "group"          => @group,
        "partition"      => partition,
        "subscriptionId" => @subscription_id,
      })
    end

    # Get current position for a partition
    def position(partition : Int32) : Int64
      response = @client.rpc_call("consumer.position", {
        "topic"          => @topic,
        "group"          => @group,
        "partition"      => partition,
        "subscriptionId" => @subscription_id,
      })
      response["offset"]?.try(&.as_i64) || 0_i64
    end

    # Pause consumption
    def pause : Nil
      @client.rpc_call("consumer.pause", {
        "subscriptionId" => @subscription_id,
      })
    end

    # Resume consumption
    def resume : Nil
      @client.rpc_call("consumer.resume", {
        "subscriptionId" => @subscription_id,
      })
    end

    # Get assigned partitions
    def partitions : Array(Int32)
      response = @client.rpc_call("consumer.partitions", {
        "topic" => @topic,
        "group" => @group,
        "subscriptionId" => @subscription_id,
      })
      response["partitions"]?.try(&.as_a.map(&.as_i)) || [] of Int32
    end

    # Close the consumer
    def close : Nil
      unsubscribe
    end
  end

  # Batch consumer for processing records in batches
  class BatchConsumer
    include Enumerable(Batch)

    getter topic : String
    getter group : String
    getter config : BatchConfig
    @client : Client
    @consumer : Consumer

    def initialize(@client : Client, @topic : String, @group : String, @config : BatchConfig)
      consumer_config = ConsumerConfig.new(
        auto_commit: false, # We commit at batch level
        max_poll_records: @config.batch_size
      )
      @consumer = Consumer.new(@client, @topic, @group, consumer_config)
    end

    # Iterator implementation for each
    def each(&)
      loop do
        records = @consumer.poll(@config.batch_timeout)
        next if records.empty?

        batch = Batch.new(records, @client)
        yield batch

        if @config.auto_commit
          batch.commit
        end
      end
    end

    # Take a specific number of batches
    def take(n : Int32) : Array(Batch)
      result = [] of Batch
      count = 0
      each do |batch|
        result << batch
        count += 1
        break if count >= n
      end
      result
    end

    # Close the batch consumer
    def close : Nil
      @consumer.close
    end
  end

  # Async consumer using fibers
  class AsyncConsumer
    getter topic : String
    getter group : String
    @client : Client
    @consumer : Consumer
    @channel : Channel(Record)
    @running : Bool = false

    def initialize(@client : Client, @topic : String, @group : String, config : ConsumerConfig = ConsumerConfig.default)
      @consumer = Consumer.new(@client, @topic, @group, config)
      @channel = Channel(Record).new(100)
    end

    # Start consuming in background fiber
    def start : Nil
      return if @running
      @running = true

      spawn do
        while @running
          records = @consumer.poll
          records.each do |record|
            @channel.send(record)
          end
        end
      end
    end

    # Stop consuming
    def stop : Nil
      @running = false
      @channel.close
      @consumer.close
    end

    # Get next record asynchronously
    def receive : Record
      @channel.receive
    end

    # Get next record with timeout
    def receive?(timeout : Time::Span) : Record?
      select
      when record = @channel.receive
        record
      when timeout(timeout)
        nil
      end
    end

    # Iterate with block
    def each(&)
      start unless @running
      while record = @channel.receive?
        yield record
      end
    end
  end
end
