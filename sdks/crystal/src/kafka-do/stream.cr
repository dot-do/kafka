module Kafka
  # Stream processing abstraction for Kafka topics
  class Stream(T)
    @client : Client
    @topic : String
    @operations : Array(StreamOperation) = [] of StreamOperation
    @output_topic : String?

    def initialize(@client : Client, @topic : String)
    end

    # Create from existing operations
    private def initialize(@client : Client, @topic : String, @operations : Array(StreamOperation))
    end

    # Filter records based on a predicate
    #
    # Example:
    #   stream.filter { |o| o["amount"].as_f > 100 }
    def filter(&block : T -> Bool) : Stream(T)
      new_ops = @operations.dup
      new_ops << FilterOperation.new(block.as(Proc(JSON::Any, Bool)))
      Stream(T).new(@client, @topic, new_ops)
    end

    # Map records to a new type
    #
    # Example:
    #   stream.map { |o| o["customer_id"] }
    def map(&block : T -> U) : Stream(U) forall U
      new_ops = @operations.dup
      new_ops << MapOperation.new(block.as(Proc(JSON::Any, JSON::Any)))
      Stream(U).new(@client, @topic, new_ops)
    end

    # FlatMap records
    #
    # Example:
    #   stream.flat_map { |o| o["items"].as_a }
    def flat_map(&block : T -> Array(U)) : Stream(U) forall U
      new_ops = @operations.dup
      new_ops << FlatMapOperation.new(block.as(Proc(JSON::Any, Array(JSON::Any))))
      Stream(U).new(@client, @topic, new_ops)
    end

    # Group records by key
    #
    # Example:
    #   stream.group_by { |o| o["customer_id"].as_s }
    def group_by(&block : T -> K) : GroupedStream(K, T) forall K
      GroupedStream(K, T).new(@client, @topic, @operations, block.as(Proc(JSON::Any, String)))
    end

    # Apply a windowing function
    #
    # Example:
    #   stream.window(Kafka.tumbling_window(5.minutes))
    def window(window : Window) : WindowedStream(T)
      WindowedStream(T).new(@client, @topic, @operations, window)
    end

    # Branch records to different topics
    #
    # Example:
    #   stream.branch([
    #     {->(o : JSON::Any) { o["region"].as_s == "us" }, "us-orders"},
    #     {->(o : JSON::Any) { o["region"].as_s == "eu" }, "eu-orders"},
    #     {->(o : JSON::Any) { true }, "other-orders"},
    #   ])
    def branch(branches : Array({Proc(T, Bool), String})) : Nil
      @client.rpc_call("stream.branch", {
        "sourceTopic" => @topic,
        "operations"  => serialize_operations,
        "branches"    => branches.map { |pred, topic| {"topic" => topic} },
      })
    end

    # Send transformed records to an output topic
    #
    # Example:
    #   stream.filter { |o| o["amount"].as_f > 100 }.to("high-value-orders")
    def to(topic : String) : Nil
      @output_topic = topic
      @client.rpc_call("stream.process", {
        "sourceTopic" => @topic,
        "outputTopic" => topic,
        "operations"  => serialize_operations,
      })
    end

    # Process each record with a callback
    #
    # Example:
    #   stream.filter { |o| o["amount"].as_f > 100 }.for_each { |o| notify(o) }
    def for_each(&block : T ->) : Nil
      consumer = @client.consumer(@topic, group: "stream-processor-#{Random.new.hex(4)}")

      consumer.each do |record|
        value = record.value
        skip = false

        @operations.each do |op|
          case op
          when FilterOperation
            unless op.predicate.call(value)
              skip = true
              break
            end
          when MapOperation
            value = op.mapper.call(value)
          end
        end

        unless skip
          block.call(value.as(T))
        end

        record.commit
      end
    end

    # Serialize operations for server-side processing
    private def serialize_operations : Array(Hash(String, String))
      @operations.map do |op|
        case op
        when FilterOperation
          {"type" => "filter", "expression" => ""}
        when MapOperation
          {"type" => "map", "expression" => ""}
        when FlatMapOperation
          {"type" => "flatMap", "expression" => ""}
        else
          {"type" => "unknown"}
        end
      end
    end
  end

  # Grouped stream for aggregations
  class GroupedStream(K, T)
    @client : Client
    @topic : String
    @operations : Array(StreamOperation)
    @key_selector : Proc(JSON::Any, String)

    def initialize(@client : Client, @topic : String, @operations : Array(StreamOperation), @key_selector : Proc(JSON::Any, String))
    end

    # Count records per group
    #
    # Example:
    #   stream.group_by { |o| o["customer_id"].as_s }.count
    def count : Stream({K, Int64})
      @client.rpc_call("stream.aggregate", {
        "sourceTopic" => @topic,
        "operations"  => serialize_operations,
        "aggregation" => "count",
      })
      Stream({K, Int64}).new(@client, @topic)
    end

    # Sum values per group
    #
    # Example:
    #   stream.group_by { |o| o["customer_id"].as_s }
    #         .sum { |o| o["amount"].as_f }
    def sum(&value_selector : T -> Number) : Stream({K, Float64})
      @client.rpc_call("stream.aggregate", {
        "sourceTopic" => @topic,
        "operations"  => serialize_operations,
        "aggregation" => "sum",
      })
      Stream({K, Float64}).new(@client, @topic)
    end

    # Reduce records per group
    #
    # Example:
    #   stream.group_by { |o| o["customer_id"].as_s }
    #         .reduce(0.0) { |acc, o| acc + o["amount"].as_f }
    def reduce(initial : S, &block : (S, T) -> S) : Stream({K, S}) forall S
      @client.rpc_call("stream.aggregate", {
        "sourceTopic" => @topic,
        "operations"  => serialize_operations,
        "aggregation" => "reduce",
      })
      Stream({K, S}).new(@client, @topic)
    end

    # Apply aggregation and call handler for each result
    def for_each(&block : (K, _) ->) : Nil
      # Implementation would process aggregated results
    end

    private def serialize_operations : Array(Hash(String, String))
      @operations.map { |_| {"type" => "unknown"} }
    end
  end

  # Windowed stream for time-based processing
  class WindowedStream(T)
    @client : Client
    @topic : String
    @operations : Array(StreamOperation)
    @window : Window

    def initialize(@client : Client, @topic : String, @operations : Array(StreamOperation), @window : Window)
    end

    # Group windowed records by key
    def group_by(&block : T -> K) : WindowedGroupedStream(K, T) forall K
      WindowedGroupedStream(K, T).new(@client, @topic, @operations, @window, block.as(Proc(JSON::Any, String)))
    end

    # Count all records in the window
    def count : Stream(WindowResult(Int64))
      Stream(WindowResult(Int64)).new(@client, @topic)
    end

    # Process each window
    def for_each(&block : WindowResult(Array(T)) ->) : Nil
      # Implementation would process windowed results
    end
  end

  # Windowed and grouped stream
  class WindowedGroupedStream(K, T)
    @client : Client
    @topic : String
    @operations : Array(StreamOperation)
    @window : Window
    @key_selector : Proc(JSON::Any, String)

    def initialize(@client : Client, @topic : String, @operations : Array(StreamOperation), @window : Window, @key_selector : Proc(JSON::Any, String))
    end

    # Count records per group in window
    def count : Stream({K, WindowResult(Int64)})
      @client.rpc_call("stream.windowedAggregate", {
        "sourceTopic" => @topic,
        "window"      => @window.to_json,
        "aggregation" => "count",
      })
      Stream({K, WindowResult(Int64)}).new(@client, @topic)
    end

    # Process each windowed group result
    def for_each(&block : (K, WindowResult(_)) ->) : Nil
      # Implementation would process windowed aggregated results
    end
  end

  # Window result container
  struct WindowResult(T)
    getter value : T
    getter start_time : Time
    getter end_time : Time

    def initialize(@value : T, @start_time : Time, @end_time : Time)
    end
  end

  # Base class for windows
  abstract struct Window
    abstract def to_json : Hash(String, _)
  end

  # Tumbling window (non-overlapping fixed-size windows)
  struct TumblingWindow < Window
    getter duration : Time::Span

    def initialize(@duration : Time::Span)
    end

    def to_json : Hash(String, _)
      {
        "type"       => "tumbling",
        "durationMs" => @duration.total_milliseconds.to_i,
      }
    end
  end

  # Sliding window (overlapping windows)
  struct SlidingWindow < Window
    getter duration : Time::Span
    getter slide : Time::Span

    def initialize(@duration : Time::Span, @slide : Time::Span)
    end

    def to_json : Hash(String, _)
      {
        "type"       => "sliding",
        "durationMs" => @duration.total_milliseconds.to_i,
        "slideMs"    => @slide.total_milliseconds.to_i,
      }
    end
  end

  # Session window (gap-based windows)
  struct SessionWindow < Window
    getter gap : Time::Span

    def initialize(@gap : Time::Span)
    end

    def to_json : Hash(String, _)
      {
        "type"  => "session",
        "gapMs" => @gap.total_milliseconds.to_i,
      }
    end
  end

  # Hopping window (fixed-size overlapping windows)
  struct HoppingWindow < Window
    getter duration : Time::Span
    getter advance : Time::Span

    def initialize(@duration : Time::Span, @advance : Time::Span)
    end

    def to_json : Hash(String, _)
      {
        "type"       => "hopping",
        "durationMs" => @duration.total_milliseconds.to_i,
        "advanceMs"  => @advance.total_milliseconds.to_i,
      }
    end
  end

  # Helper methods for creating windows
  def self.tumbling_window(duration : Time::Span) : TumblingWindow
    TumblingWindow.new(duration)
  end

  def self.sliding_window(duration : Time::Span, slide : Time::Span) : SlidingWindow
    SlidingWindow.new(duration, slide)
  end

  def self.session_window(gap : Time::Span) : SessionWindow
    SessionWindow.new(gap)
  end

  def self.hopping_window(duration : Time::Span, advance : Time::Span) : HoppingWindow
    HoppingWindow.new(duration, advance)
  end

  # Stream operation types
  abstract class StreamOperation
  end

  class FilterOperation < StreamOperation
    getter predicate : Proc(JSON::Any, Bool)

    def initialize(@predicate : Proc(JSON::Any, Bool))
    end
  end

  class MapOperation < StreamOperation
    getter mapper : Proc(JSON::Any, JSON::Any)

    def initialize(@mapper : Proc(JSON::Any, JSON::Any))
    end
  end

  class FlatMapOperation < StreamOperation
    getter mapper : Proc(JSON::Any, Array(JSON::Any))

    def initialize(@mapper : Proc(JSON::Any, Array(JSON::Any)))
    end
  end
end
