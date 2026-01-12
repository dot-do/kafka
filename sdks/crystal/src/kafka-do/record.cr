module Kafka
  # Represents a Kafka record/message
  class Record
    getter topic : String
    getter partition : Int32
    getter offset : Int64
    getter key : String?
    getter value : JSON::Any
    getter timestamp : Time
    getter headers : Hash(String, String)

    @client : Client?
    @committed : Bool = false

    def initialize(
      @topic : String,
      @partition : Int32,
      @offset : Int64,
      @value : JSON::Any,
      @key : String? = nil,
      @timestamp : Time = Time.utc,
      @headers : Hash(String, String) = {} of String => String,
      @client : Client? = nil
    )
    end

    # Commit this record's offset
    def commit : Nil
      return if @committed

      if client = @client
        client.commit_offset(@topic, @partition, @offset)
      end
      @committed = true
    end

    # Check if committed
    def committed? : Bool
      @committed
    end

    # Parse the value as a specific type
    def value_as(type : T.class) : T forall T
      T.from_json(@value.to_json)
    end

    # Create from JSON response
    def self.from_json(json : JSON::Any, client : Client? = nil) : Record
      headers = {} of String => String
      json["headers"]?.try(&.as_h.each { |k, v| headers[k] = v.as_s })

      Record.new(
        topic: json["topic"].as_s,
        partition: json["partition"].as_i,
        offset: json["offset"].as_i64,
        key: json["key"]?.try(&.as_s?),
        value: json["value"],
        timestamp: Time.parse_iso8601(json["timestamp"]?.try(&.as_s) || Time.utc.to_s),
        headers: headers,
        client: client
      )
    end
  end

  # Record metadata returned after producing
  struct RecordMetadata
    getter topic : String
    getter partition : Int32
    getter offset : Int64
    getter timestamp : Time

    def initialize(@topic : String, @partition : Int32, @offset : Int64, @timestamp : Time = Time.utc)
    end

    def self.from_json(json : JSON::Any) : RecordMetadata
      RecordMetadata.new(
        topic: json["topic"].as_s,
        partition: json["partition"].as_i,
        offset: json["offset"].as_i64,
        timestamp: Time.parse_iso8601(json["timestamp"]?.try(&.as_s) || Time.utc.to_s)
      )
    end
  end

  # Message for sending with explicit key and headers
  struct Message(T)
    getter key : String?
    getter value : T
    getter headers : Hash(String, String)

    def initialize(@value : T, @key : String? = nil, @headers : Hash(String, String) = {} of String => String)
    end
  end

  # Batch of records for batch consumption
  class Batch
    getter records : Array(Record)
    @client : Client?
    @committed : Bool = false

    def initialize(@records : Array(Record), @client : Client? = nil)
    end

    # Commit all records in the batch
    def commit : Nil
      return if @committed
      @records.each(&.commit)
      @committed = true
    end

    # Iterate over records
    def each(&)
      @records.each { |r| yield r }
    end

    # Get batch size
    def size : Int32
      @records.size
    end

    # Check if empty
    def empty? : Bool
      @records.empty?
    end

    # Get first record
    def first : Record?
      @records.first?
    end

    # Get last record
    def last : Record?
      @records.last?
    end
  end
end
