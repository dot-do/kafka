module Kafka
  # Admin client for managing Kafka topics and consumer groups
  class Admin
    @client : Client

    def initialize(@client : Client)
    end

    # Create a new topic
    #
    # Example:
    #   admin.create_topic("orders", TopicConfig.new(partitions: 3))
    def create_topic(name : String, config : TopicConfig = TopicConfig.new) : Nil
      @client.rpc_call("admin.createTopic", {
        "name"              => name,
        "partitions"        => config.partitions,
        "replicationFactor" => config.replication_factor,
        "retentionMs"       => config.retention_ms,
        "cleanupPolicy"     => config.cleanup_policy,
        "compressionType"   => config.compression_type,
      })
    end

    # Delete a topic
    #
    # Example:
    #   admin.delete_topic("old-events")
    def delete_topic(name : String) : Nil
      @client.rpc_call("admin.deleteTopic", {
        "name" => name,
      })
    end

    # List all topics
    #
    # Example:
    #   for topic in admin.list_topics
    #     puts "#{topic.name}: #{topic.partitions} partitions"
    #   end
    def list_topics : Array(TopicInfo)
      response = @client.rpc_call("admin.listTopics")
      topics = response["topics"]?.try(&.as_a) || [] of JSON::Any
      topics.map { |t| TopicInfo.from_json(t) }
    end

    # Describe a specific topic
    #
    # Example:
    #   info = admin.describe_topic("orders")
    #   puts "Partitions: #{info.partitions}"
    def describe_topic(name : String) : TopicInfo
      response = @client.rpc_call("admin.describeTopic", {
        "name" => name,
      })
      TopicInfo.from_json(response)
    end

    # Alter topic configuration
    #
    # Example:
    #   admin.alter_topic("orders", TopicConfig.new(retention_ms: 30.days.total_milliseconds.to_i64))
    def alter_topic(name : String, config : TopicConfig) : Nil
      @client.rpc_call("admin.alterTopic", {
        "name"            => name,
        "retentionMs"     => config.retention_ms,
        "cleanupPolicy"   => config.cleanup_policy,
        "compressionType" => config.compression_type,
      })
    end

    # List consumer groups
    #
    # Example:
    #   for group in admin.list_groups
    #     puts "#{group.id}: #{group.member_count} members"
    #   end
    def list_groups : Array(GroupInfo)
      response = @client.rpc_call("admin.listGroups")
      groups = response["groups"]?.try(&.as_a) || [] of JSON::Any
      groups.map { |g| GroupInfo.from_json(g) }
    end

    # Describe a consumer group
    #
    # Example:
    #   info = admin.describe_group("order-processor")
    #   puts "State: #{info.state}"
    #   puts "Total lag: #{info.total_lag}"
    def describe_group(group_id : String) : GroupInfo
      response = @client.rpc_call("admin.describeGroup", {
        "groupId" => group_id,
      })
      GroupInfo.from_json(response)
    end

    # Delete a consumer group
    def delete_group(group_id : String) : Nil
      @client.rpc_call("admin.deleteGroup", {
        "groupId" => group_id,
      })
    end

    # Reset offsets for a consumer group
    #
    # Example:
    #   admin.reset_offsets("order-processor", "orders", Offset::Earliest)
    def reset_offsets(group_id : String, topic : String, offset : Offset | TimestampOffset) : Nil
      offset_config = case offset
                      when Offset::Earliest
                        {"type" => "earliest"}
                      when Offset::Latest
                        {"type" => "latest"}
                      when TimestampOffset
                        {"type" => "timestamp", "timestamp" => offset.as(TimestampOffset).timestamp.to_unix_ms}
                      else
                        {"type" => "earliest"}
                      end

      @client.rpc_call("admin.resetOffsets", {
        "groupId" => group_id,
        "topic"   => topic,
        "offset"  => offset_config,
      })
    end

    # Get consumer group offsets
    def get_offsets(group_id : String, topic : String) : Hash(Int32, Int64)
      response = @client.rpc_call("admin.getOffsets", {
        "groupId" => group_id,
        "topic"   => topic,
      })

      result = {} of Int32 => Int64
      response["offsets"]?.try(&.as_h.each { |k, v| result[k.to_i] = v.as_i64 })
      result
    end

    # Get topic partition info
    def get_partitions(topic : String) : Array(PartitionInfo)
      response = @client.rpc_call("admin.getPartitions", {
        "topic" => topic,
      })
      partitions = response["partitions"]?.try(&.as_a) || [] of JSON::Any
      partitions.map { |p| PartitionInfo.from_json(p) }
    end
  end

  # Topic information
  struct TopicInfo
    getter name : String
    getter partitions : Int32
    getter replication_factor : Int32
    getter retention_ms : Int64
    getter cleanup_policy : String
    getter internal : Bool

    def initialize(
      @name : String,
      @partitions : Int32,
      @replication_factor : Int32 = 1,
      @retention_ms : Int64 = 0_i64,
      @cleanup_policy : String = "delete",
      @internal : Bool = false
    )
    end

    def self.from_json(json : JSON::Any) : TopicInfo
      TopicInfo.new(
        name: json["name"].as_s,
        partitions: json["partitions"]?.try(&.as_i) || 1,
        replication_factor: json["replicationFactor"]?.try(&.as_i) || 1,
        retention_ms: json["retentionMs"]?.try(&.as_i64) || 0_i64,
        cleanup_policy: json["cleanupPolicy"]?.try(&.as_s) || "delete",
        internal: json["internal"]?.try(&.as_bool) || false
      )
    end
  end

  # Consumer group information
  struct GroupInfo
    getter id : String
    getter state : String
    getter member_count : Int32
    getter members : Array(GroupMember)
    getter total_lag : Int64

    def initialize(
      @id : String,
      @state : String,
      @member_count : Int32 = 0,
      @members : Array(GroupMember) = [] of GroupMember,
      @total_lag : Int64 = 0_i64
    )
    end

    def self.from_json(json : JSON::Any) : GroupInfo
      members = json["members"]?.try(&.as_a.map { |m| GroupMember.from_json(m) }) || [] of GroupMember

      GroupInfo.new(
        id: json["id"]?.try(&.as_s) || json["groupId"]?.try(&.as_s) || "",
        state: json["state"]?.try(&.as_s) || "unknown",
        member_count: json["memberCount"]?.try(&.as_i) || members.size,
        members: members,
        total_lag: json["totalLag"]?.try(&.as_i64) || 0_i64
      )
    end
  end

  # Consumer group member information
  struct GroupMember
    getter id : String
    getter client_id : String
    getter host : String
    getter partitions : Array(Int32)

    def initialize(@id : String, @client_id : String, @host : String, @partitions : Array(Int32) = [] of Int32)
    end

    def self.from_json(json : JSON::Any) : GroupMember
      partitions = json["partitions"]?.try(&.as_a.map(&.as_i)) || [] of Int32

      GroupMember.new(
        id: json["memberId"]?.try(&.as_s) || "",
        client_id: json["clientId"]?.try(&.as_s) || "",
        host: json["host"]?.try(&.as_s) || "",
        partitions: partitions
      )
    end
  end

  # Partition information
  struct PartitionInfo
    getter id : Int32
    getter leader : Int32
    getter replicas : Array(Int32)
    getter isr : Array(Int32)
    getter beginning_offset : Int64
    getter end_offset : Int64

    def initialize(
      @id : Int32,
      @leader : Int32,
      @replicas : Array(Int32) = [] of Int32,
      @isr : Array(Int32) = [] of Int32,
      @beginning_offset : Int64 = 0_i64,
      @end_offset : Int64 = 0_i64
    )
    end

    def self.from_json(json : JSON::Any) : PartitionInfo
      replicas = json["replicas"]?.try(&.as_a.map(&.as_i)) || [] of Int32
      isr = json["isr"]?.try(&.as_a.map(&.as_i)) || [] of Int32

      PartitionInfo.new(
        id: json["partition"]?.try(&.as_i) || json["id"]?.try(&.as_i) || 0,
        leader: json["leader"]?.try(&.as_i) || 0,
        replicas: replicas,
        isr: isr,
        beginning_offset: json["beginningOffset"]?.try(&.as_i64) || 0_i64,
        end_offset: json["endOffset"]?.try(&.as_i64) || 0_i64
      )
    end
  end
end
