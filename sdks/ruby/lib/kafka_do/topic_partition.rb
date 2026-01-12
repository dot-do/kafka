# frozen_string_literal: true

module KafkaDo
  # Represents a topic and partition pair
  #
  # @example Creating a TopicPartition
  #   tp = TopicPartition.new('my-topic', 0)
  #   puts tp.topic      # => "my-topic"
  #   puts tp.partition  # => 0
  #
  class TopicPartition
    # @return [String] the topic name
    attr_reader :topic

    # @return [Integer] the partition number
    attr_reader :partition

    # Create a new TopicPartition
    #
    # @param topic [String] the topic name
    # @param partition [Integer] the partition number
    def initialize(topic, partition)
      @topic = topic
      @partition = partition
    end

    # Equality check
    #
    # @param other [TopicPartition] the other topic partition
    # @return [Boolean]
    def ==(other)
      return false unless other.is_a?(TopicPartition)

      topic == other.topic && partition == other.partition
    end
    alias eql? ==

    # Hash code for use in Hash keys
    #
    # @return [Integer]
    def hash
      [topic, partition].hash
    end

    # Returns a string representation
    #
    # @return [String]
    def to_s
      "#{topic}-#{partition}"
    end

    # Returns a detailed inspection string
    #
    # @return [String]
    def inspect
      "#<KafkaDo::TopicPartition topic=#{topic.inspect} partition=#{partition}>"
    end

    # Convert to hash for RPC serialization
    #
    # @return [Hash]
    def to_h
      { 'topic' => topic, 'partition' => partition }
    end
  end

  # Metadata about a topic
  class TopicMetadata
    # @return [String] the topic name
    attr_reader :name

    # @return [Integer] the number of partitions
    attr_reader :partitions

    # @return [Integer] the replication factor
    attr_reader :replication_factor

    # @return [Hash<String, String>] topic configuration
    attr_reader :config

    def initialize(name:, partitions:, replication_factor:, config: nil)
      @name = name
      @partitions = partitions
      @replication_factor = replication_factor
      @config = config || {}
    end

    def self.from_rpc(data)
      new(
        name: data['name'],
        partitions: data['partitions'].to_i,
        replication_factor: data['replicationFactor'].to_i,
        config: data['config'] || {}
      )
    end

    def to_s
      "#<KafkaDo::TopicMetadata name=#{name} partitions=#{partitions}>"
    end

    def inspect
      "#<KafkaDo::TopicMetadata name=#{name.inspect} partitions=#{partitions} " \
        "replication_factor=#{replication_factor} config=#{config.inspect}>"
    end
  end

  # Metadata about a partition
  class PartitionMetadata
    attr_reader :topic, :partition, :leader, :replicas, :isr

    def initialize(topic:, partition:, leader:, replicas:, isr:)
      @topic = topic
      @partition = partition
      @leader = leader
      @replicas = replicas
      @isr = isr
    end

    def to_s
      "#<KafkaDo::PartitionMetadata topic=#{topic} partition=#{partition} leader=#{leader}>"
    end
  end

  # Metadata about a consumer group
  class ConsumerGroupMetadata
    attr_reader :group_id, :state, :members, :coordinator

    def initialize(group_id:, state:, members:, coordinator:)
      @group_id = group_id
      @state = state
      @members = members
      @coordinator = coordinator
    end

    def self.from_rpc(data)
      new(
        group_id: data['groupId'],
        state: data['state'],
        members: data['members'] || [],
        coordinator: data['coordinator'].to_i
      )
    end

    def to_s
      "#<KafkaDo::ConsumerGroupMetadata group_id=#{group_id} state=#{state}>"
    end
  end
end
