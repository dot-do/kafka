# frozen_string_literal: true

module KafkaDo
  # Kafka admin client for managing topics and consumer groups
  #
  # @example Creating and managing topics
  #   admin = kafka.admin
  #   admin.create_topic('orders', num_partitions: 3)
  #   admin.list_topics
  #   admin.delete_topic('old-topic')
  #
  class Admin
    # @return [Kafka] the parent Kafka client
    attr_reader :kafka

    # Create a new Admin client
    #
    # @param kafka [Kafka] the parent Kafka client
    def initialize(kafka)
      @kafka = kafka
      @closed = false
    end

    # Create a new topic
    #
    # @param name [String] the topic name
    # @param num_partitions [Integer] number of partitions
    # @param replication_factor [Integer] replication factor
    # @param config [Hash] additional topic configuration
    # @return [void]
    #
    # @example Create a topic with defaults
    #   admin.create_topic('my-topic')
    #
    # @example Create a topic with options
    #   admin.create_topic('orders',
    #     num_partitions: 6,
    #     replication_factor: 3,
    #     config: { 'retention.ms' => '604800000' }
    #   )
    #
    def create_topic(name, num_partitions: 1, replication_factor: 1, config: {})
      raise ClosedError, 'Admin client is closed' if @closed

      client = kafka.rpc_client
      promise = client.call('kafka.admin.createTopics', {
                              topics: [{
                                name: name,
                                numPartitions: num_partitions,
                                replicationFactor: replication_factor,
                                config: config
                              }],
                              validateOnly: false
                            })

      result = promise.await
      check_topic_result(result, name)
    end

    # Create multiple topics
    #
    # @param topics [Array<Hash>] array of topic configurations
    # @return [void]
    #
    # @example
    #   admin.create_topics([
    #     { name: 'orders', num_partitions: 3 },
    #     { name: 'payments', num_partitions: 3 }
    #   ])
    #
    def create_topics(topics)
      raise ClosedError, 'Admin client is closed' if @closed

      topic_configs = topics.map do |t|
        {
          name: t[:name],
          numPartitions: t[:num_partitions] || 1,
          replicationFactor: t[:replication_factor] || 1,
          config: t[:config] || {}
        }
      end

      client = kafka.rpc_client
      promise = client.call('kafka.admin.createTopics', {
                              topics: topic_configs,
                              validateOnly: false
                            })

      result = promise.await
      topics.each { |t| check_topic_result(result, t[:name]) }
    end

    # Delete a topic
    #
    # @param name [String] the topic name
    # @return [void]
    def delete_topic(name)
      raise ClosedError, 'Admin client is closed' if @closed

      client = kafka.rpc_client
      promise = client.call('kafka.admin.deleteTopics', {
                              topics: [name]
                            })

      result = promise.await
      check_delete_result(result, name)
    end

    # Delete multiple topics
    #
    # @param names [Array<String>] topic names
    # @return [void]
    def delete_topics(names)
      raise ClosedError, 'Admin client is closed' if @closed

      client = kafka.rpc_client
      promise = client.call('kafka.admin.deleteTopics', {
                              topics: names
                            })

      result = promise.await
      names.each { |name| check_delete_result(result, name) }
    end

    # List all topics
    #
    # @param include_internal [Boolean] whether to include internal topics
    # @return [Array<String>] list of topic names
    def list_topics(include_internal: false)
      raise ClosedError, 'Admin client is closed' if @closed

      client = kafka.rpc_client
      promise = client.call('kafka.admin.listTopics', {
                              includeInternal: include_internal
                            })

      result = promise.await
      result.is_a?(Array) ? result : []
    end

    # Describe a topic
    #
    # @param name [String] the topic name
    # @return [TopicMetadata] topic metadata
    def describe_topic(name)
      raise ClosedError, 'Admin client is closed' if @closed

      topics = describe_topics([name])
      topics.first or raise TopicNotFoundError.new(name)
    end

    # Describe multiple topics
    #
    # @param names [Array<String>] topic names
    # @return [Array<TopicMetadata>] topic metadata
    def describe_topics(names)
      raise ClosedError, 'Admin client is closed' if @closed

      client = kafka.rpc_client
      promise = client.call('kafka.admin.describeTopics', {
                              topics: names
                            })

      result = promise.await
      parse_topic_metadata_list(result)
    end

    # Get partitions for a topic
    #
    # @param topic [String] the topic name
    # @return [Array<Integer>] list of partition numbers
    def partitions_for(topic)
      metadata = describe_topic(topic)
      (0...metadata.partitions).to_a
    end

    # Describe partitions for a topic
    #
    # @param topic [String] the topic name
    # @return [Array<PartitionMetadata>] partition metadata
    def describe_partitions(topic)
      raise ClosedError, 'Admin client is closed' if @closed

      client = kafka.rpc_client
      promise = client.call('kafka.admin.describePartitions', {
                              topic: topic
                            })

      result = promise.await
      parse_partition_metadata(result, topic)
    end

    # Alter topic configuration
    #
    # @param topic [String] the topic name
    # @param config [Hash] configuration to update
    # @return [void]
    def alter_topic_config(topic, config)
      raise ClosedError, 'Admin client is closed' if @closed

      client = kafka.rpc_client
      promise = client.call('kafka.admin.alterTopicConfig', {
                              topic: topic,
                              config: config
                            })

      result = promise.await
      check_alter_result(result, topic)
    end
    alias alter_topic alter_topic_config

    # List all consumer groups
    #
    # @return [Array<String>] list of group IDs
    def list_consumer_groups
      raise ClosedError, 'Admin client is closed' if @closed

      client = kafka.rpc_client
      promise = client.call('kafka.admin.listConsumerGroups')

      result = promise.await
      result.is_a?(Array) ? result : []
    end
    alias list_groups list_consumer_groups

    # Describe a consumer group
    #
    # @param group_id [String] the group ID
    # @return [ConsumerGroupMetadata] group metadata
    def describe_consumer_group(group_id)
      groups = describe_consumer_groups([group_id])
      groups.first
    end
    alias describe_group describe_consumer_group

    # Describe multiple consumer groups
    #
    # @param group_ids [Array<String>] group IDs
    # @return [Array<ConsumerGroupMetadata>] group metadata
    def describe_consumer_groups(group_ids)
      raise ClosedError, 'Admin client is closed' if @closed

      client = kafka.rpc_client
      promise = client.call('kafka.admin.describeConsumerGroups', {
                              groupIds: group_ids
                            })

      result = promise.await
      parse_consumer_group_metadata_list(result)
    end
    alias describe_groups describe_consumer_groups

    # Delete a consumer group
    #
    # @param group_id [String] the group ID
    # @return [void]
    def delete_consumer_group(group_id)
      delete_consumer_groups([group_id])
    end
    alias delete_group delete_consumer_group

    # Delete multiple consumer groups
    #
    # @param group_ids [Array<String>] group IDs
    # @return [void]
    def delete_consumer_groups(group_ids)
      raise ClosedError, 'Admin client is closed' if @closed

      client = kafka.rpc_client
      promise = client.call('kafka.admin.deleteConsumerGroups', {
                              groupIds: group_ids
                            })

      promise.await
    end
    alias delete_groups delete_consumer_groups

    # Fetch committed offsets for a consumer group
    #
    # @param group_id [String] the consumer group ID
    # @param topics [Array<String>, nil] optional list of topics to fetch
    # @return [Hash<TopicPartition, Integer>] committed offsets
    #
    # @example Get offsets for a group
    #   offsets = admin.fetch_offsets('my-group')
    #   offsets.each { |tp, offset| puts "#{tp}: #{offset}" }
    #
    def fetch_offsets(group_id, topics: nil)
      raise ClosedError, 'Admin client is closed' if @closed

      client = kafka.rpc_client
      args = { groupId: group_id }
      args[:topics] = topics if topics

      promise = client.call('kafka.admin.fetchOffsets', args)

      result = promise.await
      parse_group_offsets(result)
    end
    alias list_offsets fetch_offsets

    # Reset consumer group offsets to the earliest position
    #
    # @param group_id [String] the consumer group ID
    # @param topic [String] the topic name
    # @return [void]
    #
    # @example Reset to beginning
    #   admin.reset_offsets('my-group', 'my-topic')
    #
    def reset_offsets(group_id, topic)
      raise ClosedError, 'Admin client is closed' if @closed

      client = kafka.rpc_client
      promise = client.call('kafka.admin.resetOffsets', {
                              groupId: group_id,
                              topic: topic,
                              earliest: true
                            })

      promise.await
    end

    # Reset consumer group offsets to the latest position
    #
    # @param group_id [String] the consumer group ID
    # @param topic [String] the topic name
    # @return [void]
    #
    # @example Reset to end
    #   admin.reset_offsets_to_latest('my-group', 'my-topic')
    #
    def reset_offsets_to_latest(group_id, topic)
      raise ClosedError, 'Admin client is closed' if @closed

      client = kafka.rpc_client
      promise = client.call('kafka.admin.resetOffsets', {
                              groupId: group_id,
                              topic: topic,
                              earliest: false
                            })

      promise.await
    end

    # Set consumer group offsets to specific values
    #
    # @param group_id [String] the consumer group ID
    # @param topic [String] the topic name
    # @param offsets [Hash<Integer, Integer>] partition => offset mapping
    # @return [void]
    #
    # @example Set offsets for specific partitions
    #   admin.set_offsets('my-group', 'my-topic', { 0 => 1000, 1 => 2000 })
    #
    def set_offsets(group_id, topic, offsets)
      raise ClosedError, 'Admin client is closed' if @closed

      partitions = offsets.map do |partition, offset|
        { partition: partition, offset: offset }
      end

      client = kafka.rpc_client
      promise = client.call('kafka.admin.setOffsets', {
                              groupId: group_id,
                              topic: topic,
                              partitions: partitions
                            })

      promise.await
    end

    # Get the earliest and latest offsets for a topic's partitions
    #
    # @param topic [String] the topic name
    # @return [Array<Hash>] partition offset information
    #
    # @example Get topic offsets
    #   offsets = admin.list_topic_offsets('my-topic')
    #   offsets.each do |p|
    #     puts "Partition #{p[:partition]}: #{p[:low]} - #{p[:high]}"
    #   end
    #
    def list_topic_offsets(topic)
      raise ClosedError, 'Admin client is closed' if @closed

      client = kafka.rpc_client
      promise = client.call('kafka.admin.listTopicOffsets', {
                              topic: topic
                            })

      result = promise.await
      parse_topic_offsets(result)
    end

    # Create additional partitions for a topic
    #
    # @param topic [String] the topic name
    # @param total_partitions [Integer] the new total number of partitions
    # @return [void]
    #
    # @example Add partitions
    #   admin.create_partitions('my-topic', 10)
    #
    def create_partitions(topic, total_partitions)
      raise ClosedError, 'Admin client is closed' if @closed

      client = kafka.rpc_client
      promise = client.call('kafka.admin.createPartitions', {
                              topic: topic,
                              count: total_partitions
                            })

      promise.await
    end

    # Close the admin client
    #
    # @return [void]
    def close
      @closed = true
    end

    # Check if the admin client is closed
    #
    # @return [Boolean]
    def closed?
      @closed
    end

    private

    def check_topic_result(result, name)
      return unless result.is_a?(Array)

      result.each do |r|
        next unless r.is_a?(Hash)
        next unless r['name'] == name

        if r['error']
          code = r['error']['code']
          message = r['error']['message']

          raise TopicAlreadyExistsError.new(name) if code == 'TOPIC_ALREADY_EXISTS'

          raise AdminError.new(message, code: code)
        end
      end
    end

    def check_delete_result(result, name)
      return unless result.is_a?(Array)

      result.each do |r|
        next unless r.is_a?(Hash)
        next unless r['name'] == name

        if r['error']
          code = r['error']['code']
          message = r['error']['message']

          raise TopicNotFoundError.new(name) if code == 'UNKNOWN_TOPIC_OR_PARTITION'

          raise AdminError.new(message, code: code)
        end
      end
    end

    def check_alter_result(result, topic)
      return unless result.is_a?(Hash) && result['error']

      code = result['error']['code']
      message = result['error']['message']

      raise TopicNotFoundError.new(topic) if code == 'UNKNOWN_TOPIC_OR_PARTITION'

      raise AdminError.new(message, code: code)
    end

    def parse_topic_metadata_list(result)
      return [] unless result.is_a?(Array)

      result.map do |data|
        next unless data.is_a?(Hash)

        if data['error']
          code = data['error']['code']
          raise TopicNotFoundError.new(data['name']) if code == 'UNKNOWN_TOPIC_OR_PARTITION'

          raise AdminError.new(data['error']['message'], code: code)
        end

        TopicMetadata.from_rpc(data)
      end.compact
    end

    def parse_partition_metadata(result, topic)
      return [] unless result.is_a?(Hash)

      if result['error']
        code = result['error']['code']
        raise TopicNotFoundError.new(topic) if code == 'UNKNOWN_TOPIC_OR_PARTITION'

        raise AdminError.new(result['error']['message'], code: code)
      end

      partitions = result['partitions']
      return [] unless partitions.is_a?(Array)

      partitions.map do |p|
        PartitionMetadata.new(
          topic: topic,
          partition: p['partition'].to_i,
          leader: p['leader'].to_i,
          replicas: Array(p['replicas']).map(&:to_i),
          isr: Array(p['isr']).map(&:to_i)
        )
      end
    end

    def parse_consumer_group_metadata_list(result)
      return [] unless result.is_a?(Array)

      result.map do |data|
        next unless data.is_a?(Hash)

        if data['error']
          raise AdminError.new(data['error']['message'], code: data['error']['code'])
        end

        ConsumerGroupMetadata.from_rpc(data)
      end.compact
    end

    def parse_group_offsets(result)
      return {} unless result.is_a?(Array)

      offsets = {}
      result.each do |data|
        next unless data.is_a?(Hash)

        topic = data['topic']
        partitions = data['partitions']
        next unless topic && partitions.is_a?(Array)

        partitions.each do |p|
          tp = TopicPartition.new(topic, p['partition'].to_i)
          offsets[tp] = p['offset'].to_i if p['offset']
        end
      end
      offsets
    end

    def parse_topic_offsets(result)
      return [] unless result.is_a?(Array)

      result.map do |p|
        next unless p.is_a?(Hash)

        {
          partition: p['partition'].to_i,
          offset: p['offset'].to_i,
          high: p['high'].to_i,
          low: p['low'].to_i
        }
      end.compact
    end
  end
end
