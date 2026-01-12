# frozen_string_literal: true

require 'spec_helper'

RSpec.describe KafkaDo::Admin do
  let(:kafka) { KafkaDo::Kafka.new(['broker1:9092']) }
  let(:mock_client) { MockRpcClient.new }

  before do
    kafka.rpc_client = mock_client
  end

  describe '#initialize' do
    it 'creates an admin client' do
      admin = kafka.admin
      expect(admin).to be_a(described_class)
      expect(admin.kafka).to eq(kafka)
    end

    it 'starts not closed' do
      admin = kafka.admin
      expect(admin).not_to be_closed
    end
  end

  describe '#create_topic' do
    let(:admin) { kafka.admin }

    before do
      mock_client.mock_response('kafka.admin.createTopics', [{ 'name' => 'new-topic' }])
    end

    it 'creates a topic with default options' do
      admin.create_topic('new-topic')

      call = mock_client.last_call_for('kafka.admin.createTopics')
      expect(call[:args].first[:topics]).to be_an(Array)
      expect(call[:args].first[:topics].first[:name]).to eq('new-topic')
      expect(call[:args].first[:topics].first[:numPartitions]).to eq(1)
      expect(call[:args].first[:topics].first[:replicationFactor]).to eq(1)
    end

    it 'creates a topic with partitions' do
      admin.create_topic('new-topic', num_partitions: 6)

      call = mock_client.last_call_for('kafka.admin.createTopics')
      expect(call[:args].first[:topics].first[:numPartitions]).to eq(6)
    end

    it 'creates a topic with replication factor' do
      admin.create_topic('new-topic', replication_factor: 3)

      call = mock_client.last_call_for('kafka.admin.createTopics')
      expect(call[:args].first[:topics].first[:replicationFactor]).to eq(3)
    end

    it 'creates a topic with config' do
      admin.create_topic('new-topic', config: { 'retention.ms' => '604800000' })

      call = mock_client.last_call_for('kafka.admin.createTopics')
      expect(call[:args].first[:topics].first[:config]).to eq({ 'retention.ms' => '604800000' })
    end

    it 'raises TopicAlreadyExistsError when topic exists' do
      mock_client.mock_response('kafka.admin.createTopics', [
                                  {
                                    'name' => 'existing-topic',
                                    'error' => { 'code' => 'TOPIC_ALREADY_EXISTS', 'message' => 'Already exists' }
                                  }
                                ])

      expect { admin.create_topic('existing-topic') }
        .to raise_error(KafkaDo::TopicAlreadyExistsError)
    end

    it 'raises ClosedError if admin is closed' do
      admin.close
      expect { admin.create_topic('new-topic') }.to raise_error(KafkaDo::ClosedError)
    end
  end

  describe '#create_topics' do
    let(:admin) { kafka.admin }

    before do
      mock_client.mock_response('kafka.admin.createTopics', [
                                  { 'name' => 'topic-a' },
                                  { 'name' => 'topic-b' }
                                ])
    end

    it 'creates multiple topics' do
      admin.create_topics([
                            { name: 'topic-a', num_partitions: 3 },
                            { name: 'topic-b', num_partitions: 6 }
                          ])

      call = mock_client.last_call_for('kafka.admin.createTopics')
      expect(call[:args].first[:topics].size).to eq(2)
    end
  end

  describe '#delete_topic' do
    let(:admin) { kafka.admin }

    before do
      mock_client.mock_response('kafka.admin.deleteTopics', [{ 'name' => 'old-topic' }])
    end

    it 'deletes a topic' do
      admin.delete_topic('old-topic')

      call = mock_client.last_call_for('kafka.admin.deleteTopics')
      expect(call[:args].first[:topics]).to eq(['old-topic'])
    end

    it 'raises TopicNotFoundError when topic does not exist' do
      mock_client.mock_response('kafka.admin.deleteTopics', [
                                  {
                                    'name' => 'missing-topic',
                                    'error' => { 'code' => 'UNKNOWN_TOPIC_OR_PARTITION', 'message' => 'Not found' }
                                  }
                                ])

      expect { admin.delete_topic('missing-topic') }
        .to raise_error(KafkaDo::TopicNotFoundError)
    end
  end

  describe '#delete_topics' do
    let(:admin) { kafka.admin }

    before do
      mock_client.mock_response('kafka.admin.deleteTopics', [
                                  { 'name' => 'topic-a' },
                                  { 'name' => 'topic-b' }
                                ])
    end

    it 'deletes multiple topics' do
      admin.delete_topics(%w[topic-a topic-b])

      call = mock_client.last_call_for('kafka.admin.deleteTopics')
      expect(call[:args].first[:topics]).to eq(%w[topic-a topic-b])
    end
  end

  describe '#list_topics' do
    let(:admin) { kafka.admin }

    before do
      mock_client.mock_response('kafka.admin.listTopics', %w[topic-a topic-b topic-c])
    end

    it 'lists all topics' do
      topics = admin.list_topics
      expect(topics).to eq(%w[topic-a topic-b topic-c])
    end

    it 'passes include_internal option' do
      admin.list_topics(include_internal: true)

      call = mock_client.last_call_for('kafka.admin.listTopics')
      expect(call[:args].first[:includeInternal]).to be true
    end

    it 'returns empty array when no topics exist' do
      mock_client.mock_response('kafka.admin.listTopics', [])
      topics = admin.list_topics
      expect(topics).to eq([])
    end
  end

  describe '#describe_topic' do
    let(:admin) { kafka.admin }

    before do
      mock_client.mock_response('kafka.admin.describeTopics', [
                                  {
                                    'name' => 'my-topic',
                                    'partitions' => 6,
                                    'replicationFactor' => 3,
                                    'config' => { 'retention.ms' => '604800000' }
                                  }
                                ])
    end

    it 'returns topic metadata' do
      metadata = admin.describe_topic('my-topic')

      expect(metadata).to be_a(KafkaDo::TopicMetadata)
      expect(metadata.name).to eq('my-topic')
      expect(metadata.partitions).to eq(6)
      expect(metadata.replication_factor).to eq(3)
      expect(metadata.config['retention.ms']).to eq('604800000')
    end

    it 'raises TopicNotFoundError when topic does not exist' do
      mock_client.mock_response('kafka.admin.describeTopics', [
                                  {
                                    'name' => 'missing-topic',
                                    'error' => { 'code' => 'UNKNOWN_TOPIC_OR_PARTITION', 'message' => 'Not found' }
                                  }
                                ])

      expect { admin.describe_topic('missing-topic') }
        .to raise_error(KafkaDo::TopicNotFoundError)
    end
  end

  describe '#describe_topics' do
    let(:admin) { kafka.admin }

    before do
      mock_client.mock_response('kafka.admin.describeTopics', [
                                  { 'name' => 'topic-a', 'partitions' => 3, 'replicationFactor' => 2 },
                                  { 'name' => 'topic-b', 'partitions' => 6, 'replicationFactor' => 3 }
                                ])
    end

    it 'returns metadata for multiple topics' do
      metadata = admin.describe_topics(%w[topic-a topic-b])

      expect(metadata.size).to eq(2)
      expect(metadata[0].name).to eq('topic-a')
      expect(metadata[1].name).to eq('topic-b')
    end
  end

  describe '#partitions_for' do
    let(:admin) { kafka.admin }

    before do
      mock_client.mock_response('kafka.admin.describeTopics', [
                                  { 'name' => 'my-topic', 'partitions' => 6, 'replicationFactor' => 3 }
                                ])
    end

    it 'returns partition numbers' do
      partitions = admin.partitions_for('my-topic')
      expect(partitions).to eq([0, 1, 2, 3, 4, 5])
    end
  end

  describe '#describe_partitions' do
    let(:admin) { kafka.admin }

    before do
      mock_client.mock_response('kafka.admin.describePartitions', {
                                  'partitions' => [
                                    { 'partition' => 0, 'leader' => 1, 'replicas' => [1, 2, 3], 'isr' => [1, 2, 3] },
                                    { 'partition' => 1, 'leader' => 2, 'replicas' => [2, 3, 1], 'isr' => [2, 3, 1] }
                                  ]
                                })
    end

    it 'returns partition metadata' do
      partitions = admin.describe_partitions('my-topic')

      expect(partitions.size).to eq(2)
      expect(partitions[0]).to be_a(KafkaDo::PartitionMetadata)
      expect(partitions[0].topic).to eq('my-topic')
      expect(partitions[0].partition).to eq(0)
      expect(partitions[0].leader).to eq(1)
      expect(partitions[0].replicas).to eq([1, 2, 3])
      expect(partitions[0].isr).to eq([1, 2, 3])
    end
  end

  describe '#alter_topic_config' do
    let(:admin) { kafka.admin }

    before do
      mock_client.mock_response('kafka.admin.alterTopicConfig', { 'success' => true })
    end

    it 'alters topic configuration' do
      admin.alter_topic_config('my-topic', { 'retention.ms' => '2592000000' })

      call = mock_client.last_call_for('kafka.admin.alterTopicConfig')
      expect(call[:args].first[:topic]).to eq('my-topic')
      expect(call[:args].first[:config]).to eq({ 'retention.ms' => '2592000000' })
    end

    it 'has alter_topic alias' do
      expect(admin).to respond_to(:alter_topic)
    end
  end

  describe '#list_consumer_groups' do
    let(:admin) { kafka.admin }

    before do
      mock_client.mock_response('kafka.admin.listConsumerGroups', %w[group-a group-b])
    end

    it 'lists all consumer groups' do
      groups = admin.list_consumer_groups
      expect(groups).to eq(%w[group-a group-b])
    end

    it 'has list_groups alias' do
      expect(admin).to respond_to(:list_groups)
    end
  end

  describe '#describe_consumer_group' do
    let(:admin) { kafka.admin }

    before do
      mock_client.mock_response('kafka.admin.describeConsumerGroups', [
                                  {
                                    'groupId' => 'my-group',
                                    'state' => 'Stable',
                                    'members' => %w[member-1 member-2],
                                    'coordinator' => 1
                                  }
                                ])
    end

    it 'returns consumer group metadata' do
      metadata = admin.describe_consumer_group('my-group')

      expect(metadata).to be_a(KafkaDo::ConsumerGroupMetadata)
      expect(metadata.group_id).to eq('my-group')
      expect(metadata.state).to eq('Stable')
      expect(metadata.members).to eq(%w[member-1 member-2])
      expect(metadata.coordinator).to eq(1)
    end

    it 'has describe_group alias' do
      expect(admin).to respond_to(:describe_group)
    end
  end

  describe '#describe_consumer_groups' do
    let(:admin) { kafka.admin }

    before do
      mock_client.mock_response('kafka.admin.describeConsumerGroups', [
                                  { 'groupId' => 'group-a', 'state' => 'Stable', 'members' => [], 'coordinator' => 1 },
                                  { 'groupId' => 'group-b', 'state' => 'Empty', 'members' => [], 'coordinator' => 2 }
                                ])
    end

    it 'returns metadata for multiple groups' do
      metadata = admin.describe_consumer_groups(%w[group-a group-b])

      expect(metadata.size).to eq(2)
      expect(metadata[0].group_id).to eq('group-a')
      expect(metadata[1].group_id).to eq('group-b')
    end

    it 'has describe_groups alias' do
      expect(admin).to respond_to(:describe_groups)
    end
  end

  describe '#delete_consumer_group' do
    let(:admin) { kafka.admin }

    before do
      mock_client.mock_response('kafka.admin.deleteConsumerGroups', [{ 'groupId' => 'old-group' }])
    end

    it 'deletes a consumer group' do
      admin.delete_consumer_group('old-group')

      call = mock_client.last_call_for('kafka.admin.deleteConsumerGroups')
      expect(call[:args].first[:groupIds]).to eq(['old-group'])
    end

    it 'has delete_group alias' do
      expect(admin).to respond_to(:delete_group)
    end
  end

  describe '#delete_consumer_groups' do
    let(:admin) { kafka.admin }

    before do
      mock_client.mock_response('kafka.admin.deleteConsumerGroups', [
                                  { 'groupId' => 'group-a' },
                                  { 'groupId' => 'group-b' }
                                ])
    end

    it 'deletes multiple consumer groups' do
      admin.delete_consumer_groups(%w[group-a group-b])

      call = mock_client.last_call_for('kafka.admin.deleteConsumerGroups')
      expect(call[:args].first[:groupIds]).to eq(%w[group-a group-b])
    end

    it 'has delete_groups alias' do
      expect(admin).to respond_to(:delete_groups)
    end
  end

  describe '#close' do
    let(:admin) { kafka.admin }

    it 'closes the admin client' do
      admin.close
      expect(admin).to be_closed
    end

    it 'is idempotent' do
      admin.close
      admin.close
      expect(admin).to be_closed
    end
  end

  describe '#fetch_offsets' do
    let(:admin) { kafka.admin }

    before do
      mock_client.mock_response('kafka.admin.fetchOffsets', [
                                  {
                                    'topic' => 'my-topic',
                                    'partitions' => [
                                      { 'partition' => 0, 'offset' => 1000 },
                                      { 'partition' => 1, 'offset' => 2000 }
                                    ]
                                  }
                                ])
    end

    it 'returns committed offsets for a group' do
      offsets = admin.fetch_offsets('my-group')

      expect(offsets.size).to eq(2)
      tp = KafkaDo::TopicPartition.new('my-topic', 0)
      expect(offsets[tp]).to eq(1000)
    end

    it 'has list_offsets alias' do
      expect(admin).to respond_to(:list_offsets)
    end

    it 'calls the RPC method with correct parameters' do
      admin.fetch_offsets('my-group', topics: ['my-topic'])

      call = mock_client.last_call_for('kafka.admin.fetchOffsets')
      expect(call[:args].first[:groupId]).to eq('my-group')
      expect(call[:args].first[:topics]).to eq(['my-topic'])
    end
  end

  describe '#reset_offsets' do
    let(:admin) { kafka.admin }

    before do
      mock_client.mock_response('kafka.admin.resetOffsets', { 'success' => true })
    end

    it 'resets offsets to earliest' do
      admin.reset_offsets('my-group', 'my-topic')

      call = mock_client.last_call_for('kafka.admin.resetOffsets')
      expect(call[:args].first[:groupId]).to eq('my-group')
      expect(call[:args].first[:topic]).to eq('my-topic')
      expect(call[:args].first[:earliest]).to be true
    end
  end

  describe '#reset_offsets_to_latest' do
    let(:admin) { kafka.admin }

    before do
      mock_client.mock_response('kafka.admin.resetOffsets', { 'success' => true })
    end

    it 'resets offsets to latest' do
      admin.reset_offsets_to_latest('my-group', 'my-topic')

      call = mock_client.last_call_for('kafka.admin.resetOffsets')
      expect(call[:args].first[:earliest]).to be false
    end
  end

  describe '#set_offsets' do
    let(:admin) { kafka.admin }

    before do
      mock_client.mock_response('kafka.admin.setOffsets', { 'success' => true })
    end

    it 'sets specific offsets' do
      admin.set_offsets('my-group', 'my-topic', { 0 => 1000, 1 => 2000 })

      call = mock_client.last_call_for('kafka.admin.setOffsets')
      expect(call[:args].first[:groupId]).to eq('my-group')
      expect(call[:args].first[:topic]).to eq('my-topic')
      expect(call[:args].first[:partitions]).to be_an(Array)
      expect(call[:args].first[:partitions].size).to eq(2)
    end
  end

  describe '#list_topic_offsets' do
    let(:admin) { kafka.admin }

    before do
      mock_client.mock_response('kafka.admin.listTopicOffsets', [
                                  { 'partition' => 0, 'offset' => 100, 'low' => 0, 'high' => 100 },
                                  { 'partition' => 1, 'offset' => 200, 'low' => 50, 'high' => 200 }
                                ])
    end

    it 'returns partition offset info' do
      offsets = admin.list_topic_offsets('my-topic')

      expect(offsets.size).to eq(2)
      expect(offsets[0][:partition]).to eq(0)
      expect(offsets[0][:high]).to eq(100)
      expect(offsets[0][:low]).to eq(0)
    end
  end

  describe '#create_partitions' do
    let(:admin) { kafka.admin }

    before do
      mock_client.mock_response('kafka.admin.createPartitions', { 'success' => true })
    end

    it 'creates additional partitions' do
      admin.create_partitions('my-topic', 10)

      call = mock_client.last_call_for('kafka.admin.createPartitions')
      expect(call[:args].first[:topic]).to eq('my-topic')
      expect(call[:args].first[:count]).to eq(10)
    end
  end
end
