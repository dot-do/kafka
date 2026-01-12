# frozen_string_literal: true

require 'spec_helper'
require 'base64'

RSpec.describe KafkaDo::Consumer do
  let(:kafka) { KafkaDo::Kafka.new(['broker1:9092']) }
  let(:mock_client) { MockRpcClient.new }

  before do
    kafka.rpc_client = mock_client
  end

  describe '#initialize' do
    it 'creates a consumer with group_id' do
      consumer = kafka.consumer(group_id: 'my-group')
      expect(consumer.group_id).to eq('my-group')
      expect(consumer.kafka).to eq(kafka)
    end

    it 'starts with empty subscriptions' do
      consumer = kafka.consumer(group_id: 'my-group')
      expect(consumer.subscriptions).to be_empty
    end

    it 'accepts configuration options' do
      consumer = kafka.consumer(
        group_id: 'my-group',
        session_timeout: 60,
        heartbeat_interval: 15,
        auto_offset_reset: :earliest,
        enable_auto_commit: false
      )
      expect(consumer).to be_a(described_class)
    end
  end

  describe '#subscribe' do
    let(:consumer) { kafka.consumer(group_id: 'my-group') }

    before do
      mock_client.mock_response('kafka.consumer.subscribe', { 'success' => true })
    end

    it 'subscribes to a single topic' do
      consumer.subscribe('my-topic')
      expect(consumer.subscriptions).to eq(['my-topic'])
    end

    it 'subscribes to multiple topics' do
      consumer.subscribe('topic-a', 'topic-b', 'topic-c')
      expect(consumer.subscriptions).to contain_exactly('topic-a', 'topic-b', 'topic-c')
    end

    it 'subscribes to topics passed as array' do
      consumer.subscribe(%w[topic-a topic-b])
      expect(consumer.subscriptions).to contain_exactly('topic-a', 'topic-b')
    end

    it 'calls the RPC method with correct parameters' do
      consumer.subscribe('my-topic')

      call = mock_client.last_call_for('kafka.consumer.subscribe')
      expect(call).not_to be_nil
      expect(call[:args].first[:topics]).to eq(['my-topic'])
      expect(call[:args].first[:groupId]).to eq('my-group')
    end

    it 'returns self for method chaining' do
      result = consumer.subscribe('my-topic')
      expect(result).to eq(consumer)
    end

    it 'raises ClosedError if consumer is closed' do
      consumer.close
      expect { consumer.subscribe('my-topic') }.to raise_error(KafkaDo::ClosedError)
    end

    it 'accepts start_from_beginning option' do
      consumer.subscribe('my-topic', start_from_beginning: true)

      call = mock_client.last_call_for('kafka.consumer.subscribe')
      expect(call[:args].first[:autoOffsetReset]).to eq('earliest')
    end
  end

  describe '#unsubscribe' do
    let(:consumer) { kafka.consumer(group_id: 'my-group') }

    before do
      mock_client.mock_response('kafka.consumer.subscribe', { 'success' => true })
      mock_client.mock_response('kafka.consumer.unsubscribe', { 'success' => true })
    end

    it 'unsubscribes from all topics' do
      consumer.subscribe('topic-a', 'topic-b')
      consumer.unsubscribe

      expect(consumer.subscriptions).to be_empty
    end

    it 'calls the RPC method' do
      consumer.subscribe('my-topic')
      consumer.unsubscribe

      expect(mock_client.calls_for('kafka.consumer.unsubscribe').size).to eq(1)
    end
  end

  describe '#assign' do
    let(:consumer) { kafka.consumer(group_id: 'my-group') }

    before do
      mock_client.mock_response('kafka.consumer.assign', { 'success' => true })
    end

    it 'assigns specific partitions' do
      tp1 = KafkaDo::TopicPartition.new('my-topic', 0)
      tp2 = KafkaDo::TopicPartition.new('my-topic', 1)

      consumer.assign(tp1, tp2)
      expect(consumer.assignment).to contain_exactly(tp1, tp2)
    end

    it 'calls the RPC method with correct parameters' do
      tp = KafkaDo::TopicPartition.new('my-topic', 0)
      consumer.assign(tp)

      call = mock_client.last_call_for('kafka.consumer.assign')
      expect(call[:args].first[:partitions]).to be_an(Array)
    end
  end

  describe '#poll' do
    let(:consumer) { kafka.consumer(group_id: 'my-group') }

    before do
      mock_client.mock_response('kafka.consumer.subscribe', { 'success' => true })
      mock_client.mock_response('kafka.consumer.poll', [
                                  {
                                    'topic' => 'my-topic',
                                    'partition' => 0,
                                    'offset' => 100,
                                    'key' => Base64.strict_encode64('key-1'),
                                    'value' => Base64.strict_encode64('Hello!'),
                                    'timestamp' => 1_704_067_200_000
                                  },
                                  {
                                    'topic' => 'my-topic',
                                    'partition' => 0,
                                    'offset' => 101,
                                    'value' => Base64.strict_encode64('World!'),
                                    'timestamp' => 1_704_067_200_100
                                  }
                                ])
      consumer.subscribe('my-topic')
    end

    it 'returns messages' do
      messages = consumer.poll

      expect(messages.size).to eq(2)
      expect(messages[0]).to be_a(KafkaDo::Message)
      expect(messages[0].topic).to eq('my-topic')
      expect(messages[0].partition).to eq(0)
      expect(messages[0].offset).to eq(100)
      expect(messages[0].key).to eq('key-1')
      expect(messages[0].value).to eq('Hello!')
    end

    it 'parses message timestamps' do
      messages = consumer.poll

      expect(messages[0].timestamp).to be_a(Time)
    end

    it 'calls the RPC method with parameters' do
      consumer.poll(timeout_ms: 5000, max_records: 100)

      call = mock_client.last_call_for('kafka.consumer.poll')
      expect(call[:args].first[:timeoutMs]).to eq(5000)
      expect(call[:args].first[:maxRecords]).to eq(100)
    end

    it 'raises ClosedError if consumer is closed' do
      consumer.close
      expect { consumer.poll }.to raise_error(KafkaDo::ClosedError)
    end
  end

  describe '#each_message' do
    let(:consumer) { kafka.consumer(group_id: 'my-group') }
    let(:poll_count) { 0 }

    before do
      mock_client.mock_response('kafka.consumer.subscribe', { 'success' => true })
      mock_client.mock_response('kafka.consumer.commit', { 'success' => true })

      # Set up poll to return messages once, then stop the consumer
      @poll_count = 0
      allow(consumer).to receive(:poll) do
        @poll_count += 1
        if @poll_count == 1
          [
            KafkaDo::Message.new(topic: 'my-topic', partition: 0, offset: 100, value: 'Hello!'),
            KafkaDo::Message.new(topic: 'my-topic', partition: 0, offset: 101, value: 'World!')
          ]
        else
          consumer.stop
          []
        end
      end

      consumer.subscribe('my-topic')
    end

    it 'yields each message' do
      messages = []
      consumer.each_message { |msg| messages << msg }

      expect(messages.size).to eq(2)
      expect(messages[0].value).to eq('Hello!')
      expect(messages[1].value).to eq('World!')
    end

    it 'returns an Enumerator when no block given' do
      enumerator = consumer.each_message
      expect(enumerator).to be_an(Enumerator)
    end

    it 'raises ConsumerError when no subscriptions' do
      consumer.unsubscribe
      expect { consumer.each_message { |_| } }.to raise_error(KafkaDo::ConsumerError)
    end
  end

  describe '#commit_offsets' do
    let(:consumer) { kafka.consumer(group_id: 'my-group') }

    before do
      mock_client.mock_response('kafka.consumer.subscribe', { 'success' => true })
      mock_client.mock_response('kafka.consumer.commit', { 'success' => true })
      consumer.subscribe('my-topic')
    end

    it 'commits all offsets when called without arguments' do
      consumer.commit_offsets

      call = mock_client.last_call_for('kafka.consumer.commit')
      expect(call).not_to be_nil
      expect(call[:args].first).to eq({})
    end

    it 'commits specific message offset' do
      message = KafkaDo::Message.new(topic: 'my-topic', partition: 0, offset: 100, value: 'Hello!')
      consumer.commit_offsets(message)

      call = mock_client.last_call_for('kafka.consumer.commit')
      expect(call[:args].first[:offsets]).to be_an(Array)
      expect(call[:args].first[:offsets].first[:offset]).to eq(101) # offset + 1
    end
  end

  describe '#seek' do
    let(:consumer) { kafka.consumer(group_id: 'my-group') }

    before do
      mock_client.mock_response('kafka.consumer.seek', { 'success' => true })
    end

    it 'seeks to a specific offset' do
      tp = KafkaDo::TopicPartition.new('my-topic', 0)
      consumer.seek(tp, 1000)

      call = mock_client.last_call_for('kafka.consumer.seek')
      expect(call[:args].first[:topic]).to eq('my-topic')
      expect(call[:args].first[:partition]).to eq(0)
      expect(call[:args].first[:offset]).to eq(1000)
    end
  end

  describe '#seek_to_beginning' do
    let(:consumer) { kafka.consumer(group_id: 'my-group') }

    before do
      mock_client.mock_response('kafka.consumer.seekToBeginning', { 'success' => true })
    end

    it 'seeks to the beginning of partitions' do
      tp = KafkaDo::TopicPartition.new('my-topic', 0)
      consumer.seek_to_beginning(tp)

      call = mock_client.last_call_for('kafka.consumer.seekToBeginning')
      expect(call[:args].first[:partitions]).to be_an(Array)
    end
  end

  describe '#seek_to_end' do
    let(:consumer) { kafka.consumer(group_id: 'my-group') }

    before do
      mock_client.mock_response('kafka.consumer.seekToEnd', { 'success' => true })
    end

    it 'seeks to the end of partitions' do
      tp = KafkaDo::TopicPartition.new('my-topic', 0)
      consumer.seek_to_end(tp)

      call = mock_client.last_call_for('kafka.consumer.seekToEnd')
      expect(call[:args].first[:partitions]).to be_an(Array)
    end
  end

  describe '#position' do
    let(:consumer) { kafka.consumer(group_id: 'my-group') }

    before do
      mock_client.mock_response('kafka.consumer.position', 1500)
    end

    it 'returns the current position for a partition' do
      tp = KafkaDo::TopicPartition.new('my-topic', 0)
      position = consumer.position(tp)

      expect(position).to eq(1500)
    end
  end

  describe '#pause and #resume' do
    let(:consumer) { kafka.consumer(group_id: 'my-group') }

    before do
      mock_client.mock_response('kafka.consumer.pause', { 'success' => true })
      mock_client.mock_response('kafka.consumer.resume', { 'success' => true })
    end

    it 'pauses consumption from partitions' do
      tp = KafkaDo::TopicPartition.new('my-topic', 0)
      consumer.pause(tp)

      expect(consumer.paused).to include(tp)
    end

    it 'resumes consumption from paused partitions' do
      tp = KafkaDo::TopicPartition.new('my-topic', 0)
      consumer.pause(tp)
      consumer.resume(tp)

      expect(consumer.paused).not_to include(tp)
    end
  end

  describe '#close' do
    let(:consumer) { kafka.consumer(group_id: 'my-group') }

    before do
      mock_client.mock_response('kafka.consumer.subscribe', { 'success' => true })
      mock_client.mock_response('kafka.consumer.commit', { 'success' => true })
    end

    it 'closes the consumer' do
      consumer.close
      expect(consumer).to be_closed
    end

    it 'clears subscriptions' do
      consumer.subscribe('my-topic')
      consumer.close

      expect(consumer.subscriptions).to be_empty
    end

    it 'is idempotent' do
      consumer.close
      consumer.close
      expect(consumer).to be_closed
    end
  end

  describe 'Enumerable integration' do
    let(:consumer) { kafka.consumer(group_id: 'my-group') }

    before do
      mock_client.mock_response('kafka.consumer.subscribe', { 'success' => true })
      consumer.subscribe('my-topic')
    end

    it 'includes Enumerable' do
      expect(consumer).to be_a(Enumerable)
    end

    it 'responds to take' do
      expect(consumer).to respond_to(:take)
    end

    it 'responds to select' do
      expect(consumer).to respond_to(:select)
    end

    it 'responds to map' do
      expect(consumer).to respond_to(:map)
    end
  end
end

RSpec.describe KafkaDo::BatchConsumer do
  let(:kafka) { KafkaDo::Kafka.new(['broker1:9092']) }
  let(:mock_client) { MockRpcClient.new }

  before do
    kafka.rpc_client = mock_client
  end

  describe '#each_batch' do
    it 'is defined' do
      consumer = KafkaDo::BatchConsumer.new(kafka, group_id: 'my-group')
      expect(consumer).to respond_to(:each_batch)
    end

    it 'returns an Enumerator when no block given' do
      consumer = KafkaDo::BatchConsumer.new(kafka, group_id: 'my-group')
      mock_client.mock_response('kafka.consumer.subscribe', { 'success' => true })
      consumer.subscribe('my-topic')

      enumerator = consumer.each_batch
      expect(enumerator).to be_an(Enumerator)
    end
  end
end
