# frozen_string_literal: true

require 'spec_helper'
require 'kafka_do/testing'

RSpec.describe KafkaDo::Testing do
  before do
    described_class.reset!
  end

  describe '.client' do
    it 'returns a Kafka client' do
      kafka = described_class.client
      expect(kafka).to be_a(KafkaDo::Kafka)
    end

    it 'configures the mock RPC client' do
      kafka = described_class.client
      expect(kafka.rpc_client).to be_a(KafkaDo::Testing::MockRpcClient)
    end

    it 'accepts broker configuration' do
      kafka = described_class.client(brokers: ['custom:9092'])
      expect(kafka.brokers).to eq(['custom:9092'])
    end
  end

  describe '.mock_rpc' do
    it 'returns a MockRpcClient' do
      expect(described_class.mock_rpc).to be_a(KafkaDo::Testing::MockRpcClient)
    end

    it 'returns the same instance on multiple calls' do
      mock1 = described_class.mock_rpc
      mock2 = described_class.mock_rpc
      expect(mock1).to eq(mock2)
    end
  end

  describe '.reset!' do
    it 'clears the mock RPC client' do
      mock1 = described_class.mock_rpc
      described_class.reset!
      mock2 = described_class.mock_rpc

      expect(mock1).not_to eq(mock2)
    end
  end

  describe 'producer testing' do
    it 'captures produced messages' do
      kafka = described_class.client
      producer = kafka.producer

      producer.produce('Hello!', topic: 'test-topic')
      producer.produce('World!', topic: 'test-topic', key: 'greeting')
      producer.deliver_messages

      messages = described_class.messages('test-topic')
      expect(messages.size).to eq(2)
      expect(messages[0].value).to eq('Hello!')
      expect(messages[1].key).to eq('greeting')
    end
  end

  describe 'consumer testing' do
    it 'allows producing test messages for consumers' do
      described_class.produce_messages('test-topic', [
                                         { value: 'Message 1', key: 'key1' },
                                         { value: 'Message 2', key: 'key2' },
                                         'Plain message'
                                       ])

      messages = described_class.messages('test-topic')
      expect(messages.size).to eq(3)
      expect(messages[0].value).to eq('Message 1')
      expect(messages[2].value).to eq('Plain message')
    end
  end

  describe '.topics' do
    it 'returns all topics' do
      described_class.create_topic('topic-a')
      described_class.create_topic('topic-b')

      kafka = described_class.client
      producer = kafka.producer
      producer.produce('msg', topic: 'topic-c')
      producer.deliver_messages

      topics = described_class.topics
      expect(topics).to include('topic-c')
    end
  end

  describe '.create_topic' do
    it 'creates a topic' do
      described_class.create_topic('new-topic', partitions: 3)

      kafka = described_class.client
      admin = kafka.admin
      topics = admin.list_topics

      expect(topics).to include('new-topic')
    end
  end

  describe '.clear_topic' do
    it 'clears messages from a topic' do
      described_class.produce_messages('test-topic', ['msg1', 'msg2'])
      expect(described_class.messages('test-topic').size).to eq(2)

      described_class.clear_topic('test-topic')
      expect(described_class.messages('test-topic')).to be_empty
    end
  end

  describe '.calls' do
    it 'returns all RPC calls' do
      kafka = described_class.client
      kafka.producer.produce('msg', topic: 'test').deliver_messages
      kafka.admin.list_topics

      calls = described_class.calls
      expect(calls.size).to be >= 2
    end

    it 'filters by method name' do
      kafka = described_class.client
      kafka.admin.list_topics

      calls = described_class.calls('kafka.admin.listTopics')
      expect(calls.size).to eq(1)
    end
  end

  describe '.called?' do
    it 'returns true if method was called' do
      kafka = described_class.client
      kafka.admin.list_topics

      expect(described_class.called?('kafka.admin.listTopics')).to be true
    end

    it 'returns false if method was not called' do
      described_class.client

      expect(described_class.called?('kafka.admin.deleteTopics')).to be false
    end
  end

  describe '.last_call' do
    it 'returns the last call for a method' do
      kafka = described_class.client
      producer = kafka.producer
      producer.produce('msg1', topic: 'test').deliver_messages
      producer.produce('msg2', topic: 'test').deliver_messages

      last = described_class.last_call('kafka.producer.sendBatch')
      expect(last).not_to be_nil
      expect(last[:args].first[:messages].size).to eq(1)
    end
  end
end

RSpec.describe KafkaDo::Testing::MockRpcClient do
  let(:mock_client) { described_class.new }

  describe '#mock_response' do
    it 'sets up custom responses' do
      mock_client.mock_response('custom.method', { 'data' => 'test' })

      result = mock_client.call('custom.method').await
      expect(result['data']).to eq('test')
    end

    it 'supports error responses' do
      mock_client.mock_response('error.method', nil, error: StandardError.new('Test error'))

      expect { mock_client.call('error.method').await }.to raise_error(StandardError, 'Test error')
    end
  end

  describe '#call' do
    it 'records calls' do
      mock_client.call('test.method', { arg1: 'value' })

      expect(mock_client.calls.size).to eq(1)
      expect(mock_client.calls.first[:method]).to eq('test.method')
      expect(mock_client.calls.first[:args]).to eq([{ arg1: 'value' }])
    end
  end

  describe '#close' do
    it 'marks the client as closed' do
      mock_client.close
      expect(mock_client).to be_closed
    end
  end

  describe '#calls_for' do
    it 'filters calls by method' do
      mock_client.call('method.a', {})
      mock_client.call('method.b', {})
      mock_client.call('method.a', {})

      calls = mock_client.calls_for('method.a')
      expect(calls.size).to eq(2)
    end

    it 'returns all calls when method is nil' do
      mock_client.call('method.a', {})
      mock_client.call('method.b', {})

      calls = mock_client.calls_for(nil)
      expect(calls.size).to eq(2)
    end
  end

  describe '#last_call_for' do
    it 'returns the last call for a method' do
      mock_client.call('method.a', { call: 1 })
      mock_client.call('method.a', { call: 2 })

      last = mock_client.last_call_for('method.a')
      expect(last[:args]).to eq([{ call: 2 }])
    end

    it 'returns nil if no calls found' do
      expect(mock_client.last_call_for('nonexistent')).to be_nil
    end
  end

  describe 'built-in handlers' do
    describe 'producer send' do
      it 'stores messages and returns metadata' do
        result = mock_client.call('kafka.producer.sendBatch', {
                                    topic: 'test-topic',
                                    messages: [
                                      { value: 'SGVsbG8h', partition: -1 }
                                    ]
                                  }).await

        expect(result).to be_an(Array)
        expect(result.first['topic']).to eq('test-topic')
        expect(result.first['offset']).to eq(0)
      end
    end

    describe 'consumer subscribe' do
      it 'returns success' do
        result = mock_client.call('kafka.consumer.subscribe', {
                                    topics: ['test-topic'],
                                    groupId: 'test-group'
                                  }).await

        expect(result['success']).to be true
      end
    end

    describe 'admin list topics' do
      it 'returns topic names' do
        mock_client.create_topic('topic-a')
        mock_client.create_topic('topic-b')

        result = mock_client.call('kafka.admin.listTopics', {}).await

        expect(result).to include('topic-a', 'topic-b')
      end
    end

    describe 'admin create topics' do
      it 'creates topics' do
        result = mock_client.call('kafka.admin.createTopics', {
                                    topics: [{ name: 'new-topic', numPartitions: 3 }]
                                  }).await

        expect(result.first['name']).to eq('new-topic')
      end

      it 'returns error for existing topics' do
        mock_client.create_topic('existing')

        result = mock_client.call('kafka.admin.createTopics', {
                                    topics: [{ name: 'existing' }]
                                  }).await

        expect(result.first['error']['code']).to eq('TOPIC_ALREADY_EXISTS')
      end
    end

    describe 'admin delete topics' do
      it 'deletes topics' do
        mock_client.create_topic('to-delete')

        result = mock_client.call('kafka.admin.deleteTopics', {
                                    topics: ['to-delete']
                                  }).await

        expect(result.first['name']).to eq('to-delete')
        expect(result.first['error']).to be_nil
      end

      it 'returns error for missing topics' do
        result = mock_client.call('kafka.admin.deleteTopics', {
                                    topics: ['nonexistent']
                                  }).await

        expect(result.first['error']['code']).to eq('UNKNOWN_TOPIC_OR_PARTITION')
      end
    end
  end
end
