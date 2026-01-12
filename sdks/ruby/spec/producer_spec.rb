# frozen_string_literal: true

require 'spec_helper'

RSpec.describe KafkaDo::Producer do
  let(:kafka) { KafkaDo::Kafka.new(['broker1:9092']) }
  let(:mock_client) { MockRpcClient.new }

  before do
    kafka.rpc_client = mock_client
  end

  describe '#initialize' do
    it 'creates a producer with default options' do
      producer = kafka.producer
      expect(producer.kafka).to eq(kafka)
      expect(producer.buffer_size).to eq(0)
      expect(producer.buffer_bytesize).to eq(0)
    end

    it 'accepts compression_codec option' do
      producer = kafka.producer(compression_codec: :gzip)
      expect(producer).to be_a(described_class)
    end

    it 'accepts required_acks option' do
      producer = kafka.producer(required_acks: -1)
      expect(producer).to be_a(described_class)
    end
  end

  describe '#produce' do
    let(:producer) { kafka.producer }

    before do
      mock_client.mock_response('kafka.producer.sendBatch', [
                                  { 'topic' => 'my-topic', 'partition' => 0, 'offset' => 100 }
                                ])
    end

    it 'buffers a message' do
      producer.produce('Hello!', topic: 'my-topic')
      expect(producer.buffer_size).to eq(1)
    end

    it 'buffers multiple messages' do
      producer.produce('Hello!', topic: 'my-topic')
      producer.produce('World!', topic: 'my-topic')
      expect(producer.buffer_size).to eq(2)
    end

    it 'returns self for method chaining' do
      result = producer.produce('Hello!', topic: 'my-topic')
      expect(result).to eq(producer)
    end

    it 'accepts a key' do
      producer.produce('Hello!', topic: 'my-topic', key: 'greeting')
      expect(producer.buffer_size).to eq(1)
    end

    it 'accepts a partition' do
      producer.produce('Hello!', topic: 'my-topic', partition: 2)
      expect(producer.buffer_size).to eq(1)
    end

    it 'accepts headers' do
      producer.produce('Hello!', topic: 'my-topic',
                       headers: { 'correlation-id' => 'abc-123' })
      expect(producer.buffer_size).to eq(1)
    end

    it 'encodes Hash values as JSON' do
      producer.produce({ order_id: '123', amount: 99.99 }, topic: 'orders')
      expect(producer.buffer_size).to eq(1)
    end

    it 'raises ClosedError if producer is closed' do
      producer.close
      expect { producer.produce('Hello!', topic: 'my-topic') }
        .to raise_error(KafkaDo::ClosedError)
    end
  end

  describe '#deliver_messages' do
    let(:producer) { kafka.producer }

    before do
      mock_client.mock_response('kafka.producer.sendBatch', [
                                  { 'topic' => 'my-topic', 'partition' => 0, 'offset' => 100 },
                                  { 'topic' => 'my-topic', 'partition' => 0, 'offset' => 101 }
                                ])
    end

    it 'sends buffered messages' do
      producer.produce('Hello!', topic: 'my-topic')
      producer.produce('World!', topic: 'my-topic')
      results = producer.deliver_messages

      expect(results.size).to eq(2)
      expect(results[0]).to be_a(KafkaDo::RecordMetadata)
      expect(results[0].topic).to eq('my-topic')
      expect(results[0].offset).to eq(100)
    end

    it 'clears the buffer after delivery' do
      producer.produce('Hello!', topic: 'my-topic')
      producer.deliver_messages

      expect(producer.buffer_size).to eq(0)
      expect(producer.buffer_bytesize).to eq(0)
    end

    it 'calls the RPC method with correct parameters' do
      producer.produce('Hello!', topic: 'my-topic', key: 'greeting')
      producer.deliver_messages

      call = mock_client.last_call_for('kafka.producer.sendBatch')
      expect(call).not_to be_nil
      expect(call[:args].first[:topic]).to eq('my-topic')
      expect(call[:args].first[:messages]).to be_an(Array)
    end

    it 'returns empty array when buffer is empty' do
      results = producer.deliver_messages
      expect(results).to eq([])
    end

    it 'groups messages by topic' do
      mock_client.mock_response('kafka.producer.sendBatch', [
                                  { 'topic' => 'topic-a', 'partition' => 0, 'offset' => 100 }
                                ])

      producer.produce('A', topic: 'topic-a')
      producer.produce('B', topic: 'topic-b')
      producer.deliver_messages

      calls = mock_client.calls_for('kafka.producer.sendBatch')
      expect(calls.size).to eq(2)
    end
  end

  describe '#clear_buffer' do
    let(:producer) { kafka.producer }

    it 'clears buffered messages without sending' do
      producer.produce('Hello!', topic: 'my-topic')
      producer.produce('World!', topic: 'my-topic')
      producer.clear_buffer

      expect(producer.buffer_size).to eq(0)
      expect(mock_client.calls_for('kafka.producer.sendBatch')).to be_empty
    end
  end

  describe '#buffer_empty?' do
    let(:producer) { kafka.producer }

    it 'returns true when buffer is empty' do
      expect(producer).to be_buffer_empty
    end

    it 'returns false when buffer has messages' do
      producer.produce('Hello!', topic: 'my-topic')
      expect(producer).not_to be_buffer_empty
    end
  end

  describe '#shutdown' do
    let(:producer) { kafka.producer }

    before do
      mock_client.mock_response('kafka.producer.sendBatch', [
                                  { 'topic' => 'my-topic', 'partition' => 0, 'offset' => 100 }
                                ])
    end

    it 'delivers remaining messages and closes' do
      producer.produce('Hello!', topic: 'my-topic')
      producer.shutdown

      expect(producer).to be_closed
      expect(mock_client.calls_for('kafka.producer.sendBatch').size).to eq(1)
    end
  end

  describe '#close' do
    let(:producer) { kafka.producer }

    it 'closes without delivering messages' do
      producer.produce('Hello!', topic: 'my-topic')
      producer.close

      expect(producer).to be_closed
      expect(mock_client.calls_for('kafka.producer.sendBatch')).to be_empty
    end

    it 'is idempotent' do
      producer.close
      producer.close
      expect(producer).to be_closed
    end
  end

  describe 'error handling' do
    let(:producer) { kafka.producer }

    it 'raises ProducerError on RPC failure' do
      mock_client.mock_response('kafka.producer.sendBatch', nil,
                                error: StandardError.new('Network error'))

      producer.produce('Hello!', topic: 'my-topic')
      expect { producer.deliver_messages }.to raise_error(KafkaDo::ProducerError)
    end

    it 'raises ProducerError on message-level error' do
      mock_client.mock_response('kafka.producer.sendBatch', [
                                  { 'error' => { 'code' => 'MESSAGE_TOO_LARGE', 'message' => 'Too big' } }
                                ])

      producer.produce('Hello!', topic: 'my-topic')
      expect { producer.deliver_messages }.to raise_error(KafkaDo::ProducerError)
    end
  end
end
