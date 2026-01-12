# frozen_string_literal: true

require 'spec_helper'
require 'base64'

RSpec.describe KafkaDo::Message do
  describe '#initialize' do
    it 'creates a message with required attributes' do
      message = described_class.new(
        topic: 'my-topic',
        partition: 0,
        offset: 100,
        value: 'Hello!'
      )

      expect(message.topic).to eq('my-topic')
      expect(message.partition).to eq(0)
      expect(message.offset).to eq(100)
      expect(message.value).to eq('Hello!')
    end

    it 'creates a message with optional key' do
      message = described_class.new(
        topic: 'my-topic',
        partition: 0,
        offset: 100,
        value: 'Hello!',
        key: 'greeting'
      )

      expect(message.key).to eq('greeting')
    end

    it 'creates a message with optional timestamp' do
      timestamp = Time.now
      message = described_class.new(
        topic: 'my-topic',
        partition: 0,
        offset: 100,
        value: 'Hello!',
        timestamp: timestamp
      )

      expect(message.timestamp).to eq(timestamp)
    end

    it 'creates a message with optional headers' do
      message = described_class.new(
        topic: 'my-topic',
        partition: 0,
        offset: 100,
        value: 'Hello!',
        headers: { 'correlation-id' => 'abc-123' }
      )

      expect(message.headers['correlation-id']).to eq('abc-123')
    end

    it 'defaults key to nil' do
      message = described_class.new(
        topic: 'my-topic',
        partition: 0,
        offset: 100,
        value: 'Hello!'
      )

      expect(message.key).to be_nil
    end

    it 'defaults headers to empty hash' do
      message = described_class.new(
        topic: 'my-topic',
        partition: 0,
        offset: 100,
        value: 'Hello!'
      )

      expect(message.headers).to eq({})
    end

    it 'defaults timestamp to current time' do
      before_time = Time.now
      message = described_class.new(
        topic: 'my-topic',
        partition: 0,
        offset: 100,
        value: 'Hello!'
      )
      after_time = Time.now

      expect(message.timestamp).to be >= before_time
      expect(message.timestamp).to be <= after_time
    end
  end

  describe '.from_rpc' do
    it 'parses a message from RPC response' do
      data = {
        'topic' => 'my-topic',
        'partition' => 2,
        'offset' => 500,
        'key' => Base64.strict_encode64('my-key'),
        'value' => Base64.strict_encode64('Hello, World!'),
        'timestamp' => 1_704_067_200_000
      }

      message = described_class.from_rpc(data)

      expect(message.topic).to eq('my-topic')
      expect(message.partition).to eq(2)
      expect(message.offset).to eq(500)
      expect(message.key).to eq('my-key')
      expect(message.value).to eq('Hello, World!')
      expect(message.timestamp).to eq(Time.at(1_704_067_200))
    end

    it 'parses a message without key' do
      data = {
        'topic' => 'my-topic',
        'partition' => 0,
        'offset' => 100,
        'value' => Base64.strict_encode64('Hello!')
      }

      message = described_class.from_rpc(data)

      expect(message.key).to be_nil
    end

    it 'parses a message with headers' do
      data = {
        'topic' => 'my-topic',
        'partition' => 0,
        'offset' => 100,
        'value' => Base64.strict_encode64('Hello!'),
        'headers' => [
          { 'key' => 'correlation-id', 'value' => Base64.strict_encode64('abc-123') },
          { 'key' => 'trace-id', 'value' => Base64.strict_encode64('xyz-789') }
        ]
      }

      message = described_class.from_rpc(data)

      expect(message.headers['correlation-id']).to eq('abc-123')
      expect(message.headers['trace-id']).to eq('xyz-789')
    end
  end

  describe '#topic_partition' do
    it 'returns a TopicPartition' do
      message = described_class.new(
        topic: 'my-topic',
        partition: 5,
        offset: 100,
        value: 'Hello!'
      )

      tp = message.topic_partition

      expect(tp).to be_a(KafkaDo::TopicPartition)
      expect(tp.topic).to eq('my-topic')
      expect(tp.partition).to eq(5)
    end
  end

  describe '#to_s' do
    it 'returns a string representation' do
      message = described_class.new(
        topic: 'my-topic',
        partition: 0,
        offset: 100,
        value: 'Hello!'
      )

      expect(message.to_s).to include('my-topic')
      expect(message.to_s).to include('0')
      expect(message.to_s).to include('100')
    end
  end

  describe '#inspect' do
    it 'returns a detailed inspection string' do
      message = described_class.new(
        topic: 'my-topic',
        partition: 0,
        offset: 100,
        value: 'Hello!',
        key: 'greeting'
      )

      expect(message.inspect).to include('my-topic')
      expect(message.inspect).to include('Hello!')
      expect(message.inspect).to include('greeting')
    end
  end

  describe '#==' do
    it 'returns true for messages with same topic, partition, offset' do
      msg1 = described_class.new(topic: 'my-topic', partition: 0, offset: 100, value: 'Hello!')
      msg2 = described_class.new(topic: 'my-topic', partition: 0, offset: 100, value: 'Different!')

      expect(msg1).to eq(msg2)
    end

    it 'returns false for messages with different topic' do
      msg1 = described_class.new(topic: 'topic-a', partition: 0, offset: 100, value: 'Hello!')
      msg2 = described_class.new(topic: 'topic-b', partition: 0, offset: 100, value: 'Hello!')

      expect(msg1).not_to eq(msg2)
    end

    it 'returns false for messages with different partition' do
      msg1 = described_class.new(topic: 'my-topic', partition: 0, offset: 100, value: 'Hello!')
      msg2 = described_class.new(topic: 'my-topic', partition: 1, offset: 100, value: 'Hello!')

      expect(msg1).not_to eq(msg2)
    end

    it 'returns false for messages with different offset' do
      msg1 = described_class.new(topic: 'my-topic', partition: 0, offset: 100, value: 'Hello!')
      msg2 = described_class.new(topic: 'my-topic', partition: 0, offset: 101, value: 'Hello!')

      expect(msg1).not_to eq(msg2)
    end

    it 'returns false when compared to non-Message' do
      msg = described_class.new(topic: 'my-topic', partition: 0, offset: 100, value: 'Hello!')
      expect(msg).not_to eq('not a message')
    end
  end

  describe '#hash' do
    it 'returns the same hash for equal messages' do
      msg1 = described_class.new(topic: 'my-topic', partition: 0, offset: 100, value: 'Hello!')
      msg2 = described_class.new(topic: 'my-topic', partition: 0, offset: 100, value: 'Different!')

      expect(msg1.hash).to eq(msg2.hash)
    end

    it 'can be used as a Hash key' do
      msg = described_class.new(topic: 'my-topic', partition: 0, offset: 100, value: 'Hello!')
      hash = { msg => 'value' }

      expect(hash[msg]).to eq('value')
    end
  end
end
