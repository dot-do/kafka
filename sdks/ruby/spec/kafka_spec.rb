# frozen_string_literal: true

require 'spec_helper'

RSpec.describe KafkaDo::Kafka do
  describe '#initialize' do
    it 'creates a client with default options' do
      kafka = described_class.new(['broker1:9092'])
      expect(kafka.brokers).to eq(['broker1:9092'])
      expect(kafka.client_id).to be_nil
      expect(kafka.connection_url).to eq('https://kafka.do')
    end

    it 'accepts multiple brokers' do
      kafka = described_class.new(['broker1:9092', 'broker2:9092', 'broker3:9092'])
      expect(kafka.brokers).to eq(['broker1:9092', 'broker2:9092', 'broker3:9092'])
    end

    it 'accepts a client_id' do
      kafka = described_class.new(['broker1:9092'], client_id: 'my-app')
      expect(kafka.client_id).to eq('my-app')
    end

    it 'accepts a custom connection_url' do
      kafka = described_class.new(['broker1:9092'], connection_url: 'https://custom.kafka.do')
      expect(kafka.connection_url).to eq('https://custom.kafka.do')
    end

    it 'handles empty broker list' do
      kafka = described_class.new([])
      expect(kafka.brokers).to eq([])
    end

    it 'handles single broker as array' do
      kafka = described_class.new('broker1:9092')
      expect(kafka.brokers).to eq(['broker1:9092'])
    end
  end

  describe '#producer' do
    let(:kafka) { described_class.new(['broker1:9092']) }

    it 'creates a producer with default options' do
      producer = kafka.producer
      expect(producer).to be_a(KafkaDo::Producer)
      expect(producer.kafka).to eq(kafka)
    end

    it 'creates a producer with compression codec' do
      producer = kafka.producer(compression_codec: :gzip)
      expect(producer).to be_a(KafkaDo::Producer)
    end

    it 'creates a producer with required_acks' do
      producer = kafka.producer(required_acks: -1)
      expect(producer).to be_a(KafkaDo::Producer)
    end

    it 'raises ClosedError if client is closed' do
      kafka.close
      expect { kafka.producer }.to raise_error(KafkaDo::ClosedError)
    end
  end

  describe '#consumer' do
    let(:kafka) { described_class.new(['broker1:9092']) }

    it 'creates a consumer with group_id' do
      consumer = kafka.consumer(group_id: 'my-group')
      expect(consumer).to be_a(KafkaDo::Consumer)
      expect(consumer.group_id).to eq('my-group')
    end

    it 'creates a consumer with options' do
      consumer = kafka.consumer(
        group_id: 'my-group',
        session_timeout: 60,
        heartbeat_interval: 15
      )
      expect(consumer).to be_a(KafkaDo::Consumer)
    end

    it 'raises ClosedError if client is closed' do
      kafka.close
      expect { kafka.consumer(group_id: 'my-group') }.to raise_error(KafkaDo::ClosedError)
    end
  end

  describe '#admin' do
    let(:kafka) { described_class.new(['broker1:9092']) }

    it 'creates an admin client' do
      admin = kafka.admin
      expect(admin).to be_a(KafkaDo::Admin)
      expect(admin.kafka).to eq(kafka)
    end

    it 'raises ClosedError if client is closed' do
      kafka.close
      expect { kafka.admin }.to raise_error(KafkaDo::ClosedError)
    end
  end

  describe '#create_topic' do
    let(:kafka) { described_class.new(['broker1:9092']) }
    let(:mock_client) { MockRpcClient.new }

    before do
      kafka.rpc_client = mock_client
      mock_client.mock_response('kafka.admin.createTopics', [{ 'name' => 'new-topic' }])
    end

    it 'creates a topic with defaults' do
      kafka.create_topic('new-topic')

      call = mock_client.last_call_for('kafka.admin.createTopics')
      expect(call).not_to be_nil
      expect(call[:args].first[:topics].first[:name]).to eq('new-topic')
      expect(call[:args].first[:topics].first[:numPartitions]).to eq(1)
    end

    it 'creates a topic with partitions' do
      kafka.create_topic('new-topic', num_partitions: 3)

      call = mock_client.last_call_for('kafka.admin.createTopics')
      expect(call[:args].first[:topics].first[:numPartitions]).to eq(3)
    end
  end

  describe '#close' do
    let(:kafka) { described_class.new(['broker1:9092']) }
    let(:mock_client) { MockRpcClient.new }

    before do
      kafka.rpc_client = mock_client
    end

    it 'closes the client' do
      kafka.close
      expect(kafka).to be_closed
    end

    it 'closes the RPC client' do
      kafka.close
      expect(mock_client).to be_closed
    end

    it 'is idempotent' do
      kafka.close
      kafka.close
      expect(kafka).to be_closed
    end
  end

  describe 'constants' do
    it 'defines compression types' do
      expect(described_class::COMPRESSION_NONE).to eq(:none)
      expect(described_class::COMPRESSION_GZIP).to eq(:gzip)
      expect(described_class::COMPRESSION_SNAPPY).to eq(:snappy)
      expect(described_class::COMPRESSION_LZ4).to eq(:lz4)
      expect(described_class::COMPRESSION_ZSTD).to eq(:zstd)
    end

    it 'defines offset reset policies' do
      expect(described_class::OFFSET_EARLIEST).to eq(:earliest)
      expect(described_class::OFFSET_LATEST).to eq(:latest)
    end

    it 'defines acknowledgment levels' do
      expect(described_class::ACKS_NONE).to eq(0)
      expect(described_class::ACKS_LEADER).to eq(1)
      expect(described_class::ACKS_ALL).to eq(-1)
    end
  end
end

RSpec.describe 'Kafka alias' do
  it 'aliases KafkaDo::Kafka as Kafka' do
    expect(Kafka).to eq(KafkaDo::Kafka)
  end

  it 'allows creating clients via Kafka.new' do
    kafka = Kafka.new(['broker1:9092'])
    expect(kafka).to be_a(KafkaDo::Kafka)
  end
end
