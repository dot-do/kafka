# frozen_string_literal: true

require 'spec_helper'

RSpec.describe KafkaDo::Stream do
  let(:kafka) { KafkaDo::Kafka.new(['broker1:9092']) }
  let(:mock_client) { MockRpcClient.new }

  before do
    kafka.rpc_client = mock_client
  end

  describe '#initialize' do
    it 'creates a stream with a source topic' do
      stream = kafka.stream('my-topic')

      expect(stream).to be_a(described_class)
      expect(stream.kafka).to eq(kafka)
      expect(stream.source_topic).to eq('my-topic')
    end

    it 'accepts a custom group_id' do
      stream = kafka.stream('my-topic', group_id: 'my-custom-group')

      expect(stream).to be_a(described_class)
    end

    it 'raises ClosedError if client is closed' do
      kafka.close
      expect { kafka.stream('my-topic') }.to raise_error(KafkaDo::ClosedError)
    end
  end

  describe '#map' do
    it 'returns a new stream' do
      stream = kafka.stream('my-topic')
      new_stream = stream.map { |msg| msg.value.upcase }

      expect(new_stream).to be_a(described_class)
      expect(new_stream).not_to eq(stream)
    end

    it 'requires a block' do
      stream = kafka.stream('my-topic')

      expect { stream.map }.to raise_error(ArgumentError)
    end

    it 'adds a transformation' do
      stream = kafka.stream('my-topic')
      new_stream = stream.map { |msg| msg }

      expect(new_stream.transformations.size).to eq(1)
      expect(new_stream.transformations.first[:type]).to eq(:map)
    end
  end

  describe '#flat_map' do
    it 'returns a new stream' do
      stream = kafka.stream('my-topic')
      new_stream = stream.flat_map { |msg| [msg.value] }

      expect(new_stream).to be_a(described_class)
    end

    it 'requires a block' do
      stream = kafka.stream('my-topic')

      expect { stream.flat_map }.to raise_error(ArgumentError)
    end
  end

  describe '#filter' do
    it 'returns a new stream' do
      stream = kafka.stream('my-topic')
      new_stream = stream.filter { |msg| msg.value.length > 5 }

      expect(new_stream).to be_a(described_class)
      expect(new_stream).not_to eq(stream)
    end

    it 'requires a block' do
      stream = kafka.stream('my-topic')

      expect { stream.filter }.to raise_error(ArgumentError)
    end

    it 'adds a filter transformation' do
      stream = kafka.stream('my-topic')
      new_stream = stream.filter { |_| true }

      expect(new_stream.transformations.first[:type]).to eq(:filter)
    end

    it 'has a select alias' do
      stream = kafka.stream('my-topic')
      expect(stream).to respond_to(:select)
    end
  end

  describe '#filter_not' do
    it 'returns a new stream' do
      stream = kafka.stream('my-topic')
      new_stream = stream.filter_not { |msg| msg.value.empty? }

      expect(new_stream).to be_a(described_class)
    end

    it 'has a reject alias' do
      stream = kafka.stream('my-topic')
      expect(stream).to respond_to(:reject)
    end
  end

  describe '#peek' do
    it 'returns a new stream' do
      stream = kafka.stream('my-topic')
      new_stream = stream.peek { |msg| puts msg }

      expect(new_stream).to be_a(described_class)
    end

    it 'requires a block' do
      stream = kafka.stream('my-topic')

      expect { stream.peek }.to raise_error(ArgumentError)
    end

    it 'has a tap alias' do
      stream = kafka.stream('my-topic')
      expect(stream).to respond_to(:tap)
    end
  end

  describe '#map_key' do
    it 'returns a new stream' do
      stream = kafka.stream('my-topic')
      new_stream = stream.map_key { |msg| msg.value['id'] }

      expect(new_stream).to be_a(described_class)
    end

    it 'requires a block' do
      stream = kafka.stream('my-topic')

      expect { stream.map_key }.to raise_error(ArgumentError)
    end

    it 'has a select_key alias' do
      stream = kafka.stream('my-topic')
      expect(stream).to respond_to(:select_key)
    end
  end

  describe '#group_by' do
    it 'returns a GroupedStream' do
      stream = kafka.stream('my-topic')
      grouped = stream.group_by { |msg| msg.key }

      expect(grouped).to be_a(KafkaDo::GroupedStream)
    end
  end

  describe '#group_by_key' do
    it 'returns a GroupedStream' do
      stream = kafka.stream('my-topic')
      grouped = stream.group_by_key

      expect(grouped).to be_a(KafkaDo::GroupedStream)
    end
  end

  describe '#merge' do
    it 'returns a MergedStream' do
      stream1 = kafka.stream('topic-a')
      stream2 = kafka.stream('topic-b')
      merged = stream1.merge(stream2)

      expect(merged).to be_a(KafkaDo::MergedStream)
    end
  end

  describe '#branch' do
    it 'returns multiple streams' do
      stream = kafka.stream('my-topic')
      branches = stream.branch(
        ->(m) { m.value.start_with?('A') },
        ->(m) { m.value.start_with?('B') }
      )

      expect(branches).to be_an(Array)
      expect(branches.size).to eq(2)
      expect(branches.first).to be_a(described_class)
    end
  end

  describe 'chaining' do
    it 'allows chaining multiple transformations' do
      stream = kafka.stream('my-topic')
      chained = stream
                .filter { |msg| msg.value.length > 0 }
                .map { |msg| msg.value.upcase }
                .peek { |_| nil }

      expect(chained.transformations.size).to eq(3)
    end

    it 'does not modify the original stream' do
      original = kafka.stream('my-topic')
      _modified = original.filter { |_| true }.map { |m| m }

      expect(original.transformations.size).to eq(0)
    end
  end

  describe '#stop and #stopped?' do
    it 'stops the stream' do
      stream = kafka.stream('my-topic')
      expect(stream.stopped?).to be false

      stream.stop
      expect(stream.stopped?).to be true
    end
  end

  describe 'Enumerable' do
    it 'includes Enumerable' do
      stream = kafka.stream('my-topic')
      expect(stream).to be_a(Enumerable)
    end
  end
end

RSpec.describe KafkaDo::GroupedStream do
  let(:kafka) { KafkaDo::Kafka.new(['broker1:9092']) }
  let(:mock_client) { MockRpcClient.new }
  let(:stream) { kafka.stream('my-topic') }

  before do
    kafka.rpc_client = mock_client
  end

  describe '#count' do
    it 'returns a Table' do
      grouped = stream.group_by { |msg| msg.key }
      table = grouped.count

      expect(table).to be_a(KafkaDo::Table)
    end
  end

  describe '#reduce' do
    it 'returns a Table' do
      grouped = stream.group_by { |msg| msg.key }
      table = grouped.reduce(0) { |acc, val| acc + val }

      expect(table).to be_a(KafkaDo::Table)
    end
  end

  describe '#aggregate' do
    it 'returns a Table' do
      grouped = stream.group_by { |msg| msg.key }
      table = grouped.aggregate({}) { |_k, _v, agg| agg }

      expect(table).to be_a(KafkaDo::Table)
    end
  end
end

RSpec.describe KafkaDo::Table do
  let(:kafka) { KafkaDo::Kafka.new(['broker1:9092']) }
  let(:mock_client) { MockRpcClient.new }

  before do
    kafka.rpc_client = mock_client
  end

  describe '#[]' do
    it 'returns a value by key' do
      table = KafkaDo::Table.new(kafka, { 'key1' => 10 }, nil, nil)

      expect(table['key1']).to eq(10)
    end

    it 'returns nil for missing keys' do
      table = KafkaDo::Table.new(kafka, {}, nil, nil)

      expect(table['missing']).to be_nil
    end
  end

  describe '#all' do
    it 'returns all entries' do
      table = KafkaDo::Table.new(kafka, { 'a' => 1, 'b' => 2 }, nil, nil)

      expect(table.all).to eq({ 'a' => 1, 'b' => 2 })
    end

    it 'returns a copy of the state' do
      table = KafkaDo::Table.new(kafka, { 'a' => 1 }, nil, nil)
      all = table.all
      all['c'] = 3

      expect(table['c']).to be_nil
    end
  end

  describe '#each' do
    it 'iterates over key-value pairs' do
      table = KafkaDo::Table.new(kafka, { 'a' => 1, 'b' => 2 }, nil, nil)
      pairs = []

      table.each { |k, v| pairs << [k, v] }

      expect(pairs).to contain_exactly(['a', 1], ['b', 2])
    end
  end

  describe '#to_stream' do
    it 'returns a TableStream' do
      table = KafkaDo::Table.new(kafka, {}, nil, nil)
      stream = table.to_stream

      expect(stream).to be_a(KafkaDo::TableStream)
    end
  end

  describe 'Enumerable' do
    it 'includes Enumerable' do
      table = KafkaDo::Table.new(kafka, {}, nil, nil)
      expect(table).to be_a(Enumerable)
    end
  end
end

RSpec.describe KafkaDo::KeyValue do
  describe '#initialize' do
    it 'creates a key-value pair' do
      kv = described_class.new('key', 'value')

      expect(kv.key).to eq('key')
      expect(kv.value).to eq('value')
    end
  end

  describe '#to_a' do
    it 'returns an array' do
      kv = described_class.new('key', 'value')

      expect(kv.to_a).to eq(['key', 'value'])
    end
  end

  describe '#to_h' do
    it 'returns a hash' do
      kv = described_class.new('key', 'value')

      expect(kv.to_h).to eq({ 'key' => 'value' })
    end
  end

  describe '#==' do
    it 'returns true for equal key-value pairs' do
      kv1 = described_class.new('key', 'value')
      kv2 = described_class.new('key', 'value')

      expect(kv1).to eq(kv2)
    end

    it 'returns false for different key-value pairs' do
      kv1 = described_class.new('key1', 'value')
      kv2 = described_class.new('key2', 'value')

      expect(kv1).not_to eq(kv2)
    end
  end

  describe '#hash' do
    it 'returns the same hash for equal pairs' do
      kv1 = described_class.new('key', 'value')
      kv2 = described_class.new('key', 'value')

      expect(kv1.hash).to eq(kv2.hash)
    end
  end
end
