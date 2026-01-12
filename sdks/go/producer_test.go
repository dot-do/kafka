package kafka

import (
	"context"
	"encoding/base64"
	"errors"
	"testing"
	"time"
)

// mockPromise is a mock implementation of RPCPromise.
type mockPromise struct {
	result any
	err    error
}

func (m *mockPromise) Await() (any, error) {
	return m.result, m.err
}

// mockRPCClient is a mock implementation of RPCClient.
type mockRPCClient struct {
	calls       []mockCall
	callIndex   int
	closeCalled bool
	closeErr    error
}

type mockCall struct {
	method string
	args   []any
	result any
	err    error
}

func (m *mockRPCClient) Call(method string, args ...any) RPCPromise {
	if m.callIndex >= len(m.calls) {
		return &mockPromise{nil, errors.New("unexpected call")}
	}
	call := m.calls[m.callIndex]
	m.callIndex++
	return &mockPromise{call.result, call.err}
}

func (m *mockRPCClient) Close() error {
	m.closeCalled = true
	return m.closeErr
}

func TestProducerInit(t *testing.T) {
	kafka := NewKafka([]string{"localhost:9092"})

	t.Run("default settings", func(t *testing.T) {
		producer, err := kafka.NewProducer()
		if err != nil {
			t.Fatalf("NewProducer failed: %v", err)
		}

		if producer.config.Acks != AcksOne {
			t.Errorf("expected Acks to be AcksOne, got %v", producer.config.Acks)
		}
		if producer.config.CompressionType != CompressionNone {
			t.Errorf("expected CompressionType to be CompressionNone, got %v", producer.config.CompressionType)
		}
		if producer.config.BatchSize != 16384 {
			t.Errorf("expected BatchSize to be 16384, got %d", producer.config.BatchSize)
		}
		if producer.config.LingerMs != 0 {
			t.Errorf("expected LingerMs to be 0, got %d", producer.config.LingerMs)
		}
	})

	t.Run("custom settings", func(t *testing.T) {
		producer, err := kafka.NewProducer(
			WithAcks(AcksAll),
			WithCompression(CompressionGzip),
			WithBatchSize(32768),
			WithLingerMs(100),
			WithProducerClientID("custom-client"),
		)
		if err != nil {
			t.Fatalf("NewProducer failed: %v", err)
		}

		if producer.config.Acks != AcksAll {
			t.Errorf("expected Acks to be AcksAll, got %v", producer.config.Acks)
		}
		if producer.config.CompressionType != CompressionGzip {
			t.Errorf("expected CompressionType to be CompressionGzip, got %v", producer.config.CompressionType)
		}
		if producer.config.BatchSize != 32768 {
			t.Errorf("expected BatchSize to be 32768, got %d", producer.config.BatchSize)
		}
		if producer.config.LingerMs != 100 {
			t.Errorf("expected LingerMs to be 100, got %d", producer.config.LingerMs)
		}
		if producer.config.ClientID != "custom-client" {
			t.Errorf("expected ClientID to be 'custom-client', got %s", producer.config.ClientID)
		}
	})
}

func TestProducerConnected(t *testing.T) {
	kafka := NewKafka([]string{"localhost:9092"})
	producer, _ := kafka.NewProducer()

	if producer.Connected() {
		t.Error("expected producer to not be connected initially")
	}

	producer.connected = true
	if !producer.Connected() {
		t.Error("expected producer to be connected")
	}

	producer.closed = true
	if producer.Connected() {
		t.Error("expected producer to not be connected when closed")
	}
}

func TestProducerConnect(t *testing.T) {
	t.Run("successful connection", func(t *testing.T) {
		kafka := NewKafka([]string{"localhost:9092"})
		mockClient := &mockRPCClient{}
		kafka.SetRPCClient(mockClient)

		producer, _ := kafka.NewProducer()
		err := producer.Connect(context.Background())
		if err != nil {
			t.Fatalf("Connect failed: %v", err)
		}

		if !producer.Connected() {
			t.Error("expected producer to be connected")
		}
	})

	t.Run("already connected", func(t *testing.T) {
		kafka := NewKafka([]string{"localhost:9092"})
		mockClient := &mockRPCClient{}
		kafka.SetRPCClient(mockClient)

		producer, _ := kafka.NewProducer()
		producer.connected = true
		producer.client = mockClient

		err := producer.Connect(context.Background())
		if err != nil {
			t.Fatalf("Connect failed: %v", err)
		}
	})

	t.Run("connection failure", func(t *testing.T) {
		kafka := NewKafka([]string{"localhost:9092"})
		// No RPC client set, should fail

		producer, _ := kafka.NewProducer()
		err := producer.Connect(context.Background())
		if err == nil {
			t.Fatal("expected connection error")
		}

		var connErr *ConnectionError
		if !errors.As(err, &connErr) {
			t.Errorf("expected ConnectionError, got %T", err)
		}
	})

	t.Run("connect after close", func(t *testing.T) {
		kafka := NewKafka([]string{"localhost:9092"})
		producer, _ := kafka.NewProducer()
		producer.closed = true

		err := producer.Connect(context.Background())
		if !errors.Is(err, ErrClosed) {
			t.Errorf("expected ErrClosed, got %v", err)
		}
	})
}

func TestProducerClose(t *testing.T) {
	t.Run("close connected producer", func(t *testing.T) {
		kafka := NewKafka([]string{"localhost:9092"})
		mockClient := &mockRPCClient{
			calls: []mockCall{
				{method: "kafka.producer.flush", result: nil},
			},
		}
		kafka.SetRPCClient(mockClient)

		producer, _ := kafka.NewProducer()
		_ = producer.Connect(context.Background())

		err := producer.Close(context.Background())
		if err != nil {
			t.Fatalf("Close failed: %v", err)
		}

		if producer.Connected() {
			t.Error("expected producer to be disconnected after close")
		}
	})

	t.Run("close already closed", func(t *testing.T) {
		kafka := NewKafka([]string{"localhost:9092"})
		producer, _ := kafka.NewProducer()
		producer.closed = true

		err := producer.Close(context.Background())
		if err != nil {
			t.Fatalf("Close failed: %v", err)
		}
	})
}

func TestProducerProduce(t *testing.T) {
	t.Run("successful send", func(t *testing.T) {
		kafka := NewKafka([]string{"localhost:9092"})
		mockClient := &mockRPCClient{
			calls: []mockCall{
				{
					method: "kafka.producer.send",
					result: map[string]any{
						"topic":                 "test-topic",
						"partition":             float64(0),
						"offset":                float64(42),
						"timestamp":             float64(1234567890000),
						"serialized_key_size":   float64(0),
						"serialized_value_size": float64(5),
					},
				},
			},
		}
		kafka.SetRPCClient(mockClient)

		producer, _ := kafka.NewProducer()
		producer.connected = true
		producer.client = mockClient

		metadata, err := producer.Produce(context.Background(), "test-topic", nil, []byte("Hello"))
		if err != nil {
			t.Fatalf("Produce failed: %v", err)
		}

		if metadata.Topic != "test-topic" {
			t.Errorf("expected topic 'test-topic', got %s", metadata.Topic)
		}
		if metadata.Partition != 0 {
			t.Errorf("expected partition 0, got %d", metadata.Partition)
		}
		if metadata.Offset != 42 {
			t.Errorf("expected offset 42, got %d", metadata.Offset)
		}
	})

	t.Run("send with key", func(t *testing.T) {
		kafka := NewKafka([]string{"localhost:9092"})
		mockClient := &mockRPCClient{
			calls: []mockCall{
				{
					method: "kafka.producer.send",
					result: map[string]any{
						"topic":     "test-topic",
						"partition": float64(0),
						"offset":    float64(42),
					},
				},
			},
		}
		kafka.SetRPCClient(mockClient)

		producer, _ := kafka.NewProducer()
		producer.connected = true
		producer.client = mockClient

		_, err := producer.Produce(context.Background(), "test-topic", []byte("my-key"), []byte("Hello"))
		if err != nil {
			t.Fatalf("Produce failed: %v", err)
		}
	})

	t.Run("not connected", func(t *testing.T) {
		kafka := NewKafka([]string{"localhost:9092"})
		producer, _ := kafka.NewProducer()

		_, err := producer.Produce(context.Background(), "test-topic", nil, []byte("Hello"))
		if err == nil {
			t.Fatal("expected error")
		}

		var connErr *ConnectionError
		if !errors.As(err, &connErr) {
			t.Errorf("expected ConnectionError, got %T", err)
		}
	})

	t.Run("closed producer", func(t *testing.T) {
		kafka := NewKafka([]string{"localhost:9092"})
		producer, _ := kafka.NewProducer()
		producer.connected = true
		producer.closed = true

		_, err := producer.Produce(context.Background(), "test-topic", nil, []byte("Hello"))
		if !errors.Is(err, ErrClosed) {
			t.Errorf("expected ErrClosed, got %v", err)
		}
	})

	t.Run("send failure", func(t *testing.T) {
		kafka := NewKafka([]string{"localhost:9092"})
		mockClient := &mockRPCClient{
			calls: []mockCall{
				{
					method: "kafka.producer.send",
					err:    errors.New("send failed"),
				},
			},
		}
		kafka.SetRPCClient(mockClient)

		producer, _ := kafka.NewProducer()
		producer.connected = true
		producer.client = mockClient

		_, err := producer.Produce(context.Background(), "test-topic", nil, []byte("Hello"))
		if err == nil {
			t.Fatal("expected error")
		}

		var prodErr *ProducerError
		if !errors.As(err, &prodErr) {
			t.Errorf("expected ProducerError, got %T", err)
		}
	})
}

func TestProducerProduceRecord(t *testing.T) {
	t.Run("with all fields", func(t *testing.T) {
		kafka := NewKafka([]string{"localhost:9092"})
		mockClient := &mockRPCClient{
			calls: []mockCall{
				{
					method: "kafka.producer.send",
					result: map[string]any{
						"topic":     "test-topic",
						"partition": float64(2),
						"offset":    float64(100),
					},
				},
			},
		}
		kafka.SetRPCClient(mockClient)

		producer, _ := kafka.NewProducer()
		producer.connected = true
		producer.client = mockClient

		record := ProducerRecord{
			Topic:     "test-topic",
			Key:       []byte("key1"),
			Value:     []byte("value1"),
			Partition: 2,
			Timestamp: time.Now(),
			Headers: map[string][]byte{
				"header1": []byte("value1"),
			},
		}

		metadata, err := producer.ProduceRecord(context.Background(), record)
		if err != nil {
			t.Fatalf("ProduceRecord failed: %v", err)
		}

		if metadata.Partition != 2 {
			t.Errorf("expected partition 2, got %d", metadata.Partition)
		}
	})
}

func TestProducerProduceBatch(t *testing.T) {
	t.Run("successful batch", func(t *testing.T) {
		kafka := NewKafka([]string{"localhost:9092"})
		mockClient := &mockRPCClient{
			calls: []mockCall{
				{
					method: "kafka.producer.sendBatch",
					result: []any{
						map[string]any{
							"topic":     "topic1",
							"partition": float64(0),
							"offset":    float64(42),
						},
						map[string]any{
							"topic":     "topic2",
							"partition": float64(0),
							"offset":    float64(43),
						},
					},
				},
			},
		}
		kafka.SetRPCClient(mockClient)

		producer, _ := kafka.NewProducer()
		producer.connected = true
		producer.client = mockClient

		records := []ProducerRecord{
			{Topic: "topic1", Value: []byte("msg1")},
			{Topic: "topic2", Value: []byte("msg2")},
		}

		result, err := producer.ProduceBatch(context.Background(), records)
		if err != nil {
			t.Fatalf("ProduceBatch failed: %v", err)
		}

		if len(result.Successful) != 2 {
			t.Errorf("expected 2 successful, got %d", len(result.Successful))
		}
		if len(result.Failed) != 0 {
			t.Errorf("expected 0 failed, got %d", len(result.Failed))
		}
	})

	t.Run("partial failure", func(t *testing.T) {
		kafka := NewKafka([]string{"localhost:9092"})
		mockClient := &mockRPCClient{
			calls: []mockCall{
				{
					method: "kafka.producer.sendBatch",
					result: []any{
						map[string]any{
							"topic":     "topic1",
							"partition": float64(0),
							"offset":    float64(42),
						},
						map[string]any{
							"error": map[string]any{
								"code":    "TIMEOUT",
								"message": "Request timed out",
							},
						},
					},
				},
			},
		}
		kafka.SetRPCClient(mockClient)

		producer, _ := kafka.NewProducer()
		producer.connected = true
		producer.client = mockClient

		records := []ProducerRecord{
			{Topic: "topic1", Value: []byte("msg1")},
			{Topic: "topic2", Value: []byte("msg2")},
		}

		result, err := producer.ProduceBatch(context.Background(), records)
		if err != nil {
			t.Fatalf("ProduceBatch failed: %v", err)
		}

		if len(result.Successful) != 1 {
			t.Errorf("expected 1 successful, got %d", len(result.Successful))
		}
		if len(result.Failed) != 1 {
			t.Errorf("expected 1 failed, got %d", len(result.Failed))
		}
	})

	t.Run("total failure", func(t *testing.T) {
		kafka := NewKafka([]string{"localhost:9092"})
		mockClient := &mockRPCClient{
			calls: []mockCall{
				{
					method: "kafka.producer.sendBatch",
					err:    errors.New("batch failed"),
				},
			},
		}
		kafka.SetRPCClient(mockClient)

		producer, _ := kafka.NewProducer()
		producer.connected = true
		producer.client = mockClient

		records := []ProducerRecord{
			{Topic: "topic1", Value: []byte("msg1")},
			{Topic: "topic2", Value: []byte("msg2")},
		}

		result, err := producer.ProduceBatch(context.Background(), records)
		if err != nil {
			t.Fatalf("ProduceBatch failed: %v", err)
		}

		if len(result.Successful) != 0 {
			t.Errorf("expected 0 successful, got %d", len(result.Successful))
		}
		if len(result.Failed) != 2 {
			t.Errorf("expected 2 failed, got %d", len(result.Failed))
		}
	})

	t.Run("not connected", func(t *testing.T) {
		kafka := NewKafka([]string{"localhost:9092"})
		producer, _ := kafka.NewProducer()

		records := []ProducerRecord{
			{Topic: "topic1", Value: []byte("msg1")},
		}

		_, err := producer.ProduceBatch(context.Background(), records)
		if err == nil {
			t.Fatal("expected error")
		}

		var connErr *ConnectionError
		if !errors.As(err, &connErr) {
			t.Errorf("expected ConnectionError, got %T", err)
		}
	})
}

func TestProducerFlush(t *testing.T) {
	t.Run("successful flush", func(t *testing.T) {
		kafka := NewKafka([]string{"localhost:9092"})
		mockClient := &mockRPCClient{
			calls: []mockCall{
				{method: "kafka.producer.flush", result: nil},
			},
		}
		kafka.SetRPCClient(mockClient)

		producer, _ := kafka.NewProducer()
		producer.connected = true
		producer.client = mockClient

		err := producer.Flush(context.Background())
		if err != nil {
			t.Fatalf("Flush failed: %v", err)
		}
	})

	t.Run("not connected", func(t *testing.T) {
		kafka := NewKafka([]string{"localhost:9092"})
		producer, _ := kafka.NewProducer()

		err := producer.Flush(context.Background())
		if err != nil {
			t.Fatalf("Flush should not fail when not connected: %v", err)
		}
	})

	t.Run("flush error ignored", func(t *testing.T) {
		kafka := NewKafka([]string{"localhost:9092"})
		mockClient := &mockRPCClient{
			calls: []mockCall{
				{method: "kafka.producer.flush", err: errors.New("flush failed")},
			},
		}
		kafka.SetRPCClient(mockClient)

		producer, _ := kafka.NewProducer()
		producer.connected = true
		producer.client = mockClient

		err := producer.Flush(context.Background())
		if err != nil {
			t.Fatalf("Flush errors should be ignored: %v", err)
		}
	})
}

func TestProducerPartitionsFor(t *testing.T) {
	t.Run("successful", func(t *testing.T) {
		kafka := NewKafka([]string{"localhost:9092"})
		mockClient := &mockRPCClient{
			calls: []mockCall{
				{
					method: "kafka.producer.partitionsFor",
					result: []any{float64(0), float64(1), float64(2)},
				},
			},
		}
		kafka.SetRPCClient(mockClient)

		producer, _ := kafka.NewProducer()
		producer.connected = true
		producer.client = mockClient

		partitions, err := producer.PartitionsFor(context.Background(), "test-topic")
		if err != nil {
			t.Fatalf("PartitionsFor failed: %v", err)
		}

		if len(partitions) != 3 {
			t.Errorf("expected 3 partitions, got %d", len(partitions))
		}
		if partitions[0] != 0 || partitions[1] != 1 || partitions[2] != 2 {
			t.Errorf("unexpected partitions: %v", partitions)
		}
	})

	t.Run("not connected", func(t *testing.T) {
		kafka := NewKafka([]string{"localhost:9092"})
		producer, _ := kafka.NewProducer()

		_, err := producer.PartitionsFor(context.Background(), "test-topic")
		if err == nil {
			t.Fatal("expected error")
		}

		var connErr *ConnectionError
		if !errors.As(err, &connErr) {
			t.Errorf("expected ConnectionError, got %T", err)
		}
	})

	t.Run("failure", func(t *testing.T) {
		kafka := NewKafka([]string{"localhost:9092"})
		mockClient := &mockRPCClient{
			calls: []mockCall{
				{
					method: "kafka.producer.partitionsFor",
					err:    errors.New("failed"),
				},
			},
		}
		kafka.SetRPCClient(mockClient)

		producer, _ := kafka.NewProducer()
		producer.connected = true
		producer.client = mockClient

		_, err := producer.PartitionsFor(context.Background(), "test-topic")
		if err == nil {
			t.Fatal("expected error")
		}

		var kafkaErr *KafkaError
		if !errors.As(err, &kafkaErr) {
			t.Errorf("expected KafkaError, got %T", err)
		}
	})
}

func TestSerializeProducerRecord(t *testing.T) {
	t.Run("basic record", func(t *testing.T) {
		record := ProducerRecord{
			Topic: "test",
			Value: []byte("Hello"),
		}

		result := serializeProducerRecord(record)

		if result["topic"] != "test" {
			t.Errorf("expected topic 'test', got %v", result["topic"])
		}

		decoded, _ := base64.StdEncoding.DecodeString(result["value"].(string))
		if string(decoded) != "Hello" {
			t.Errorf("expected value 'Hello', got %s", string(decoded))
		}

		if _, ok := result["key"]; ok {
			t.Error("expected no key in result")
		}

		if _, ok := result["partition"]; ok {
			t.Error("expected no partition in result")
		}
	})

	t.Run("full record", func(t *testing.T) {
		record := ProducerRecord{
			Topic:     "test",
			Value:     []byte("Hello"),
			Key:       []byte("key1"),
			Partition: 2,
			Timestamp: time.Unix(1234567890, 0),
			Headers: map[string][]byte{
				"h1": []byte("v1"),
			},
		}

		result := serializeProducerRecord(record)

		if result["topic"] != "test" {
			t.Errorf("expected topic 'test', got %v", result["topic"])
		}

		decodedKey, _ := base64.StdEncoding.DecodeString(result["key"].(string))
		if string(decodedKey) != "key1" {
			t.Errorf("expected key 'key1', got %s", string(decodedKey))
		}

		if result["partition"] != 2 {
			t.Errorf("expected partition 2, got %v", result["partition"])
		}

		if result["timestamp"] != int64(1234567890000) {
			t.Errorf("expected timestamp 1234567890000, got %v", result["timestamp"])
		}

		headers := result["headers"].([]map[string]string)
		if len(headers) != 1 {
			t.Errorf("expected 1 header, got %d", len(headers))
		}
	})
}

func TestParseRecordMetadata(t *testing.T) {
	t.Run("valid metadata", func(t *testing.T) {
		result := map[string]any{
			"topic":                 "test-topic",
			"partition":             float64(0),
			"offset":                float64(42),
			"timestamp":             float64(1234567890000),
			"serialized_key_size":   float64(10),
			"serialized_value_size": float64(20),
		}

		metadata, err := parseRecordMetadata(result)
		if err != nil {
			t.Fatalf("parseRecordMetadata failed: %v", err)
		}

		if metadata.Topic != "test-topic" {
			t.Errorf("expected topic 'test-topic', got %s", metadata.Topic)
		}
		if metadata.Partition != 0 {
			t.Errorf("expected partition 0, got %d", metadata.Partition)
		}
		if metadata.Offset != 42 {
			t.Errorf("expected offset 42, got %d", metadata.Offset)
		}
		if metadata.SerializedKeySize != 10 {
			t.Errorf("expected key size 10, got %d", metadata.SerializedKeySize)
		}
		if metadata.SerializedValueSize != 20 {
			t.Errorf("expected value size 20, got %d", metadata.SerializedValueSize)
		}
	})

	t.Run("invalid format", func(t *testing.T) {
		_, err := parseRecordMetadata("invalid")
		if err == nil {
			t.Fatal("expected error")
		}

		var prodErr *ProducerError
		if !errors.As(err, &prodErr) {
			t.Errorf("expected ProducerError, got %T", err)
		}
	})
}

func TestParseBatchResult(t *testing.T) {
	t.Run("all successful", func(t *testing.T) {
		result := []any{
			map[string]any{
				"topic":     "topic1",
				"partition": float64(0),
				"offset":    float64(42),
			},
			map[string]any{
				"topic":     "topic2",
				"partition": float64(0),
				"offset":    float64(43),
			},
		}

		records := []ProducerRecord{
			{Topic: "topic1", Value: []byte("msg1")},
			{Topic: "topic2", Value: []byte("msg2")},
		}

		batchResult, err := parseBatchResult(result, records)
		if err != nil {
			t.Fatalf("parseBatchResult failed: %v", err)
		}

		if len(batchResult.Successful) != 2 {
			t.Errorf("expected 2 successful, got %d", len(batchResult.Successful))
		}
		if len(batchResult.Failed) != 0 {
			t.Errorf("expected 0 failed, got %d", len(batchResult.Failed))
		}
	})

	t.Run("with errors", func(t *testing.T) {
		result := []any{
			map[string]any{
				"topic":     "topic1",
				"partition": float64(0),
				"offset":    float64(42),
			},
			map[string]any{
				"error": map[string]any{
					"code":    "TIMEOUT",
					"message": "Request timed out",
				},
			},
		}

		records := []ProducerRecord{
			{Topic: "topic1", Value: []byte("msg1")},
			{Topic: "topic2", Value: []byte("msg2")},
		}

		batchResult, err := parseBatchResult(result, records)
		if err != nil {
			t.Fatalf("parseBatchResult failed: %v", err)
		}

		if len(batchResult.Successful) != 1 {
			t.Errorf("expected 1 successful, got %d", len(batchResult.Successful))
		}
		if len(batchResult.Failed) != 1 {
			t.Errorf("expected 1 failed, got %d", len(batchResult.Failed))
		}
	})

	t.Run("invalid format", func(t *testing.T) {
		_, err := parseBatchResult("invalid", nil)
		if err == nil {
			t.Fatal("expected error")
		}

		var prodErr *ProducerError
		if !errors.As(err, &prodErr) {
			t.Errorf("expected ProducerError, got %T", err)
		}
	})
}

func TestParseIntSlice(t *testing.T) {
	t.Run("float64 slice", func(t *testing.T) {
		result := []any{float64(0), float64(1), float64(2)}
		ints, err := parseIntSlice(result)
		if err != nil {
			t.Fatalf("parseIntSlice failed: %v", err)
		}

		if len(ints) != 3 {
			t.Errorf("expected 3 elements, got %d", len(ints))
		}
		if ints[0] != 0 || ints[1] != 1 || ints[2] != 2 {
			t.Errorf("unexpected values: %v", ints)
		}
	})

	t.Run("int slice", func(t *testing.T) {
		result := []int{0, 1, 2}
		ints, err := parseIntSlice(result)
		if err != nil {
			t.Fatalf("parseIntSlice failed: %v", err)
		}

		if len(ints) != 3 {
			t.Errorf("expected 3 elements, got %d", len(ints))
		}
	})

	t.Run("float64 typed slice", func(t *testing.T) {
		result := []float64{0.0, 1.0, 2.0}
		ints, err := parseIntSlice(result)
		if err != nil {
			t.Fatalf("parseIntSlice failed: %v", err)
		}

		if len(ints) != 3 {
			t.Errorf("expected 3 elements, got %d", len(ints))
		}
	})

	t.Run("mixed types in any slice", func(t *testing.T) {
		result := []any{int(0), int64(1), float64(2)}
		ints, err := parseIntSlice(result)
		if err != nil {
			t.Fatalf("parseIntSlice failed: %v", err)
		}

		if len(ints) != 3 {
			t.Errorf("expected 3 elements, got %d", len(ints))
		}
	})

	t.Run("invalid type", func(t *testing.T) {
		_, err := parseIntSlice("invalid")
		if err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("invalid element type", func(t *testing.T) {
		result := []any{"invalid"}
		_, err := parseIntSlice(result)
		if err == nil {
			t.Fatal("expected error")
		}
	})
}
