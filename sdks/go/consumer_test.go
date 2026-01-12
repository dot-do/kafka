package kafka

import (
	"context"
	"encoding/base64"
	"errors"
	"testing"
)

func TestConsumerInit(t *testing.T) {
	kafka := NewKafka([]string{"localhost:9092"})

	t.Run("default settings", func(t *testing.T) {
		consumer, err := kafka.NewConsumer("my-group")
		if err != nil {
			t.Fatalf("NewConsumer failed: %v", err)
		}

		if consumer.GroupID() != "my-group" {
			t.Errorf("expected GroupID 'my-group', got %s", consumer.GroupID())
		}
		if !consumer.config.AutoCommit {
			t.Error("expected AutoCommit to be true")
		}
		if consumer.config.AutoCommitIntervalMs != 5000 {
			t.Errorf("expected AutoCommitIntervalMs 5000, got %d", consumer.config.AutoCommitIntervalMs)
		}
		if consumer.config.SessionTimeoutMs != 10000 {
			t.Errorf("expected SessionTimeoutMs 10000, got %d", consumer.config.SessionTimeoutMs)
		}
		if consumer.config.MaxPollRecords != 500 {
			t.Errorf("expected MaxPollRecords 500, got %d", consumer.config.MaxPollRecords)
		}
		if consumer.config.IsolationLevel != IsolationReadUncommitted {
			t.Errorf("expected IsolationReadUncommitted, got %v", consumer.config.IsolationLevel)
		}
		if consumer.config.AutoOffsetReset != OffsetLatest {
			t.Errorf("expected OffsetLatest, got %v", consumer.config.AutoOffsetReset)
		}
	})

	t.Run("custom settings", func(t *testing.T) {
		consumer, err := kafka.NewConsumer("my-group",
			WithAutoCommit(false),
			WithAutoCommitInterval(10000),
			WithSessionTimeout(20000),
			WithHeartbeatInterval(5000),
			WithMaxPollRecords(1000),
			WithMaxPollInterval(600000),
			WithIsolationLevel(IsolationReadCommitted),
			WithAutoOffsetReset(OffsetEarliest),
			WithConsumerClientID("custom-client"),
		)
		if err != nil {
			t.Fatalf("NewConsumer failed: %v", err)
		}

		if consumer.config.AutoCommit {
			t.Error("expected AutoCommit to be false")
		}
		if consumer.config.AutoCommitIntervalMs != 10000 {
			t.Errorf("expected AutoCommitIntervalMs 10000, got %d", consumer.config.AutoCommitIntervalMs)
		}
		if consumer.config.SessionTimeoutMs != 20000 {
			t.Errorf("expected SessionTimeoutMs 20000, got %d", consumer.config.SessionTimeoutMs)
		}
		if consumer.config.HeartbeatIntervalMs != 5000 {
			t.Errorf("expected HeartbeatIntervalMs 5000, got %d", consumer.config.HeartbeatIntervalMs)
		}
		if consumer.config.MaxPollRecords != 1000 {
			t.Errorf("expected MaxPollRecords 1000, got %d", consumer.config.MaxPollRecords)
		}
		if consumer.config.MaxPollIntervalMs != 600000 {
			t.Errorf("expected MaxPollIntervalMs 600000, got %d", consumer.config.MaxPollIntervalMs)
		}
		if consumer.config.IsolationLevel != IsolationReadCommitted {
			t.Errorf("expected IsolationReadCommitted, got %v", consumer.config.IsolationLevel)
		}
		if consumer.config.AutoOffsetReset != OffsetEarliest {
			t.Errorf("expected OffsetEarliest, got %v", consumer.config.AutoOffsetReset)
		}
		if consumer.config.ClientID != "custom-client" {
			t.Errorf("expected ClientID 'custom-client', got %s", consumer.config.ClientID)
		}
	})
}

func TestConsumerConnected(t *testing.T) {
	kafka := NewKafka([]string{"localhost:9092"})
	consumer, _ := kafka.NewConsumer("my-group")

	if consumer.Connected() {
		t.Error("expected consumer to not be connected initially")
	}

	consumer.connected = true
	if !consumer.Connected() {
		t.Error("expected consumer to be connected")
	}

	consumer.closed = true
	if consumer.Connected() {
		t.Error("expected consumer to not be connected when closed")
	}
}

func TestConsumerConnect(t *testing.T) {
	t.Run("successful connection", func(t *testing.T) {
		kafka := NewKafka([]string{"localhost:9092"})
		mockClient := &mockRPCClient{}
		kafka.SetRPCClient(mockClient)

		consumer, _ := kafka.NewConsumer("my-group")
		err := consumer.Connect(context.Background())
		if err != nil {
			t.Fatalf("Connect failed: %v", err)
		}

		if !consumer.Connected() {
			t.Error("expected consumer to be connected")
		}
	})

	t.Run("already connected", func(t *testing.T) {
		kafka := NewKafka([]string{"localhost:9092"})
		mockClient := &mockRPCClient{}
		kafka.SetRPCClient(mockClient)

		consumer, _ := kafka.NewConsumer("my-group")
		consumer.connected = true
		consumer.client = mockClient

		err := consumer.Connect(context.Background())
		if err != nil {
			t.Fatalf("Connect failed: %v", err)
		}
	})

	t.Run("connection failure", func(t *testing.T) {
		kafka := NewKafka([]string{"localhost:9092"})

		consumer, _ := kafka.NewConsumer("my-group")
		err := consumer.Connect(context.Background())
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
		consumer, _ := kafka.NewConsumer("my-group")
		consumer.closed = true

		err := consumer.Connect(context.Background())
		if !errors.Is(err, ErrClosed) {
			t.Errorf("expected ErrClosed, got %v", err)
		}
	})
}

func TestConsumerClose(t *testing.T) {
	t.Run("close connected consumer", func(t *testing.T) {
		kafka := NewKafka([]string{"localhost:9092"})
		mockClient := &mockRPCClient{
			calls: []mockCall{
				{method: "kafka.consumer.commit", result: nil},
			},
		}
		kafka.SetRPCClient(mockClient)

		consumer, _ := kafka.NewConsumer("my-group")
		consumer.connected = true
		consumer.client = mockClient

		err := consumer.Close(context.Background())
		if err != nil {
			t.Fatalf("Close failed: %v", err)
		}

		if consumer.Connected() {
			t.Error("expected consumer to be disconnected after close")
		}
	})

	t.Run("close already closed", func(t *testing.T) {
		kafka := NewKafka([]string{"localhost:9092"})
		consumer, _ := kafka.NewConsumer("my-group")
		consumer.closed = true

		err := consumer.Close(context.Background())
		if err != nil {
			t.Fatalf("Close failed: %v", err)
		}
	})
}

func TestConsumerSubscribe(t *testing.T) {
	t.Run("successful subscribe", func(t *testing.T) {
		kafka := NewKafka([]string{"localhost:9092"})
		mockClient := &mockRPCClient{
			calls: []mockCall{
				{method: "kafka.consumer.subscribe", result: nil},
			},
		}
		kafka.SetRPCClient(mockClient)

		consumer, _ := kafka.NewConsumer("my-group")
		consumer.connected = true
		consumer.client = mockClient

		err := consumer.Subscribe(context.Background(), []string{"topic1", "topic2"})
		if err != nil {
			t.Fatalf("Subscribe failed: %v", err)
		}

		subs := consumer.Subscriptions()
		if len(subs) != 2 {
			t.Errorf("expected 2 subscriptions, got %d", len(subs))
		}
	})

	t.Run("not connected", func(t *testing.T) {
		kafka := NewKafka([]string{"localhost:9092"})
		consumer, _ := kafka.NewConsumer("my-group")

		err := consumer.Subscribe(context.Background(), []string{"topic1"})
		if err == nil {
			t.Fatal("expected error")
		}

		var connErr *ConnectionError
		if !errors.As(err, &connErr) {
			t.Errorf("expected ConnectionError, got %T", err)
		}
	})

	t.Run("subscribe failure", func(t *testing.T) {
		kafka := NewKafka([]string{"localhost:9092"})
		mockClient := &mockRPCClient{
			calls: []mockCall{
				{method: "kafka.consumer.subscribe", err: errors.New("subscribe failed")},
			},
		}
		kafka.SetRPCClient(mockClient)

		consumer, _ := kafka.NewConsumer("my-group")
		consumer.connected = true
		consumer.client = mockClient

		err := consumer.Subscribe(context.Background(), []string{"topic1"})
		if err == nil {
			t.Fatal("expected error")
		}

		var consumerErr *ConsumerError
		if !errors.As(err, &consumerErr) {
			t.Errorf("expected ConsumerError, got %T", err)
		}
	})
}

func TestConsumerUnsubscribe(t *testing.T) {
	t.Run("successful unsubscribe", func(t *testing.T) {
		kafka := NewKafka([]string{"localhost:9092"})
		mockClient := &mockRPCClient{
			calls: []mockCall{
				{method: "kafka.consumer.unsubscribe", result: nil},
			},
		}
		kafka.SetRPCClient(mockClient)

		consumer, _ := kafka.NewConsumer("my-group")
		consumer.connected = true
		consumer.client = mockClient
		consumer.subscriptions["topic1"] = struct{}{}

		err := consumer.Unsubscribe(context.Background())
		if err != nil {
			t.Fatalf("Unsubscribe failed: %v", err)
		}

		if len(consumer.Subscriptions()) != 0 {
			t.Error("expected no subscriptions after unsubscribe")
		}
	})

	t.Run("not connected", func(t *testing.T) {
		kafka := NewKafka([]string{"localhost:9092"})
		consumer, _ := kafka.NewConsumer("my-group")

		err := consumer.Unsubscribe(context.Background())
		if err == nil {
			t.Fatal("expected error")
		}
	})
}

func TestConsumerAssign(t *testing.T) {
	t.Run("successful assign", func(t *testing.T) {
		kafka := NewKafka([]string{"localhost:9092"})
		mockClient := &mockRPCClient{
			calls: []mockCall{
				{method: "kafka.consumer.assign", result: nil},
			},
		}
		kafka.SetRPCClient(mockClient)

		consumer, _ := kafka.NewConsumer("my-group")
		consumer.connected = true
		consumer.client = mockClient

		partitions := []TopicPartition{
			{Topic: "topic1", Partition: 0},
			{Topic: "topic1", Partition: 1},
		}

		err := consumer.Assign(context.Background(), partitions)
		if err != nil {
			t.Fatalf("Assign failed: %v", err)
		}

		assignments := consumer.Assignments()
		if len(assignments) != 2 {
			t.Errorf("expected 2 assignments, got %d", len(assignments))
		}
	})

	t.Run("not connected", func(t *testing.T) {
		kafka := NewKafka([]string{"localhost:9092"})
		consumer, _ := kafka.NewConsumer("my-group")

		err := consumer.Assign(context.Background(), []TopicPartition{{Topic: "topic1", Partition: 0}})
		if err == nil {
			t.Fatal("expected error")
		}
	})
}

func TestConsumerPoll(t *testing.T) {
	t.Run("successful poll", func(t *testing.T) {
		kafka := NewKafka([]string{"localhost:9092"})
		mockClient := &mockRPCClient{
			calls: []mockCall{
				{
					method: "kafka.consumer.poll",
					result: []any{
						map[string]any{
							"topic":     "test-topic",
							"partition": float64(0),
							"offset":    float64(42),
							"timestamp": float64(1234567890000),
							"key":       base64.StdEncoding.EncodeToString([]byte("key1")),
							"value":     base64.StdEncoding.EncodeToString([]byte("value1")),
							"headers": []any{
								map[string]any{
									"key":   "header1",
									"value": base64.StdEncoding.EncodeToString([]byte("hvalue1")),
								},
							},
						},
					},
				},
			},
		}
		kafka.SetRPCClient(mockClient)

		consumer, _ := kafka.NewConsumer("my-group")
		consumer.connected = true
		consumer.client = mockClient

		messages, err := consumer.Poll(context.Background(), 1000)
		if err != nil {
			t.Fatalf("Poll failed: %v", err)
		}

		if len(messages) != 1 {
			t.Fatalf("expected 1 message, got %d", len(messages))
		}

		msg := messages[0]
		if msg.Topic != "test-topic" {
			t.Errorf("expected topic 'test-topic', got %s", msg.Topic)
		}
		if msg.Partition != 0 {
			t.Errorf("expected partition 0, got %d", msg.Partition)
		}
		if msg.Offset != 42 {
			t.Errorf("expected offset 42, got %d", msg.Offset)
		}
		if string(msg.Key) != "key1" {
			t.Errorf("expected key 'key1', got %s", string(msg.Key))
		}
		if string(msg.Value) != "value1" {
			t.Errorf("expected value 'value1', got %s", string(msg.Value))
		}
		if string(msg.Headers["header1"]) != "hvalue1" {
			t.Errorf("expected header value 'hvalue1', got %s", string(msg.Headers["header1"]))
		}
	})

	t.Run("empty poll", func(t *testing.T) {
		kafka := NewKafka([]string{"localhost:9092"})
		mockClient := &mockRPCClient{
			calls: []mockCall{
				{method: "kafka.consumer.poll", result: []any{}},
			},
		}
		kafka.SetRPCClient(mockClient)

		consumer, _ := kafka.NewConsumer("my-group")
		consumer.connected = true
		consumer.client = mockClient

		messages, err := consumer.Poll(context.Background(), 1000)
		if err != nil {
			t.Fatalf("Poll failed: %v", err)
		}

		if len(messages) != 0 {
			t.Errorf("expected 0 messages, got %d", len(messages))
		}
	})

	t.Run("not connected", func(t *testing.T) {
		kafka := NewKafka([]string{"localhost:9092"})
		consumer, _ := kafka.NewConsumer("my-group")

		_, err := consumer.Poll(context.Background(), 1000)
		if err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("poll failure", func(t *testing.T) {
		kafka := NewKafka([]string{"localhost:9092"})
		mockClient := &mockRPCClient{
			calls: []mockCall{
				{method: "kafka.consumer.poll", err: errors.New("poll failed")},
			},
		}
		kafka.SetRPCClient(mockClient)

		consumer, _ := kafka.NewConsumer("my-group")
		consumer.connected = true
		consumer.client = mockClient

		_, err := consumer.Poll(context.Background(), 1000)
		if err == nil {
			t.Fatal("expected error")
		}

		var consumerErr *ConsumerError
		if !errors.As(err, &consumerErr) {
			t.Errorf("expected ConsumerError, got %T", err)
		}
	})
}

func TestConsumerCommit(t *testing.T) {
	t.Run("commit all", func(t *testing.T) {
		kafka := NewKafka([]string{"localhost:9092"})
		mockClient := &mockRPCClient{
			calls: []mockCall{
				{method: "kafka.consumer.commit", result: nil},
			},
		}
		kafka.SetRPCClient(mockClient)

		consumer, _ := kafka.NewConsumer("my-group")
		consumer.connected = true
		consumer.client = mockClient

		err := consumer.Commit(context.Background(), nil)
		if err != nil {
			t.Fatalf("Commit failed: %v", err)
		}
	})

	t.Run("commit specific offsets", func(t *testing.T) {
		kafka := NewKafka([]string{"localhost:9092"})
		mockClient := &mockRPCClient{
			calls: []mockCall{
				{method: "kafka.consumer.commit", result: nil},
			},
		}
		kafka.SetRPCClient(mockClient)

		consumer, _ := kafka.NewConsumer("my-group")
		consumer.connected = true
		consumer.client = mockClient

		offsets := map[TopicPartition]int64{
			{Topic: "topic1", Partition: 0}: 100,
			{Topic: "topic1", Partition: 1}: 200,
		}

		err := consumer.Commit(context.Background(), offsets)
		if err != nil {
			t.Fatalf("Commit failed: %v", err)
		}
	})

	t.Run("not connected", func(t *testing.T) {
		kafka := NewKafka([]string{"localhost:9092"})
		consumer, _ := kafka.NewConsumer("my-group")

		err := consumer.Commit(context.Background(), nil)
		if err == nil {
			t.Fatal("expected error")
		}
	})
}

func TestConsumerCommitted(t *testing.T) {
	t.Run("get committed offsets", func(t *testing.T) {
		kafka := NewKafka([]string{"localhost:9092"})
		mockClient := &mockRPCClient{
			calls: []mockCall{
				{
					method: "kafka.consumer.committed",
					result: []any{
						map[string]any{
							"topic":     "topic1",
							"partition": float64(0),
							"offset":    float64(100),
						},
					},
				},
			},
		}
		kafka.SetRPCClient(mockClient)

		consumer, _ := kafka.NewConsumer("my-group")
		consumer.connected = true
		consumer.client = mockClient

		partitions := []TopicPartition{{Topic: "topic1", Partition: 0}}
		offsets, err := consumer.Committed(context.Background(), partitions)
		if err != nil {
			t.Fatalf("Committed failed: %v", err)
		}

		if offsets[TopicPartition{Topic: "topic1", Partition: 0}] != 100 {
			t.Errorf("expected offset 100, got %d", offsets[TopicPartition{Topic: "topic1", Partition: 0}])
		}
	})

	t.Run("not connected", func(t *testing.T) {
		kafka := NewKafka([]string{"localhost:9092"})
		consumer, _ := kafka.NewConsumer("my-group")

		_, err := consumer.Committed(context.Background(), []TopicPartition{{Topic: "topic1", Partition: 0}})
		if err == nil {
			t.Fatal("expected error")
		}
	})
}

func TestConsumerSeek(t *testing.T) {
	t.Run("successful seek", func(t *testing.T) {
		kafka := NewKafka([]string{"localhost:9092"})
		mockClient := &mockRPCClient{
			calls: []mockCall{
				{method: "kafka.consumer.seek", result: nil},
			},
		}
		kafka.SetRPCClient(mockClient)

		consumer, _ := kafka.NewConsumer("my-group")
		consumer.connected = true
		consumer.client = mockClient

		err := consumer.Seek(context.Background(), TopicPartition{Topic: "topic1", Partition: 0}, 100)
		if err != nil {
			t.Fatalf("Seek failed: %v", err)
		}
	})

	t.Run("not connected", func(t *testing.T) {
		kafka := NewKafka([]string{"localhost:9092"})
		consumer, _ := kafka.NewConsumer("my-group")

		err := consumer.Seek(context.Background(), TopicPartition{Topic: "topic1", Partition: 0}, 100)
		if err == nil {
			t.Fatal("expected error")
		}
	})
}

func TestConsumerSeekToBeginning(t *testing.T) {
	t.Run("seek all partitions", func(t *testing.T) {
		kafka := NewKafka([]string{"localhost:9092"})
		mockClient := &mockRPCClient{
			calls: []mockCall{
				{method: "kafka.consumer.seekToBeginning", result: nil},
			},
		}
		kafka.SetRPCClient(mockClient)

		consumer, _ := kafka.NewConsumer("my-group")
		consumer.connected = true
		consumer.client = mockClient

		err := consumer.SeekToBeginning(context.Background(), nil)
		if err != nil {
			t.Fatalf("SeekToBeginning failed: %v", err)
		}
	})

	t.Run("seek specific partitions", func(t *testing.T) {
		kafka := NewKafka([]string{"localhost:9092"})
		mockClient := &mockRPCClient{
			calls: []mockCall{
				{method: "kafka.consumer.seekToBeginning", result: nil},
			},
		}
		kafka.SetRPCClient(mockClient)

		consumer, _ := kafka.NewConsumer("my-group")
		consumer.connected = true
		consumer.client = mockClient

		partitions := []TopicPartition{{Topic: "topic1", Partition: 0}}
		err := consumer.SeekToBeginning(context.Background(), partitions)
		if err != nil {
			t.Fatalf("SeekToBeginning failed: %v", err)
		}
	})

	t.Run("not connected", func(t *testing.T) {
		kafka := NewKafka([]string{"localhost:9092"})
		consumer, _ := kafka.NewConsumer("my-group")

		err := consumer.SeekToBeginning(context.Background(), nil)
		if err == nil {
			t.Fatal("expected error")
		}
	})
}

func TestConsumerSeekToEnd(t *testing.T) {
	t.Run("seek all partitions", func(t *testing.T) {
		kafka := NewKafka([]string{"localhost:9092"})
		mockClient := &mockRPCClient{
			calls: []mockCall{
				{method: "kafka.consumer.seekToEnd", result: nil},
			},
		}
		kafka.SetRPCClient(mockClient)

		consumer, _ := kafka.NewConsumer("my-group")
		consumer.connected = true
		consumer.client = mockClient

		err := consumer.SeekToEnd(context.Background(), nil)
		if err != nil {
			t.Fatalf("SeekToEnd failed: %v", err)
		}
	})

	t.Run("seek specific partitions", func(t *testing.T) {
		kafka := NewKafka([]string{"localhost:9092"})
		mockClient := &mockRPCClient{
			calls: []mockCall{
				{method: "kafka.consumer.seekToEnd", result: nil},
			},
		}
		kafka.SetRPCClient(mockClient)

		consumer, _ := kafka.NewConsumer("my-group")
		consumer.connected = true
		consumer.client = mockClient

		partitions := []TopicPartition{{Topic: "topic1", Partition: 0}}
		err := consumer.SeekToEnd(context.Background(), partitions)
		if err != nil {
			t.Fatalf("SeekToEnd failed: %v", err)
		}
	})

	t.Run("not connected", func(t *testing.T) {
		kafka := NewKafka([]string{"localhost:9092"})
		consumer, _ := kafka.NewConsumer("my-group")

		err := consumer.SeekToEnd(context.Background(), nil)
		if err == nil {
			t.Fatal("expected error")
		}
	})
}

func TestConsumerPosition(t *testing.T) {
	t.Run("get position", func(t *testing.T) {
		kafka := NewKafka([]string{"localhost:9092"})
		mockClient := &mockRPCClient{
			calls: []mockCall{
				{method: "kafka.consumer.position", result: float64(42)},
			},
		}
		kafka.SetRPCClient(mockClient)

		consumer, _ := kafka.NewConsumer("my-group")
		consumer.connected = true
		consumer.client = mockClient

		pos, err := consumer.Position(context.Background(), TopicPartition{Topic: "topic1", Partition: 0})
		if err != nil {
			t.Fatalf("Position failed: %v", err)
		}

		if pos != 42 {
			t.Errorf("expected position 42, got %d", pos)
		}
	})

	t.Run("get position int64", func(t *testing.T) {
		kafka := NewKafka([]string{"localhost:9092"})
		mockClient := &mockRPCClient{
			calls: []mockCall{
				{method: "kafka.consumer.position", result: int64(42)},
			},
		}
		kafka.SetRPCClient(mockClient)

		consumer, _ := kafka.NewConsumer("my-group")
		consumer.connected = true
		consumer.client = mockClient

		pos, err := consumer.Position(context.Background(), TopicPartition{Topic: "topic1", Partition: 0})
		if err != nil {
			t.Fatalf("Position failed: %v", err)
		}

		if pos != 42 {
			t.Errorf("expected position 42, got %d", pos)
		}
	})

	t.Run("get position int", func(t *testing.T) {
		kafka := NewKafka([]string{"localhost:9092"})
		mockClient := &mockRPCClient{
			calls: []mockCall{
				{method: "kafka.consumer.position", result: int(42)},
			},
		}
		kafka.SetRPCClient(mockClient)

		consumer, _ := kafka.NewConsumer("my-group")
		consumer.connected = true
		consumer.client = mockClient

		pos, err := consumer.Position(context.Background(), TopicPartition{Topic: "topic1", Partition: 0})
		if err != nil {
			t.Fatalf("Position failed: %v", err)
		}

		if pos != 42 {
			t.Errorf("expected position 42, got %d", pos)
		}
	})

	t.Run("invalid response", func(t *testing.T) {
		kafka := NewKafka([]string{"localhost:9092"})
		mockClient := &mockRPCClient{
			calls: []mockCall{
				{method: "kafka.consumer.position", result: "invalid"},
			},
		}
		kafka.SetRPCClient(mockClient)

		consumer, _ := kafka.NewConsumer("my-group")
		consumer.connected = true
		consumer.client = mockClient

		_, err := consumer.Position(context.Background(), TopicPartition{Topic: "topic1", Partition: 0})
		if err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("not connected", func(t *testing.T) {
		kafka := NewKafka([]string{"localhost:9092"})
		consumer, _ := kafka.NewConsumer("my-group")

		_, err := consumer.Position(context.Background(), TopicPartition{Topic: "topic1", Partition: 0})
		if err == nil {
			t.Fatal("expected error")
		}
	})
}

func TestConsumerPause(t *testing.T) {
	t.Run("successful pause", func(t *testing.T) {
		kafka := NewKafka([]string{"localhost:9092"})
		mockClient := &mockRPCClient{
			calls: []mockCall{
				{method: "kafka.consumer.pause", result: nil},
			},
		}
		kafka.SetRPCClient(mockClient)

		consumer, _ := kafka.NewConsumer("my-group")
		consumer.connected = true
		consumer.client = mockClient

		partitions := []TopicPartition{{Topic: "topic1", Partition: 0}}
		err := consumer.Pause(context.Background(), partitions)
		if err != nil {
			t.Fatalf("Pause failed: %v", err)
		}

		paused := consumer.Paused()
		if len(paused) != 1 {
			t.Errorf("expected 1 paused partition, got %d", len(paused))
		}
	})

	t.Run("not connected", func(t *testing.T) {
		kafka := NewKafka([]string{"localhost:9092"})
		consumer, _ := kafka.NewConsumer("my-group")

		err := consumer.Pause(context.Background(), []TopicPartition{{Topic: "topic1", Partition: 0}})
		if err == nil {
			t.Fatal("expected error")
		}
	})
}

func TestConsumerResume(t *testing.T) {
	t.Run("successful resume", func(t *testing.T) {
		kafka := NewKafka([]string{"localhost:9092"})
		mockClient := &mockRPCClient{
			calls: []mockCall{
				{method: "kafka.consumer.resume", result: nil},
			},
		}
		kafka.SetRPCClient(mockClient)

		consumer, _ := kafka.NewConsumer("my-group")
		consumer.connected = true
		consumer.client = mockClient
		consumer.paused[TopicPartition{Topic: "topic1", Partition: 0}] = struct{}{}

		partitions := []TopicPartition{{Topic: "topic1", Partition: 0}}
		err := consumer.Resume(context.Background(), partitions)
		if err != nil {
			t.Fatalf("Resume failed: %v", err)
		}

		paused := consumer.Paused()
		if len(paused) != 0 {
			t.Errorf("expected 0 paused partitions, got %d", len(paused))
		}
	})

	t.Run("not connected", func(t *testing.T) {
		kafka := NewKafka([]string{"localhost:9092"})
		consumer, _ := kafka.NewConsumer("my-group")

		err := consumer.Resume(context.Background(), []TopicPartition{{Topic: "topic1", Partition: 0}})
		if err == nil {
			t.Fatal("expected error")
		}
	})
}

func TestConsumerMessages(t *testing.T) {
	t.Run("returns message channel", func(t *testing.T) {
		kafka := NewKafka([]string{"localhost:9092"})
		consumer, _ := kafka.NewConsumer("my-group")
		consumer.connected = true

		// Just test that Messages() returns a channel and doesn't panic
		// when called multiple times
		ch1 := consumer.Messages()
		ch2 := consumer.Messages()

		if ch1 == nil {
			t.Error("expected non-nil channel")
		}
		if ch1 != ch2 {
			t.Error("expected same channel on second call")
		}

		// Close the consumer to clean up
		consumer.closed = true
		consumer.pollCancel()
	})
}

func TestParseMessages(t *testing.T) {
	t.Run("valid messages", func(t *testing.T) {
		result := []any{
			map[string]any{
				"topic":     "test-topic",
				"partition": float64(0),
				"offset":    float64(42),
				"timestamp": float64(1234567890000),
				"key":       base64.StdEncoding.EncodeToString([]byte("key1")),
				"value":     base64.StdEncoding.EncodeToString([]byte("value1")),
			},
		}

		messages, err := parseMessages(result)
		if err != nil {
			t.Fatalf("parseMessages failed: %v", err)
		}

		if len(messages) != 1 {
			t.Errorf("expected 1 message, got %d", len(messages))
		}
	})

	t.Run("nil result", func(t *testing.T) {
		messages, err := parseMessages(nil)
		if err != nil {
			t.Fatalf("parseMessages failed: %v", err)
		}

		if len(messages) != 0 {
			t.Errorf("expected 0 messages, got %d", len(messages))
		}
	})

	t.Run("invalid format", func(t *testing.T) {
		_, err := parseMessages("invalid")
		if err == nil {
			t.Fatal("expected error")
		}
	})
}

func TestParseMessage(t *testing.T) {
	t.Run("valid message", func(t *testing.T) {
		result := map[string]any{
			"topic":     "test-topic",
			"partition": float64(0),
			"offset":    float64(42),
			"timestamp": float64(1234567890000),
			"key":       base64.StdEncoding.EncodeToString([]byte("key1")),
			"value":     base64.StdEncoding.EncodeToString([]byte("value1")),
			"headers": []any{
				map[string]any{
					"key":   "h1",
					"value": base64.StdEncoding.EncodeToString([]byte("v1")),
				},
			},
		}

		msg, err := parseMessage(result)
		if err != nil {
			t.Fatalf("parseMessage failed: %v", err)
		}

		if msg.Topic != "test-topic" {
			t.Errorf("expected topic 'test-topic', got %s", msg.Topic)
		}
		if string(msg.Key) != "key1" {
			t.Errorf("expected key 'key1', got %s", string(msg.Key))
		}
		if string(msg.Value) != "value1" {
			t.Errorf("expected value 'value1', got %s", string(msg.Value))
		}
		if string(msg.Headers["h1"]) != "v1" {
			t.Errorf("expected header value 'v1', got %s", string(msg.Headers["h1"]))
		}
	})

	t.Run("invalid format", func(t *testing.T) {
		_, err := parseMessage("invalid")
		if err == nil {
			t.Fatal("expected error")
		}
	})
}

func TestParseCommittedOffsets(t *testing.T) {
	t.Run("valid offsets", func(t *testing.T) {
		result := []any{
			map[string]any{
				"topic":     "topic1",
				"partition": float64(0),
				"offset":    float64(100),
			},
		}

		offsets, err := parseCommittedOffsets(result)
		if err != nil {
			t.Fatalf("parseCommittedOffsets failed: %v", err)
		}

		if offsets[TopicPartition{Topic: "topic1", Partition: 0}] != 100 {
			t.Errorf("expected offset 100")
		}
	})

	t.Run("invalid format", func(t *testing.T) {
		_, err := parseCommittedOffsets("invalid")
		if err == nil {
			t.Fatal("expected error")
		}
	})
}
