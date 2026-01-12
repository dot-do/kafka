package kafka

import (
	"context"
	"errors"
	"testing"
)

func TestAdminInit(t *testing.T) {
	kafka := NewKafka([]string{"localhost:9092"})

	t.Run("default settings", func(t *testing.T) {
		admin, err := kafka.NewAdmin()
		if err != nil {
			t.Fatalf("NewAdmin failed: %v", err)
		}

		if admin.config.RequestTimeoutMs != 30000 {
			t.Errorf("expected RequestTimeoutMs 30000, got %d", admin.config.RequestTimeoutMs)
		}
	})

	t.Run("custom settings", func(t *testing.T) {
		admin, err := kafka.NewAdmin(WithRequestTimeout(60000))
		if err != nil {
			t.Fatalf("NewAdmin failed: %v", err)
		}

		if admin.config.RequestTimeoutMs != 60000 {
			t.Errorf("expected RequestTimeoutMs 60000, got %d", admin.config.RequestTimeoutMs)
		}
	})
}

func TestAdminConnected(t *testing.T) {
	kafka := NewKafka([]string{"localhost:9092"})
	admin, _ := kafka.NewAdmin()

	if admin.Connected() {
		t.Error("expected admin to not be connected initially")
	}

	admin.connected = true
	if !admin.Connected() {
		t.Error("expected admin to be connected")
	}

	admin.closed = true
	if admin.Connected() {
		t.Error("expected admin to not be connected when closed")
	}
}

func TestAdminConnect(t *testing.T) {
	t.Run("successful connection", func(t *testing.T) {
		kafka := NewKafka([]string{"localhost:9092"})
		mockClient := &mockRPCClient{}
		kafka.SetRPCClient(mockClient)

		admin, _ := kafka.NewAdmin()
		err := admin.Connect(context.Background())
		if err != nil {
			t.Fatalf("Connect failed: %v", err)
		}

		if !admin.Connected() {
			t.Error("expected admin to be connected")
		}
	})

	t.Run("already connected", func(t *testing.T) {
		kafka := NewKafka([]string{"localhost:9092"})
		mockClient := &mockRPCClient{}
		kafka.SetRPCClient(mockClient)

		admin, _ := kafka.NewAdmin()
		admin.connected = true
		admin.client = mockClient

		err := admin.Connect(context.Background())
		if err != nil {
			t.Fatalf("Connect failed: %v", err)
		}
	})

	t.Run("connection failure", func(t *testing.T) {
		kafka := NewKafka([]string{"localhost:9092"})

		admin, _ := kafka.NewAdmin()
		err := admin.Connect(context.Background())
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
		admin, _ := kafka.NewAdmin()
		admin.closed = true

		err := admin.Connect(context.Background())
		if !errors.Is(err, ErrClosed) {
			t.Errorf("expected ErrClosed, got %v", err)
		}
	})
}

func TestAdminClose(t *testing.T) {
	t.Run("close connected admin", func(t *testing.T) {
		kafka := NewKafka([]string{"localhost:9092"})
		mockClient := &mockRPCClient{}
		kafka.SetRPCClient(mockClient)

		admin, _ := kafka.NewAdmin()
		admin.connected = true
		admin.client = mockClient

		err := admin.Close()
		if err != nil {
			t.Fatalf("Close failed: %v", err)
		}

		if admin.Connected() {
			t.Error("expected admin to be disconnected after close")
		}
	})

	t.Run("close already closed", func(t *testing.T) {
		kafka := NewKafka([]string{"localhost:9092"})
		admin, _ := kafka.NewAdmin()
		admin.closed = true

		err := admin.Close()
		if err != nil {
			t.Fatalf("Close failed: %v", err)
		}
	})
}

func TestAdminCreateTopics(t *testing.T) {
	t.Run("create topics with strings", func(t *testing.T) {
		kafka := NewKafka([]string{"localhost:9092"})
		mockClient := &mockRPCClient{
			calls: []mockCall{
				{method: "kafka.admin.createTopics", result: []any{}},
			},
		}
		kafka.SetRPCClient(mockClient)

		admin, _ := kafka.NewAdmin()
		admin.connected = true
		admin.client = mockClient

		err := admin.CreateTopics(context.Background(), []string{"topic1", "topic2"})
		if err != nil {
			t.Fatalf("CreateTopics failed: %v", err)
		}
	})

	t.Run("create topics with config", func(t *testing.T) {
		kafka := NewKafka([]string{"localhost:9092"})
		mockClient := &mockRPCClient{
			calls: []mockCall{
				{method: "kafka.admin.createTopics", result: []any{}},
			},
		}
		kafka.SetRPCClient(mockClient)

		admin, _ := kafka.NewAdmin()
		admin.connected = true
		admin.client = mockClient

		configs := []TopicConfig{
			{
				Name:              "topic1",
				NumPartitions:     3,
				ReplicationFactor: 2,
				Config:            map[string]string{"retention.ms": "86400000"},
			},
		}

		err := admin.CreateTopicsWithConfig(context.Background(), configs, false)
		if err != nil {
			t.Fatalf("CreateTopicsWithConfig failed: %v", err)
		}
	})

	t.Run("topic already exists", func(t *testing.T) {
		kafka := NewKafka([]string{"localhost:9092"})
		mockClient := &mockRPCClient{
			calls: []mockCall{
				{
					method: "kafka.admin.createTopics",
					result: []any{
						map[string]any{
							"name": "topic1",
							"error": map[string]any{
								"code":    "TOPIC_ALREADY_EXISTS",
								"message": "Topic already exists",
							},
						},
					},
				},
			},
		}
		kafka.SetRPCClient(mockClient)

		admin, _ := kafka.NewAdmin()
		admin.connected = true
		admin.client = mockClient

		err := admin.CreateTopics(context.Background(), []string{"topic1"})
		if err == nil {
			t.Fatal("expected error")
		}

		var topicErr *TopicAlreadyExistsError
		if !errors.As(err, &topicErr) {
			t.Errorf("expected TopicAlreadyExistsError, got %T", err)
		}
	})

	t.Run("not connected", func(t *testing.T) {
		kafka := NewKafka([]string{"localhost:9092"})
		admin, _ := kafka.NewAdmin()

		err := admin.CreateTopics(context.Background(), []string{"topic1"})
		if err == nil {
			t.Fatal("expected error")
		}

		var connErr *ConnectionError
		if !errors.As(err, &connErr) {
			t.Errorf("expected ConnectionError, got %T", err)
		}
	})

	t.Run("general error", func(t *testing.T) {
		kafka := NewKafka([]string{"localhost:9092"})
		mockClient := &mockRPCClient{
			calls: []mockCall{
				{
					method: "kafka.admin.createTopics",
					result: []any{
						map[string]any{
							"name": "topic1",
							"error": map[string]any{
								"code":    "UNKNOWN_ERROR",
								"message": "Unknown error",
							},
						},
					},
				},
			},
		}
		kafka.SetRPCClient(mockClient)

		admin, _ := kafka.NewAdmin()
		admin.connected = true
		admin.client = mockClient

		err := admin.CreateTopics(context.Background(), []string{"topic1"})
		if err == nil {
			t.Fatal("expected error")
		}

		var kafkaErr *KafkaError
		if !errors.As(err, &kafkaErr) {
			t.Errorf("expected KafkaError, got %T", err)
		}
	})
}

func TestAdminDeleteTopics(t *testing.T) {
	t.Run("successful delete", func(t *testing.T) {
		kafka := NewKafka([]string{"localhost:9092"})
		mockClient := &mockRPCClient{
			calls: []mockCall{
				{method: "kafka.admin.deleteTopics", result: []any{}},
			},
		}
		kafka.SetRPCClient(mockClient)

		admin, _ := kafka.NewAdmin()
		admin.connected = true
		admin.client = mockClient

		err := admin.DeleteTopics(context.Background(), []string{"topic1"})
		if err != nil {
			t.Fatalf("DeleteTopics failed: %v", err)
		}
	})

	t.Run("topic not found", func(t *testing.T) {
		kafka := NewKafka([]string{"localhost:9092"})
		mockClient := &mockRPCClient{
			calls: []mockCall{
				{
					method: "kafka.admin.deleteTopics",
					result: []any{
						map[string]any{
							"name": "topic1",
							"error": map[string]any{
								"code":    "TOPIC_NOT_FOUND",
								"message": "Topic not found",
							},
						},
					},
				},
			},
		}
		kafka.SetRPCClient(mockClient)

		admin, _ := kafka.NewAdmin()
		admin.connected = true
		admin.client = mockClient

		err := admin.DeleteTopics(context.Background(), []string{"topic1"})
		if err == nil {
			t.Fatal("expected error")
		}

		var topicErr *TopicNotFoundError
		if !errors.As(err, &topicErr) {
			t.Errorf("expected TopicNotFoundError, got %T", err)
		}
	})

	t.Run("not connected", func(t *testing.T) {
		kafka := NewKafka([]string{"localhost:9092"})
		admin, _ := kafka.NewAdmin()

		err := admin.DeleteTopics(context.Background(), []string{"topic1"})
		if err == nil {
			t.Fatal("expected error")
		}
	})
}

func TestAdminListTopics(t *testing.T) {
	t.Run("list topics", func(t *testing.T) {
		kafka := NewKafka([]string{"localhost:9092"})
		mockClient := &mockRPCClient{
			calls: []mockCall{
				{method: "kafka.admin.listTopics", result: []any{"topic1", "topic2"}},
			},
		}
		kafka.SetRPCClient(mockClient)

		admin, _ := kafka.NewAdmin()
		admin.connected = true
		admin.client = mockClient

		topics, err := admin.ListTopics(context.Background())
		if err != nil {
			t.Fatalf("ListTopics failed: %v", err)
		}

		if len(topics) != 2 {
			t.Errorf("expected 2 topics, got %d", len(topics))
		}
	})

	t.Run("list topics with internal", func(t *testing.T) {
		kafka := NewKafka([]string{"localhost:9092"})
		mockClient := &mockRPCClient{
			calls: []mockCall{
				{method: "kafka.admin.listTopics", result: []any{"topic1", "__consumer_offsets"}},
			},
		}
		kafka.SetRPCClient(mockClient)

		admin, _ := kafka.NewAdmin()
		admin.connected = true
		admin.client = mockClient

		topics, err := admin.ListTopicsWithOptions(context.Background(), true)
		if err != nil {
			t.Fatalf("ListTopicsWithOptions failed: %v", err)
		}

		if len(topics) != 2 {
			t.Errorf("expected 2 topics, got %d", len(topics))
		}
	})

	t.Run("not connected", func(t *testing.T) {
		kafka := NewKafka([]string{"localhost:9092"})
		admin, _ := kafka.NewAdmin()

		_, err := admin.ListTopics(context.Background())
		if err == nil {
			t.Fatal("expected error")
		}
	})
}

func TestAdminDescribeTopics(t *testing.T) {
	t.Run("describe topics", func(t *testing.T) {
		kafka := NewKafka([]string{"localhost:9092"})
		mockClient := &mockRPCClient{
			calls: []mockCall{
				{
					method: "kafka.admin.describeTopics",
					result: []any{
						map[string]any{
							"name":              "topic1",
							"partitions":        float64(3),
							"replicationFactor": float64(2),
							"config": map[string]any{
								"retention.ms": "86400000",
							},
						},
					},
				},
			},
		}
		kafka.SetRPCClient(mockClient)

		admin, _ := kafka.NewAdmin()
		admin.connected = true
		admin.client = mockClient

		metadata, err := admin.DescribeTopics(context.Background(), []string{"topic1"})
		if err != nil {
			t.Fatalf("DescribeTopics failed: %v", err)
		}

		if len(metadata) != 1 {
			t.Fatalf("expected 1 topic metadata, got %d", len(metadata))
		}

		if metadata[0].Name != "topic1" {
			t.Errorf("expected name 'topic1', got %s", metadata[0].Name)
		}
		if metadata[0].Partitions != 3 {
			t.Errorf("expected 3 partitions, got %d", metadata[0].Partitions)
		}
		if metadata[0].ReplicationFactor != 2 {
			t.Errorf("expected replication factor 2, got %d", metadata[0].ReplicationFactor)
		}
		if metadata[0].Config["retention.ms"] != "86400000" {
			t.Errorf("expected retention.ms '86400000', got %s", metadata[0].Config["retention.ms"])
		}
	})

	t.Run("topic not found", func(t *testing.T) {
		kafka := NewKafka([]string{"localhost:9092"})
		mockClient := &mockRPCClient{
			calls: []mockCall{
				{
					method: "kafka.admin.describeTopics",
					result: []any{
						map[string]any{
							"name": "topic1",
							"error": map[string]any{
								"code":    "TOPIC_NOT_FOUND",
								"message": "Topic not found",
							},
						},
					},
				},
			},
		}
		kafka.SetRPCClient(mockClient)

		admin, _ := kafka.NewAdmin()
		admin.connected = true
		admin.client = mockClient

		_, err := admin.DescribeTopics(context.Background(), []string{"topic1"})
		if err == nil {
			t.Fatal("expected error")
		}

		var topicErr *TopicNotFoundError
		if !errors.As(err, &topicErr) {
			t.Errorf("expected TopicNotFoundError, got %T", err)
		}
	})

	t.Run("not connected", func(t *testing.T) {
		kafka := NewKafka([]string{"localhost:9092"})
		admin, _ := kafka.NewAdmin()

		_, err := admin.DescribeTopics(context.Background(), []string{"topic1"})
		if err == nil {
			t.Fatal("expected error")
		}
	})
}

func TestAdminDescribePartitions(t *testing.T) {
	t.Run("describe partitions", func(t *testing.T) {
		kafka := NewKafka([]string{"localhost:9092"})
		mockClient := &mockRPCClient{
			calls: []mockCall{
				{
					method: "kafka.admin.describePartitions",
					result: map[string]any{
						"partitions": []any{
							map[string]any{
								"partition": float64(0),
								"leader":    float64(1),
								"replicas":  []any{float64(1), float64(2)},
								"isr":       []any{float64(1), float64(2)},
							},
							map[string]any{
								"partition": float64(1),
								"leader":    float64(2),
								"replicas":  []any{float64(2), float64(1)},
								"isr":       []any{float64(2), float64(1)},
							},
						},
					},
				},
			},
		}
		kafka.SetRPCClient(mockClient)

		admin, _ := kafka.NewAdmin()
		admin.connected = true
		admin.client = mockClient

		partitions, err := admin.DescribePartitions(context.Background(), "topic1")
		if err != nil {
			t.Fatalf("DescribePartitions failed: %v", err)
		}

		if len(partitions) != 2 {
			t.Fatalf("expected 2 partitions, got %d", len(partitions))
		}

		if partitions[0].Topic != "topic1" {
			t.Errorf("expected topic 'topic1', got %s", partitions[0].Topic)
		}
		if partitions[0].Partition != 0 {
			t.Errorf("expected partition 0, got %d", partitions[0].Partition)
		}
		if partitions[0].Leader != 1 {
			t.Errorf("expected leader 1, got %d", partitions[0].Leader)
		}
		if len(partitions[0].Replicas) != 2 {
			t.Errorf("expected 2 replicas, got %d", len(partitions[0].Replicas))
		}
		if len(partitions[0].ISR) != 2 {
			t.Errorf("expected 2 ISR, got %d", len(partitions[0].ISR))
		}
	})

	t.Run("topic not found", func(t *testing.T) {
		kafka := NewKafka([]string{"localhost:9092"})
		mockClient := &mockRPCClient{
			calls: []mockCall{
				{
					method: "kafka.admin.describePartitions",
					result: map[string]any{
						"error": map[string]any{
							"code":    "TOPIC_NOT_FOUND",
							"message": "Topic not found",
						},
					},
				},
			},
		}
		kafka.SetRPCClient(mockClient)

		admin, _ := kafka.NewAdmin()
		admin.connected = true
		admin.client = mockClient

		_, err := admin.DescribePartitions(context.Background(), "topic1")
		if err == nil {
			t.Fatal("expected error")
		}

		var topicErr *TopicNotFoundError
		if !errors.As(err, &topicErr) {
			t.Errorf("expected TopicNotFoundError, got %T", err)
		}
	})

	t.Run("not connected", func(t *testing.T) {
		kafka := NewKafka([]string{"localhost:9092"})
		admin, _ := kafka.NewAdmin()

		_, err := admin.DescribePartitions(context.Background(), "topic1")
		if err == nil {
			t.Fatal("expected error")
		}
	})
}

func TestAdminListConsumerGroups(t *testing.T) {
	t.Run("list consumer groups", func(t *testing.T) {
		kafka := NewKafka([]string{"localhost:9092"})
		mockClient := &mockRPCClient{
			calls: []mockCall{
				{method: "kafka.admin.listConsumerGroups", result: []any{"group1", "group2"}},
			},
		}
		kafka.SetRPCClient(mockClient)

		admin, _ := kafka.NewAdmin()
		admin.connected = true
		admin.client = mockClient

		groups, err := admin.ListConsumerGroups(context.Background())
		if err != nil {
			t.Fatalf("ListConsumerGroups failed: %v", err)
		}

		if len(groups) != 2 {
			t.Errorf("expected 2 groups, got %d", len(groups))
		}
	})

	t.Run("not connected", func(t *testing.T) {
		kafka := NewKafka([]string{"localhost:9092"})
		admin, _ := kafka.NewAdmin()

		_, err := admin.ListConsumerGroups(context.Background())
		if err == nil {
			t.Fatal("expected error")
		}
	})
}

func TestAdminDescribeConsumerGroups(t *testing.T) {
	t.Run("describe consumer groups", func(t *testing.T) {
		kafka := NewKafka([]string{"localhost:9092"})
		mockClient := &mockRPCClient{
			calls: []mockCall{
				{
					method: "kafka.admin.describeConsumerGroups",
					result: []any{
						map[string]any{
							"groupId":     "group1",
							"state":       "Stable",
							"members":     []any{"member1", "member2"},
							"coordinator": float64(1),
						},
					},
				},
			},
		}
		kafka.SetRPCClient(mockClient)

		admin, _ := kafka.NewAdmin()
		admin.connected = true
		admin.client = mockClient

		groups, err := admin.DescribeConsumerGroups(context.Background(), []string{"group1"})
		if err != nil {
			t.Fatalf("DescribeConsumerGroups failed: %v", err)
		}

		if len(groups) != 1 {
			t.Fatalf("expected 1 group, got %d", len(groups))
		}

		if groups[0].GroupID != "group1" {
			t.Errorf("expected groupId 'group1', got %s", groups[0].GroupID)
		}
		if groups[0].State != "Stable" {
			t.Errorf("expected state 'Stable', got %s", groups[0].State)
		}
		if len(groups[0].Members) != 2 {
			t.Errorf("expected 2 members, got %d", len(groups[0].Members))
		}
		if groups[0].Coordinator != 1 {
			t.Errorf("expected coordinator 1, got %d", groups[0].Coordinator)
		}
	})

	t.Run("with error", func(t *testing.T) {
		kafka := NewKafka([]string{"localhost:9092"})
		mockClient := &mockRPCClient{
			calls: []mockCall{
				{
					method: "kafka.admin.describeConsumerGroups",
					result: []any{
						map[string]any{
							"groupId": "group1",
							"error": map[string]any{
								"code":    "UNKNOWN_ERROR",
								"message": "Unknown error",
							},
						},
					},
				},
			},
		}
		kafka.SetRPCClient(mockClient)

		admin, _ := kafka.NewAdmin()
		admin.connected = true
		admin.client = mockClient

		_, err := admin.DescribeConsumerGroups(context.Background(), []string{"group1"})
		if err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("not connected", func(t *testing.T) {
		kafka := NewKafka([]string{"localhost:9092"})
		admin, _ := kafka.NewAdmin()

		_, err := admin.DescribeConsumerGroups(context.Background(), []string{"group1"})
		if err == nil {
			t.Fatal("expected error")
		}
	})
}

func TestAdminDeleteConsumerGroups(t *testing.T) {
	t.Run("successful delete", func(t *testing.T) {
		kafka := NewKafka([]string{"localhost:9092"})
		mockClient := &mockRPCClient{
			calls: []mockCall{
				{method: "kafka.admin.deleteConsumerGroups", result: []any{}},
			},
		}
		kafka.SetRPCClient(mockClient)

		admin, _ := kafka.NewAdmin()
		admin.connected = true
		admin.client = mockClient

		err := admin.DeleteConsumerGroups(context.Background(), []string{"group1"})
		if err != nil {
			t.Fatalf("DeleteConsumerGroups failed: %v", err)
		}
	})

	t.Run("with error", func(t *testing.T) {
		kafka := NewKafka([]string{"localhost:9092"})
		mockClient := &mockRPCClient{
			calls: []mockCall{
				{
					method: "kafka.admin.deleteConsumerGroups",
					result: []any{
						map[string]any{
							"groupId": "group1",
							"error": map[string]any{
								"code":    "GROUP_NOT_EMPTY",
								"message": "Group not empty",
							},
						},
					},
				},
			},
		}
		kafka.SetRPCClient(mockClient)

		admin, _ := kafka.NewAdmin()
		admin.connected = true
		admin.client = mockClient

		err := admin.DeleteConsumerGroups(context.Background(), []string{"group1"})
		if err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("not connected", func(t *testing.T) {
		kafka := NewKafka([]string{"localhost:9092"})
		admin, _ := kafka.NewAdmin()

		err := admin.DeleteConsumerGroups(context.Background(), []string{"group1"})
		if err == nil {
			t.Fatal("expected error")
		}
	})
}

func TestAdminAlterTopicConfig(t *testing.T) {
	t.Run("successful alter", func(t *testing.T) {
		kafka := NewKafka([]string{"localhost:9092"})
		mockClient := &mockRPCClient{
			calls: []mockCall{
				{method: "kafka.admin.alterTopicConfig", result: map[string]any{}},
			},
		}
		kafka.SetRPCClient(mockClient)

		admin, _ := kafka.NewAdmin()
		admin.connected = true
		admin.client = mockClient

		config := map[string]string{"retention.ms": "172800000"}
		err := admin.AlterTopicConfig(context.Background(), "topic1", config)
		if err != nil {
			t.Fatalf("AlterTopicConfig failed: %v", err)
		}
	})

	t.Run("topic not found", func(t *testing.T) {
		kafka := NewKafka([]string{"localhost:9092"})
		mockClient := &mockRPCClient{
			calls: []mockCall{
				{
					method: "kafka.admin.alterTopicConfig",
					result: map[string]any{
						"error": map[string]any{
							"code":    "TOPIC_NOT_FOUND",
							"message": "Topic not found",
						},
					},
				},
			},
		}
		kafka.SetRPCClient(mockClient)

		admin, _ := kafka.NewAdmin()
		admin.connected = true
		admin.client = mockClient

		config := map[string]string{"retention.ms": "172800000"}
		err := admin.AlterTopicConfig(context.Background(), "topic1", config)
		if err == nil {
			t.Fatal("expected error")
		}

		var topicErr *TopicNotFoundError
		if !errors.As(err, &topicErr) {
			t.Errorf("expected TopicNotFoundError, got %T", err)
		}
	})

	t.Run("not connected", func(t *testing.T) {
		kafka := NewKafka([]string{"localhost:9092"})
		admin, _ := kafka.NewAdmin()

		config := map[string]string{"retention.ms": "172800000"}
		err := admin.AlterTopicConfig(context.Background(), "topic1", config)
		if err == nil {
			t.Fatal("expected error")
		}
	})
}

func TestParseStringSlice(t *testing.T) {
	t.Run("any slice", func(t *testing.T) {
		result := []any{"a", "b", "c"}
		strs, err := parseStringSlice(result)
		if err != nil {
			t.Fatalf("parseStringSlice failed: %v", err)
		}

		if len(strs) != 3 {
			t.Errorf("expected 3 elements, got %d", len(strs))
		}
	})

	t.Run("string slice", func(t *testing.T) {
		result := []string{"a", "b", "c"}
		strs, err := parseStringSlice(result)
		if err != nil {
			t.Fatalf("parseStringSlice failed: %v", err)
		}

		if len(strs) != 3 {
			t.Errorf("expected 3 elements, got %d", len(strs))
		}
	})

	t.Run("invalid type", func(t *testing.T) {
		_, err := parseStringSlice("invalid")
		if err == nil {
			t.Fatal("expected error")
		}
	})
}

func TestParseTopicMetadataList(t *testing.T) {
	t.Run("valid metadata", func(t *testing.T) {
		result := []any{
			map[string]any{
				"name":              "topic1",
				"partitions":        float64(3),
				"replicationFactor": float64(2),
				"config": map[string]any{
					"retention.ms": "86400000",
				},
			},
		}

		metadata, err := parseTopicMetadataList(result)
		if err != nil {
			t.Fatalf("parseTopicMetadataList failed: %v", err)
		}

		if len(metadata) != 1 {
			t.Errorf("expected 1 metadata, got %d", len(metadata))
		}
	})

	t.Run("with error", func(t *testing.T) {
		result := []any{
			map[string]any{
				"name": "topic1",
				"error": map[string]any{
					"code":    "TOPIC_NOT_FOUND",
					"message": "Topic not found",
				},
			},
		}

		_, err := parseTopicMetadataList(result)
		if err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("invalid format", func(t *testing.T) {
		_, err := parseTopicMetadataList("invalid")
		if err == nil {
			t.Fatal("expected error")
		}
	})
}

func TestParsePartitionMetadataList(t *testing.T) {
	t.Run("valid metadata", func(t *testing.T) {
		result := map[string]any{
			"partitions": []any{
				map[string]any{
					"partition": float64(0),
					"leader":    float64(1),
					"replicas":  []any{float64(1)},
					"isr":       []any{float64(1)},
				},
			},
		}

		metadata, err := parsePartitionMetadataList(result, "topic1")
		if err != nil {
			t.Fatalf("parsePartitionMetadataList failed: %v", err)
		}

		if len(metadata) != 1 {
			t.Errorf("expected 1 partition, got %d", len(metadata))
		}
	})

	t.Run("with error", func(t *testing.T) {
		result := map[string]any{
			"error": map[string]any{
				"code":    "TOPIC_NOT_FOUND",
				"message": "Topic not found",
			},
		}

		_, err := parsePartitionMetadataList(result, "topic1")
		if err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("invalid format", func(t *testing.T) {
		_, err := parsePartitionMetadataList("invalid", "topic1")
		if err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("invalid partitions format", func(t *testing.T) {
		result := map[string]any{
			"partitions": "invalid",
		}

		_, err := parsePartitionMetadataList(result, "topic1")
		if err == nil {
			t.Fatal("expected error")
		}
	})
}

func TestParseConsumerGroupMetadataList(t *testing.T) {
	t.Run("valid metadata", func(t *testing.T) {
		result := []any{
			map[string]any{
				"groupId":     "group1",
				"state":       "Stable",
				"members":     []any{"member1"},
				"coordinator": float64(1),
			},
		}

		metadata, err := parseConsumerGroupMetadataList(result)
		if err != nil {
			t.Fatalf("parseConsumerGroupMetadataList failed: %v", err)
		}

		if len(metadata) != 1 {
			t.Errorf("expected 1 group, got %d", len(metadata))
		}
	})

	t.Run("with error", func(t *testing.T) {
		result := []any{
			map[string]any{
				"groupId": "group1",
				"error": map[string]any{
					"code":    "UNKNOWN_ERROR",
					"message": "Unknown error",
				},
			},
		}

		_, err := parseConsumerGroupMetadataList(result)
		if err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("invalid format", func(t *testing.T) {
		_, err := parseConsumerGroupMetadataList("invalid")
		if err == nil {
			t.Fatal("expected error")
		}
	})
}
