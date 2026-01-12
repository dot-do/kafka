package kafka

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestNewKafka(t *testing.T) {
	t.Run("with default options", func(t *testing.T) {
		kafka := NewKafka([]string{"broker1:9092", "broker2:9092"})

		brokers := kafka.Brokers()
		if len(brokers) != 2 {
			t.Errorf("expected 2 brokers, got %d", len(brokers))
		}
		if brokers[0] != "broker1:9092" {
			t.Errorf("expected broker1:9092, got %s", brokers[0])
		}
		if kafka.config.Timeout != 30*time.Second {
			t.Errorf("expected timeout 30s, got %v", kafka.config.Timeout)
		}
		if kafka.config.ConnectionURL != "kafka.do" {
			t.Errorf("expected connection URL 'kafka.do', got %s", kafka.config.ConnectionURL)
		}
	})

	t.Run("with custom options", func(t *testing.T) {
		kafka := NewKafka([]string{"broker1:9092"},
			WithClientID("test-client"),
			WithTimeout(60*time.Second),
			WithConnectionURL("custom.kafka.do"),
		)

		if kafka.ClientID() != "test-client" {
			t.Errorf("expected client ID 'test-client', got %s", kafka.ClientID())
		}
		if kafka.config.Timeout != 60*time.Second {
			t.Errorf("expected timeout 60s, got %v", kafka.config.Timeout)
		}
		if kafka.config.ConnectionURL != "custom.kafka.do" {
			t.Errorf("expected connection URL 'custom.kafka.do', got %s", kafka.config.ConnectionURL)
		}
	})
}

func TestKafkaBrokers(t *testing.T) {
	kafka := NewKafka([]string{"broker1:9092", "broker2:9092"})

	// Test that Brokers returns a copy
	brokers1 := kafka.Brokers()
	brokers2 := kafka.Brokers()

	brokers1[0] = "modified"
	if brokers2[0] == "modified" {
		t.Error("Brokers should return a copy, not the original slice")
	}
}

func TestKafkaSetRPCClient(t *testing.T) {
	kafka := NewKafka([]string{"broker1:9092"})
	mockClient := &mockRPCClient{}

	kafka.SetRPCClient(mockClient)

	client, err := kafka.GetRPCClient(context.Background())
	if err != nil {
		t.Fatalf("GetRPCClient failed: %v", err)
	}

	if client != mockClient {
		t.Error("expected GetRPCClient to return the set client")
	}
}

func TestKafkaGetRPCClient(t *testing.T) {
	t.Run("client already set", func(t *testing.T) {
		kafka := NewKafka([]string{"broker1:9092"})
		mockClient := &mockRPCClient{}
		kafka.SetRPCClient(mockClient)

		client, err := kafka.GetRPCClient(context.Background())
		if err != nil {
			t.Fatalf("GetRPCClient failed: %v", err)
		}

		if client != mockClient {
			t.Error("expected to get the set client")
		}
	})

	t.Run("no client configured", func(t *testing.T) {
		kafka := NewKafka([]string{"broker1:9092"})

		_, err := kafka.GetRPCClient(context.Background())
		if err == nil {
			t.Fatal("expected error when no client configured")
		}

		var connErr *ConnectionError
		if !errors.As(err, &connErr) {
			t.Errorf("expected ConnectionError, got %T", err)
		}
	})
}

func TestKafkaClose(t *testing.T) {
	t.Run("close with client", func(t *testing.T) {
		kafka := NewKafka([]string{"broker1:9092"})
		mockClient := &mockRPCClient{}
		kafka.SetRPCClient(mockClient)

		err := kafka.Close()
		if err != nil {
			t.Fatalf("Close failed: %v", err)
		}

		if !mockClient.closeCalled {
			t.Error("expected Close to be called on client")
		}
	})

	t.Run("close with close error", func(t *testing.T) {
		kafka := NewKafka([]string{"broker1:9092"})
		mockClient := &mockRPCClient{closeErr: errors.New("close error")}
		kafka.SetRPCClient(mockClient)

		err := kafka.Close()
		if err == nil {
			t.Fatal("expected error")
		}

		if err.Error() != "close error" {
			t.Errorf("expected 'close error', got %v", err)
		}
	})

	t.Run("close without client", func(t *testing.T) {
		kafka := NewKafka([]string{"broker1:9092"})

		err := kafka.Close()
		if err != nil {
			t.Fatalf("Close should not fail without client: %v", err)
		}
	})
}

func TestKafkaNewProducer(t *testing.T) {
	kafka := NewKafka([]string{"broker1:9092"})

	producer, err := kafka.NewProducer()
	if err != nil {
		t.Fatalf("NewProducer failed: %v", err)
	}

	if producer == nil {
		t.Fatal("expected producer to be non-nil")
	}

	if producer.kafka != kafka {
		t.Error("expected producer.kafka to be set")
	}
}

func TestKafkaNewConsumer(t *testing.T) {
	kafka := NewKafka([]string{"broker1:9092"})

	consumer, err := kafka.NewConsumer("test-group")
	if err != nil {
		t.Fatalf("NewConsumer failed: %v", err)
	}

	if consumer == nil {
		t.Fatal("expected consumer to be non-nil")
	}

	if consumer.kafka != kafka {
		t.Error("expected consumer.kafka to be set")
	}

	if consumer.GroupID() != "test-group" {
		t.Errorf("expected group ID 'test-group', got %s", consumer.GroupID())
	}
}

func TestKafkaNewAdmin(t *testing.T) {
	kafka := NewKafka([]string{"broker1:9092"})

	admin, err := kafka.NewAdmin()
	if err != nil {
		t.Fatalf("NewAdmin failed: %v", err)
	}

	if admin == nil {
		t.Fatal("expected admin to be non-nil")
	}

	if admin.kafka != kafka {
		t.Error("expected admin.kafka to be set")
	}
}

func TestTopicPartition(t *testing.T) {
	tp1 := TopicPartition{Topic: "topic1", Partition: 0}
	tp2 := TopicPartition{Topic: "topic1", Partition: 0}
	tp3 := TopicPartition{Topic: "topic1", Partition: 1}
	tp4 := TopicPartition{Topic: "topic2", Partition: 0}

	// Test equality
	if tp1 != tp2 {
		t.Error("expected tp1 == tp2")
	}
	if tp1 == tp3 {
		t.Error("expected tp1 != tp3")
	}
	if tp1 == tp4 {
		t.Error("expected tp1 != tp4")
	}
}

func TestConstants(t *testing.T) {
	// Test compression types
	if CompressionNone != "none" {
		t.Error("unexpected CompressionNone value")
	}
	if CompressionGzip != "gzip" {
		t.Error("unexpected CompressionGzip value")
	}
	if CompressionSnappy != "snappy" {
		t.Error("unexpected CompressionSnappy value")
	}
	if CompressionLZ4 != "lz4" {
		t.Error("unexpected CompressionLZ4 value")
	}
	if CompressionZstd != "zstd" {
		t.Error("unexpected CompressionZstd value")
	}

	// Test isolation levels
	if IsolationReadUncommitted != "read_uncommitted" {
		t.Error("unexpected IsolationReadUncommitted value")
	}
	if IsolationReadCommitted != "read_committed" {
		t.Error("unexpected IsolationReadCommitted value")
	}

	// Test offset reset policies
	if OffsetEarliest != "earliest" {
		t.Error("unexpected OffsetEarliest value")
	}
	if OffsetLatest != "latest" {
		t.Error("unexpected OffsetLatest value")
	}
	if OffsetNone != "none" {
		t.Error("unexpected OffsetNone value")
	}

	// Test acks
	if AcksNone != "0" {
		t.Error("unexpected AcksNone value")
	}
	if AcksOne != "1" {
		t.Error("unexpected AcksOne value")
	}
	if AcksAll != "all" {
		t.Error("unexpected AcksAll value")
	}
}
