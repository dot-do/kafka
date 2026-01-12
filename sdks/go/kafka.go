// Package kafka provides a Kafka client SDK for Go using go.rpc.do as the underlying transport.
//
// This package provides a confluent-kafka-go compatible API with context.Context
// and channel-based message consumption.
//
// Example usage:
//
//	kafka := NewKafka([]string{"broker1:9092"})
//
//	// Producer
//	producer, _ := kafka.NewProducer()
//	producer.Produce(ctx, "my-topic", []byte("key"), []byte("value"))
//	producer.Flush(ctx)
//
//	// Consumer
//	consumer, _ := kafka.NewConsumer("my-group")
//	consumer.Subscribe(ctx, []string{"my-topic"})
//	for msg := range consumer.Messages() {
//	    fmt.Printf("%s\n", msg.Value)
//	}
//
//	// Admin
//	admin, _ := kafka.NewAdmin()
//	admin.CreateTopics(ctx, []string{"new-topic"})
package kafka

import (
	"context"
	"sync"
	"time"
)

// CompressionType represents the compression algorithm for messages.
type CompressionType string

const (
	CompressionNone   CompressionType = "none"
	CompressionGzip   CompressionType = "gzip"
	CompressionSnappy CompressionType = "snappy"
	CompressionLZ4    CompressionType = "lz4"
	CompressionZstd   CompressionType = "zstd"
)

// IsolationLevel represents the transaction isolation level for consumers.
type IsolationLevel string

const (
	IsolationReadUncommitted IsolationLevel = "read_uncommitted"
	IsolationReadCommitted   IsolationLevel = "read_committed"
)

// OffsetResetPolicy represents the offset reset policy for consumers.
type OffsetResetPolicy string

const (
	OffsetEarliest OffsetResetPolicy = "earliest"
	OffsetLatest   OffsetResetPolicy = "latest"
	OffsetNone     OffsetResetPolicy = "none"
)

// Acks represents the acknowledgment level for producers.
type Acks string

const (
	AcksNone Acks = "0"
	AcksOne  Acks = "1"
	AcksAll  Acks = "all"
)

// RPCClient is the interface for the underlying RPC transport.
// This is implemented by go.rpc.do but can be mocked for testing.
type RPCClient interface {
	Call(method string, args ...any) RPCPromise
	Close() error
}

// RPCPromise is the interface for RPC promise results.
type RPCPromise interface {
	Await() (any, error)
}

// Config holds the configuration for the Kafka client.
type Config struct {
	// Brokers is the list of Kafka broker addresses.
	Brokers []string

	// ClientID is an optional client identifier.
	ClientID string

	// Timeout is the connection timeout.
	Timeout time.Duration

	// ConnectionURL is the URL for the kafka.do RPC service.
	ConnectionURL string
}

// Kafka is the main Kafka client.
type Kafka struct {
	config Config
	client RPCClient
	mu     sync.RWMutex
}

// Option is a function that configures a Kafka client.
type Option func(*Config)

// WithClientID sets the client ID.
func WithClientID(id string) Option {
	return func(c *Config) {
		c.ClientID = id
	}
}

// WithTimeout sets the connection timeout.
func WithTimeout(d time.Duration) Option {
	return func(c *Config) {
		c.Timeout = d
	}
}

// WithConnectionURL sets the RPC connection URL.
func WithConnectionURL(url string) Option {
	return func(c *Config) {
		c.ConnectionURL = url
	}
}

// NewKafka creates a new Kafka client with the specified brokers.
func NewKafka(brokers []string, opts ...Option) *Kafka {
	config := Config{
		Brokers:       brokers,
		Timeout:       30 * time.Second,
		ConnectionURL: "kafka.do",
	}

	for _, opt := range opts {
		opt(&config)
	}

	return &Kafka{
		config: config,
	}
}

// SetRPCClient sets the underlying RPC client. This is primarily used for testing.
func (k *Kafka) SetRPCClient(client RPCClient) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.client = client
}

// GetRPCClient returns the underlying RPC client, connecting if necessary.
func (k *Kafka) GetRPCClient(ctx context.Context) (RPCClient, error) {
	k.mu.RLock()
	if k.client != nil {
		k.mu.RUnlock()
		return k.client, nil
	}
	k.mu.RUnlock()

	k.mu.Lock()
	defer k.mu.Unlock()

	// Double-check after acquiring write lock
	if k.client != nil {
		return k.client, nil
	}

	// In a real implementation, this would connect via go.rpc.do
	// For now, return an error if no client is set
	return nil, &ConnectionError{Message: "RPC client not configured"}
}

// Brokers returns the configured broker addresses.
func (k *Kafka) Brokers() []string {
	return append([]string{}, k.config.Brokers...)
}

// ClientID returns the configured client ID.
func (k *Kafka) ClientID() string {
	return k.config.ClientID
}

// Close closes the Kafka client and releases resources.
func (k *Kafka) Close() error {
	k.mu.Lock()
	defer k.mu.Unlock()

	if k.client != nil {
		err := k.client.Close()
		k.client = nil
		return err
	}
	return nil
}

// NewProducer creates a new Producer with the specified options.
func (k *Kafka) NewProducer(opts ...ProducerOption) (*Producer, error) {
	config := ProducerConfig{
		Acks:            AcksOne,
		CompressionType: CompressionNone,
		BatchSize:       16384,
		LingerMs:        0,
	}

	for _, opt := range opts {
		opt(&config)
	}

	return &Producer{
		kafka:  k,
		config: config,
	}, nil
}

// NewConsumer creates a new Consumer with the specified group ID and options.
func (k *Kafka) NewConsumer(groupID string, opts ...ConsumerOption) (*Consumer, error) {
	config := ConsumerConfig{
		GroupID:              groupID,
		AutoCommit:           true,
		AutoCommitIntervalMs: 5000,
		SessionTimeoutMs:     10000,
		HeartbeatIntervalMs:  3000,
		MaxPollRecords:       500,
		MaxPollIntervalMs:    300000,
		IsolationLevel:       IsolationReadUncommitted,
		AutoOffsetReset:      OffsetLatest,
	}

	for _, opt := range opts {
		opt(&config)
	}

	return &Consumer{
		kafka:         k,
		config:        config,
		subscriptions: make(map[string]struct{}),
		assignments:   make(map[TopicPartition]struct{}),
		paused:        make(map[TopicPartition]struct{}),
		msgChan:       make(chan *Message, 100),
	}, nil
}

// NewAdmin creates a new Admin client with the specified options.
func (k *Kafka) NewAdmin(opts ...AdminOption) (*Admin, error) {
	config := AdminConfig{
		RequestTimeoutMs: 30000,
	}

	for _, opt := range opts {
		opt(&config)
	}

	return &Admin{
		kafka:  k,
		config: config,
	}, nil
}

// TopicPartition represents a topic and partition pair.
type TopicPartition struct {
	Topic     string
	Partition int
}

// Message represents a Kafka message.
type Message struct {
	// Topic is the topic the message belongs to.
	Topic string

	// Partition is the partition number.
	Partition int

	// Offset is the message offset within the partition.
	Offset int64

	// Key is the message key (may be nil).
	Key []byte

	// Value is the message value.
	Value []byte

	// Timestamp is the message timestamp.
	Timestamp time.Time

	// Headers are optional message headers.
	Headers map[string][]byte
}

// RecordMetadata contains metadata about a successfully produced record.
type RecordMetadata struct {
	// Topic is the topic the record was sent to.
	Topic string

	// Partition is the partition the record was sent to.
	Partition int

	// Offset is the offset of the record in the partition.
	Offset int64

	// Timestamp is the timestamp of the record.
	Timestamp time.Time

	// SerializedKeySize is the size of the serialized key.
	SerializedKeySize int

	// SerializedValueSize is the size of the serialized value.
	SerializedValueSize int
}

// TopicConfig holds configuration for creating a new topic.
type TopicConfig struct {
	// Name is the topic name.
	Name string

	// NumPartitions is the number of partitions.
	NumPartitions int

	// ReplicationFactor is the replication factor.
	ReplicationFactor int

	// Config holds additional topic configuration.
	Config map[string]string
}

// TopicMetadata contains metadata about a topic.
type TopicMetadata struct {
	// Name is the topic name.
	Name string

	// Partitions is the number of partitions.
	Partitions int

	// ReplicationFactor is the replication factor.
	ReplicationFactor int

	// Config holds the topic configuration.
	Config map[string]string
}

// PartitionMetadata contains metadata about a partition.
type PartitionMetadata struct {
	// Topic is the topic name.
	Topic string

	// Partition is the partition number.
	Partition int

	// Leader is the broker ID of the leader.
	Leader int

	// Replicas is the list of replica broker IDs.
	Replicas []int

	// ISR is the list of in-sync replica broker IDs.
	ISR []int
}

// ConsumerGroupMetadata contains metadata about a consumer group.
type ConsumerGroupMetadata struct {
	// GroupID is the consumer group ID.
	GroupID string

	// State is the current state of the group.
	State string

	// Members is the list of member IDs.
	Members []string

	// Coordinator is the broker ID of the group coordinator.
	Coordinator int
}
