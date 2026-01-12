package kafka

import (
	"context"
	"encoding/base64"
	"fmt"
	"sync"
	"time"
)

// ProducerConfig holds configuration for a Producer.
type ProducerConfig struct {
	// Acks is the number of acknowledgments required.
	Acks Acks

	// CompressionType is the compression algorithm for messages.
	CompressionType CompressionType

	// BatchSize is the maximum batch size in bytes.
	BatchSize int

	// LingerMs is the time to wait before sending batched messages.
	LingerMs int

	// ClientID is an optional client identifier.
	ClientID string
}

// ProducerOption is a function that configures a Producer.
type ProducerOption func(*ProducerConfig)

// WithAcks sets the acknowledgment level.
func WithAcks(acks Acks) ProducerOption {
	return func(c *ProducerConfig) {
		c.Acks = acks
	}
}

// WithCompression sets the compression type.
func WithCompression(compression CompressionType) ProducerOption {
	return func(c *ProducerConfig) {
		c.CompressionType = compression
	}
}

// WithBatchSize sets the batch size.
func WithBatchSize(size int) ProducerOption {
	return func(c *ProducerConfig) {
		c.BatchSize = size
	}
}

// WithLingerMs sets the linger time in milliseconds.
func WithLingerMs(ms int) ProducerOption {
	return func(c *ProducerConfig) {
		c.LingerMs = ms
	}
}

// WithProducerClientID sets the client ID for the producer.
func WithProducerClientID(id string) ProducerOption {
	return func(c *ProducerConfig) {
		c.ClientID = id
	}
}

// Producer is a Kafka message producer.
type Producer struct {
	kafka     *Kafka
	config    ProducerConfig
	client    RPCClient
	connected bool
	closed    bool
	mu        sync.RWMutex
}

// ProducerRecord is a record to be sent to Kafka.
type ProducerRecord struct {
	// Topic is the topic to send to.
	Topic string

	// Key is the message key (optional).
	Key []byte

	// Value is the message value.
	Value []byte

	// Partition is the specific partition to send to (optional, -1 for auto).
	Partition int

	// Timestamp is the message timestamp (optional).
	Timestamp time.Time

	// Headers are optional message headers.
	Headers map[string][]byte
}

// BatchResult contains the results of a batch send operation.
type BatchResult struct {
	// Successful contains metadata for successfully sent records.
	Successful []RecordMetadata

	// Failed contains the records that failed to send along with their errors.
	Failed []FailedRecord
}

// FailedRecord represents a record that failed to send.
type FailedRecord struct {
	Record ProducerRecord
	Error  error
}

// Connected returns true if the producer is connected.
func (p *Producer) Connected() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.connected && !p.closed
}

// Connect establishes the connection to the Kafka service.
func (p *Producer) Connect(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.connected {
		return nil
	}

	if p.closed {
		return ErrClosed
	}

	client, err := p.kafka.GetRPCClient(ctx)
	if err != nil {
		return &ConnectionError{Message: "failed to connect producer", Cause: err}
	}

	p.client = client
	p.connected = true
	return nil
}

// Close closes the producer, flushing any pending messages.
func (p *Producer) Close(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil
	}

	// Flush pending messages
	if p.connected && p.client != nil {
		p.flushInternal(ctx)
	}

	p.closed = true
	p.connected = false
	p.client = nil
	return nil
}

// Produce sends a single message to a Kafka topic.
func (p *Producer) Produce(ctx context.Context, topic string, key, value []byte) (*RecordMetadata, error) {
	return p.ProduceRecord(ctx, ProducerRecord{
		Topic:     topic,
		Key:       key,
		Value:     value,
		Partition: -1,
	})
}

// ProduceRecord sends a ProducerRecord to Kafka.
func (p *Producer) ProduceRecord(ctx context.Context, record ProducerRecord) (*RecordMetadata, error) {
	p.mu.RLock()
	if !p.connected {
		p.mu.RUnlock()
		return nil, &ConnectionError{Message: "producer not connected"}
	}
	if p.closed {
		p.mu.RUnlock()
		return nil, ErrClosed
	}
	client := p.client
	p.mu.RUnlock()

	// Serialize the record
	serialized := serializeProducerRecord(record)

	// Call the RPC method
	promise := client.Call("kafka.producer.send", serialized)
	result, err := promise.Await()
	if err != nil {
		return nil, &ProducerError{Message: "failed to send message", Cause: err}
	}

	// Parse the result
	return parseRecordMetadata(result)
}

// ProduceBatch sends a batch of records to Kafka.
func (p *Producer) ProduceBatch(ctx context.Context, records []ProducerRecord) (*BatchResult, error) {
	p.mu.RLock()
	if !p.connected {
		p.mu.RUnlock()
		return nil, &ConnectionError{Message: "producer not connected"}
	}
	if p.closed {
		p.mu.RUnlock()
		return nil, ErrClosed
	}
	client := p.client
	p.mu.RUnlock()

	// Serialize all records
	serialized := make([]map[string]any, len(records))
	for i, record := range records {
		serialized[i] = serializeProducerRecord(record)
	}

	// Call the RPC method
	promise := client.Call("kafka.producer.sendBatch", serialized)
	result, err := promise.Await()
	if err != nil {
		// Total batch failure
		batchResult := &BatchResult{}
		for _, record := range records {
			batchResult.Failed = append(batchResult.Failed, FailedRecord{
				Record: record,
				Error:  &ProducerError{Message: "batch send failed", Cause: err},
			})
		}
		return batchResult, nil
	}

	// Parse the results
	return parseBatchResult(result, records)
}

// Flush flushes any pending messages.
func (p *Producer) Flush(ctx context.Context) error {
	p.mu.RLock()
	if !p.connected {
		p.mu.RUnlock()
		return nil
	}
	client := p.client
	p.mu.RUnlock()

	return p.flushWithClient(ctx, client)
}

func (p *Producer) flushInternal(ctx context.Context) error {
	if p.client == nil {
		return nil
	}
	return p.flushWithClient(ctx, p.client)
}

func (p *Producer) flushWithClient(ctx context.Context, client RPCClient) error {
	promise := client.Call("kafka.producer.flush", nil)
	_, err := promise.Await()
	// Ignore flush errors
	_ = err
	return nil
}

// PartitionsFor returns the partitions for a topic.
func (p *Producer) PartitionsFor(ctx context.Context, topic string) ([]int, error) {
	p.mu.RLock()
	if !p.connected {
		p.mu.RUnlock()
		return nil, &ConnectionError{Message: "producer not connected"}
	}
	if p.closed {
		p.mu.RUnlock()
		return nil, ErrClosed
	}
	client := p.client
	p.mu.RUnlock()

	promise := client.Call("kafka.producer.partitionsFor", topic)
	result, err := promise.Await()
	if err != nil {
		return nil, &KafkaError{Code: "", Message: "failed to get partitions", Cause: err}
	}

	return parseIntSlice(result)
}

// serializeProducerRecord converts a ProducerRecord to a map for RPC transmission.
func serializeProducerRecord(record ProducerRecord) map[string]any {
	data := map[string]any{
		"topic": record.Topic,
		"value": base64.StdEncoding.EncodeToString(record.Value),
	}

	if record.Key != nil {
		data["key"] = base64.StdEncoding.EncodeToString(record.Key)
	}

	if record.Partition >= 0 {
		data["partition"] = record.Partition
	}

	if !record.Timestamp.IsZero() {
		data["timestamp"] = record.Timestamp.UnixMilli()
	}

	if len(record.Headers) > 0 {
		headers := make([]map[string]string, 0, len(record.Headers))
		for k, v := range record.Headers {
			headers = append(headers, map[string]string{
				"key":   k,
				"value": base64.StdEncoding.EncodeToString(v),
			})
		}
		data["headers"] = headers
	}

	return data
}

// parseRecordMetadata parses RPC result into RecordMetadata.
func parseRecordMetadata(result any) (*RecordMetadata, error) {
	m, ok := result.(map[string]any)
	if !ok {
		return nil, &ProducerError{Message: "invalid response format"}
	}

	metadata := &RecordMetadata{}

	if topic, ok := m["topic"].(string); ok {
		metadata.Topic = topic
	}

	if partition, ok := m["partition"].(float64); ok {
		metadata.Partition = int(partition)
	}

	if offset, ok := m["offset"].(float64); ok {
		metadata.Offset = int64(offset)
	}

	if ts, ok := m["timestamp"].(float64); ok {
		metadata.Timestamp = time.UnixMilli(int64(ts))
	}

	if keySize, ok := m["serialized_key_size"].(float64); ok {
		metadata.SerializedKeySize = int(keySize)
	}

	if valueSize, ok := m["serialized_value_size"].(float64); ok {
		metadata.SerializedValueSize = int(valueSize)
	}

	return metadata, nil
}

// parseBatchResult parses RPC batch result into BatchResult.
func parseBatchResult(result any, records []ProducerRecord) (*BatchResult, error) {
	results, ok := result.([]any)
	if !ok {
		return nil, &ProducerError{Message: "invalid batch response format"}
	}

	batchResult := &BatchResult{}

	for i, r := range results {
		m, ok := r.(map[string]any)
		if !ok {
			if i < len(records) {
				batchResult.Failed = append(batchResult.Failed, FailedRecord{
					Record: records[i],
					Error:  &ProducerError{Message: "invalid result format"},
				})
			}
			continue
		}

		// Check for error
		if errData, hasErr := m["error"]; hasErr {
			var errMsg string
			var errCode string
			if errMap, ok := errData.(map[string]any); ok {
				if msg, ok := errMap["message"].(string); ok {
					errMsg = msg
				}
				if code, ok := errMap["code"].(string); ok {
					errCode = code
				}
			}
			if i < len(records) {
				batchResult.Failed = append(batchResult.Failed, FailedRecord{
					Record: records[i],
					Error:  &KafkaError{Code: errCode, Message: errMsg},
				})
			}
			continue
		}

		// Parse successful result
		metadata, err := parseRecordMetadata(r)
		if err != nil {
			if i < len(records) {
				batchResult.Failed = append(batchResult.Failed, FailedRecord{
					Record: records[i],
					Error:  err,
				})
			}
			continue
		}

		batchResult.Successful = append(batchResult.Successful, *metadata)
	}

	return batchResult, nil
}

// parseIntSlice parses an any value to []int.
func parseIntSlice(result any) ([]int, error) {
	switch v := result.(type) {
	case []any:
		ints := make([]int, len(v))
		for i, item := range v {
			switch n := item.(type) {
			case float64:
				ints[i] = int(n)
			case int:
				ints[i] = n
			case int64:
				ints[i] = int(n)
			default:
				return nil, fmt.Errorf("unexpected type in slice: %T", item)
			}
		}
		return ints, nil
	case []int:
		return v, nil
	case []float64:
		ints := make([]int, len(v))
		for i, n := range v {
			ints[i] = int(n)
		}
		return ints, nil
	default:
		return nil, fmt.Errorf("unexpected result type: %T", result)
	}
}
