package kafka

import (
	"context"
	"encoding/base64"
	"sync"
	"time"
)

// ConsumerConfig holds configuration for a Consumer.
type ConsumerConfig struct {
	// GroupID is the consumer group ID.
	GroupID string

	// AutoCommit enables auto-commit of offsets.
	AutoCommit bool

	// AutoCommitIntervalMs is the interval for auto-commit in milliseconds.
	AutoCommitIntervalMs int

	// SessionTimeoutMs is the session timeout for group membership.
	SessionTimeoutMs int

	// HeartbeatIntervalMs is the heartbeat interval for group coordination.
	HeartbeatIntervalMs int

	// MaxPollRecords is the maximum records per poll.
	MaxPollRecords int

	// MaxPollIntervalMs is the maximum interval between polls.
	MaxPollIntervalMs int

	// IsolationLevel is the transaction isolation level.
	IsolationLevel IsolationLevel

	// AutoOffsetReset is the offset reset policy for new consumers.
	AutoOffsetReset OffsetResetPolicy

	// ClientID is an optional client identifier.
	ClientID string
}

// ConsumerOption is a function that configures a Consumer.
type ConsumerOption func(*ConsumerConfig)

// WithAutoCommit sets whether auto-commit is enabled.
func WithAutoCommit(enabled bool) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.AutoCommit = enabled
	}
}

// WithAutoCommitInterval sets the auto-commit interval.
func WithAutoCommitInterval(ms int) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.AutoCommitIntervalMs = ms
	}
}

// WithSessionTimeout sets the session timeout.
func WithSessionTimeout(ms int) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.SessionTimeoutMs = ms
	}
}

// WithHeartbeatInterval sets the heartbeat interval.
func WithHeartbeatInterval(ms int) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.HeartbeatIntervalMs = ms
	}
}

// WithMaxPollRecords sets the maximum poll records.
func WithMaxPollRecords(n int) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.MaxPollRecords = n
	}
}

// WithMaxPollInterval sets the maximum poll interval.
func WithMaxPollInterval(ms int) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.MaxPollIntervalMs = ms
	}
}

// WithIsolationLevel sets the isolation level.
func WithIsolationLevel(level IsolationLevel) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.IsolationLevel = level
	}
}

// WithAutoOffsetReset sets the auto offset reset policy.
func WithAutoOffsetReset(policy OffsetResetPolicy) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.AutoOffsetReset = policy
	}
}

// WithConsumerClientID sets the client ID for the consumer.
func WithConsumerClientID(id string) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.ClientID = id
	}
}

// Consumer is a Kafka message consumer.
type Consumer struct {
	kafka         *Kafka
	config        ConsumerConfig
	client        RPCClient
	connected     bool
	closed        bool
	mu            sync.RWMutex
	subscriptions map[string]struct{}
	assignments   map[TopicPartition]struct{}
	paused        map[TopicPartition]struct{}
	msgChan       chan *Message
	pollCancel    context.CancelFunc
	pollWg        sync.WaitGroup
}

// GroupID returns the consumer group ID.
func (c *Consumer) GroupID() string {
	return c.config.GroupID
}

// Connected returns true if the consumer is connected.
func (c *Consumer) Connected() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.connected && !c.closed
}

// Subscriptions returns the set of subscribed topics.
func (c *Consumer) Subscriptions() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	topics := make([]string, 0, len(c.subscriptions))
	for topic := range c.subscriptions {
		topics = append(topics, topic)
	}
	return topics
}

// Assignments returns the set of assigned partitions.
func (c *Consumer) Assignments() []TopicPartition {
	c.mu.RLock()
	defer c.mu.RUnlock()
	partitions := make([]TopicPartition, 0, len(c.assignments))
	for tp := range c.assignments {
		partitions = append(partitions, tp)
	}
	return partitions
}

// Paused returns the set of paused partitions.
func (c *Consumer) Paused() []TopicPartition {
	c.mu.RLock()
	defer c.mu.RUnlock()
	partitions := make([]TopicPartition, 0, len(c.paused))
	for tp := range c.paused {
		partitions = append(partitions, tp)
	}
	return partitions
}

// Connect establishes the connection to the Kafka service.
func (c *Consumer) Connect(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.connected {
		return nil
	}

	if c.closed {
		return ErrClosed
	}

	client, err := c.kafka.GetRPCClient(ctx)
	if err != nil {
		return &ConnectionError{Message: "failed to connect consumer", Cause: err}
	}

	c.client = client
	c.connected = true
	return nil
}

// Close closes the consumer.
func (c *Consumer) Close(ctx context.Context) error {
	c.mu.Lock()

	if c.closed {
		c.mu.Unlock()
		return nil
	}

	// Cancel polling
	if c.pollCancel != nil {
		c.pollCancel()
	}

	c.closed = true
	c.connected = false

	// Close message channel
	close(c.msgChan)

	c.mu.Unlock()

	// Wait for poll goroutine to finish
	c.pollWg.Wait()

	// Commit final offsets if auto-commit enabled
	if c.config.AutoCommit && c.config.GroupID != "" {
		_ = c.Commit(ctx, nil)
	}

	c.mu.Lock()
	c.client = nil
	c.subscriptions = make(map[string]struct{})
	c.assignments = make(map[TopicPartition]struct{})
	c.paused = make(map[TopicPartition]struct{})
	c.mu.Unlock()

	return nil
}

// Subscribe subscribes to the specified topics.
func (c *Consumer) Subscribe(ctx context.Context, topics []string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.connected {
		return &ConnectionError{Message: "consumer not connected"}
	}

	if c.closed {
		return ErrClosed
	}

	promise := c.client.Call("kafka.consumer.subscribe", map[string]any{
		"topics":          topics,
		"groupId":         c.config.GroupID,
		"autoOffsetReset": string(c.config.AutoOffsetReset),
	})

	_, err := promise.Await()
	if err != nil {
		return &ConsumerError{Message: "failed to subscribe", Cause: err}
	}

	c.subscriptions = make(map[string]struct{})
	for _, topic := range topics {
		c.subscriptions[topic] = struct{}{}
	}

	return nil
}

// Unsubscribe unsubscribes from all topics.
func (c *Consumer) Unsubscribe(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.connected {
		return &ConnectionError{Message: "consumer not connected"}
	}

	if c.closed {
		return ErrClosed
	}

	promise := c.client.Call("kafka.consumer.unsubscribe")
	_, err := promise.Await()
	if err != nil {
		return &ConsumerError{Message: "failed to unsubscribe", Cause: err}
	}

	c.subscriptions = make(map[string]struct{})
	c.assignments = make(map[TopicPartition]struct{})

	return nil
}

// Assign manually assigns partitions to this consumer.
func (c *Consumer) Assign(ctx context.Context, partitions []TopicPartition) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.connected {
		return &ConnectionError{Message: "consumer not connected"}
	}

	if c.closed {
		return ErrClosed
	}

	partitionData := make([]map[string]any, len(partitions))
	for i, p := range partitions {
		partitionData[i] = map[string]any{
			"topic":     p.Topic,
			"partition": p.Partition,
		}
	}

	promise := c.client.Call("kafka.consumer.assign", map[string]any{
		"partitions": partitionData,
	})

	_, err := promise.Await()
	if err != nil {
		return &ConsumerError{Message: "failed to assign partitions", Cause: err}
	}

	c.assignments = make(map[TopicPartition]struct{})
	for _, p := range partitions {
		c.assignments[p] = struct{}{}
	}

	return nil
}

// Poll polls for messages with the specified timeout.
func (c *Consumer) Poll(ctx context.Context, timeoutMs int) ([]*Message, error) {
	c.mu.RLock()
	if !c.connected {
		c.mu.RUnlock()
		return nil, &ConnectionError{Message: "consumer not connected"}
	}
	if c.closed {
		c.mu.RUnlock()
		return nil, ErrClosed
	}
	client := c.client
	c.mu.RUnlock()

	promise := client.Call("kafka.consumer.poll", map[string]any{
		"timeoutMs":  timeoutMs,
		"maxRecords": c.config.MaxPollRecords,
	})

	result, err := promise.Await()
	if err != nil {
		return nil, &ConsumerError{Message: "failed to poll", Cause: err}
	}

	return parseMessages(result)
}

// Messages returns a channel that receives messages from subscribed topics.
// This starts a background goroutine that polls for messages.
func (c *Consumer) Messages() <-chan *Message {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.pollCancel != nil {
		return c.msgChan
	}

	ctx, cancel := context.WithCancel(context.Background())
	c.pollCancel = cancel

	c.pollWg.Add(1)
	go c.pollLoop(ctx)

	return c.msgChan
}

func (c *Consumer) pollLoop(ctx context.Context) {
	defer c.pollWg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		c.mu.RLock()
		if c.closed || !c.connected {
			c.mu.RUnlock()
			return
		}
		c.mu.RUnlock()

		messages, err := c.Poll(ctx, 1000)
		if err != nil {
			// Check if we should stop
			c.mu.RLock()
			closed := c.closed
			c.mu.RUnlock()
			if closed {
				return
			}
			// Sleep briefly on error
			time.Sleep(100 * time.Millisecond)
			continue
		}

		for _, msg := range messages {
			select {
			case <-ctx.Done():
				return
			case c.msgChan <- msg:
			}
		}
	}
}

// Commit commits offsets.
func (c *Consumer) Commit(ctx context.Context, offsets map[TopicPartition]int64) error {
	c.mu.RLock()
	if !c.connected {
		c.mu.RUnlock()
		return &ConnectionError{Message: "consumer not connected"}
	}
	if c.closed {
		c.mu.RUnlock()
		return nil
	}
	client := c.client
	c.mu.RUnlock()

	var args map[string]any
	if offsets != nil {
		offsetData := make([]map[string]any, 0, len(offsets))
		for tp, offset := range offsets {
			offsetData = append(offsetData, map[string]any{
				"topic":     tp.Topic,
				"partition": tp.Partition,
				"offset":    offset,
			})
		}
		args = map[string]any{"offsets": offsetData}
	}

	promise := client.Call("kafka.consumer.commit", args)
	_, err := promise.Await()
	if err != nil {
		return &ConsumerError{Message: "failed to commit", Cause: err}
	}

	return nil
}

// Committed returns committed offsets for the specified partitions.
func (c *Consumer) Committed(ctx context.Context, partitions []TopicPartition) (map[TopicPartition]int64, error) {
	c.mu.RLock()
	if !c.connected {
		c.mu.RUnlock()
		return nil, &ConnectionError{Message: "consumer not connected"}
	}
	if c.closed {
		c.mu.RUnlock()
		return nil, ErrClosed
	}
	client := c.client
	c.mu.RUnlock()

	partitionData := make([]map[string]any, len(partitions))
	for i, p := range partitions {
		partitionData[i] = map[string]any{
			"topic":     p.Topic,
			"partition": p.Partition,
		}
	}

	promise := client.Call("kafka.consumer.committed", map[string]any{
		"partitions": partitionData,
	})

	result, err := promise.Await()
	if err != nil {
		return nil, &ConsumerError{Message: "failed to get committed offsets", Cause: err}
	}

	return parseCommittedOffsets(result)
}

// Seek seeks to a specific offset.
func (c *Consumer) Seek(ctx context.Context, partition TopicPartition, offset int64) error {
	c.mu.RLock()
	if !c.connected {
		c.mu.RUnlock()
		return &ConnectionError{Message: "consumer not connected"}
	}
	if c.closed {
		c.mu.RUnlock()
		return ErrClosed
	}
	client := c.client
	c.mu.RUnlock()

	promise := client.Call("kafka.consumer.seek", map[string]any{
		"topic":     partition.Topic,
		"partition": partition.Partition,
		"offset":    offset,
	})

	_, err := promise.Await()
	if err != nil {
		return &ConsumerError{Message: "failed to seek", Cause: err}
	}

	return nil
}

// SeekToBeginning seeks to the beginning of partitions.
func (c *Consumer) SeekToBeginning(ctx context.Context, partitions []TopicPartition) error {
	c.mu.RLock()
	if !c.connected {
		c.mu.RUnlock()
		return &ConnectionError{Message: "consumer not connected"}
	}
	if c.closed {
		c.mu.RUnlock()
		return ErrClosed
	}
	client := c.client
	c.mu.RUnlock()

	var args map[string]any
	if partitions != nil {
		partitionData := make([]map[string]any, len(partitions))
		for i, p := range partitions {
			partitionData[i] = map[string]any{
				"topic":     p.Topic,
				"partition": p.Partition,
			}
		}
		args = map[string]any{"partitions": partitionData}
	}

	promise := client.Call("kafka.consumer.seekToBeginning", args)
	_, err := promise.Await()
	if err != nil {
		return &ConsumerError{Message: "failed to seek to beginning", Cause: err}
	}

	return nil
}

// SeekToEnd seeks to the end of partitions.
func (c *Consumer) SeekToEnd(ctx context.Context, partitions []TopicPartition) error {
	c.mu.RLock()
	if !c.connected {
		c.mu.RUnlock()
		return &ConnectionError{Message: "consumer not connected"}
	}
	if c.closed {
		c.mu.RUnlock()
		return ErrClosed
	}
	client := c.client
	c.mu.RUnlock()

	var args map[string]any
	if partitions != nil {
		partitionData := make([]map[string]any, len(partitions))
		for i, p := range partitions {
			partitionData[i] = map[string]any{
				"topic":     p.Topic,
				"partition": p.Partition,
			}
		}
		args = map[string]any{"partitions": partitionData}
	}

	promise := client.Call("kafka.consumer.seekToEnd", args)
	_, err := promise.Await()
	if err != nil {
		return &ConsumerError{Message: "failed to seek to end", Cause: err}
	}

	return nil
}

// Position returns the current position (offset) for a partition.
func (c *Consumer) Position(ctx context.Context, partition TopicPartition) (int64, error) {
	c.mu.RLock()
	if !c.connected {
		c.mu.RUnlock()
		return 0, &ConnectionError{Message: "consumer not connected"}
	}
	if c.closed {
		c.mu.RUnlock()
		return 0, ErrClosed
	}
	client := c.client
	c.mu.RUnlock()

	promise := client.Call("kafka.consumer.position", map[string]any{
		"topic":     partition.Topic,
		"partition": partition.Partition,
	})

	result, err := promise.Await()
	if err != nil {
		return 0, &ConsumerError{Message: "failed to get position", Cause: err}
	}

	switch v := result.(type) {
	case float64:
		return int64(v), nil
	case int64:
		return v, nil
	case int:
		return int64(v), nil
	default:
		return 0, &ConsumerError{Message: "invalid position response"}
	}
}

// Pause pauses consumption from partitions.
func (c *Consumer) Pause(ctx context.Context, partitions []TopicPartition) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.connected {
		return &ConnectionError{Message: "consumer not connected"}
	}

	if c.closed {
		return ErrClosed
	}

	partitionData := make([]map[string]any, len(partitions))
	for i, p := range partitions {
		partitionData[i] = map[string]any{
			"topic":     p.Topic,
			"partition": p.Partition,
		}
	}

	promise := c.client.Call("kafka.consumer.pause", map[string]any{
		"partitions": partitionData,
	})

	_, err := promise.Await()
	if err != nil {
		return &ConsumerError{Message: "failed to pause", Cause: err}
	}

	for _, p := range partitions {
		c.paused[p] = struct{}{}
	}

	return nil
}

// Resume resumes consumption from paused partitions.
func (c *Consumer) Resume(ctx context.Context, partitions []TopicPartition) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.connected {
		return &ConnectionError{Message: "consumer not connected"}
	}

	if c.closed {
		return ErrClosed
	}

	partitionData := make([]map[string]any, len(partitions))
	for i, p := range partitions {
		partitionData[i] = map[string]any{
			"topic":     p.Topic,
			"partition": p.Partition,
		}
	}

	promise := c.client.Call("kafka.consumer.resume", map[string]any{
		"partitions": partitionData,
	})

	_, err := promise.Await()
	if err != nil {
		return &ConsumerError{Message: "failed to resume", Cause: err}
	}

	for _, p := range partitions {
		delete(c.paused, p)
	}

	return nil
}

// parseMessages parses RPC result into messages.
func parseMessages(result any) ([]*Message, error) {
	results, ok := result.([]any)
	if !ok {
		// Empty result
		if result == nil {
			return []*Message{}, nil
		}
		return nil, &ConsumerError{Message: "invalid poll response format"}
	}

	messages := make([]*Message, 0, len(results))
	for _, r := range results {
		msg, err := parseMessage(r)
		if err != nil {
			continue
		}
		messages = append(messages, msg)
	}

	return messages, nil
}

// parseMessage parses a single message from RPC result.
func parseMessage(result any) (*Message, error) {
	m, ok := result.(map[string]any)
	if !ok {
		return nil, &ConsumerError{Message: "invalid message format"}
	}

	msg := &Message{
		Headers: make(map[string][]byte),
	}

	if topic, ok := m["topic"].(string); ok {
		msg.Topic = topic
	}

	if partition, ok := m["partition"].(float64); ok {
		msg.Partition = int(partition)
	}

	if offset, ok := m["offset"].(float64); ok {
		msg.Offset = int64(offset)
	}

	if ts, ok := m["timestamp"].(float64); ok {
		msg.Timestamp = time.UnixMilli(int64(ts))
	}

	if key, ok := m["key"].(string); ok && key != "" {
		decoded, err := base64.StdEncoding.DecodeString(key)
		if err == nil {
			msg.Key = decoded
		}
	}

	if value, ok := m["value"].(string); ok {
		decoded, err := base64.StdEncoding.DecodeString(value)
		if err == nil {
			msg.Value = decoded
		}
	}

	if headers, ok := m["headers"].([]any); ok {
		for _, h := range headers {
			if hm, ok := h.(map[string]any); ok {
				if key, ok := hm["key"].(string); ok {
					if value, ok := hm["value"].(string); ok {
						decoded, err := base64.StdEncoding.DecodeString(value)
						if err == nil {
							msg.Headers[key] = decoded
						}
					}
				}
			}
		}
	}

	return msg, nil
}

// parseCommittedOffsets parses RPC result into committed offsets.
func parseCommittedOffsets(result any) (map[TopicPartition]int64, error) {
	results, ok := result.([]any)
	if !ok {
		return nil, &ConsumerError{Message: "invalid committed response format"}
	}

	offsets := make(map[TopicPartition]int64)
	for _, r := range results {
		m, ok := r.(map[string]any)
		if !ok {
			continue
		}

		topic, _ := m["topic"].(string)
		partition, _ := m["partition"].(float64)
		offset, hasOffset := m["offset"].(float64)

		if hasOffset {
			tp := TopicPartition{
				Topic:     topic,
				Partition: int(partition),
			}
			offsets[tp] = int64(offset)
		}
	}

	return offsets, nil
}
