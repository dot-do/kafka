package kafka

import (
	"context"
	"sync"
)

// AdminConfig holds configuration for an Admin client.
type AdminConfig struct {
	// RequestTimeoutMs is the request timeout in milliseconds.
	RequestTimeoutMs int
}

// AdminOption is a function that configures an Admin client.
type AdminOption func(*AdminConfig)

// WithRequestTimeout sets the request timeout.
func WithRequestTimeout(ms int) AdminOption {
	return func(c *AdminConfig) {
		c.RequestTimeoutMs = ms
	}
}

// Admin is a Kafka admin client for managing topics and consumer groups.
type Admin struct {
	kafka     *Kafka
	config    AdminConfig
	client    RPCClient
	connected bool
	closed    bool
	mu        sync.RWMutex
}

// Connected returns true if the admin client is connected.
func (a *Admin) Connected() bool {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.connected && !a.closed
}

// Connect establishes the connection to the Kafka service.
func (a *Admin) Connect(ctx context.Context) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.connected {
		return nil
	}

	if a.closed {
		return ErrClosed
	}

	client, err := a.kafka.GetRPCClient(ctx)
	if err != nil {
		return &ConnectionError{Message: "failed to connect admin", Cause: err}
	}

	a.client = client
	a.connected = true
	return nil
}

// Close closes the admin client.
func (a *Admin) Close() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.closed {
		return nil
	}

	a.closed = true
	a.connected = false
	a.client = nil
	return nil
}

// CreateTopics creates new topics.
func (a *Admin) CreateTopics(ctx context.Context, topics []string) error {
	configs := make([]TopicConfig, len(topics))
	for i, topic := range topics {
		configs[i] = TopicConfig{
			Name:              topic,
			NumPartitions:     1,
			ReplicationFactor: 1,
		}
	}
	return a.CreateTopicsWithConfig(ctx, configs, false)
}

// CreateTopicsWithConfig creates new topics with configuration.
func (a *Admin) CreateTopicsWithConfig(ctx context.Context, topics []TopicConfig, validateOnly bool) error {
	a.mu.RLock()
	if !a.connected {
		a.mu.RUnlock()
		return &ConnectionError{Message: "admin not connected"}
	}
	if a.closed {
		a.mu.RUnlock()
		return ErrClosed
	}
	client := a.client
	a.mu.RUnlock()

	topicConfigs := make([]map[string]any, len(topics))
	for i, topic := range topics {
		config := map[string]any{
			"name":              topic.Name,
			"numPartitions":     topic.NumPartitions,
			"replicationFactor": topic.ReplicationFactor,
		}
		if topic.Config != nil {
			config["config"] = topic.Config
		} else {
			config["config"] = map[string]string{}
		}
		topicConfigs[i] = config
	}

	promise := client.Call("kafka.admin.createTopics", map[string]any{
		"topics":       topicConfigs,
		"validateOnly": validateOnly,
	})

	result, err := promise.Await()
	if err != nil {
		return &AdminError{Message: "failed to create topics", Cause: err}
	}

	// Check for errors in result
	if results, ok := result.([]any); ok {
		for _, r := range results {
			if rm, ok := r.(map[string]any); ok {
				if errData, hasErr := rm["error"]; hasErr && errData != nil {
					if errMap, ok := errData.(map[string]any); ok {
						code, _ := errMap["code"].(string)
						message, _ := errMap["message"].(string)
						topicName, _ := rm["name"].(string)

						if code == ErrCodeTopicAlreadyExists {
							return &TopicAlreadyExistsError{Topic: topicName}
						}
						return &KafkaError{Code: code, Message: message}
					}
				}
			}
		}
	}

	return nil
}

// DeleteTopics deletes topics.
func (a *Admin) DeleteTopics(ctx context.Context, topics []string) error {
	a.mu.RLock()
	if !a.connected {
		a.mu.RUnlock()
		return &ConnectionError{Message: "admin not connected"}
	}
	if a.closed {
		a.mu.RUnlock()
		return ErrClosed
	}
	client := a.client
	a.mu.RUnlock()

	promise := client.Call("kafka.admin.deleteTopics", map[string]any{
		"topics": topics,
	})

	result, err := promise.Await()
	if err != nil {
		return &AdminError{Message: "failed to delete topics", Cause: err}
	}

	// Check for errors in result
	if results, ok := result.([]any); ok {
		for _, r := range results {
			if rm, ok := r.(map[string]any); ok {
				if errData, hasErr := rm["error"]; hasErr && errData != nil {
					if errMap, ok := errData.(map[string]any); ok {
						code, _ := errMap["code"].(string)
						message, _ := errMap["message"].(string)
						topicName, _ := rm["name"].(string)

						if code == ErrCodeTopicNotFound {
							return &TopicNotFoundError{Topic: topicName}
						}
						return &KafkaError{Code: code, Message: message}
					}
				}
			}
		}
	}

	return nil
}

// ListTopics lists all topics.
func (a *Admin) ListTopics(ctx context.Context) ([]string, error) {
	return a.ListTopicsWithOptions(ctx, false)
}

// ListTopicsWithOptions lists topics with options.
func (a *Admin) ListTopicsWithOptions(ctx context.Context, includeInternal bool) ([]string, error) {
	a.mu.RLock()
	if !a.connected {
		a.mu.RUnlock()
		return nil, &ConnectionError{Message: "admin not connected"}
	}
	if a.closed {
		a.mu.RUnlock()
		return nil, ErrClosed
	}
	client := a.client
	a.mu.RUnlock()

	promise := client.Call("kafka.admin.listTopics", map[string]any{
		"includeInternal": includeInternal,
	})

	result, err := promise.Await()
	if err != nil {
		return nil, &AdminError{Message: "failed to list topics", Cause: err}
	}

	return parseStringSlice(result)
}

// DescribeTopics returns metadata about topics.
func (a *Admin) DescribeTopics(ctx context.Context, topics []string) ([]TopicMetadata, error) {
	a.mu.RLock()
	if !a.connected {
		a.mu.RUnlock()
		return nil, &ConnectionError{Message: "admin not connected"}
	}
	if a.closed {
		a.mu.RUnlock()
		return nil, ErrClosed
	}
	client := a.client
	a.mu.RUnlock()

	promise := client.Call("kafka.admin.describeTopics", map[string]any{
		"topics": topics,
	})

	result, err := promise.Await()
	if err != nil {
		return nil, &AdminError{Message: "failed to describe topics", Cause: err}
	}

	return parseTopicMetadataList(result)
}

// DescribePartitions returns partition metadata for a topic.
func (a *Admin) DescribePartitions(ctx context.Context, topic string) ([]PartitionMetadata, error) {
	a.mu.RLock()
	if !a.connected {
		a.mu.RUnlock()
		return nil, &ConnectionError{Message: "admin not connected"}
	}
	if a.closed {
		a.mu.RUnlock()
		return nil, ErrClosed
	}
	client := a.client
	a.mu.RUnlock()

	promise := client.Call("kafka.admin.describePartitions", map[string]any{
		"topic": topic,
	})

	result, err := promise.Await()
	if err != nil {
		return nil, &AdminError{Message: "failed to describe partitions", Cause: err}
	}

	return parsePartitionMetadataList(result, topic)
}

// ListConsumerGroups lists all consumer groups.
func (a *Admin) ListConsumerGroups(ctx context.Context) ([]string, error) {
	a.mu.RLock()
	if !a.connected {
		a.mu.RUnlock()
		return nil, &ConnectionError{Message: "admin not connected"}
	}
	if a.closed {
		a.mu.RUnlock()
		return nil, ErrClosed
	}
	client := a.client
	a.mu.RUnlock()

	promise := client.Call("kafka.admin.listConsumerGroups")
	result, err := promise.Await()
	if err != nil {
		return nil, &AdminError{Message: "failed to list consumer groups", Cause: err}
	}

	return parseStringSlice(result)
}

// DescribeConsumerGroups returns metadata about consumer groups.
func (a *Admin) DescribeConsumerGroups(ctx context.Context, groupIDs []string) ([]ConsumerGroupMetadata, error) {
	a.mu.RLock()
	if !a.connected {
		a.mu.RUnlock()
		return nil, &ConnectionError{Message: "admin not connected"}
	}
	if a.closed {
		a.mu.RUnlock()
		return nil, ErrClosed
	}
	client := a.client
	a.mu.RUnlock()

	promise := client.Call("kafka.admin.describeConsumerGroups", map[string]any{
		"groupIds": groupIDs,
	})

	result, err := promise.Await()
	if err != nil {
		return nil, &AdminError{Message: "failed to describe consumer groups", Cause: err}
	}

	return parseConsumerGroupMetadataList(result)
}

// DeleteConsumerGroups deletes consumer groups.
func (a *Admin) DeleteConsumerGroups(ctx context.Context, groupIDs []string) error {
	a.mu.RLock()
	if !a.connected {
		a.mu.RUnlock()
		return &ConnectionError{Message: "admin not connected"}
	}
	if a.closed {
		a.mu.RUnlock()
		return ErrClosed
	}
	client := a.client
	a.mu.RUnlock()

	promise := client.Call("kafka.admin.deleteConsumerGroups", map[string]any{
		"groupIds": groupIDs,
	})

	result, err := promise.Await()
	if err != nil {
		return &AdminError{Message: "failed to delete consumer groups", Cause: err}
	}

	// Check for errors in result
	if results, ok := result.([]any); ok {
		for _, r := range results {
			if rm, ok := r.(map[string]any); ok {
				if errData, hasErr := rm["error"]; hasErr && errData != nil {
					if errMap, ok := errData.(map[string]any); ok {
						code, _ := errMap["code"].(string)
						message, _ := errMap["message"].(string)
						return &KafkaError{Code: code, Message: message}
					}
				}
			}
		}
	}

	return nil
}

// AlterTopicConfig alters topic configuration.
func (a *Admin) AlterTopicConfig(ctx context.Context, topic string, config map[string]string) error {
	a.mu.RLock()
	if !a.connected {
		a.mu.RUnlock()
		return &ConnectionError{Message: "admin not connected"}
	}
	if a.closed {
		a.mu.RUnlock()
		return ErrClosed
	}
	client := a.client
	a.mu.RUnlock()

	promise := client.Call("kafka.admin.alterTopicConfig", map[string]any{
		"topic":  topic,
		"config": config,
	})

	result, err := promise.Await()
	if err != nil {
		return &AdminError{Message: "failed to alter topic config", Cause: err}
	}

	// Check for error in result
	if rm, ok := result.(map[string]any); ok {
		if errData, hasErr := rm["error"]; hasErr && errData != nil {
			if errMap, ok := errData.(map[string]any); ok {
				code, _ := errMap["code"].(string)
				message, _ := errMap["message"].(string)

				if code == ErrCodeTopicNotFound {
					return &TopicNotFoundError{Topic: topic}
				}
				return &KafkaError{Code: code, Message: message}
			}
		}
	}

	return nil
}

// parseStringSlice parses an any value to []string.
func parseStringSlice(result any) ([]string, error) {
	switch v := result.(type) {
	case []any:
		strs := make([]string, 0, len(v))
		for _, item := range v {
			if s, ok := item.(string); ok {
				strs = append(strs, s)
			}
		}
		return strs, nil
	case []string:
		return v, nil
	default:
		return nil, &AdminError{Message: "invalid response format"}
	}
}

// parseTopicMetadataList parses RPC result into topic metadata list.
func parseTopicMetadataList(result any) ([]TopicMetadata, error) {
	results, ok := result.([]any)
	if !ok {
		return nil, &AdminError{Message: "invalid describe topics response format"}
	}

	metadata := make([]TopicMetadata, 0, len(results))
	for _, r := range results {
		m, ok := r.(map[string]any)
		if !ok {
			continue
		}

		// Check for error
		if errData, hasErr := m["error"]; hasErr && errData != nil {
			if errMap, ok := errData.(map[string]any); ok {
				code, _ := errMap["code"].(string)
				topicName, _ := m["name"].(string)

				if code == ErrCodeTopicNotFound {
					return nil, &TopicNotFoundError{Topic: topicName}
				}
				message, _ := errMap["message"].(string)
				return nil, &KafkaError{Code: code, Message: message}
			}
		}

		tm := TopicMetadata{
			Config: make(map[string]string),
		}

		if name, ok := m["name"].(string); ok {
			tm.Name = name
		}

		if partitions, ok := m["partitions"].(float64); ok {
			tm.Partitions = int(partitions)
		}

		if rf, ok := m["replicationFactor"].(float64); ok {
			tm.ReplicationFactor = int(rf)
		}

		if config, ok := m["config"].(map[string]any); ok {
			for k, v := range config {
				if s, ok := v.(string); ok {
					tm.Config[k] = s
				}
			}
		}

		metadata = append(metadata, tm)
	}

	return metadata, nil
}

// parsePartitionMetadataList parses RPC result into partition metadata list.
func parsePartitionMetadataList(result any, topic string) ([]PartitionMetadata, error) {
	m, ok := result.(map[string]any)
	if !ok {
		return nil, &AdminError{Message: "invalid describe partitions response format"}
	}

	// Check for error
	if errData, hasErr := m["error"]; hasErr && errData != nil {
		if errMap, ok := errData.(map[string]any); ok {
			code, _ := errMap["code"].(string)

			if code == ErrCodeTopicNotFound {
				return nil, &TopicNotFoundError{Topic: topic}
			}
			message, _ := errMap["message"].(string)
			return nil, &KafkaError{Code: code, Message: message}
		}
	}

	partitions, ok := m["partitions"].([]any)
	if !ok {
		return nil, &AdminError{Message: "invalid partitions format"}
	}

	metadata := make([]PartitionMetadata, 0, len(partitions))
	for _, p := range partitions {
		pm, ok := p.(map[string]any)
		if !ok {
			continue
		}

		part := PartitionMetadata{
			Topic:    topic,
			Replicas: []int{},
			ISR:      []int{},
		}

		if partition, ok := pm["partition"].(float64); ok {
			part.Partition = int(partition)
		}

		if leader, ok := pm["leader"].(float64); ok {
			part.Leader = int(leader)
		}

		if replicas, ok := pm["replicas"].([]any); ok {
			for _, r := range replicas {
				if n, ok := r.(float64); ok {
					part.Replicas = append(part.Replicas, int(n))
				}
			}
		}

		if isr, ok := pm["isr"].([]any); ok {
			for _, r := range isr {
				if n, ok := r.(float64); ok {
					part.ISR = append(part.ISR, int(n))
				}
			}
		}

		metadata = append(metadata, part)
	}

	return metadata, nil
}

// parseConsumerGroupMetadataList parses RPC result into consumer group metadata list.
func parseConsumerGroupMetadataList(result any) ([]ConsumerGroupMetadata, error) {
	results, ok := result.([]any)
	if !ok {
		return nil, &AdminError{Message: "invalid describe consumer groups response format"}
	}

	metadata := make([]ConsumerGroupMetadata, 0, len(results))
	for _, r := range results {
		m, ok := r.(map[string]any)
		if !ok {
			continue
		}

		// Check for error
		if errData, hasErr := m["error"]; hasErr && errData != nil {
			if errMap, ok := errData.(map[string]any); ok {
				code, _ := errMap["code"].(string)
				message, _ := errMap["message"].(string)
				return nil, &KafkaError{Code: code, Message: message}
			}
		}

		gm := ConsumerGroupMetadata{
			Members: []string{},
		}

		if groupID, ok := m["groupId"].(string); ok {
			gm.GroupID = groupID
		}

		if state, ok := m["state"].(string); ok {
			gm.State = state
		}

		if members, ok := m["members"].([]any); ok {
			for _, member := range members {
				if s, ok := member.(string); ok {
					gm.Members = append(gm.Members, s)
				}
			}
		}

		if coordinator, ok := m["coordinator"].(float64); ok {
			gm.Coordinator = int(coordinator)
		}

		metadata = append(metadata, gm)
	}

	return metadata, nil
}
