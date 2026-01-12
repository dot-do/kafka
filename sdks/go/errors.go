package kafka

import (
	"errors"
	"fmt"
)

// Error codes for Kafka errors.
const (
	ErrCodeTopicNotFound       = "TOPIC_NOT_FOUND"
	ErrCodePartitionNotFound   = "PARTITION_NOT_FOUND"
	ErrCodeMessageTooLarge     = "MESSAGE_TOO_LARGE"
	ErrCodeNotLeader           = "NOT_LEADER"
	ErrCodeOffsetOutOfRange    = "OFFSET_OUT_OF_RANGE"
	ErrCodeGroupCoordinator    = "GROUP_COORDINATOR"
	ErrCodeRebalanceInProgress = "REBALANCE_IN_PROGRESS"
	ErrCodeUnauthorized        = "UNAUTHORIZED"
	ErrCodeQuotaExceeded       = "QUOTA_EXCEEDED"
	ErrCodeConnection          = "CONNECTION_ERROR"
	ErrCodeTimeout             = "TIMEOUT"
	ErrCodeTopicAlreadyExists  = "TOPIC_ALREADY_EXISTS"
)

// Sentinel errors for common error conditions.
var (
	ErrClosed       = errors.New("kafka: client closed")
	ErrDisconnected = errors.New("kafka: disconnected")
	ErrNotConnected = errors.New("kafka: not connected")
)

// KafkaError is the base error type for Kafka operations.
type KafkaError struct {
	// Code is the error code.
	Code string

	// Message is the error message.
	Message string

	// Retriable indicates whether the operation can be retried.
	Retriable bool

	// Cause is the underlying error, if any.
	Cause error
}

// Error implements the error interface.
func (e *KafkaError) Error() string {
	if e.Code != "" {
		return fmt.Sprintf("[%s] %s", e.Code, e.Message)
	}
	return e.Message
}

// Unwrap returns the underlying error.
func (e *KafkaError) Unwrap() error {
	return e.Cause
}

// Is implements error matching.
func (e *KafkaError) Is(target error) bool {
	t, ok := target.(*KafkaError)
	if !ok {
		return false
	}
	return e.Code == t.Code
}

// NewKafkaError creates a new KafkaError.
func NewKafkaError(code, message string, retriable bool) *KafkaError {
	return &KafkaError{
		Code:      code,
		Message:   message,
		Retriable: retriable,
	}
}

// WrapKafkaError wraps an error with Kafka error details.
func WrapKafkaError(code, message string, retriable bool, cause error) *KafkaError {
	return &KafkaError{
		Code:      code,
		Message:   message,
		Retriable: retriable,
		Cause:     cause,
	}
}

// ConnectionError represents a connection error.
type ConnectionError struct {
	Message string
	Cause   error
}

// Error implements the error interface.
func (e *ConnectionError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("kafka connection error: %s: %v", e.Message, e.Cause)
	}
	return fmt.Sprintf("kafka connection error: %s", e.Message)
}

// Unwrap returns the underlying error.
func (e *ConnectionError) Unwrap() error {
	return e.Cause
}

// TimeoutError represents a timeout error.
type TimeoutError struct {
	Message string
	Cause   error
}

// Error implements the error interface.
func (e *TimeoutError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("kafka timeout: %s: %v", e.Message, e.Cause)
	}
	return fmt.Sprintf("kafka timeout: %s", e.Message)
}

// Unwrap returns the underlying error.
func (e *TimeoutError) Unwrap() error {
	return e.Cause
}

// TopicNotFoundError is returned when a topic is not found.
type TopicNotFoundError struct {
	Topic string
}

// Error implements the error interface.
func (e *TopicNotFoundError) Error() string {
	return fmt.Sprintf("topic not found: %s", e.Topic)
}

// TopicAlreadyExistsError is returned when attempting to create a topic that already exists.
type TopicAlreadyExistsError struct {
	Topic string
}

// Error implements the error interface.
func (e *TopicAlreadyExistsError) Error() string {
	return fmt.Sprintf("topic already exists: %s", e.Topic)
}

// ProducerError represents a producer operation error.
type ProducerError struct {
	Message string
	Cause   error
}

// Error implements the error interface.
func (e *ProducerError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("producer error: %s: %v", e.Message, e.Cause)
	}
	return fmt.Sprintf("producer error: %s", e.Message)
}

// Unwrap returns the underlying error.
func (e *ProducerError) Unwrap() error {
	return e.Cause
}

// ConsumerError represents a consumer operation error.
type ConsumerError struct {
	Message string
	Cause   error
}

// Error implements the error interface.
func (e *ConsumerError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("consumer error: %s: %v", e.Message, e.Cause)
	}
	return fmt.Sprintf("consumer error: %s", e.Message)
}

// Unwrap returns the underlying error.
func (e *ConsumerError) Unwrap() error {
	return e.Cause
}

// AdminError represents an admin operation error.
type AdminError struct {
	Message string
	Cause   error
}

// Error implements the error interface.
func (e *AdminError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("admin error: %s: %v", e.Message, e.Cause)
	}
	return fmt.Sprintf("admin error: %s", e.Message)
}

// Unwrap returns the underlying error.
func (e *AdminError) Unwrap() error {
	return e.Cause
}

// IsRetriable returns true if the error is retriable.
func IsRetriable(err error) bool {
	var kafkaErr *KafkaError
	if errors.As(err, &kafkaErr) {
		return kafkaErr.Retriable
	}

	var connErr *ConnectionError
	if errors.As(err, &connErr) {
		return true
	}

	var timeoutErr *TimeoutError
	if errors.As(err, &timeoutErr) {
		return true
	}

	return false
}

// IsTopicNotFound returns true if the error is a topic not found error.
func IsTopicNotFound(err error) bool {
	var topicErr *TopicNotFoundError
	return errors.As(err, &topicErr)
}

// IsTopicAlreadyExists returns true if the error is a topic already exists error.
func IsTopicAlreadyExists(err error) bool {
	var topicErr *TopicAlreadyExistsError
	return errors.As(err, &topicErr)
}

// IsConnectionError returns true if the error is a connection error.
func IsConnectionError(err error) bool {
	var connErr *ConnectionError
	return errors.As(err, &connErr)
}

// IsTimeoutError returns true if the error is a timeout error.
func IsTimeoutError(err error) bool {
	var timeoutErr *TimeoutError
	return errors.As(err, &timeoutErr)
}
