package kafka

import (
	"errors"
	"testing"
)

func TestKafkaError(t *testing.T) {
	t.Run("error with code", func(t *testing.T) {
		err := NewKafkaError("TEST_CODE", "test message", true)

		if err.Error() != "[TEST_CODE] test message" {
			t.Errorf("unexpected error message: %s", err.Error())
		}
		if err.Code != "TEST_CODE" {
			t.Errorf("expected code 'TEST_CODE', got %s", err.Code)
		}
		if err.Message != "test message" {
			t.Errorf("expected message 'test message', got %s", err.Message)
		}
		if !err.Retriable {
			t.Error("expected retriable to be true")
		}
	})

	t.Run("error without code", func(t *testing.T) {
		err := NewKafkaError("", "test message", false)

		if err.Error() != "test message" {
			t.Errorf("unexpected error message: %s", err.Error())
		}
	})

	t.Run("wrapped error", func(t *testing.T) {
		cause := errors.New("underlying error")
		err := WrapKafkaError("TEST_CODE", "test message", true, cause)

		if err.Unwrap() != cause {
			t.Error("expected Unwrap to return cause")
		}
	})

	t.Run("error matching", func(t *testing.T) {
		err1 := NewKafkaError("TEST_CODE", "message 1", true)
		err2 := NewKafkaError("TEST_CODE", "message 2", false)
		err3 := NewKafkaError("OTHER_CODE", "message 3", true)

		if !err1.Is(err2) {
			t.Error("expected err1.Is(err2) to be true")
		}
		if err1.Is(err3) {
			t.Error("expected err1.Is(err3) to be false")
		}

		var notKafkaErr = errors.New("not a kafka error")
		if err1.Is(notKafkaErr) {
			t.Error("expected err1.Is(notKafkaErr) to be false")
		}
	})
}

func TestConnectionError(t *testing.T) {
	t.Run("without cause", func(t *testing.T) {
		err := &ConnectionError{Message: "connection failed"}

		if err.Error() != "kafka connection error: connection failed" {
			t.Errorf("unexpected error message: %s", err.Error())
		}
		if err.Unwrap() != nil {
			t.Error("expected Unwrap to return nil")
		}
	})

	t.Run("with cause", func(t *testing.T) {
		cause := errors.New("dial failed")
		err := &ConnectionError{Message: "connection failed", Cause: cause}

		expected := "kafka connection error: connection failed: dial failed"
		if err.Error() != expected {
			t.Errorf("unexpected error message: %s", err.Error())
		}
		if err.Unwrap() != cause {
			t.Error("expected Unwrap to return cause")
		}
	})
}

func TestTimeoutError(t *testing.T) {
	t.Run("without cause", func(t *testing.T) {
		err := &TimeoutError{Message: "request timed out"}

		if err.Error() != "kafka timeout: request timed out" {
			t.Errorf("unexpected error message: %s", err.Error())
		}
		if err.Unwrap() != nil {
			t.Error("expected Unwrap to return nil")
		}
	})

	t.Run("with cause", func(t *testing.T) {
		cause := errors.New("context deadline exceeded")
		err := &TimeoutError{Message: "request timed out", Cause: cause}

		expected := "kafka timeout: request timed out: context deadline exceeded"
		if err.Error() != expected {
			t.Errorf("unexpected error message: %s", err.Error())
		}
		if err.Unwrap() != cause {
			t.Error("expected Unwrap to return cause")
		}
	})
}

func TestTopicNotFoundError(t *testing.T) {
	err := &TopicNotFoundError{Topic: "test-topic"}

	if err.Error() != "topic not found: test-topic" {
		t.Errorf("unexpected error message: %s", err.Error())
	}
	if err.Topic != "test-topic" {
		t.Errorf("expected topic 'test-topic', got %s", err.Topic)
	}
}

func TestTopicAlreadyExistsError(t *testing.T) {
	err := &TopicAlreadyExistsError{Topic: "test-topic"}

	if err.Error() != "topic already exists: test-topic" {
		t.Errorf("unexpected error message: %s", err.Error())
	}
	if err.Topic != "test-topic" {
		t.Errorf("expected topic 'test-topic', got %s", err.Topic)
	}
}

func TestProducerError(t *testing.T) {
	t.Run("without cause", func(t *testing.T) {
		err := &ProducerError{Message: "send failed"}

		if err.Error() != "producer error: send failed" {
			t.Errorf("unexpected error message: %s", err.Error())
		}
		if err.Unwrap() != nil {
			t.Error("expected Unwrap to return nil")
		}
	})

	t.Run("with cause", func(t *testing.T) {
		cause := errors.New("network error")
		err := &ProducerError{Message: "send failed", Cause: cause}

		expected := "producer error: send failed: network error"
		if err.Error() != expected {
			t.Errorf("unexpected error message: %s", err.Error())
		}
		if err.Unwrap() != cause {
			t.Error("expected Unwrap to return cause")
		}
	})
}

func TestConsumerError(t *testing.T) {
	t.Run("without cause", func(t *testing.T) {
		err := &ConsumerError{Message: "poll failed"}

		if err.Error() != "consumer error: poll failed" {
			t.Errorf("unexpected error message: %s", err.Error())
		}
		if err.Unwrap() != nil {
			t.Error("expected Unwrap to return nil")
		}
	})

	t.Run("with cause", func(t *testing.T) {
		cause := errors.New("network error")
		err := &ConsumerError{Message: "poll failed", Cause: cause}

		expected := "consumer error: poll failed: network error"
		if err.Error() != expected {
			t.Errorf("unexpected error message: %s", err.Error())
		}
		if err.Unwrap() != cause {
			t.Error("expected Unwrap to return cause")
		}
	})
}

func TestAdminError(t *testing.T) {
	t.Run("without cause", func(t *testing.T) {
		err := &AdminError{Message: "create failed"}

		if err.Error() != "admin error: create failed" {
			t.Errorf("unexpected error message: %s", err.Error())
		}
		if err.Unwrap() != nil {
			t.Error("expected Unwrap to return nil")
		}
	})

	t.Run("with cause", func(t *testing.T) {
		cause := errors.New("network error")
		err := &AdminError{Message: "create failed", Cause: cause}

		expected := "admin error: create failed: network error"
		if err.Error() != expected {
			t.Errorf("unexpected error message: %s", err.Error())
		}
		if err.Unwrap() != cause {
			t.Error("expected Unwrap to return cause")
		}
	})
}

func TestIsRetriable(t *testing.T) {
	t.Run("retriable KafkaError", func(t *testing.T) {
		err := NewKafkaError("TEST", "test", true)
		if !IsRetriable(err) {
			t.Error("expected IsRetriable to return true")
		}
	})

	t.Run("non-retriable KafkaError", func(t *testing.T) {
		err := NewKafkaError("TEST", "test", false)
		if IsRetriable(err) {
			t.Error("expected IsRetriable to return false")
		}
	})

	t.Run("ConnectionError is retriable", func(t *testing.T) {
		err := &ConnectionError{Message: "connection failed"}
		if !IsRetriable(err) {
			t.Error("expected ConnectionError to be retriable")
		}
	})

	t.Run("TimeoutError is retriable", func(t *testing.T) {
		err := &TimeoutError{Message: "timeout"}
		if !IsRetriable(err) {
			t.Error("expected TimeoutError to be retriable")
		}
	})

	t.Run("other errors not retriable", func(t *testing.T) {
		err := errors.New("some error")
		if IsRetriable(err) {
			t.Error("expected other errors to not be retriable")
		}
	})
}

func TestIsTopicNotFound(t *testing.T) {
	t.Run("TopicNotFoundError", func(t *testing.T) {
		err := &TopicNotFoundError{Topic: "test"}
		if !IsTopicNotFound(err) {
			t.Error("expected IsTopicNotFound to return true")
		}
	})

	t.Run("other errors", func(t *testing.T) {
		err := errors.New("some error")
		if IsTopicNotFound(err) {
			t.Error("expected IsTopicNotFound to return false for other errors")
		}
	})
}

func TestIsTopicAlreadyExists(t *testing.T) {
	t.Run("TopicAlreadyExistsError", func(t *testing.T) {
		err := &TopicAlreadyExistsError{Topic: "test"}
		if !IsTopicAlreadyExists(err) {
			t.Error("expected IsTopicAlreadyExists to return true")
		}
	})

	t.Run("other errors", func(t *testing.T) {
		err := errors.New("some error")
		if IsTopicAlreadyExists(err) {
			t.Error("expected IsTopicAlreadyExists to return false for other errors")
		}
	})
}

func TestIsConnectionError(t *testing.T) {
	t.Run("ConnectionError", func(t *testing.T) {
		err := &ConnectionError{Message: "test"}
		if !IsConnectionError(err) {
			t.Error("expected IsConnectionError to return true")
		}
	})

	t.Run("other errors", func(t *testing.T) {
		err := errors.New("some error")
		if IsConnectionError(err) {
			t.Error("expected IsConnectionError to return false for other errors")
		}
	})
}

func TestIsTimeoutError(t *testing.T) {
	t.Run("TimeoutError", func(t *testing.T) {
		err := &TimeoutError{Message: "test"}
		if !IsTimeoutError(err) {
			t.Error("expected IsTimeoutError to return true")
		}
	})

	t.Run("other errors", func(t *testing.T) {
		err := errors.New("some error")
		if IsTimeoutError(err) {
			t.Error("expected IsTimeoutError to return false for other errors")
		}
	})
}

func TestErrorCodes(t *testing.T) {
	codes := []string{
		ErrCodeTopicNotFound,
		ErrCodePartitionNotFound,
		ErrCodeMessageTooLarge,
		ErrCodeNotLeader,
		ErrCodeOffsetOutOfRange,
		ErrCodeGroupCoordinator,
		ErrCodeRebalanceInProgress,
		ErrCodeUnauthorized,
		ErrCodeQuotaExceeded,
		ErrCodeConnection,
		ErrCodeTimeout,
		ErrCodeTopicAlreadyExists,
	}

	for _, code := range codes {
		if code == "" {
			t.Error("error code should not be empty")
		}
	}
}

func TestSentinelErrors(t *testing.T) {
	// Test that sentinel errors are properly defined
	if ErrClosed.Error() != "kafka: client closed" {
		t.Errorf("unexpected ErrClosed message: %s", ErrClosed.Error())
	}
	if ErrDisconnected.Error() != "kafka: disconnected" {
		t.Errorf("unexpected ErrDisconnected message: %s", ErrDisconnected.Error())
	}
	if ErrNotConnected.Error() != "kafka: not connected" {
		t.Errorf("unexpected ErrNotConnected message: %s", ErrNotConnected.Error())
	}
}

func TestErrorsAs(t *testing.T) {
	t.Run("wrapped KafkaError", func(t *testing.T) {
		cause := errors.New("cause")
		err := WrapKafkaError("CODE", "message", true, cause)

		var kafkaErr *KafkaError
		if !errors.As(err, &kafkaErr) {
			t.Error("expected errors.As to succeed for KafkaError")
		}
	})

	t.Run("wrapped ConnectionError", func(t *testing.T) {
		cause := errors.New("cause")
		err := &ConnectionError{Message: "message", Cause: cause}

		var connErr *ConnectionError
		if !errors.As(err, &connErr) {
			t.Error("expected errors.As to succeed for ConnectionError")
		}
	})
}
