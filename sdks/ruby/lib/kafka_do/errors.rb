# frozen_string_literal: true

module KafkaDo
  # Base error class for all Kafka errors
  class Error < StandardError
    attr_reader :code, :cause

    def initialize(message = nil, code: nil, cause: nil)
      @code = code
      @cause = cause
      super(message)
    end

    # Whether this error is retriable
    #
    # @return [Boolean]
    def retriable?
      false
    end
  end

  # Connection-related errors
  class ConnectionError < Error
    def retriable?
      true
    end
  end

  # Timeout errors
  class TimeoutError < Error
    def retriable?
      true
    end
  end

  # Producer-specific errors
  class ProducerError < Error; end

  # Consumer-specific errors
  class ConsumerError < Error; end

  # Admin-specific errors
  class AdminError < Error; end

  # Topic not found
  class TopicNotFoundError < Error
    attr_reader :topic

    def initialize(topic, message: nil)
      @topic = topic
      super(message || "Topic not found: #{topic}", code: 'UNKNOWN_TOPIC_OR_PARTITION')
    end
  end

  # Topic already exists
  class TopicAlreadyExistsError < Error
    attr_reader :topic

    def initialize(topic, message: nil)
      @topic = topic
      super(message || "Topic already exists: #{topic}", code: 'TOPIC_ALREADY_EXISTS')
    end
  end

  # Partition not found
  class PartitionNotFoundError < Error
    attr_reader :topic, :partition

    def initialize(topic, partition, message: nil)
      @topic = topic
      @partition = partition
      super(message || "Partition not found: #{topic}[#{partition}]", code: 'UNKNOWN_TOPIC_OR_PARTITION')
    end
  end

  # Message too large
  class MessageTooLargeError < Error
    def initialize(message = 'Message too large')
      super(message, code: 'MESSAGE_TOO_LARGE')
    end
  end

  # Offset out of range
  class OffsetOutOfRangeError < Error
    def initialize(message = 'Offset out of range')
      super(message, code: 'OFFSET_OUT_OF_RANGE')
    end

    def retriable?
      true
    end
  end

  # Group coordinator error
  class GroupCoordinatorError < Error
    def retriable?
      true
    end
  end

  # Rebalance in progress
  class RebalanceInProgressError < Error
    def initialize(message = 'Rebalance in progress')
      super(message, code: 'REBALANCE_IN_PROGRESS')
    end

    def retriable?
      true
    end
  end

  # Not leader for partition
  class NotLeaderError < Error
    def retriable?
      true
    end
  end

  # Unauthorized error
  class UnauthorizedError < Error
    def initialize(message = 'Unauthorized')
      super(message, code: 'TOPIC_AUTHORIZATION_FAILED')
    end
  end

  # Quota exceeded
  class QuotaExceededError < Error
    def initialize(message = 'Quota exceeded')
      super(message, code: 'QUOTA_EXCEEDED')
    end

    def retriable?
      true
    end
  end

  # Disconnected from broker
  class DisconnectedError < ConnectionError
    def initialize(message = 'Disconnected from broker')
      super(message, code: 'DISCONNECTED')
    end
  end

  # Serialization error
  class SerializationError < Error
    def initialize(message = 'Serialization error')
      super(message, code: 'SERIALIZATION_ERROR')
    end
  end

  # Client closed error
  class ClosedError < Error
    def initialize(message = 'Client is closed')
      super(message, code: 'CLIENT_CLOSED')
    end
  end

  # Error code constants
  module ErrorCodes
    UNKNOWN_TOPIC_OR_PARTITION = 'UNKNOWN_TOPIC_OR_PARTITION'
    TOPIC_ALREADY_EXISTS = 'TOPIC_ALREADY_EXISTS'
    MESSAGE_TOO_LARGE = 'MESSAGE_TOO_LARGE'
    OFFSET_OUT_OF_RANGE = 'OFFSET_OUT_OF_RANGE'
    REBALANCE_IN_PROGRESS = 'REBALANCE_IN_PROGRESS'
    TOPIC_AUTHORIZATION_FAILED = 'TOPIC_AUTHORIZATION_FAILED'
    QUOTA_EXCEEDED = 'QUOTA_EXCEEDED'
  end
end
