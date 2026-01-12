module Kafka
  # Base error class for all Kafka errors
  class Error < Exception
    getter code : String
    getter retriable : Bool

    def initialize(message : String, @code : String = "KAFKA_ERROR", @retriable : Bool = false)
      super(message)
    end

    def retriable? : Bool
      @retriable
    end
  end

  # Topic not found error
  class TopicNotFoundError < Error
    getter topic : String

    def initialize(@topic : String)
      super("Topic not found: #{@topic}", "TOPIC_NOT_FOUND", retriable: false)
    end
  end

  # Partition not found error
  class PartitionNotFoundError < Error
    getter partition : Int32

    def initialize(@partition : Int32)
      super("Partition not found: #{@partition}", "PARTITION_NOT_FOUND", retriable: false)
    end
  end

  # Message too large error
  class MessageTooLargeError < Error
    getter size : Int64
    getter max_size : Int64

    def initialize(@size : Int64, @max_size : Int64)
      super("Message size #{@size} exceeds max size #{@max_size}", "MESSAGE_TOO_LARGE", retriable: false)
    end
  end

  # Not leader error
  class NotLeaderError < Error
    def initialize(message : String = "Broker is not the leader for partition")
      super(message, "NOT_LEADER", retriable: true)
    end
  end

  # Offset out of range error
  class OffsetOutOfRangeError < Error
    getter offset : Int64

    def initialize(@offset : Int64)
      super("Offset out of range: #{@offset}", "OFFSET_OUT_OF_RANGE", retriable: false)
    end
  end

  # Group coordinator error
  class GroupCoordinatorError < Error
    def initialize(message : String = "Group coordinator not available")
      super(message, "GROUP_COORDINATOR_ERROR", retriable: true)
    end
  end

  # Rebalance in progress error
  class RebalanceInProgressError < Error
    def initialize
      super("Consumer group is rebalancing", "REBALANCE_IN_PROGRESS", retriable: true)
    end
  end

  # Unauthorized error
  class UnauthorizedError < Error
    def initialize(message : String = "Unauthorized access")
      super(message, "UNAUTHORIZED", retriable: false)
    end
  end

  # Quota exceeded error
  class QuotaExceededError < Error
    def initialize(message : String = "Quota exceeded")
      super(message, "QUOTA_EXCEEDED", retriable: true)
    end
  end

  # Timeout error
  class TimeoutError < Error
    def initialize(message : String = "Request timed out")
      super(message, "TIMEOUT", retriable: true)
    end
  end

  # Disconnected error
  class DisconnectedError < Error
    def initialize(message : String = "Disconnected from broker")
      super(message, "DISCONNECTED", retriable: true)
    end
  end

  # Serialization error
  class SerializationError < Error
    def initialize(message : String)
      super(message, "SERIALIZATION_ERROR", retriable: false)
    end
  end

  # Connection error
  class ConnectionError < Error
    getter address : String?

    def initialize(message : String, @address : String? = nil)
      super(message, "CONNECTION_ERROR", retriable: true)
    end
  end

  # Transaction error
  class TransactionError < Error
    def initialize(message : String)
      super(message, "TRANSACTION_ERROR", retriable: false)
    end
  end

  # Commit error
  class CommitError < Error
    def initialize(message : String = "Failed to commit offset")
      super(message, "COMMIT_ERROR", retriable: true)
    end
  end
end
