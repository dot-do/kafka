## Kafka error types for the .do platform

import std/[options, json]

type
  KafkaError* = object of CatchableError
    ## Base error type for Kafka operations
    code*: string
    retriable*: bool

  TopicNotFoundError* = object of KafkaError
    ## Topic not found error
    topic*: string

  PartitionNotFoundError* = object of KafkaError
    ## Partition not found error
    partition*: int32

  MessageTooLargeError* = object of KafkaError
    ## Message too large error
    size*: int64
    maxSize*: int64

  NotLeaderError* = object of KafkaError
    ## Broker is not leader error

  OffsetOutOfRangeError* = object of KafkaError
    ## Offset out of range error
    offset*: int64

  GroupCoordinatorError* = object of KafkaError
    ## Group coordinator error

  RebalanceInProgressError* = object of KafkaError
    ## Consumer group rebalance in progress

  UnauthorizedError* = object of KafkaError
    ## Unauthorized access

  QuotaExceededError* = object of KafkaError
    ## Quota exceeded / rate limit

  TimeoutError* = object of KafkaError
    ## Request timed out

  DisconnectedError* = object of KafkaError
    ## Disconnected from broker

  SerializationError* = object of KafkaError
    ## Serialization error

  ConnectionError* = object of KafkaError
    ## Connection error
    address*: string

  TransactionError* = object of KafkaError
    ## Transaction error

  CommitError* = object of KafkaError
    ## Commit error

# Error constructors

proc newKafkaError*(code, message: string, retriable: bool = false): ref KafkaError =
  result = newException(KafkaError, message)
  result.code = code
  result.retriable = retriable

proc newTopicNotFoundError*(topic: string): ref TopicNotFoundError =
  result = newException(TopicNotFoundError, "Topic not found: " & topic)
  result.code = "TOPIC_NOT_FOUND"
  result.retriable = false
  result.topic = topic

proc newPartitionNotFoundError*(partition: int32): ref PartitionNotFoundError =
  result = newException(PartitionNotFoundError, "Partition not found: " & $partition)
  result.code = "PARTITION_NOT_FOUND"
  result.retriable = false
  result.partition = partition

proc newMessageTooLargeError*(size, maxSize: int64): ref MessageTooLargeError =
  result = newException(MessageTooLargeError, "Message size " & $size & " exceeds max " & $maxSize)
  result.code = "MESSAGE_TOO_LARGE"
  result.retriable = false
  result.size = size
  result.maxSize = maxSize

proc newNotLeaderError*(message: string = "Broker is not leader for partition"): ref NotLeaderError =
  result = newException(NotLeaderError, message)
  result.code = "NOT_LEADER"
  result.retriable = true

proc newOffsetOutOfRangeError*(offset: int64): ref OffsetOutOfRangeError =
  result = newException(OffsetOutOfRangeError, "Offset out of range: " & $offset)
  result.code = "OFFSET_OUT_OF_RANGE"
  result.retriable = false
  result.offset = offset

proc newGroupCoordinatorError*(message: string = "Group coordinator not available"): ref GroupCoordinatorError =
  result = newException(GroupCoordinatorError, message)
  result.code = "GROUP_COORDINATOR_ERROR"
  result.retriable = true

proc newRebalanceInProgressError*(): ref RebalanceInProgressError =
  result = newException(RebalanceInProgressError, "Consumer group is rebalancing")
  result.code = "REBALANCE_IN_PROGRESS"
  result.retriable = true

proc newUnauthorizedError*(message: string = "Unauthorized access"): ref UnauthorizedError =
  result = newException(UnauthorizedError, message)
  result.code = "UNAUTHORIZED"
  result.retriable = false

proc newQuotaExceededError*(message: string = "Quota exceeded"): ref QuotaExceededError =
  result = newException(QuotaExceededError, message)
  result.code = "QUOTA_EXCEEDED"
  result.retriable = true

proc newTimeoutError*(message: string = "Request timed out"): ref TimeoutError =
  result = newException(TimeoutError, message)
  result.code = "TIMEOUT"
  result.retriable = true

proc newDisconnectedError*(message: string = "Disconnected from broker"): ref DisconnectedError =
  result = newException(DisconnectedError, message)
  result.code = "DISCONNECTED"
  result.retriable = true

proc newSerializationError*(message: string): ref SerializationError =
  result = newException(SerializationError, message)
  result.code = "SERIALIZATION_ERROR"
  result.retriable = false

proc newConnectionError*(message: string, address: string = ""): ref ConnectionError =
  result = newException(ConnectionError, message)
  result.code = "CONNECTION_ERROR"
  result.retriable = true
  result.address = address

proc newTransactionError*(message: string): ref TransactionError =
  result = newException(TransactionError, message)
  result.code = "TRANSACTION_ERROR"
  result.retriable = false

proc newCommitError*(message: string = "Failed to commit offset"): ref CommitError =
  result = newException(CommitError, message)
  result.code = "COMMIT_ERROR"
  result.retriable = true

proc isRetriable*(err: ref KafkaError): bool =
  err.retriable
