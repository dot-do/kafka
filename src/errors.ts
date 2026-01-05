/**
 * Custom error classes for kafka.do
 * Provides structured error handling with error codes
 */

/**
 * Base error class for all kafka.do errors
 */
export class KafkaError extends Error {
  readonly code: string
  readonly cause?: Error

  constructor(code: string, message: string, options?: { cause?: Error }) {
    super(message)
    this.name = 'KafkaError'
    this.code = code
    this.cause = options?.cause

    // Maintains proper stack trace for where our error was thrown (only available on V8)
    if ('captureStackTrace' in Error) {
      ;(Error as { captureStackTrace: (obj: object, constructor: Function) => void }).captureStackTrace(this, this.constructor)
    }
  }
}

/**
 * Error thrown when a topic is not found
 */
export class TopicNotFoundError extends KafkaError {
  constructor(message: string, options?: { cause?: Error }) {
    super('TOPIC_NOT_FOUND', message, options)
    this.name = 'TopicNotFoundError'
  }
}

/**
 * Error thrown when a partition is not found
 */
export class PartitionNotFoundError extends KafkaError {
  constructor(message: string, options?: { cause?: Error }) {
    super('PARTITION_NOT_FOUND', message, options)
    this.name = 'PartitionNotFoundError'
  }
}

/**
 * Error thrown for consumer group related issues
 */
export class ConsumerGroupError extends KafkaError {
  constructor(message: string, options?: { cause?: Error }) {
    super('CONSUMER_GROUP_ERROR', message, options)
    this.name = 'ConsumerGroupError'
  }
}

/**
 * Error thrown when producing messages fails
 */
export class ProduceError extends KafkaError {
  constructor(message: string, options?: { cause?: Error }) {
    super('PRODUCE_FAILED', message, options)
    this.name = 'ProduceError'
  }
}

/**
 * Error thrown when consuming messages fails
 */
export class ConsumeError extends KafkaError {
  constructor(message: string, options?: { cause?: Error }) {
    super('CONSUME_FAILED', message, options)
    this.name = 'ConsumeError'
  }
}

/**
 * Error thrown for connection related issues
 */
export class ConnectionError extends KafkaError {
  constructor(message: string, options?: { cause?: Error }) {
    super('CONNECTION_ERROR', message, options)
    this.name = 'ConnectionError'
  }
}

/**
 * Error thrown when an operation times out
 */
export class TimeoutError extends KafkaError {
  constructor(message: string, options?: { cause?: Error }) {
    super('TIMEOUT', message, options)
    this.name = 'TimeoutError'
  }
}
