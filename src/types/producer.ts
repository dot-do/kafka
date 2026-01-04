/**
 * Producer types for Kafdo
 */

import type { ProducerRecord, RecordMetadata } from './records'

/**
 * Configuration for creating a producer
 */
export interface ProducerConfig {
  /** Client identifier for tracking */
  clientId?: string
  /** Number of messages to batch before sending */
  batchSize?: number
  /** Time to wait for batch to fill (milliseconds) */
  lingerMs?: number
  /** Maximum size of a request in bytes */
  maxRequestSize?: number
  /** Acknowledgment mode: 0, 1, or 'all' */
  acks?: 0 | 1 | 'all'
  /** Enable idempotent producer */
  idempotent?: boolean
  /** Transactional ID for exactly-once semantics */
  transactionalId?: string
  /** Request timeout in milliseconds */
  requestTimeoutMs?: number
  /** Number of retries for failed sends */
  retries?: number
}

/**
 * Producer interface for sending messages
 */
export interface Producer<K = string, V = unknown> {
  /**
   * Send a single record to a topic
   */
  send(record: ProducerRecord<K, V>): Promise<RecordMetadata>

  /**
   * Send multiple records in a batch
   */
  sendBatch(records: ProducerRecord<K, V>[]): Promise<RecordMetadata[]>

  /**
   * Flush any buffered records
   */
  flush(): Promise<void>

  /**
   * Close the producer and release resources
   */
  close(): Promise<void>
}

/**
 * Extended producer with transaction support
 */
export interface TransactionalProducer<K = string, V = unknown> extends Producer<K, V> {
  /**
   * Begin a new transaction
   */
  beginTransaction(): Promise<void>

  /**
   * Commit the current transaction
   */
  commitTransaction(): Promise<void>

  /**
   * Abort the current transaction
   */
  abortTransaction(): Promise<void>
}

/**
 * Options for send operations
 */
export interface SendOptions {
  /** Timeout for the send operation */
  timeout?: number
  /** Custom partitioner function */
  partitioner?: (key: string | undefined, partitionCount: number) => number
}
