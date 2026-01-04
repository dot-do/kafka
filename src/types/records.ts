/**
 * Core record types for Kafdo messages
 */

/**
 * A record to be sent to a topic
 */
export interface ProducerRecord<K = string, V = unknown> {
  /** Target topic name */
  topic: string
  /** Optional partition key for routing */
  key?: K
  /** Message payload */
  value: V
  /** Optional explicit partition assignment */
  partition?: number
  /** Optional message headers */
  headers?: Record<string, string>
  /** Optional timestamp (defaults to current time) */
  timestamp?: number
}

/**
 * A record received from a topic
 */
export interface ConsumerRecord<K = string, V = unknown> {
  /** Topic the message came from */
  topic: string
  /** Partition the message was read from */
  partition: number
  /** Offset within the partition */
  offset: number
  /** Message key (may be null) */
  key: K | null
  /** Message payload */
  value: V
  /** Message headers */
  headers: Record<string, string>
  /** Message timestamp */
  timestamp: number
}

/**
 * Metadata returned after successfully sending a message
 */
export interface RecordMetadata {
  /** Topic the message was sent to */
  topic: string
  /** Partition the message was written to */
  partition: number
  /** Offset assigned to the message */
  offset: number
  /** Timestamp of the message */
  timestamp: number
}

/**
 * Identifies a specific topic-partition
 */
export interface TopicPartition {
  /** Topic name */
  topic: string
  /** Partition number */
  partition: number
}

/**
 * Offset with optional metadata for commits
 */
export interface OffsetAndMetadata {
  /** The offset to commit */
  offset: number
  /** Optional metadata string */
  metadata?: string
}

/**
 * Batch of records for efficient sending
 */
export interface ProducerBatch<K = string, V = unknown> {
  /** Topic for all records in batch */
  topic: string
  /** Records to send */
  records: Array<Omit<ProducerRecord<K, V>, 'topic'>>
}
