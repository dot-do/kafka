/**
 * Consumer types for Kafdo
 */

import type { ConsumerRecord, TopicPartition, OffsetAndMetadata } from './records'

/**
 * Consumer group state
 */
export type ConsumerGroupState =
  | 'Empty'
  | 'Stable'
  | 'PreparingRebalance'
  | 'CompletingRebalance'
  | 'Dead'

/**
 * Configuration for creating a consumer
 */
export interface ConsumerConfig {
  /** Consumer group ID (required) */
  groupId: string
  /** Client identifier for tracking */
  clientId?: string
  /** Session timeout in milliseconds */
  sessionTimeoutMs?: number
  /** Heartbeat interval in milliseconds */
  heartbeatIntervalMs?: number
  /** Maximum records to return per poll */
  maxPollRecords?: number
  /** Enable automatic offset commits */
  autoCommit?: boolean
  /** Interval for auto commits in milliseconds */
  autoCommitIntervalMs?: number
  /** Start from beginning if no committed offset */
  fromBeginning?: boolean
  /** Rebalance timeout in milliseconds */
  rebalanceTimeoutMs?: number
}

/**
 * Partition assignment for a consumer
 */
export interface PartitionAssignment {
  /** Member ID in the group */
  memberId: string
  /** Assigned partitions */
  partitions: TopicPartition[]
}

/**
 * Consumer interface for reading messages
 */
export interface Consumer<K = string, V = unknown> {
  /**
   * Subscribe to topics
   */
  subscribe(topics: string[]): Promise<void>

  /**
   * Unsubscribe from all topics
   */
  unsubscribe(): Promise<void>

  /**
   * Poll for new records
   */
  poll(timeout?: number): Promise<ConsumerRecord<K, V>[]>

  /**
   * Commit current offsets
   */
  commit(): Promise<void>

  /**
   * Commit specific offsets synchronously
   */
  commitSync(offsets?: Map<TopicPartition, OffsetAndMetadata>): Promise<void>

  /**
   * Seek to a specific offset
   */
  seek(partition: TopicPartition, offset: number): Promise<void>

  /**
   * Pause consumption from partitions
   */
  pause(partitions: TopicPartition[]): Promise<void>

  /**
   * Resume consumption from paused partitions
   */
  resume(partitions: TopicPartition[]): Promise<void>

  /**
   * Close the consumer and leave the group
   */
  close(): Promise<void>

  /**
   * Async iterator for consuming records
   */
  [Symbol.asyncIterator](): AsyncIterableIterator<ConsumerRecord<K, V>>
}

/**
 * Rebalance listener for consumer group events
 */
export interface RebalanceListener {
  /** Called when partitions are assigned */
  onPartitionsAssigned?(partitions: TopicPartition[]): Promise<void>
  /** Called when partitions are revoked */
  onPartitionsRevoked?(partitions: TopicPartition[]): Promise<void>
}
