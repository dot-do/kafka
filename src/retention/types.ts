/**
 * Retention Policy Types
 *
 * Type definitions for Kafka-compatible message retention enforcement.
 * These types define the interface for time-based and size-based retention,
 * as well as log compaction.
 */

/**
 * Cleanup policy for the topic
 * - 'delete': Remove old segments based on time or size
 * - 'compact': Keep only the latest value for each key
 * - 'delete,compact': Both behaviors combined
 */
export type CleanupPolicy = 'delete' | 'compact' | 'delete,compact'

/**
 * Configuration for retention policies
 */
export interface RetentionConfig {
  /** Maximum time to retain messages in milliseconds. -1 for infinite. */
  retentionMs: number
  /** Maximum size of partition in bytes. -1 for no limit. */
  retentionBytes: number
  /** Cleanup policy */
  cleanupPolicy: CleanupPolicy
}

/**
 * Message representation for retention enforcement
 */
export interface RetentionMessage {
  /** Message offset */
  offset: number
  /** Message key (used for compaction) */
  key?: string | null
  /** Message timestamp */
  timestamp: number
  /** Message value */
  value: string | null
  /** Message size in bytes (optional, can be computed) */
  size?: number
  /** Whether there are active consumers reading this message */
  activeConsumers?: boolean
}

/**
 * Metrics from a retention enforcement run
 */
export interface RetentionMetrics {
  /** Number of messages scanned */
  messagesScanned: number
  /** Messages deleted due to time-based retention */
  deletedByTime: number
  /** Messages deleted due to size-based retention */
  deletedBySize: number
  /** Messages deleted due to compaction */
  deletedByCompaction: number
  /** Total bytes reclaimed */
  bytesReclaimed: number
  /** Duration of enforcement in milliseconds */
  durationMs: number
}

/**
 * Result of a retention enforcement run
 */
export interface CleanupResult {
  /** Number of messages deleted */
  deletedCount: number
  /** Offsets of deleted messages */
  deletedOffsets: number[]
  /** New log start offset after cleanup (if changed) */
  newLogStartOffset?: number
  /** Bytes reclaimed by deletion */
  bytesReclaimed: number
  /** Total bytes remaining after cleanup */
  totalBytesAfter?: number
  /** For compaction: offsets that remain in compacted log */
  compactedOffsets?: number[]
  /** Detailed metrics */
  metrics: RetentionMetrics
}

/**
 * Retention policy interface
 */
export interface RetentionPolicy {
  /** The retention configuration */
  readonly config: RetentionConfig

  /**
   * Enforce retention policy on a set of messages
   * @param messages Messages to evaluate
   * @returns Cleanup result with deleted offsets and metrics
   */
  enforce(messages: RetentionMessage[]): Promise<CleanupResult>

  /**
   * Check if a specific message should be retained
   * @param message Message to check
   * @param currentPartitionSize Current partition size in bytes
   * @returns true if message should be retained
   */
  shouldRetain(message: RetentionMessage, currentPartitionSize: number): boolean
}

/**
 * Scheduled retention run information
 */
export interface ScheduledRetentionRun {
  /** Partition identifier */
  partitionId: string
  /** When the next run is scheduled */
  nextRunAt: number
  /** Check interval in milliseconds */
  intervalMs: number
  /** Last run timestamp */
  lastRunAt?: number
  /** Last run result */
  lastResult?: CleanupResult
}

/**
 * Retention scheduler interface
 */
export interface RetentionScheduler {
  /**
   * Schedule retention enforcement for a partition
   * @param policy Retention policy to use
   * @param partitionId Partition identifier
   * @returns Scheduled run information
   */
  schedule(policy: RetentionPolicy, partitionId: string): Promise<ScheduledRetentionRun>

  /**
   * Cancel scheduled retention for a partition
   * @param partitionId Partition identifier
   */
  cancel(partitionId: string): Promise<void>

  /**
   * Get all scheduled runs
   */
  getScheduled(): Promise<ScheduledRetentionRun[]>
}
