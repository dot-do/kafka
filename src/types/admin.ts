/**
 * Admin types for Kafdo cluster management
 */

import type { TopicPartition } from './records'
import type { ConsumerGroupState } from './consumer'

/**
 * Configuration for admin client
 */
export interface AdminConfig {
  /** Client identifier */
  clientId?: string
  /** Request timeout in milliseconds */
  requestTimeoutMs?: number
}

/**
 * Configuration for creating a topic
 */
export interface TopicConfig {
  /** Topic name */
  topic: string
  /** Number of partitions */
  partitions: number
  /** Replication factor (always 1 for Durable Objects) */
  replicationFactor?: number
  /** Topic configuration options */
  config?: Record<string, string>
}

/**
 * Partition metadata
 */
export interface PartitionMetadata {
  /** Partition number */
  partition: number
  /** Leader Durable Object ID */
  leader: string
  /** Replica Durable Object IDs */
  replicas: string[]
  /** In-sync replica IDs */
  isr: string[]
}

/**
 * Topic metadata
 */
export interface TopicMetadata {
  /** Topic name */
  topic: string
  /** Partition information */
  partitions: PartitionMetadata[]
  /** Topic configuration */
  config: Record<string, string>
}

/**
 * Consumer group member info
 */
export interface MemberDescription {
  /** Member ID in the group */
  memberId: string
  /** Client ID */
  clientId: string
  /** Client host */
  clientHost: string
  /** Assigned partitions */
  assignment: TopicPartition[]
}

/**
 * Consumer group description
 */
export interface GroupDescription {
  /** Group ID */
  groupId: string
  /** Current group state */
  state: ConsumerGroupState
  /** Protocol type (usually 'consumer') */
  protocolType: string
  /** Assignment protocol */
  protocol: string
  /** Group members */
  members: MemberDescription[]
}

/**
 * Offset information for a partition
 */
export interface OffsetInfo {
  /** Earliest available offset */
  earliest: number
  /** Latest offset (high watermark) */
  latest: number
}

/**
 * Admin interface for cluster management
 */
export interface Admin {
  /**
   * Create a new topic
   */
  createTopic(config: TopicConfig): Promise<void>

  /**
   * Delete a topic
   */
  deleteTopic(topic: string): Promise<void>

  /**
   * List all topics
   */
  listTopics(): Promise<string[]>

  /**
   * Get topic metadata
   */
  describeTopic(topic: string): Promise<TopicMetadata>

  /**
   * Add partitions to an existing topic
   */
  createPartitions(topic: string, count: number): Promise<void>

  /**
   * List consumer groups
   */
  listGroups(): Promise<string[]>

  /**
   * Get consumer group details
   */
  describeGroup(groupId: string): Promise<GroupDescription>

  /**
   * Delete a consumer group
   */
  deleteGroup(groupId: string): Promise<void>

  /**
   * Get partition offsets
   */
  listOffsets(topic: string): Promise<Map<number, OffsetInfo>>

  /**
   * Close the admin client
   */
  close(): Promise<void>
}
