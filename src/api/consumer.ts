/**
 * Consumer API - High-level interface for consuming messages from Kafdo
 */

import type { Env } from '../index'
import type { ConsumerRecord, TopicPartition, OffsetAndMetadata } from '../types/records'
import type { ConsumerConfig, Consumer, RebalanceListener } from '../types/consumer'

/**
 * KafdoConsumer - Consumes messages from Kafdo topics
 */
export class KafdoConsumer<K = string, V = unknown> implements Consumer<K, V> {
  private env: Env
  private config: Required<ConsumerConfig>
  private memberId: string | null = null
  private generationId: number = 0
  private subscribedTopics: string[] = []
  private assignedPartitions: TopicPartition[] = []
  private pausedPartitions: Set<string> = new Set()
  private currentOffsets: Map<string, number> = new Map()
  private uncommittedOffsets: Map<string, number> = new Map()
  private rebalanceListener?: RebalanceListener
  private heartbeatInterval?: ReturnType<typeof setInterval>
  private lastAutoCommit: number = 0
  private closed = false

  constructor(env: Env, config: ConsumerConfig, rebalanceListener?: RebalanceListener) {
    this.env = env
    this.rebalanceListener = rebalanceListener
    this.config = {
      groupId: config.groupId,
      clientId: config.clientId ?? 'kafdo-consumer',
      sessionTimeoutMs: config.sessionTimeoutMs ?? 30000,
      heartbeatIntervalMs: config.heartbeatIntervalMs ?? 3000,
      maxPollRecords: config.maxPollRecords ?? 500,
      autoCommit: config.autoCommit ?? true,
      autoCommitIntervalMs: config.autoCommitIntervalMs ?? 5000,
      fromBeginning: config.fromBeginning ?? false,
      rebalanceTimeoutMs: config.rebalanceTimeoutMs ?? 60000,
    }
  }

  /**
   * Get the partition key string
   */
  private partitionKey(tp: TopicPartition): string {
    return `${tp.topic}-${tp.partition}`
  }

  /**
   * Get the consumer group DO stub
   */
  private getGroupStub() {
    const groupId = this.env.CONSUMER_GROUP.idFromName(this.config.groupId)
    return this.env.CONSUMER_GROUP.get(groupId)
  }

  /**
   * Join the consumer group and get partition assignments
   */
  private async joinGroup(): Promise<void> {
    const groupStub = this.getGroupStub()

    const response = await groupStub.fetch('http://localhost/join', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        memberId: this.memberId,
        clientId: this.config.clientId,
        clientHost: 'worker',
        sessionTimeout: this.config.sessionTimeoutMs,
        rebalanceTimeout: this.config.rebalanceTimeoutMs,
        topics: this.subscribedTopics,
      }),
    })

    if (!response.ok) {
      throw new Error(`Failed to join consumer group: ${response.statusText}`)
    }

    const result = await response.json() as {
      memberId: string
      generationId: number
      leaderId: string
      members: Array<{ memberId: string; assignedPartitions: TopicPartition[] }>
    }

    this.memberId = result.memberId
    this.generationId = result.generationId

    // Simple round-robin partition assignment for now
    // In a real implementation, the leader would compute assignments
    await this.assignPartitions()
  }

  /**
   * Assign partitions to this consumer
   * Simple implementation - assigns all partitions of subscribed topics
   */
  private async assignPartitions(): Promise<void> {
    const oldPartitions = [...this.assignedPartitions]
    this.assignedPartitions = []

    // Get partition info for each subscribed topic
    const metadataId = this.env.CLUSTER_METADATA.idFromName('cluster')
    const metadataStub = this.env.CLUSTER_METADATA.get(metadataId)

    for (const topic of this.subscribedTopics) {
      const response = await metadataStub.fetch(`http://localhost/topics/${topic}`)
      if (response.ok) {
        const metadata = await response.json() as { partitions: Array<{ partition: number }> }
        for (const p of metadata.partitions) {
          this.assignedPartitions.push({ topic, partition: p.partition })
        }
      }
    }

    // Notify listener of revoked partitions
    if (oldPartitions.length > 0 && this.rebalanceListener?.onPartitionsRevoked) {
      await this.rebalanceListener.onPartitionsRevoked(oldPartitions)
    }

    // Fetch committed offsets for assigned partitions
    await this.fetchCommittedOffsets()

    // Notify listener of assigned partitions
    if (this.assignedPartitions.length > 0 && this.rebalanceListener?.onPartitionsAssigned) {
      await this.rebalanceListener.onPartitionsAssigned(this.assignedPartitions)
    }
  }

  /**
   * Fetch committed offsets for assigned partitions
   */
  private async fetchCommittedOffsets(): Promise<void> {
    if (this.assignedPartitions.length === 0) return

    const groupStub = this.getGroupStub()
    const response = await groupStub.fetch('http://localhost/fetch-offsets', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ partitions: this.assignedPartitions }),
    })

    if (!response.ok) {
      throw new Error(`Failed to fetch offsets: ${response.statusText}`)
    }

    const offsets = await response.json() as Record<string, number>

    for (const tp of this.assignedPartitions) {
      const key = this.partitionKey(tp)
      const committedOffset = offsets[key] ?? -1

      if (committedOffset >= 0) {
        // Start from next offset after committed
        this.currentOffsets.set(key, committedOffset + 1)
      } else if (this.config.fromBeginning) {
        // Start from beginning
        this.currentOffsets.set(key, 0)
      } else {
        // Start from end - fetch high watermark
        const partitionId = this.env.TOPIC_PARTITION.idFromName(key)
        const partitionStub = this.env.TOPIC_PARTITION.get(partitionId)
        const hwmResponse = await partitionStub.fetch('http://localhost/watermarks')
        if (hwmResponse.ok) {
          const { high } = await hwmResponse.json() as { low: number; high: number }
          this.currentOffsets.set(key, high)
        } else {
          this.currentOffsets.set(key, 0)
        }
      }
    }
  }

  /**
   * Start the heartbeat timer
   */
  private startHeartbeat(): void {
    if (this.heartbeatInterval) return

    this.heartbeatInterval = setInterval(async () => {
      if (this.closed || !this.memberId) return

      try {
        const groupStub = this.getGroupStub()
        const response = await groupStub.fetch('http://localhost/heartbeat', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            memberId: this.memberId,
            generationId: this.generationId,
          }),
        })

        if (response.ok) {
          const result = await response.json() as { needsRebalance: boolean }
          if (result.needsRebalance) {
            await this.joinGroup()
          }
        }
      } catch {
        // Heartbeat failed, will retry on next interval
      }
    }, this.config.heartbeatIntervalMs)
  }

  /**
   * Stop the heartbeat timer
   */
  private stopHeartbeat(): void {
    if (this.heartbeatInterval) {
      clearInterval(this.heartbeatInterval)
      this.heartbeatInterval = undefined
    }
  }

  /**
   * Subscribe to topics
   */
  async subscribe(topics: string[]): Promise<void> {
    if (this.closed) {
      throw new Error('Consumer is closed')
    }

    this.subscribedTopics = [...new Set([...this.subscribedTopics, ...topics])]
    await this.joinGroup()
    this.startHeartbeat()
  }

  /**
   * Unsubscribe from all topics
   */
  async unsubscribe(): Promise<void> {
    if (this.closed) return

    // Commit any uncommitted offsets before unsubscribing
    if (this.config.autoCommit && this.uncommittedOffsets.size > 0) {
      await this.commit()
    }

    this.subscribedTopics = []
    this.assignedPartitions = []
    this.currentOffsets.clear()
    this.uncommittedOffsets.clear()
    this.pausedPartitions.clear()
  }

  /**
   * Poll for new records
   */
  async poll(timeout: number = 1000): Promise<ConsumerRecord<K, V>[]> {
    if (this.closed) {
      throw new Error('Consumer is closed')
    }

    if (this.assignedPartitions.length === 0) {
      return []
    }

    // Auto-commit if enabled and interval has passed
    if (
      this.config.autoCommit &&
      this.uncommittedOffsets.size > 0 &&
      Date.now() - this.lastAutoCommit >= this.config.autoCommitIntervalMs
    ) {
      await this.commit()
    }

    const records: ConsumerRecord<K, V>[] = []
    const recordsPerPartition = Math.ceil(this.config.maxPollRecords / this.assignedPartitions.length)

    // Fetch from each assigned partition
    const fetchPromises = this.assignedPartitions
      .filter(tp => !this.pausedPartitions.has(this.partitionKey(tp)))
      .map(async (tp) => {
        const key = this.partitionKey(tp)
        const offset = this.currentOffsets.get(key) ?? 0

        const partitionId = this.env.TOPIC_PARTITION.idFromName(key)
        const partitionStub = this.env.TOPIC_PARTITION.get(partitionId)

        const response = await partitionStub.fetch(
          `http://localhost/read?offset=${offset}&limit=${recordsPerPartition}`
        )

        if (!response.ok) return []

        const messages = await response.json() as Array<{
          offset: number
          key: string | null
          value: string
          headers: Record<string, string>
          timestamp: number
        }>

        return messages.map(msg => ({
          topic: tp.topic,
          partition: tp.partition,
          offset: msg.offset,
          key: msg.key as K | null,
          value: (this.tryParseJson(msg.value) ?? msg.value) as V,
          headers: msg.headers,
          timestamp: msg.timestamp,
        }))
      })

    const results = await Promise.race([
      Promise.all(fetchPromises),
      new Promise<never[]>((resolve) => setTimeout(() => resolve([]), timeout)),
    ])

    for (const partitionRecords of results) {
      records.push(...partitionRecords)

      // Update current offset for the partition
      if (partitionRecords.length > 0) {
        const lastRecord = partitionRecords[partitionRecords.length - 1]
        const key = this.partitionKey({ topic: lastRecord.topic, partition: lastRecord.partition })
        this.currentOffsets.set(key, lastRecord.offset + 1)
        this.uncommittedOffsets.set(key, lastRecord.offset)
      }
    }

    // Respect maxPollRecords
    return records.slice(0, this.config.maxPollRecords)
  }

  /**
   * Try to parse a string as JSON
   */
  private tryParseJson(value: string): unknown {
    try {
      return JSON.parse(value)
    } catch {
      return null
    }
  }

  /**
   * Commit current offsets
   */
  async commit(): Promise<void> {
    if (this.uncommittedOffsets.size === 0) return

    const offsets = Array.from(this.uncommittedOffsets.entries()).map(([key, offset]) => {
      const [topic, partition] = key.split('-')
      return { topic, partition: parseInt(partition, 10), offset }
    })

    const groupStub = this.getGroupStub()
    const response = await groupStub.fetch('http://localhost/commit', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        memberId: this.memberId,
        offsets,
      }),
    })

    if (!response.ok) {
      throw new Error(`Failed to commit offsets: ${response.statusText}`)
    }

    this.uncommittedOffsets.clear()
    this.lastAutoCommit = Date.now()
  }

  /**
   * Commit specific offsets synchronously
   */
  async commitSync(offsets?: Map<TopicPartition, OffsetAndMetadata>): Promise<void> {
    const offsetsToCommit = offsets
      ? Array.from(offsets.entries()).map(([tp, om]) => ({
          topic: tp.topic,
          partition: tp.partition,
          offset: om.offset,
          metadata: om.metadata,
        }))
      : Array.from(this.uncommittedOffsets.entries()).map(([key, offset]) => {
          const [topic, partition] = key.split('-')
          return { topic, partition: parseInt(partition, 10), offset }
        })

    if (offsetsToCommit.length === 0) return

    const groupStub = this.getGroupStub()
    const response = await groupStub.fetch('http://localhost/commit', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        memberId: this.memberId,
        offsets: offsetsToCommit,
      }),
    })

    if (!response.ok) {
      throw new Error(`Failed to commit offsets: ${response.statusText}`)
    }

    if (!offsets) {
      this.uncommittedOffsets.clear()
    }
    this.lastAutoCommit = Date.now()
  }

  /**
   * Seek to a specific offset
   */
  async seek(partition: TopicPartition, offset: number): Promise<void> {
    const key = this.partitionKey(partition)
    this.currentOffsets.set(key, offset)
    // Clear uncommitted offset since we're seeking
    this.uncommittedOffsets.delete(key)
  }

  /**
   * Pause consumption from partitions
   */
  async pause(partitions: TopicPartition[]): Promise<void> {
    for (const tp of partitions) {
      this.pausedPartitions.add(this.partitionKey(tp))
    }
  }

  /**
   * Resume consumption from paused partitions
   */
  async resume(partitions: TopicPartition[]): Promise<void> {
    for (const tp of partitions) {
      this.pausedPartitions.delete(this.partitionKey(tp))
    }
  }

  /**
   * Close the consumer and leave the group
   */
  async close(): Promise<void> {
    if (this.closed) return

    this.closed = true
    this.stopHeartbeat()

    // Commit any uncommitted offsets
    if (this.config.autoCommit && this.uncommittedOffsets.size > 0) {
      try {
        await this.commit()
      } catch {
        // Best effort commit on close
      }
    }

    // Leave the group
    if (this.memberId) {
      try {
        const groupStub = this.getGroupStub()
        await groupStub.fetch('http://localhost/leave', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ memberId: this.memberId }),
        })
      } catch {
        // Best effort leave on close
      }
    }

    this.memberId = null
    this.subscribedTopics = []
    this.assignedPartitions = []
    this.currentOffsets.clear()
    this.uncommittedOffsets.clear()
    this.pausedPartitions.clear()
  }

  /**
   * Async iterator for consuming records
   */
  async *[Symbol.asyncIterator](): AsyncIterableIterator<ConsumerRecord<K, V>> {
    while (!this.closed) {
      const records = await this.poll()
      for (const record of records) {
        yield record
      }
      // Small delay if no records to prevent busy loop
      if (records.length === 0) {
        await new Promise(resolve => setTimeout(resolve, 100))
      }
    }
  }
}

/**
 * Create a new consumer
 */
export function createConsumer<K = string, V = unknown>(
  env: Env,
  config: ConsumerConfig,
  rebalanceListener?: RebalanceListener
): KafdoConsumer<K, V> {
  return new KafdoConsumer(env, config, rebalanceListener)
}
