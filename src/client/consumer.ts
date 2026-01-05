/**
 * kafka.do Consumer Client
 */

import type { KafkaClientConfig } from './client'
import type { ConsumerRecord, TopicPartition } from '../types/records'
import { ConsumeError, ConsumerGroupError } from '../errors'

export interface ConsumerOptions {
  /** Consumer group ID */
  groupId: string
  /** Topics to subscribe to */
  topics: string[]
  /** Client identifier */
  clientId?: string
  /** Session timeout in ms */
  sessionTimeoutMs?: number
  /** Auto-commit interval in ms */
  autoCommitIntervalMs?: number
  /** Enable auto-commit */
  autoCommit?: boolean
}

interface JoinResult {
  memberId: string
  generationId: number
  leaderId: string
  members: Array<{
    memberId: string
    assignedPartitions: TopicPartition[]
  }>
}

/**
 * KafkaConsumerClient - HTTP client for consuming messages
 */
export class KafkaConsumerClient {
  private config: KafkaClientConfig
  private options: ConsumerOptions
  private memberId: string | null = null
  private generationId: number = 0
  private heartbeatInterval?: ReturnType<typeof setInterval>
  private uncommittedOffsets: Map<string, number> = new Map()
  private closed = false

  constructor(config: KafkaClientConfig, options: ConsumerOptions) {
    this.config = config
    this.options = {
      sessionTimeoutMs: 30000,
      autoCommitIntervalMs: 5000,
      autoCommit: true,
      ...options,
    }
  }

  private get fetchFn(): typeof fetch {
    return this.config.fetch ?? globalThis.fetch.bind(globalThis)
  }

  private get baseUrl(): string {
    return (this.config.baseUrl ?? '').replace(/\/$/, '')
  }

  /**
   * Connect and join the consumer group
   */
  async connect(): Promise<JoinResult> {
    const response = await this.fetchFn(
      `${this.baseUrl}/consumer-groups/${this.options.groupId}/join`,
      {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          ...this.config.headers,
        },
        body: JSON.stringify({
          clientId: this.options.clientId ?? this.config.clientId,
          topics: this.options.topics,
          sessionTimeoutMs: this.options.sessionTimeoutMs,
        }),
      }
    )

    if (!response.ok) {
      const error = await response.text()
      throw new ConsumerGroupError(`Failed to join consumer group: ${error}`)
    }

    const result = (await response.json()) as JoinResult
    this.memberId = result.memberId
    this.generationId = result.generationId

    // Start heartbeat
    this.startHeartbeat()

    return result
  }

  /**
   * Start heartbeat timer
   */
  private startHeartbeat(): void {
    if (this.heartbeatInterval) return

    const heartbeatMs = Math.floor((this.options.sessionTimeoutMs ?? 30000) / 3)
    this.heartbeatInterval = setInterval(async () => {
      if (this.closed || !this.memberId) return

      try {
        await this.heartbeat()
      } catch {
        // Heartbeat failed, will retry
      }
    }, heartbeatMs)
  }

  /**
   * Send heartbeat
   */
  async heartbeat(): Promise<{ needsRebalance: boolean }> {
    if (!this.memberId) {
      throw new ConsumerGroupError('Not connected to consumer group')
    }

    const response = await this.fetchFn(
      `${this.baseUrl}/consumer-groups/${this.options.groupId}/heartbeat`,
      {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          ...this.config.headers,
        },
        body: JSON.stringify({
          memberId: this.memberId,
          generationId: this.generationId,
        }),
      }
    )

    if (!response.ok) {
      throw new ConsumerGroupError(`Heartbeat failed: ${response.statusText}`)
    }

    return response.json()
  }

  /**
   * Fetch messages from a partition
   */
  async fetch(
    topic: string,
    partition: number,
    options?: { offset?: number; limit?: number }
  ): Promise<{
    topic: string
    partition: number
    messages: ConsumerRecord[]
  }> {
    const offset = options?.offset ?? 0
    const limit = options?.limit ?? 100

    const response = await this.fetchFn(
      `${this.baseUrl}/topics/${topic}/partitions/${partition}/messages?offset=${offset}&limit=${limit}`,
      {
        headers: this.config.headers,
      }
    )

    if (!response.ok) {
      throw new ConsumeError(`Failed to fetch messages: ${response.statusText}`)
    }

    const result = await response.json() as {
      topic: string
      partition: number
      messages: ConsumerRecord[]
    }

    // Track uncommitted offsets (using :: delimiter to support hyphenated topic names)
    if (result.messages.length > 0) {
      const lastMessage = result.messages[result.messages.length - 1]
      const key = `${topic}::${partition}`
      this.uncommittedOffsets.set(key, lastMessage.offset)
    }

    return result
  }

  /**
   * Commit offsets
   */
  async commit(
    offsets?: Array<{ topic: string; partition: number; offset: number }>
  ): Promise<void> {
    if (!this.memberId) {
      throw new ConsumerGroupError('Not connected to consumer group')
    }

    const toCommit =
      offsets ??
      Array.from(this.uncommittedOffsets.entries()).map(([key, offset]) => {
        const [topic, partition] = key.split('::')
        return { topic, partition: parseInt(partition, 10), offset }
      })

    if (toCommit.length === 0) return

    const response = await this.fetchFn(
      `${this.baseUrl}/consumer-groups/${this.options.groupId}/commit`,
      {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          ...this.config.headers,
        },
        body: JSON.stringify({
          memberId: this.memberId,
          offsets: toCommit,
        }),
      }
    )

    if (!response.ok) {
      throw new ConsumerGroupError(`Failed to commit offsets: ${response.statusText}`)
    }

    if (!offsets) {
      this.uncommittedOffsets.clear()
    }
  }

  /**
   * Get partition offsets
   */
  async getOffsets(
    topic: string,
    partition: number
  ): Promise<{
    topic: string
    partition: number
    earliest: number
    latest: number
  }> {
    const response = await this.fetchFn(
      `${this.baseUrl}/topics/${topic}/partitions/${partition}/offsets`,
      {
        headers: this.config.headers,
      }
    )

    if (!response.ok) {
      throw new ConsumeError(`Failed to get offsets: ${response.statusText}`)
    }

    return response.json()
  }

  /**
   * Disconnect from the consumer group
   */
  async disconnect(): Promise<void> {
    if (this.closed) return
    this.closed = true

    // Stop heartbeat
    if (this.heartbeatInterval) {
      clearInterval(this.heartbeatInterval)
      this.heartbeatInterval = undefined
    }

    // Commit remaining offsets if auto-commit enabled
    if (this.options.autoCommit && this.uncommittedOffsets.size > 0) {
      try {
        await this.commit()
      } catch {
        // Best effort
      }
    }

    // Leave the group
    if (this.memberId) {
      try {
        await this.fetchFn(
          `${this.baseUrl}/consumer-groups/${this.options.groupId}/leave`,
          {
            method: 'POST',
            headers: {
              'Content-Type': 'application/json',
              ...this.config.headers,
            },
            body: JSON.stringify({ memberId: this.memberId }),
          }
        )
      } catch {
        // Best effort
      }
    }

    this.memberId = null
    this.uncommittedOffsets.clear()
  }
}
