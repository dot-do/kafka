/**
 * kafka.do Admin Client
 */

import type { KafkaClientConfig } from './client'
import type {
  TopicConfig,
  TopicMetadata,
  GroupDescription,
} from '../types/admin'
import { TopicNotFoundError, ConsumerGroupError, KafkaError } from '../errors'

/**
 * KafkaAdminClient - HTTP client for admin operations
 */
export class KafkaAdminClient {
  private config: KafkaClientConfig

  constructor(config: KafkaClientConfig) {
    this.config = config
  }

  private get fetchFn(): typeof fetch {
    return this.config.fetch ?? globalThis.fetch.bind(globalThis)
  }

  private get baseUrl(): string {
    return (this.config.baseUrl ?? '').replace(/\/$/, '')
  }

  // ============================================================================
  // Topic Operations
  // ============================================================================

  /**
   * Create a new topic
   */
  async createTopic(config: TopicConfig): Promise<{ success: boolean; topic: string }> {
    const response = await this.fetchFn(`${this.baseUrl}/admin/topics`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        ...this.config.headers,
      },
      body: JSON.stringify(config),
    })

    if (!response.ok) {
      const error = await response.text()
      throw new KafkaError('TOPIC_CREATE_FAILED', `Failed to create topic: ${error}`)
    }

    return response.json()
  }

  /**
   * List all topics
   */
  async listTopics(): Promise<string[]> {
    const response = await this.fetchFn(`${this.baseUrl}/admin/topics`, {
      headers: this.config.headers,
    })

    if (!response.ok) {
      throw new KafkaError('LIST_TOPICS_FAILED', `Failed to list topics: ${response.statusText}`)
    }

    const result = (await response.json()) as { topics: string[] }
    return result.topics
  }

  /**
   * Describe a topic
   */
  async describeTopic(topic: string): Promise<TopicMetadata> {
    const response = await this.fetchFn(`${this.baseUrl}/admin/topics/${topic}`, {
      headers: this.config.headers,
    })

    if (!response.ok) {
      if (response.status === 404) {
        throw new TopicNotFoundError(`Topic ${topic} not found`)
      }
      throw new TopicNotFoundError(`Failed to describe topic: ${response.statusText}`)
    }

    return response.json()
  }

  /**
   * Delete a topic
   */
  async deleteTopic(topic: string): Promise<void> {
    const response = await this.fetchFn(`${this.baseUrl}/admin/topics/${topic}`, {
      method: 'DELETE',
      headers: this.config.headers,
    })

    if (!response.ok) {
      throw new TopicNotFoundError(`Failed to delete topic: ${response.statusText}`)
    }
  }

  /**
   * Add partitions to a topic
   */
  async addPartitions(topic: string, count: number): Promise<void> {
    const response = await this.fetchFn(`${this.baseUrl}/admin/topics/${topic}/partitions`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        ...this.config.headers,
      },
      body: JSON.stringify({ count }),
    })

    if (!response.ok) {
      throw new TopicNotFoundError(`Failed to add partitions: ${response.statusText}`)
    }
  }

  /**
   * Get topic offsets
   */
  async getTopicOffsets(topic: string): Promise<{
    topic: string
    partitions: Array<{
      partition: number
      earliest: number
      latest: number
    }>
  }> {
    const response = await this.fetchFn(`${this.baseUrl}/admin/topics/${topic}/offsets`, {
      headers: this.config.headers,
    })

    if (!response.ok) {
      throw new TopicNotFoundError(`Failed to get topic offsets: ${response.statusText}`)
    }

    return response.json()
  }

  // ============================================================================
  // Consumer Group Operations
  // ============================================================================

  /**
   * List all consumer groups
   */
  async listGroups(): Promise<string[]> {
    const response = await this.fetchFn(`${this.baseUrl}/admin/groups`, {
      headers: this.config.headers,
    })

    if (!response.ok) {
      throw new ConsumerGroupError(`Failed to list groups: ${response.statusText}`)
    }

    const result = (await response.json()) as { groups: string[] }
    return result.groups
  }

  /**
   * Describe a consumer group
   */
  async describeGroup(groupId: string): Promise<GroupDescription> {
    const response = await this.fetchFn(`${this.baseUrl}/admin/groups/${groupId}`, {
      headers: this.config.headers,
    })

    if (!response.ok) {
      throw new ConsumerGroupError(`Failed to describe group: ${response.statusText}`)
    }

    return response.json()
  }

  /**
   * Delete a consumer group
   */
  async deleteGroup(groupId: string): Promise<void> {
    const response = await this.fetchFn(`${this.baseUrl}/admin/groups/${groupId}`, {
      method: 'DELETE',
      headers: this.config.headers,
    })

    if (!response.ok) {
      throw new ConsumerGroupError(`Failed to delete group: ${response.statusText}`)
    }
  }
}
