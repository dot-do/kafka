/**
 * Admin API - High-level interface for managing kafka.do topics and groups
 */

import type { Env } from '../index'
import type {
  Admin,
  AdminConfig,
  TopicConfig,
  TopicMetadata,
  GroupDescription,
  OffsetInfo,
  PartitionMetadata,
} from '../types/admin'
import { TopicNotFoundError, ConsumerGroupError, KafkaError } from '../errors'

/**
 * KafkaAdmin - Manages kafka.do topics and consumer groups
 */
export class KafkaAdmin implements Admin {
  private env: Env

  constructor(env: Env, _config: AdminConfig = {}) {
    this.env = env
  }

  /**
   * Get the cluster metadata DO stub
   */
  private getMetadataStub() {
    const metadataId = this.env.CLUSTER_METADATA.idFromName('cluster')
    return this.env.CLUSTER_METADATA.get(metadataId)
  }

  /**
   * Create a new topic
   */
  async createTopic(config: TopicConfig): Promise<void> {
    const metadataStub = this.getMetadataStub()

    const response = await metadataStub.fetch('http://localhost/topics', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        topic: config.topic,
        partitions: config.partitions,
        replicationFactor: config.replicationFactor ?? 1,
        config: config.config ?? {},
      }),
    })

    if (!response.ok) {
      const error = await response.text()
      throw new KafkaError('TOPIC_CREATE_FAILED', `Failed to create topic: ${error}`)
    }
  }

  /**
   * Delete a topic
   */
  async deleteTopic(topic: string): Promise<void> {
    const metadataStub = this.getMetadataStub()

    const response = await metadataStub.fetch(`http://localhost/topics/${topic}`, {
      method: 'DELETE',
    })

    if (!response.ok) {
      throw new TopicNotFoundError(`Failed to delete topic: ${response.statusText}`)
    }
  }

  /**
   * List all topics
   */
  async listTopics(): Promise<string[]> {
    const metadataStub = this.getMetadataStub()

    const response = await metadataStub.fetch('http://localhost/topics')
    if (!response.ok) {
      throw new KafkaError('LIST_TOPICS_FAILED', `Failed to list topics: ${response.statusText}`)
    }

    const topics = await response.json() as string[]
    return topics
  }

  /**
   * Get topic metadata
   */
  async describeTopic(topic: string): Promise<TopicMetadata> {
    const metadataStub = this.getMetadataStub()

    const response = await metadataStub.fetch(`http://localhost/topics/${topic}`)
    if (!response.ok) {
      throw new TopicNotFoundError(`Topic ${topic} not found`)
    }

    const data = await response.json() as {
      topic: string
      partitions: Array<{ partition: number }>
      config: Record<string, string>
    }

    // Build partition metadata
    const partitions: PartitionMetadata[] = data.partitions.map((p) => {
      const leaderId = `${topic}-${p.partition}`
      return {
        partition: p.partition,
        leader: leaderId,
        replicas: [leaderId], // Single replica for Durable Objects
        isr: [leaderId],
      }
    })

    return {
      topic: data.topic,
      partitions,
      config: data.config,
    }
  }

  /**
   * Add partitions to an existing topic
   */
  async createPartitions(topic: string, count: number): Promise<void> {
    const metadataStub = this.getMetadataStub()

    const response = await metadataStub.fetch(`http://localhost/topics/${topic}/partitions`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ count }),
    })

    if (!response.ok) {
      throw new TopicNotFoundError(`Failed to add partitions: ${response.statusText}`)
    }
  }

  /**
   * List consumer groups
   */
  async listGroups(): Promise<string[]> {
    const metadataStub = this.getMetadataStub()

    const response = await metadataStub.fetch('http://localhost/groups')
    if (!response.ok) {
      throw new ConsumerGroupError(`Failed to list groups: ${response.statusText}`)
    }

    const groups = await response.json() as string[]
    return groups
  }

  /**
   * Get consumer group details
   */
  async describeGroup(groupId: string): Promise<GroupDescription> {
    const groupDOId = this.env.CONSUMER_GROUP.idFromName(groupId)
    const groupStub = this.env.CONSUMER_GROUP.get(groupDOId)

    const response = await groupStub.fetch('http://localhost/describe')
    if (!response.ok) {
      throw new ConsumerGroupError(`Failed to describe group: ${response.statusText}`)
    }

    const data = await response.json() as {
      groupId: string
      state: string
      generationId: number
      members: Array<{
        memberId: string
        clientId: string
        clientHost: string
        assignedPartitions: Array<{ topic: string; partition: number }>
      }>
    }

    return {
      groupId: data.groupId,
      state: data.state as GroupDescription['state'],
      protocolType: 'consumer',
      protocol: 'range',
      members: data.members.map((m) => ({
        memberId: m.memberId,
        clientId: m.clientId,
        clientHost: m.clientHost,
        assignment: m.assignedPartitions,
      })),
    }
  }

  /**
   * Delete a consumer group
   */
  async deleteGroup(groupId: string): Promise<void> {
    const metadataStub = this.getMetadataStub()

    const response = await metadataStub.fetch(`http://localhost/groups/${groupId}`, {
      method: 'DELETE',
    })

    if (!response.ok) {
      throw new ConsumerGroupError(`Failed to delete group: ${response.statusText}`)
    }
  }

  /**
   * Get partition offsets for a topic
   */
  async listOffsets(topic: string): Promise<Map<number, OffsetInfo>> {
    const metadata = await this.describeTopic(topic)
    const offsets = new Map<number, OffsetInfo>()

    // Fetch offsets from each partition
    for (const partition of metadata.partitions) {
      const partitionId = this.env.TOPIC_PARTITION.idFromName(`${topic}-${partition.partition}`)
      const partitionStub = this.env.TOPIC_PARTITION.get(partitionId)

      const response = await partitionStub.fetch('http://localhost/watermarks')
      if (response.ok) {
        const { low, high } = await response.json() as { low: number; high: number }
        offsets.set(partition.partition, { earliest: low, latest: high })
      } else {
        offsets.set(partition.partition, { earliest: 0, latest: 0 })
      }
    }

    return offsets
  }

  /**
   * Close the admin client
   */
  async close(): Promise<void> {
    // No cleanup needed
  }
}

/**
 * Create a new admin client
 */
export function createAdmin(env: Env, config?: AdminConfig): KafkaAdmin {
  return new KafkaAdmin(env, config)
}
