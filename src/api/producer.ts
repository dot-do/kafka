/**
 * Producer API - High-level interface for sending messages to kafka.do
 */

import type { Env } from '../index'
import type { ProducerRecord, RecordMetadata } from '../types/records'
import type { ProducerConfig } from '../types/producer'
import { TopicNotFoundError, ProduceError } from '../errors'

/**
 * Default partitioner using murmur2 hash of the key
 */
function defaultPartitioner(key: string | undefined, partitionCount: number): number {
  if (!key) {
    return Math.floor(Math.random() * partitionCount)
  }
  // Simple hash-based partitioning
  let hash = 0
  for (let i = 0; i < key.length; i++) {
    const char = key.charCodeAt(i)
    hash = ((hash << 5) - hash) + char
    hash = hash & hash // Convert to 32bit integer
  }
  return Math.abs(hash) % partitionCount
}

/**
 * KafkaProducer - Sends messages to kafka.do topics
 */
export class KafkaProducer {
  private env: Env
  private topicMetadataCache: Map<string, { partitions: number; timestamp: number }> = new Map()
  private readonly CACHE_TTL = 60000 // 1 minute

  constructor(env: Env, _config: ProducerConfig = {}) {
    this.env = env
  }

  /**
   * Get the number of partitions for a topic
   */
  private async getPartitionCount(topic: string): Promise<number> {
    const cached = this.topicMetadataCache.get(topic)
    if (cached && Date.now() - cached.timestamp < this.CACHE_TTL) {
      return cached.partitions
    }

    // Query cluster metadata
    const metadataId = this.env.CLUSTER_METADATA.idFromName('cluster')
    const metadataStub = this.env.CLUSTER_METADATA.get(metadataId)

    const response = await metadataStub.fetch(`http://localhost/topics/${topic}`)
    if (!response.ok) {
      throw new TopicNotFoundError(`Topic ${topic} not found`)
    }

    const metadata = await response.json() as { partitions: Array<{ partition: number }> }
    const partitionCount = metadata.partitions.length

    this.topicMetadataCache.set(topic, { partitions: partitionCount, timestamp: Date.now() })
    return partitionCount
  }

  /**
   * Get the partition for a record
   */
  private async resolvePartition(record: ProducerRecord): Promise<number> {
    if (record.partition !== undefined) {
      return record.partition
    }

    const partitionCount = await this.getPartitionCount(record.topic)
    return defaultPartitioner(record.key, partitionCount)
  }

  /**
   * Send a single record to a topic
   */
  async send<K = string, V = unknown>(record: ProducerRecord<K, V>): Promise<RecordMetadata> {
    const partition = await this.resolvePartition(record as ProducerRecord)

    // Get the partition DO
    const partitionId = this.env.TOPIC_PARTITION.idFromName(`${record.topic}-${partition}`)
    const partitionStub = this.env.TOPIC_PARTITION.get(partitionId)

    const response = await partitionStub.fetch('http://localhost/append', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        key: record.key,
        value: typeof record.value === 'string' ? record.value : JSON.stringify(record.value),
        headers: record.headers,
        timestamp: record.timestamp,
      }),
    })

    if (!response.ok) {
      throw new ProduceError(`Failed to send message: ${response.statusText}`)
    }

    const result = await response.json() as { offset: number; timestamp: number }

    return {
      topic: record.topic,
      partition,
      offset: result.offset,
      timestamp: result.timestamp,
    }
  }

  /**
   * Send multiple records in a batch
   */
  async sendBatch<K = string, V = unknown>(records: ProducerRecord<K, V>[]): Promise<RecordMetadata[]> {
    // Group records by topic-partition
    const batches = new Map<string, { partition: number; records: ProducerRecord<K, V>[] }>()

    for (const record of records) {
      const partition = await this.resolvePartition(record as ProducerRecord)
      const key = `${record.topic}::${partition}`

      if (!batches.has(key)) {
        batches.set(key, { partition, records: [] })
      }
      batches.get(key)!.records.push(record)
    }

    // Send each batch to its partition
    const results: RecordMetadata[] = []

    for (const [key, batch] of batches) {
      const [topic, partitionStr] = key.split('::')
      const partitionId = this.env.TOPIC_PARTITION.idFromName(`${topic}-${partitionStr}`)
      const partitionStub = this.env.TOPIC_PARTITION.get(partitionId)

      const response = await partitionStub.fetch('http://localhost/append-batch', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(
          batch.records.map(r => ({
            key: r.key,
            value: typeof r.value === 'string' ? r.value : JSON.stringify(r.value),
            headers: r.headers,
          }))
        ),
      })

      if (!response.ok) {
        throw new ProduceError(`Failed to send batch: ${response.statusText}`)
      }

      const batchResults = await response.json() as Array<{ offset: number; timestamp: number }>

      for (const result of batchResults) {
        results.push({
          topic,
          partition: batch.partition,
          offset: result.offset,
          timestamp: result.timestamp,
        })
      }
    }

    return results
  }

  /**
   * Flush any buffered records (no-op for now, records are sent immediately)
   */
  async flush(): Promise<void> {
    // Currently records are sent immediately
    // Future: implement batching with lingerMs
  }

  /**
   * Close the producer
   */
  async close(): Promise<void> {
    this.topicMetadataCache.clear()
  }
}

/**
 * Create a new producer
 */
export function createProducer(env: Env, config?: ProducerConfig): KafkaProducer {
  return new KafkaProducer(env, config)
}
