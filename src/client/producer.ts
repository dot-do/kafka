/**
 * Kafdo Producer Client
 */

import type { KafdoClientConfig } from './client'
import type { RecordMetadata } from '../types/records'

export interface ProducerOptions {
  /** Default topic to produce to */
  defaultTopic?: string
}

export interface ProduceRecord {
  /** Target topic (required if no defaultTopic) */
  topic?: string
  /** Message key for partitioning */
  key?: string
  /** Message value */
  value: unknown
  /** Explicit partition assignment */
  partition?: number
  /** Message headers */
  headers?: Record<string, string>
}

/**
 * KafdoProducerClient - HTTP client for producing messages
 */
export class KafdoProducerClient {
  private config: KafdoClientConfig
  private options: ProducerOptions

  constructor(config: KafdoClientConfig, options?: ProducerOptions) {
    this.config = config
    this.options = options ?? {}
  }

  private get fetchFn(): typeof fetch {
    return this.config.fetch ?? globalThis.fetch.bind(globalThis)
  }

  private get baseUrl(): string {
    return (this.config.baseUrl ?? '').replace(/\/$/, '')
  }

  /**
   * Send a single message
   */
  async send(record: ProduceRecord): Promise<RecordMetadata> {
    const topic = record.topic ?? this.options.defaultTopic
    if (!topic) {
      throw new Error('Topic is required (either in record or as defaultTopic)')
    }

    const response = await this.fetchFn(`${this.baseUrl}/topics/${topic}/produce`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        ...this.config.headers,
      },
      body: JSON.stringify({
        key: record.key,
        value: record.value,
        partition: record.partition,
        headers: record.headers,
      }),
    })

    if (!response.ok) {
      const error = await response.text()
      throw new Error(`Failed to produce message: ${error}`)
    }

    return response.json()
  }

  /**
   * Send multiple messages in a batch
   */
  async sendBatch(
    records: ProduceRecord[],
    options?: { topic?: string }
  ): Promise<{ results: RecordMetadata[] }> {
    const topic = options?.topic ?? this.options.defaultTopic
    if (!topic) {
      throw new Error('Topic is required (either in options or as defaultTopic)')
    }

    const response = await this.fetchFn(`${this.baseUrl}/topics/${topic}/produce-batch`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        ...this.config.headers,
      },
      body: JSON.stringify({
        records: records.map((r) => ({
          key: r.key,
          value: r.value,
          partition: r.partition,
          headers: r.headers,
        })),
      }),
    })

    if (!response.ok) {
      const error = await response.text()
      throw new Error(`Failed to produce batch: ${error}`)
    }

    return response.json()
  }
}
