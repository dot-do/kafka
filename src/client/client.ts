/**
 * Main kafka.do Client
 */

import { KafkaProducerClient, type ProducerOptions } from './producer'
import { KafkaConsumerClient, type ConsumerOptions } from './consumer'
import { KafkaAdminClient } from './admin'

export interface KafkaClientConfig {
  /** Base URL of the kafka.do service */
  baseUrl: string
  /** Client identifier */
  clientId?: string
  /** Optional fetch implementation (defaults to global fetch) */
  fetch?: typeof fetch
  /** Default request headers */
  headers?: Record<string, string>
  /** Request timeout in milliseconds */
  timeout?: number
}

/**
 * KafkaClient - Main entry point for the kafka.do SDK
 */
export class KafkaClient {
  private config: Required<Omit<KafkaClientConfig, 'headers'>> & { headers: Record<string, string> }

  constructor(config: KafkaClientConfig) {
    this.config = {
      baseUrl: config.baseUrl.replace(/\/$/, ''), // Remove trailing slash
      clientId: config.clientId ?? `do-client-${Date.now()}`,
      fetch: config.fetch ?? globalThis.fetch.bind(globalThis),
      headers: config.headers ?? {},
      timeout: config.timeout ?? 30000,
    }
  }

  /**
   * Create a producer client
   */
  producer(options?: ProducerOptions): KafkaProducerClient {
    return new KafkaProducerClient(this.config, options)
  }

  /**
   * Create a consumer client
   */
  consumer(options: ConsumerOptions): KafkaConsumerClient {
    return new KafkaConsumerClient(this.config, options)
  }

  /**
   * Create an admin client
   */
  admin(): KafkaAdminClient {
    return new KafkaAdminClient(this.config)
  }

  /**
   * Health check
   */
  async health(): Promise<{ status: string; timestamp: number }> {
    const response = await this.config.fetch(`${this.config.baseUrl}/health`, {
      headers: this.config.headers,
    })

    if (!response.ok) {
      throw new Error(`Health check failed: ${response.statusText}`)
    }

    return response.json()
  }

  /**
   * Get service info
   */
  async info(): Promise<{
    name: string
    description: string
    version: string
    status: string
  }> {
    const response = await this.config.fetch(`${this.config.baseUrl}/`, {
      headers: this.config.headers,
    })

    if (!response.ok) {
      throw new Error(`Info request failed: ${response.statusText}`)
    }

    return response.json()
  }
}

/**
 * Create a new kafka.do client
 */
export function createClient(config: KafkaClientConfig): KafkaClient {
  return new KafkaClient(config)
}
