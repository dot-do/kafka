/**
 * Main Kafdo Client
 */

import { KafdoProducerClient, type ProducerOptions } from './producer'
import { KafdoConsumerClient, type ConsumerOptions } from './consumer'
import { KafdoAdminClient } from './admin'

export interface KafdoClientConfig {
  /** Base URL of the Kafdo service */
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
 * KafdoClient - Main entry point for the Kafdo SDK
 */
export class KafdoClient {
  private config: Required<Omit<KafdoClientConfig, 'headers'>> & { headers: Record<string, string> }

  constructor(config: KafdoClientConfig) {
    this.config = {
      baseUrl: config.baseUrl.replace(/\/$/, ''), // Remove trailing slash
      clientId: config.clientId ?? `kafdo-client-${Date.now()}`,
      fetch: config.fetch ?? globalThis.fetch.bind(globalThis),
      headers: config.headers ?? {},
      timeout: config.timeout ?? 30000,
    }
  }

  /**
   * Create a producer client
   */
  producer(options?: ProducerOptions): KafdoProducerClient {
    return new KafdoProducerClient(this.config, options)
  }

  /**
   * Create a consumer client
   */
  consumer(options: ConsumerOptions): KafdoConsumerClient {
    return new KafdoConsumerClient(this.config, options)
  }

  /**
   * Create an admin client
   */
  admin(): KafdoAdminClient {
    return new KafdoAdminClient(this.config)
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
 * Create a new Kafdo client
 */
export function createClient(config: KafdoClientConfig): KafdoClient {
  return new KafdoClient(config)
}
