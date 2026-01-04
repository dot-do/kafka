/**
 * Kafdo Client SDK
 *
 * A lightweight client for interacting with Kafdo from any JavaScript/TypeScript environment.
 * Works in browsers, Node.js, Cloudflare Workers, and other runtimes.
 */

export { KafdoClient, type KafdoClientConfig } from './client'
export { KafdoProducerClient, type ProducerOptions } from './producer'
export { KafdoConsumerClient, type ConsumerOptions } from './consumer'
export { KafdoAdminClient } from './admin'

// Re-export useful types
export type {
  ProducerRecord,
  ConsumerRecord,
  RecordMetadata,
  TopicPartition,
} from '../types/records'
