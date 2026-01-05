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

// Re-export useful types from records
export type {
  ProducerRecord,
  ConsumerRecord,
  RecordMetadata,
  TopicPartition,
} from '../types/records'

// Re-export admin types
export type {
  TopicConfig,
  TopicMetadata,
  GroupDescription,
  PartitionMetadata,
  MemberDescription,
  OffsetInfo,
} from '../types/admin'
