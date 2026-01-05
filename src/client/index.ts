/**
 * kafka.do Client SDK
 *
 * A lightweight client for interacting with kafka.do from any JavaScript/TypeScript environment.
 * Works in browsers, Node.js, Cloudflare Workers, and other runtimes.
 */

export { KafkaClient, type KafkaClientConfig } from './client'
export { KafkaProducerClient, type ProducerOptions } from './producer'
export { KafkaConsumerClient, type ConsumerOptions } from './consumer'
export { KafkaAdminClient } from './admin'

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
