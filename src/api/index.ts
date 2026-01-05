/**
 * kafka.do API - High-level client interfaces
 */

// Producer API
export { KafkaProducer, createProducer } from './producer'

// Consumer API
export { KafkaConsumer, createConsumer } from './consumer'

// Admin API
export { KafkaAdmin, createAdmin } from './admin'

// Re-export types for convenience
export type { ProducerRecord, ConsumerRecord, RecordMetadata, TopicPartition, OffsetAndMetadata } from '../types/records'
export type { ProducerConfig } from '../types/producer'
export type { ConsumerConfig, Consumer, RebalanceListener, ConsumerGroupState } from '../types/consumer'
export type { Admin, AdminConfig, TopicConfig, TopicMetadata, GroupDescription } from '../types/admin'
