/**
 * kafka.do - KafkaJS-compatible SDK over rpc.do transport
 *
 * A drop-in replacement for KafkaJS that uses rpc.do for communication.
 * Provides the same API as the official kafkajs library.
 *
 * @example
 * ```typescript
 * import { Kafka } from 'kafka.do';
 *
 * const kafka = new Kafka({
 *   brokers: ['broker1:9092', 'broker2:9092'],
 *   clientId: 'my-app',
 * });
 *
 * // Producer
 * const producer = kafka.producer();
 * await producer.connect();
 * await producer.send({
 *   topic: 'my-topic',
 *   messages: [{ value: 'Hello Kafka!' }],
 * });
 * await producer.disconnect();
 *
 * // Consumer
 * const consumer = kafka.consumer({ groupId: 'my-group' });
 * await consumer.connect();
 * await consumer.subscribe({ topic: 'my-topic' });
 * await consumer.run({
 *   eachMessage: async ({ topic, partition, message }) => {
 *     console.log(message.value?.toString());
 *   },
 * });
 *
 * // Admin
 * const admin = kafka.admin();
 * await admin.connect();
 * await admin.createTopics({
 *   topics: [{ topic: 'new-topic' }],
 * });
 * await admin.disconnect();
 * ```
 *
 * @packageDocumentation
 */

// Main Kafka class
export { Kafka, setRpcClientFactory, getRpcClientFactory, createRpcClient } from './kafka.js';
export type { RpcClient, RpcClientFactory } from './kafka.js';

// Producer
export { Producer } from './producer.js';

// Consumer
export { Consumer } from './consumer.js';

// Admin
export { Admin } from './admin.js';

// Types
export {
  // Configuration types
  KafkaConfig,
  ProducerConfig,
  ConsumerConfig,
  AdminConfig,

  // Message types
  Message,
  TopicMessages,
  IHeaders,
  KafkaMessage,

  // Producer types
  ProducerRecord,
  ProducerBatch,
  RecordMetadata,
  CompressionTypes,
  Transaction,

  // Consumer types
  ConsumerSubscribeTopic,
  ConsumerSubscribeTopics,
  ConsumerRunConfig,
  EachMessagePayload,
  EachBatchPayload,
  Batch,
  SeekEntry,
  Offsets,
  TopicOffsets,
  PartitionOffset,
  OffsetsByTopicPartition,

  // Partition assignment types
  PartitionAssigner,
  AssignmentParams,
  MemberMetadata,
  Assignment,

  // Admin types
  CreateTopicsOptions,
  DeleteTopicsOptions,
  ITopicConfig,
  ReplicaAssignment,
  ConfigEntry,
  TopicMetadata,
  PartitionMetadata,
  FetchTopicMetadataOptions,
  CreatePartitionsOptions,

  // Consumer group types
  GroupDescription,
  GroupOverview,
  MemberDescription,
  DeleteGroupsOptions,
  FetchOffsetsOptions,
  GroupOffset,
  ResetOffsetsOptions,
  SetOffsetsOptions,

  // Configuration types
  DescribeConfigsOptions,
  AlterConfigsOptions,
  ConfigResourceResult,
  ResourceTypes,

  // Offset types
  ListTopicOffsetsOptions,
  TopicPartitionOffsets,
  PartitionReassignment,

  // State types
  ConnectionState,

  // Error types
  KafkaError,
  ConnectionError,
  TopicError,
  ConsumerError,
  ProducerError,
} from './types.js';
