/**
 * kafka.do - TypeScript type definitions
 *
 * KafkaJS-compatible type definitions for the kafka.do SDK.
 * These interfaces mirror the official kafkajs library for drop-in compatibility.
 */

/**
 * Kafka client configuration
 */
export interface KafkaConfig {
  /** Broker addresses (e.g., ['broker1:9092', 'broker2:9092']) */
  brokers: string[];
  /** Client identifier */
  clientId?: string;
  /** Connection timeout in milliseconds */
  connectionTimeout?: number;
  /** Authentication configuration */
  auth?: {
    /** Authentication mechanism */
    mechanism?: 'plain' | 'scram-sha-256' | 'scram-sha-512';
    /** Username for authentication */
    username?: string;
    /** Password for authentication */
    password?: string;
  };
  /** SSL/TLS configuration */
  ssl?: boolean | {
    /** Reject unauthorized certificates */
    rejectUnauthorized?: boolean;
    /** CA certificate */
    ca?: string;
    /** Client certificate */
    cert?: string;
    /** Client key */
    key?: string;
  };
  /** Request timeout in milliseconds */
  requestTimeout?: number;
  /** Retry configuration */
  retry?: {
    /** Maximum retry time in milliseconds */
    maxRetryTime?: number;
    /** Initial retry time in milliseconds */
    initialRetryTime?: number;
    /** Retry factor */
    factor?: number;
    /** Maximum number of retries */
    retries?: number;
  };
}

/**
 * Producer configuration
 */
export interface ProducerConfig {
  /** Whether to create topics if they don't exist */
  allowAutoTopicCreation?: boolean;
  /** Transaction ID for exactly-once semantics */
  transactionalId?: string;
  /** Transaction timeout in milliseconds */
  transactionTimeout?: number;
  /** Idempotent producer (enables exactly-once delivery) */
  idempotent?: boolean;
  /** Maximum number of in-flight requests */
  maxInFlightRequests?: number;
}

/**
 * Message header
 */
export interface IHeaders {
  [key: string]: Buffer | string | (Buffer | string)[] | undefined;
}

/**
 * Message to send
 */
export interface Message {
  /** Message key (for partitioning) */
  key?: Buffer | string | null;
  /** Message value */
  value: Buffer | string | null;
  /** Partition to send to (optional) */
  partition?: number;
  /** Message headers */
  headers?: IHeaders;
  /** Message timestamp */
  timestamp?: string;
}

/**
 * Topic messages for batch send
 */
export interface TopicMessages {
  /** Topic name */
  topic: string;
  /** Messages to send */
  messages: Message[];
}

/**
 * Producer send configuration
 */
export interface ProducerRecord {
  /** Topic to send to */
  topic: string;
  /** Messages to send */
  messages: Message[];
  /** Required acknowledgments (-1, 0, 1) */
  acks?: number;
  /** Timeout for waiting for acknowledgments */
  timeout?: number;
  /** Compression type */
  compression?: CompressionTypes;
}

/**
 * Batch send configuration
 */
export interface ProducerBatch {
  /** Topic messages to send */
  topicMessages: TopicMessages[];
  /** Required acknowledgments */
  acks?: number;
  /** Timeout for waiting for acknowledgments */
  timeout?: number;
  /** Compression type */
  compression?: CompressionTypes;
}

/**
 * Compression types
 */
export enum CompressionTypes {
  None = 0,
  GZIP = 1,
  Snappy = 2,
  LZ4 = 3,
  ZSTD = 4,
}

/**
 * Record metadata returned after send
 */
export interface RecordMetadata {
  /** Topic name */
  topicName: string;
  /** Partition */
  partition: number;
  /** Error code (0 for success) */
  errorCode: number;
  /** Base offset */
  baseOffset?: string;
  /** Log append time */
  logAppendTime?: string;
  /** Log start offset */
  logStartOffset?: string;
}

/**
 * Consumer configuration
 */
export interface ConsumerConfig {
  /** Consumer group ID */
  groupId: string;
  /** Partition assignment strategy */
  partitionAssigners?: PartitionAssigner[];
  /** Session timeout in milliseconds */
  sessionTimeout?: number;
  /** Rebalance timeout in milliseconds */
  rebalanceTimeout?: number;
  /** Heartbeat interval in milliseconds */
  heartbeatInterval?: number;
  /** Metadata max age in milliseconds */
  metadataMaxAge?: number;
  /** Allow auto topic creation */
  allowAutoTopicCreation?: boolean;
  /** Max bytes per partition */
  maxBytesPerPartition?: number;
  /** Minimum bytes to fetch */
  minBytes?: number;
  /** Maximum bytes to fetch */
  maxBytes?: number;
  /** Maximum wait time in milliseconds */
  maxWaitTimeInMs?: number;
  /** Read uncommitted messages */
  readUncommitted?: boolean;
}

/**
 * Partition assigner interface
 */
export interface PartitionAssigner {
  /** Assigner name */
  name: string;
  /** Assigner version */
  version: number;
  /** Assignment function */
  assign: (params: AssignmentParams) => Assignment[];
}

/**
 * Assignment parameters
 */
export interface AssignmentParams {
  /** Members in the group */
  members: MemberMetadata[];
  /** Topics */
  topics: string[];
}

/**
 * Member metadata
 */
export interface MemberMetadata {
  /** Member ID */
  memberId: string;
  /** Client ID */
  clientId: string;
  /** Client host */
  clientHost: string;
}

/**
 * Assignment result
 */
export interface Assignment {
  /** Member ID */
  memberId: string;
  /** Topic partitions assigned */
  assignment: { [topic: string]: number[] };
}

/**
 * Consumer subscription
 */
export interface ConsumerSubscribeTopic {
  /** Topic name or regex */
  topic: string | RegExp;
  /** Start from beginning */
  fromBeginning?: boolean;
}

/**
 * Consumer subscription (array form)
 */
export interface ConsumerSubscribeTopics {
  /** Topics to subscribe to */
  topics: (string | RegExp)[];
  /** Start from beginning */
  fromBeginning?: boolean;
}

/**
 * Kafka message received by consumer
 */
export interface KafkaMessage {
  /** Message key */
  key: Buffer | null;
  /** Message value */
  value: Buffer | null;
  /** Message timestamp */
  timestamp: string;
  /** Message attributes */
  attributes: number;
  /** Message offset */
  offset: string;
  /** Message size */
  size: number;
  /** Message headers */
  headers?: IHeaders;
}

/**
 * Each message payload
 */
export interface EachMessagePayload {
  /** Topic name */
  topic: string;
  /** Partition number */
  partition: number;
  /** Message */
  message: KafkaMessage;
  /** Heartbeat function */
  heartbeat: () => Promise<void>;
  /** Pause consumption */
  pause: () => () => void;
}

/**
 * Each batch payload
 */
export interface EachBatchPayload {
  /** Batch of messages */
  batch: Batch;
  /** Resolve offsets */
  resolveOffset: (offset: string) => void;
  /** Heartbeat function */
  heartbeat: () => Promise<void>;
  /** Commit offsets if necessary */
  commitOffsetsIfNecessary: (offsets?: Offsets) => Promise<void>;
  /** Uncommitted offsets */
  uncommittedOffsets: () => OffsetsByTopicPartition;
  /** Whether batch is running */
  isRunning: () => boolean;
  /** Whether batch is stale */
  isStale: () => boolean;
  /** Pause consumption */
  pause: () => () => void;
}

/**
 * Batch of messages
 */
export interface Batch {
  /** Topic name */
  topic: string;
  /** Partition number */
  partition: number;
  /** High watermark */
  highWatermark: string;
  /** Messages in the batch */
  messages: KafkaMessage[];
  /** Is empty */
  isEmpty: () => boolean;
  /** First offset */
  firstOffset: () => string | null;
  /** Last offset */
  lastOffset: () => string;
  /** Offset lag */
  offsetLag: () => string;
  /** Offset lag low */
  offsetLagLow: () => string;
}

/**
 * Offsets
 */
export interface Offsets {
  /** Topics */
  topics: TopicOffsets[];
}

/**
 * Topic offsets
 */
export interface TopicOffsets {
  /** Topic name */
  topic: string;
  /** Partitions */
  partitions: PartitionOffset[];
}

/**
 * Partition offset
 */
export interface PartitionOffset {
  /** Partition number */
  partition: number;
  /** Offset */
  offset: string;
}

/**
 * Offsets by topic partition
 */
export interface OffsetsByTopicPartition {
  /** Topics */
  topics: {
    [topic: string]: {
      [partition: string]: { offset: string };
    };
  };
}

/**
 * Consumer run configuration
 */
export interface ConsumerRunConfig {
  /** Auto commit enabled */
  autoCommit?: boolean;
  /** Auto commit interval in milliseconds */
  autoCommitInterval?: number;
  /** Auto commit threshold */
  autoCommitThreshold?: number;
  /** Each batch handler */
  eachBatch?: (payload: EachBatchPayload) => Promise<void>;
  /** Each message handler */
  eachMessage?: (payload: EachMessagePayload) => Promise<void>;
  /** Partitions consumed concurrently */
  partitionsConsumedConcurrently?: number;
}

/**
 * Seek configuration
 */
export interface SeekEntry {
  /** Topic name */
  topic: string;
  /** Partition number */
  partition: number;
  /** Offset to seek to */
  offset: string;
}

/**
 * Admin client configuration
 */
export interface AdminConfig {
  /** Retry configuration */
  retry?: {
    maxRetryTime?: number;
    initialRetryTime?: number;
    factor?: number;
    retries?: number;
  };
}

/**
 * Create topics configuration
 */
export interface CreateTopicsOptions {
  /** Wait for leaders */
  waitForLeaders?: boolean;
  /** Timeout in milliseconds */
  timeout?: number;
  /** Topics to create */
  topics: ITopicConfig[];
  /** Validate only (don't create) */
  validateOnly?: boolean;
}

/**
 * Topic configuration
 */
export interface ITopicConfig {
  /** Topic name */
  topic: string;
  /** Number of partitions */
  numPartitions?: number;
  /** Replication factor */
  replicationFactor?: number;
  /** Replica assignment */
  replicaAssignment?: ReplicaAssignment[];
  /** Configuration entries */
  configEntries?: ConfigEntry[];
}

/**
 * Replica assignment
 */
export interface ReplicaAssignment {
  /** Partition */
  partition: number;
  /** Replicas */
  replicas: number[];
}

/**
 * Configuration entry
 */
export interface ConfigEntry {
  /** Configuration name */
  name: string;
  /** Configuration value */
  value: string;
}

/**
 * Delete topics options
 */
export interface DeleteTopicsOptions {
  /** Topics to delete */
  topics: string[];
  /** Timeout in milliseconds */
  timeout?: number;
}

/**
 * Topic metadata
 */
export interface TopicMetadata {
  /** Topic name */
  name: string;
  /** Partitions */
  partitions: PartitionMetadata[];
}

/**
 * Partition metadata
 */
export interface PartitionMetadata {
  /** Partition error code */
  partitionErrorCode: number;
  /** Partition ID */
  partitionId: number;
  /** Leader broker ID */
  leader: number;
  /** Replica broker IDs */
  replicas: number[];
  /** In-sync replica broker IDs */
  isr: number[];
}

/**
 * Fetch topic metadata options
 */
export interface FetchTopicMetadataOptions {
  /** Topics to fetch metadata for */
  topics?: string[];
}

/**
 * Consumer group description
 */
export interface GroupDescription {
  /** Error code */
  errorCode: number;
  /** Group ID */
  groupId: string;
  /** Protocol type */
  protocolType: string;
  /** Protocol */
  protocol: string;
  /** State */
  state: string;
  /** Members */
  members: MemberDescription[];
}

/**
 * Member description
 */
export interface MemberDescription {
  /** Member ID */
  memberId: string;
  /** Client ID */
  clientId: string;
  /** Client host */
  clientHost: string;
  /** Member metadata */
  memberMetadata: Buffer;
  /** Member assignment */
  memberAssignment: Buffer;
}

/**
 * Delete groups options
 */
export interface DeleteGroupsOptions {
  /** Group IDs to delete */
  groupIds: string[];
}

/**
 * Group overview
 */
export interface GroupOverview {
  /** Group ID */
  groupId: string;
  /** Protocol type */
  protocolType: string;
}

/**
 * Partition reassignment
 */
export interface PartitionReassignment {
  /** Topic name */
  topic: string;
  /** Partition reassignments */
  partitionAssignment: {
    partition: number;
    replicas: number[];
  }[];
}

/**
 * Create partitions options
 */
export interface CreatePartitionsOptions {
  /** Topic partitions to create */
  topicPartitions: {
    /** Topic name */
    topic: string;
    /** Total number of partitions */
    count: number;
    /** Assignment */
    assignments?: number[][];
  }[];
  /** Timeout in milliseconds */
  timeout?: number;
  /** Validate only */
  validateOnly?: boolean;
}

/**
 * Fetch offsets options
 */
export interface FetchOffsetsOptions {
  /** Group ID */
  groupId: string;
  /** Topics (optional, all topics if not specified) */
  topics?: string[];
  /** Resolve stale offsets */
  resolveOffsets?: boolean;
}

/**
 * Consumer group offset */
export interface GroupOffset {
  /** Topic name */
  topic: string;
  /** Partitions */
  partitions: {
    /** Partition */
    partition: number;
    /** Offset */
    offset: string;
    /** Metadata */
    metadata?: string | null;
  }[];
}

/**
 * Reset offsets options
 */
export interface ResetOffsetsOptions {
  /** Group ID */
  groupId: string;
  /** Topic */
  topic: string;
  /** Earliest offset */
  earliest?: boolean;
}

/**
 * Set offsets options
 */
export interface SetOffsetsOptions {
  /** Group ID */
  groupId: string;
  /** Topic */
  topic: string;
  /** Partitions with offsets */
  partitions: {
    partition: number;
    offset: string;
  }[];
}

/**
 * Describe configs options
 */
export interface DescribeConfigsOptions {
  /** Include synonyms */
  includeSynonyms?: boolean;
  /** Resources to describe */
  resources: {
    type: ResourceTypes;
    name: string;
    configNames?: string[];
  }[];
}

/**
 * Resource types
 */
export enum ResourceTypes {
  UNKNOWN = 0,
  ANY = 1,
  TOPIC = 2,
  GROUP = 3,
  CLUSTER = 4,
  TRANSACTIONAL_ID = 5,
  DELEGATION_TOKEN = 6,
}

/**
 * Alter configs options
 */
export interface AlterConfigsOptions {
  /** Validate only */
  validateOnly?: boolean;
  /** Resources to alter */
  resources: {
    type: ResourceTypes;
    name: string;
    configEntries: ConfigEntry[];
  }[];
}

/**
 * Config resource result
 */
export interface ConfigResourceResult {
  /** Resource type */
  resourceType: ResourceTypes;
  /** Resource name */
  resourceName: string;
  /** Error code */
  errorCode: number;
  /** Error message */
  errorMessage: string;
  /** Config entries */
  configEntries: {
    configName: string;
    configValue: string;
    isDefault: boolean;
    configSource: number;
    isSensitive: boolean;
    readOnly: boolean;
    configSynonyms?: {
      configName: string;
      configValue: string;
      configSource: number;
    }[];
  }[];
}

/**
 * List topic offsets options
 */
export interface ListTopicOffsetsOptions {
  /** Topic name */
  topic: string;
  /** Timestamp (-1 for latest, -2 for earliest) */
  timestamp?: string;
}

/**
 * Topic partition offsets
 */
export interface TopicPartitionOffsets {
  /** Partition */
  partition: number;
  /** Offset */
  offset: string;
  /** High watermark */
  high: string;
  /** Low watermark */
  low: string;
}

/**
 * Transaction state
 */
export interface Transaction {
  /** Send offsets to transaction */
  sendOffsets: (options: { consumerGroupId: string; topics: TopicOffsets[] }) => Promise<void>;
  /** Commit transaction */
  commit: () => Promise<void>;
  /** Abort transaction */
  abort: () => Promise<void>;
  /** Whether transaction is active */
  isActive: () => boolean;
}

/**
 * Connection state
 */
export type ConnectionState = 'disconnected' | 'connecting' | 'connected';

/**
 * Kafka error
 */
export class KafkaError extends Error {
  constructor(
    message: string,
    public readonly code: string,
    public readonly retriable: boolean = false
  ) {
    super(message);
    this.name = 'KafkaError';
  }
}

/**
 * Connection error
 */
export class ConnectionError extends KafkaError {
  constructor(message: string) {
    super(message, 'KAFKA_CONNECTION_ERROR', true);
    this.name = 'ConnectionError';
  }
}

/**
 * Topic error
 */
export class TopicError extends KafkaError {
  constructor(message: string, public readonly topic: string) {
    super(message, 'KAFKA_TOPIC_ERROR', false);
    this.name = 'TopicError';
  }
}

/**
 * Consumer error
 */
export class ConsumerError extends KafkaError {
  constructor(message: string, public readonly groupId: string) {
    super(message, 'KAFKA_CONSUMER_ERROR', false);
    this.name = 'ConsumerError';
  }
}

/**
 * Producer error
 */
export class ProducerError extends KafkaError {
  constructor(message: string) {
    super(message, 'KAFKA_PRODUCER_ERROR', false);
    this.name = 'ProducerError';
  }
}
