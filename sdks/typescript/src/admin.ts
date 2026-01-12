/**
 * kafka.do - Admin class
 *
 * KafkaJS-compatible admin client that uses rpc.do for transport.
 */

import type { Kafka, RpcClient } from './kafka.js';
import type {
  AdminConfig,
  CreateTopicsOptions,
  DeleteTopicsOptions,
  TopicMetadata,
  FetchTopicMetadataOptions,
  GroupDescription,
  GroupOverview,
  DeleteGroupsOptions,
  CreatePartitionsOptions,
  FetchOffsetsOptions,
  GroupOffset,
  ResetOffsetsOptions,
  SetOffsetsOptions,
  DescribeConfigsOptions,
  AlterConfigsOptions,
  ConfigResourceResult,
  ListTopicOffsetsOptions,
  TopicPartitionOffsets,
  PartitionReassignment,
  ConnectionState,
  ITopicConfig,
} from './types.js';
import { ConnectionError, TopicError, KafkaError } from './types.js';

/**
 * Admin - manages Kafka topics, partitions, and consumer groups
 *
 * @example
 * ```typescript
 * const admin = kafka.admin();
 * await admin.connect();
 *
 * // Create topics
 * await admin.createTopics({
 *   topics: [
 *     { topic: 'my-topic', numPartitions: 3, replicationFactor: 2 },
 *   ],
 * });
 *
 * // List topics
 * const topics = await admin.listTopics();
 *
 * // Delete topics
 * await admin.deleteTopics({ topics: ['my-topic'] });
 *
 * await admin.disconnect();
 * ```
 */
export class Admin {
  private readonly _kafka: Kafka;
  private readonly _config: AdminConfig;
  private _connectionState: ConnectionState = 'disconnected';
  private _rpcClient: RpcClient | null = null;

  constructor(kafka: Kafka, config?: AdminConfig) {
    this._kafka = kafka;
    this._config = {
      retry: {
        maxRetryTime: 30000,
        initialRetryTime: 300,
        factor: 2,
        retries: 5,
      },
      ...config,
    };
  }

  /**
   * Get the current connection state
   */
  get connectionState(): ConnectionState {
    return this._connectionState;
  }

  /**
   * Check if the admin client is connected
   */
  isConnected(): boolean {
    return this._connectionState === 'connected';
  }

  /**
   * Connect to Kafka
   */
  async connect(): Promise<void> {
    if (this._connectionState === 'connected') {
      return;
    }

    this._connectionState = 'connecting';

    try {
      this._rpcClient = this._kafka.getRpcClient();

      await this._rpcClient.$.adminConnect({
        clientId: this._kafka.config.clientId,
      });

      this._connectionState = 'connected';
    } catch (error) {
      this._connectionState = 'disconnected';
      throw new ConnectionError(
        `Failed to connect admin: ${error instanceof Error ? error.message : String(error)}`
      );
    }
  }

  /**
   * Disconnect from Kafka
   */
  async disconnect(): Promise<void> {
    if (this._connectionState === 'disconnected') {
      return;
    }

    try {
      if (this._rpcClient) {
        await this._rpcClient.$.adminDisconnect();
      }
    } finally {
      this._connectionState = 'disconnected';
      this._rpcClient = null;
    }
  }

  /**
   * List all topics
   */
  async listTopics(): Promise<string[]> {
    this._ensureConnected();

    try {
      const result = await this._rpcClient!.$.adminListTopics();
      return result as string[];
    } catch (error) {
      throw new KafkaError(
        `Failed to list topics: ${error instanceof Error ? error.message : String(error)}`,
        'ADMIN_ERROR'
      );
    }
  }

  /**
   * Create topics
   */
  async createTopics(options: CreateTopicsOptions): Promise<boolean> {
    this._ensureConnected();

    const { topics, waitForLeaders = true, timeout = 30000, validateOnly = false } = options;

    try {
      const result = await this._rpcClient!.$.adminCreateTopics({
        topics: topics.map((t: ITopicConfig) => ({
          topic: t.topic,
          numPartitions: t.numPartitions ?? 1,
          replicationFactor: t.replicationFactor ?? 1,
          replicaAssignment: t.replicaAssignment,
          configEntries: t.configEntries,
        })),
        waitForLeaders,
        timeout,
        validateOnly,
      });

      return result as boolean;
    } catch (error) {
      const topicNames = topics.map((t: ITopicConfig) => t.topic).join(', ');
      throw new TopicError(
        `Failed to create topics [${topicNames}]: ${error instanceof Error ? error.message : String(error)}`,
        topicNames
      );
    }
  }

  /**
   * Delete topics
   */
  async deleteTopics(options: DeleteTopicsOptions): Promise<void> {
    this._ensureConnected();

    const { topics, timeout = 30000 } = options;

    try {
      await this._rpcClient!.$.adminDeleteTopics({
        topics,
        timeout,
      });
    } catch (error) {
      throw new TopicError(
        `Failed to delete topics: ${error instanceof Error ? error.message : String(error)}`,
        topics.join(', ')
      );
    }
  }

  /**
   * Fetch topic metadata
   */
  async fetchTopicMetadata(options?: FetchTopicMetadataOptions): Promise<{ topics: TopicMetadata[] }> {
    this._ensureConnected();

    try {
      const result = await this._rpcClient!.$.adminFetchTopicMetadata({
        topics: options?.topics,
      });

      return result as { topics: TopicMetadata[] };
    } catch (error) {
      throw new KafkaError(
        `Failed to fetch topic metadata: ${error instanceof Error ? error.message : String(error)}`,
        'ADMIN_ERROR'
      );
    }
  }

  /**
   * Fetch topic offsets for a specific topic
   */
  async fetchTopicOffsets(topic: string): Promise<TopicPartitionOffsets[]> {
    this._ensureConnected();

    try {
      const result = await this._rpcClient!.$.adminFetchTopicOffsets({
        topic,
      });

      return result as TopicPartitionOffsets[];
    } catch (error) {
      throw new TopicError(
        `Failed to fetch topic offsets: ${error instanceof Error ? error.message : String(error)}`,
        topic
      );
    }
  }

  /**
   * Fetch topic offsets by timestamp
   */
  async fetchTopicOffsetsByTimestamp(
    topic: string,
    timestamp: string
  ): Promise<TopicPartitionOffsets[]> {
    this._ensureConnected();

    try {
      const result = await this._rpcClient!.$.adminFetchTopicOffsetsByTimestamp({
        topic,
        timestamp,
      });

      return result as TopicPartitionOffsets[];
    } catch (error) {
      throw new TopicError(
        `Failed to fetch topic offsets by timestamp: ${error instanceof Error ? error.message : String(error)}`,
        topic
      );
    }
  }

  /**
   * Create partitions for existing topics
   */
  async createPartitions(options: CreatePartitionsOptions): Promise<boolean> {
    this._ensureConnected();

    const { topicPartitions, timeout = 30000, validateOnly = false } = options;

    try {
      const result = await this._rpcClient!.$.adminCreatePartitions({
        topicPartitions,
        timeout,
        validateOnly,
      });

      return result as boolean;
    } catch (error) {
      throw new KafkaError(
        `Failed to create partitions: ${error instanceof Error ? error.message : String(error)}`,
        'ADMIN_ERROR'
      );
    }
  }

  /**
   * List consumer groups
   */
  async listGroups(): Promise<{ groups: GroupOverview[] }> {
    this._ensureConnected();

    try {
      const result = await this._rpcClient!.$.adminListGroups();
      return result as { groups: GroupOverview[] };
    } catch (error) {
      throw new KafkaError(
        `Failed to list groups: ${error instanceof Error ? error.message : String(error)}`,
        'ADMIN_ERROR'
      );
    }
  }

  /**
   * Describe consumer groups
   */
  async describeGroups(groupIds: string[]): Promise<{ groups: GroupDescription[] }> {
    this._ensureConnected();

    try {
      const result = await this._rpcClient!.$.adminDescribeGroups({
        groupIds,
      });

      return result as { groups: GroupDescription[] };
    } catch (error) {
      throw new KafkaError(
        `Failed to describe groups: ${error instanceof Error ? error.message : String(error)}`,
        'ADMIN_ERROR'
      );
    }
  }

  /**
   * Delete consumer groups
   */
  async deleteGroups(options: DeleteGroupsOptions): Promise<void> {
    this._ensureConnected();

    try {
      await this._rpcClient!.$.adminDeleteGroups({
        groupIds: options.groupIds,
      });
    } catch (error) {
      throw new KafkaError(
        `Failed to delete groups: ${error instanceof Error ? error.message : String(error)}`,
        'ADMIN_ERROR'
      );
    }
  }

  /**
   * Fetch consumer group offsets
   */
  async fetchOffsets(options: FetchOffsetsOptions): Promise<GroupOffset[]> {
    this._ensureConnected();

    try {
      const result = await this._rpcClient!.$.adminFetchOffsets({
        groupId: options.groupId,
        topics: options.topics,
        resolveOffsets: options.resolveOffsets,
      });

      return result as GroupOffset[];
    } catch (error) {
      throw new KafkaError(
        `Failed to fetch offsets: ${error instanceof Error ? error.message : String(error)}`,
        'ADMIN_ERROR'
      );
    }
  }

  /**
   * Reset consumer group offsets
   */
  async resetOffsets(options: ResetOffsetsOptions): Promise<void> {
    this._ensureConnected();

    try {
      await this._rpcClient!.$.adminResetOffsets({
        groupId: options.groupId,
        topic: options.topic,
        earliest: options.earliest ?? false,
      });
    } catch (error) {
      throw new KafkaError(
        `Failed to reset offsets: ${error instanceof Error ? error.message : String(error)}`,
        'ADMIN_ERROR'
      );
    }
  }

  /**
   * Set consumer group offsets
   */
  async setOffsets(options: SetOffsetsOptions): Promise<void> {
    this._ensureConnected();

    try {
      await this._rpcClient!.$.adminSetOffsets({
        groupId: options.groupId,
        topic: options.topic,
        partitions: options.partitions,
      });
    } catch (error) {
      throw new KafkaError(
        `Failed to set offsets: ${error instanceof Error ? error.message : String(error)}`,
        'ADMIN_ERROR'
      );
    }
  }

  /**
   * Describe cluster configuration
   */
  async describeCluster(): Promise<{
    brokers: { nodeId: number; host: string; port: number }[];
    controller: number | null;
    clusterId: string;
  }> {
    this._ensureConnected();

    try {
      const result = await this._rpcClient!.$.adminDescribeCluster();
      return result as {
        brokers: { nodeId: number; host: string; port: number }[];
        controller: number | null;
        clusterId: string;
      };
    } catch (error) {
      throw new KafkaError(
        `Failed to describe cluster: ${error instanceof Error ? error.message : String(error)}`,
        'ADMIN_ERROR'
      );
    }
  }

  /**
   * Describe configurations
   */
  async describeConfigs(options: DescribeConfigsOptions): Promise<{ resources: ConfigResourceResult[] }> {
    this._ensureConnected();

    try {
      const result = await this._rpcClient!.$.adminDescribeConfigs({
        resources: options.resources,
        includeSynonyms: options.includeSynonyms ?? false,
      });

      return result as { resources: ConfigResourceResult[] };
    } catch (error) {
      throw new KafkaError(
        `Failed to describe configs: ${error instanceof Error ? error.message : String(error)}`,
        'ADMIN_ERROR'
      );
    }
  }

  /**
   * Alter configurations
   */
  async alterConfigs(options: AlterConfigsOptions): Promise<void> {
    this._ensureConnected();

    try {
      await this._rpcClient!.$.adminAlterConfigs({
        resources: options.resources,
        validateOnly: options.validateOnly ?? false,
      });
    } catch (error) {
      throw new KafkaError(
        `Failed to alter configs: ${error instanceof Error ? error.message : String(error)}`,
        'ADMIN_ERROR'
      );
    }
  }

  /**
   * Alter partition reassignments
   */
  async alterPartitionReassignments(options: {
    topics: PartitionReassignment[];
    timeout?: number;
  }): Promise<void> {
    this._ensureConnected();

    try {
      await this._rpcClient!.$.adminAlterPartitionReassignments({
        topics: options.topics,
        timeout: options.timeout ?? 30000,
      });
    } catch (error) {
      throw new KafkaError(
        `Failed to alter partition reassignments: ${error instanceof Error ? error.message : String(error)}`,
        'ADMIN_ERROR'
      );
    }
  }

  /**
   * List partition reassignments
   */
  async listPartitionReassignments(options?: {
    topics?: { topic: string; partitions?: number[] }[];
    timeout?: number;
  }): Promise<{ topics: PartitionReassignment[] }> {
    this._ensureConnected();

    try {
      const result = await this._rpcClient!.$.adminListPartitionReassignments({
        topics: options?.topics,
        timeout: options?.timeout ?? 30000,
      });

      return result as { topics: PartitionReassignment[] };
    } catch (error) {
      throw new KafkaError(
        `Failed to list partition reassignments: ${error instanceof Error ? error.message : String(error)}`,
        'ADMIN_ERROR'
      );
    }
  }

  /**
   * Delete records (up to a specific offset)
   */
  async deleteRecords(options: {
    topic: string;
    partitions: { partition: number; offset: string }[];
  }): Promise<void> {
    this._ensureConnected();

    try {
      await this._rpcClient!.$.adminDeleteRecords({
        topic: options.topic,
        partitions: options.partitions,
      });
    } catch (error) {
      throw new KafkaError(
        `Failed to delete records: ${error instanceof Error ? error.message : String(error)}`,
        'ADMIN_ERROR'
      );
    }
  }

  /**
   * Ensure the admin client is connected
   */
  private _ensureConnected(): void {
    if (this._connectionState !== 'connected' || !this._rpcClient) {
      throw new ConnectionError('Admin is not connected. Call connect() first.');
    }
  }
}
