/**
 * kafka.do - Consumer class
 *
 * KafkaJS-compatible consumer that uses rpc.do for transport.
 */

import type { Kafka, RpcClient } from './kafka.js';
import type {
  ConsumerConfig,
  ConsumerSubscribeTopic,
  ConsumerSubscribeTopics,
  ConsumerRunConfig,
  SeekEntry,
  KafkaMessage,
  EachMessagePayload,
  EachBatchPayload,
  Batch,
  Offsets,
  OffsetsByTopicPartition,
  TopicOffsets,
  ConnectionState,
} from './types.js';
import { ConsumerError, ConnectionError } from './types.js';

/**
 * Topic partition assignment
 */
interface TopicPartition {
  topic: string;
  partition: number;
}

/**
 * Consumer - reads messages from Kafka topics
 *
 * @example
 * ```typescript
 * const consumer = kafka.consumer({ groupId: 'my-group' });
 * await consumer.connect();
 *
 * await consumer.subscribe({ topic: 'my-topic', fromBeginning: true });
 *
 * await consumer.run({
 *   eachMessage: async ({ topic, partition, message }) => {
 *     console.log({
 *       topic,
 *       partition,
 *       offset: message.offset,
 *       value: message.value?.toString(),
 *     });
 *   },
 * });
 * ```
 */
export class Consumer {
  private readonly _kafka: Kafka;
  private readonly _config: ConsumerConfig;
  private _connectionState: ConnectionState = 'disconnected';
  private _rpcClient: RpcClient | null = null;
  private _subscriptions: Set<string> = new Set();
  private _isRunning: boolean = false;
  private _paused: Set<string> = new Set();
  private _runConfig: ConsumerRunConfig | null = null;
  private _pollInterval: ReturnType<typeof setInterval> | null = null;

  constructor(kafka: Kafka, config: ConsumerConfig) {
    this._kafka = kafka;
    this._config = {
      sessionTimeout: 30000,
      rebalanceTimeout: 60000,
      heartbeatInterval: 3000,
      metadataMaxAge: 300000,
      allowAutoTopicCreation: false,
      maxBytesPerPartition: 1048576,
      minBytes: 1,
      maxBytes: 10485760,
      maxWaitTimeInMs: 5000,
      readUncommitted: false,
      ...config,
    };
  }

  /**
   * Get the consumer group ID
   */
  get groupId(): string {
    return this._config.groupId;
  }

  /**
   * Get the current connection state
   */
  get connectionState(): ConnectionState {
    return this._connectionState;
  }

  /**
   * Check if the consumer is connected
   */
  isConnected(): boolean {
    return this._connectionState === 'connected';
  }

  /**
   * Check if the consumer is running
   */
  isRunning(): boolean {
    return this._isRunning;
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

      await this._rpcClient.$.consumerConnect({
        clientId: this._kafka.config.clientId,
        groupId: this._config.groupId,
        sessionTimeout: this._config.sessionTimeout,
        rebalanceTimeout: this._config.rebalanceTimeout,
        heartbeatInterval: this._config.heartbeatInterval,
        maxBytesPerPartition: this._config.maxBytesPerPartition,
        minBytes: this._config.minBytes,
        maxBytes: this._config.maxBytes,
        maxWaitTimeInMs: this._config.maxWaitTimeInMs,
        readUncommitted: this._config.readUncommitted,
      });

      this._connectionState = 'connected';
    } catch (error) {
      this._connectionState = 'disconnected';
      throw new ConnectionError(
        `Failed to connect consumer: ${error instanceof Error ? error.message : String(error)}`
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

    // Stop running if active
    await this.stop();

    try {
      if (this._rpcClient) {
        await this._rpcClient.$.consumerDisconnect({
          groupId: this._config.groupId,
        });
      }
    } finally {
      this._connectionState = 'disconnected';
      this._rpcClient = null;
      this._subscriptions.clear();
    }
  }

  /**
   * Subscribe to a topic
   */
  async subscribe(subscription: ConsumerSubscribeTopic | ConsumerSubscribeTopics): Promise<void> {
    this._ensureConnected();

    const topics: string[] = [];
    let fromBeginning = false;

    if ('topic' in subscription) {
      // Single topic subscription
      const topic = subscription.topic;
      if (typeof topic === 'string') {
        topics.push(topic);
      } else {
        // RegExp - convert to pattern string
        topics.push(topic.source);
      }
      fromBeginning = subscription.fromBeginning ?? false;
    } else {
      // Multiple topics subscription
      for (const topic of subscription.topics) {
        if (typeof topic === 'string') {
          topics.push(topic);
        } else {
          topics.push(topic.source);
        }
      }
      fromBeginning = subscription.fromBeginning ?? false;
    }

    try {
      await this._rpcClient!.$.consumerSubscribe({
        groupId: this._config.groupId,
        topics,
        fromBeginning,
      });

      for (const topic of topics) {
        this._subscriptions.add(topic);
      }
    } catch (error) {
      throw new ConsumerError(
        `Failed to subscribe to topics: ${error instanceof Error ? error.message : String(error)}`,
        this._config.groupId
      );
    }
  }

  /**
   * Unsubscribe from all topics
   */
  async unsubscribe(): Promise<void> {
    this._ensureConnected();

    try {
      await this._rpcClient!.$.consumerUnsubscribe({
        groupId: this._config.groupId,
      });

      this._subscriptions.clear();
    } catch (error) {
      throw new ConsumerError(
        `Failed to unsubscribe: ${error instanceof Error ? error.message : String(error)}`,
        this._config.groupId
      );
    }
  }

  /**
   * Start consuming messages
   */
  async run(config: ConsumerRunConfig): Promise<void> {
    this._ensureConnected();

    if (this._isRunning) {
      throw new ConsumerError('Consumer is already running', this._config.groupId);
    }

    if (!config.eachMessage && !config.eachBatch) {
      throw new ConsumerError(
        'Either eachMessage or eachBatch handler is required',
        this._config.groupId
      );
    }

    this._runConfig = {
      autoCommit: true,
      autoCommitInterval: 5000,
      autoCommitThreshold: null,
      partitionsConsumedConcurrently: 1,
      ...config,
    };

    this._isRunning = true;

    try {
      // Start the consumer on the server
      await this._rpcClient!.$.consumerRun({
        groupId: this._config.groupId,
        autoCommit: this._runConfig.autoCommit,
        autoCommitInterval: this._runConfig.autoCommitInterval,
      });

      // Start polling for messages
      this._startPolling();
    } catch (error) {
      this._isRunning = false;
      this._runConfig = null;
      throw new ConsumerError(
        `Failed to start consumer: ${error instanceof Error ? error.message : String(error)}`,
        this._config.groupId
      );
    }
  }

  /**
   * Stop consuming messages
   */
  async stop(): Promise<void> {
    if (!this._isRunning) {
      return;
    }

    this._stopPolling();
    this._isRunning = false;
    this._runConfig = null;

    if (this._rpcClient && this._connectionState === 'connected') {
      try {
        await this._rpcClient.$.consumerStop({
          groupId: this._config.groupId,
        });
      } catch {
        // Ignore errors during stop
      }
    }
  }

  /**
   * Pause consumption for specific topic-partitions
   */
  pause(topicPartitions: TopicPartition[]): void {
    for (const tp of topicPartitions) {
      this._paused.add(`${tp.topic}:${tp.partition}`);
    }
  }

  /**
   * Resume consumption for specific topic-partitions
   */
  resume(topicPartitions: TopicPartition[]): void {
    for (const tp of topicPartitions) {
      this._paused.delete(`${tp.topic}:${tp.partition}`);
    }
  }

  /**
   * Check if a topic-partition is paused
   */
  paused(): TopicPartition[] {
    return Array.from(this._paused).map((key) => {
      const [topic, partition] = key.split(':');
      return { topic, partition: parseInt(partition, 10) };
    });
  }

  /**
   * Seek to a specific offset
   */
  async seek(entry: SeekEntry): Promise<void> {
    this._ensureConnected();

    try {
      await this._rpcClient!.$.consumerSeek({
        groupId: this._config.groupId,
        topic: entry.topic,
        partition: entry.partition,
        offset: entry.offset,
      });
    } catch (error) {
      throw new ConsumerError(
        `Failed to seek: ${error instanceof Error ? error.message : String(error)}`,
        this._config.groupId
      );
    }
  }

  /**
   * Commit offsets
   */
  async commitOffsets(offsets: Offsets): Promise<void> {
    this._ensureConnected();

    try {
      await this._rpcClient!.$.consumerCommitOffsets({
        groupId: this._config.groupId,
        topics: offsets.topics,
      });
    } catch (error) {
      throw new ConsumerError(
        `Failed to commit offsets: ${error instanceof Error ? error.message : String(error)}`,
        this._config.groupId
      );
    }
  }

  /**
   * Get current assignments
   */
  assignment(): TopicPartition[] {
    // Return subscribed topics with partition 0 as a simplified implementation
    return Array.from(this._subscriptions).map((topic) => ({
      topic,
      partition: 0,
    }));
  }

  /**
   * Start polling for messages
   */
  private _startPolling(): void {
    if (this._pollInterval) {
      return;
    }

    const poll = async () => {
      if (!this._isRunning || !this._rpcClient) {
        return;
      }

      try {
        const result = (await this._rpcClient.$.consumerPoll({
          groupId: this._config.groupId,
          maxMessages: 100,
        })) as { messages: Array<{ topic: string; partition: number; message: KafkaMessage }> };

        if (result && result.messages && result.messages.length > 0) {
          await this._processMessages(result.messages);
        }
      } catch {
        // Ignore poll errors, will retry
      }
    };

    // Poll immediately, then on interval
    poll();
    this._pollInterval = setInterval(poll, this._config.maxWaitTimeInMs ?? 5000);
  }

  /**
   * Stop polling for messages
   */
  private _stopPolling(): void {
    if (this._pollInterval) {
      clearInterval(this._pollInterval);
      this._pollInterval = null;
    }
  }

  /**
   * Process received messages
   */
  private async _processMessages(
    messages: Array<{ topic: string; partition: number; message: KafkaMessage }>
  ): Promise<void> {
    if (!this._runConfig) {
      return;
    }

    const { eachMessage, eachBatch } = this._runConfig;

    if (eachMessage) {
      // Process messages one at a time
      for (const { topic, partition, message } of messages) {
        // Skip if paused
        if (this._paused.has(`${topic}:${partition}`)) {
          continue;
        }

        if (!this._isRunning) {
          break;
        }

        const payload: EachMessagePayload = {
          topic,
          partition,
          message: this._deserializeMessage(message),
          heartbeat: async () => {
            if (this._rpcClient) {
              await this._rpcClient.$.consumerHeartbeat({
                groupId: this._config.groupId,
              });
            }
          },
          pause: () => {
            this.pause([{ topic, partition }]);
            return () => this.resume([{ topic, partition }]);
          },
        };

        await eachMessage(payload);
      }
    } else if (eachBatch) {
      // Group messages by topic-partition
      const batches = new Map<string, Array<{ topic: string; partition: number; message: KafkaMessage }>>();

      for (const msg of messages) {
        const key = `${msg.topic}:${msg.partition}`;
        if (!batches.has(key)) {
          batches.set(key, []);
        }
        batches.get(key)!.push(msg);
      }

      for (const [key, batchMessages] of batches) {
        if (!this._isRunning) {
          break;
        }

        // Skip if paused
        if (this._paused.has(key)) {
          continue;
        }

        const [topic, partitionStr] = key.split(':');
        const partition = parseInt(partitionStr, 10);

        const kafkaMessages = batchMessages.map((m) => this._deserializeMessage(m.message));
        const highWatermark = kafkaMessages.length > 0 ? kafkaMessages[kafkaMessages.length - 1].offset : '0';

        const batch: Batch = {
          topic,
          partition,
          highWatermark,
          messages: kafkaMessages,
          isEmpty: () => kafkaMessages.length === 0,
          firstOffset: () => (kafkaMessages.length > 0 ? kafkaMessages[0].offset : null),
          lastOffset: () => (kafkaMessages.length > 0 ? kafkaMessages[kafkaMessages.length - 1].offset : '0'),
          offsetLag: () => '0',
          offsetLagLow: () => '0',
        };

        const uncommittedOffsets: OffsetsByTopicPartition = {
          topics: {
            [topic]: {
              [partition.toString()]: { offset: batch.lastOffset() },
            },
          },
        };

        const payload: EachBatchPayload = {
          batch,
          resolveOffset: (_offset: string) => {
            // Mark offset as resolved
          },
          heartbeat: async () => {
            if (this._rpcClient) {
              await this._rpcClient.$.consumerHeartbeat({
                groupId: this._config.groupId,
              });
            }
          },
          commitOffsetsIfNecessary: async (offsets?: Offsets) => {
            if (offsets) {
              await this.commitOffsets(offsets);
            }
          },
          uncommittedOffsets: () => uncommittedOffsets,
          isRunning: () => this._isRunning,
          isStale: () => false,
          pause: () => {
            this.pause([{ topic, partition }]);
            return () => this.resume([{ topic, partition }]);
          },
        };

        await eachBatch(payload);
      }
    }
  }

  /**
   * Deserialize a message from the server
   */
  private _deserializeMessage(message: KafkaMessage): KafkaMessage {
    return {
      key: message.key ? Buffer.from(String(message.key), 'base64') : null,
      value: message.value ? Buffer.from(String(message.value), 'base64') : null,
      timestamp: message.timestamp,
      attributes: message.attributes ?? 0,
      offset: message.offset,
      size: message.size ?? 0,
      headers: message.headers,
    };
  }

  /**
   * Ensure the consumer is connected
   */
  private _ensureConnected(): void {
    if (this._connectionState !== 'connected' || !this._rpcClient) {
      throw new ConnectionError('Consumer is not connected. Call connect() first.');
    }
  }
}
