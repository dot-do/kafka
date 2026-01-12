/**
 * kafka.do - Producer class
 *
 * KafkaJS-compatible producer that uses rpc.do for transport.
 */

import type { Kafka, RpcClient } from './kafka.js';
import type {
  ProducerConfig,
  ProducerRecord,
  ProducerBatch,
  RecordMetadata,
  Message,
  TopicMessages,
  Transaction,
  TopicOffsets,
  ConnectionState,
} from './types.js';
import { ProducerError, ConnectionError } from './types.js';

/**
 * Producer - sends messages to Kafka topics
 *
 * @example
 * ```typescript
 * const producer = kafka.producer();
 * await producer.connect();
 *
 * // Send a single message
 * await producer.send({
 *   topic: 'my-topic',
 *   messages: [{ value: 'Hello!' }],
 * });
 *
 * // Send with key for partitioning
 * await producer.send({
 *   topic: 'my-topic',
 *   messages: [{ key: 'user-123', value: JSON.stringify({ action: 'login' }) }],
 * });
 *
 * // Batch send to multiple topics
 * await producer.sendBatch({
 *   topicMessages: [
 *     { topic: 'topic-1', messages: [{ value: 'msg1' }] },
 *     { topic: 'topic-2', messages: [{ value: 'msg2' }] },
 *   ],
 * });
 *
 * await producer.disconnect();
 * ```
 */
export class Producer {
  private readonly _kafka: Kafka;
  private readonly _config: ProducerConfig;
  private _connectionState: ConnectionState = 'disconnected';
  private _rpcClient: RpcClient | null = null;
  private _activeTransaction: Transaction | null = null;

  constructor(kafka: Kafka, config?: ProducerConfig) {
    this._kafka = kafka;
    this._config = {
      allowAutoTopicCreation: true,
      idempotent: false,
      maxInFlightRequests: 5,
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
   * Check if the producer is connected
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

      // Call the connect RPC method
      await this._rpcClient.$.producerConnect({
        clientId: this._kafka.config.clientId,
        idempotent: this._config.idempotent,
        transactionalId: this._config.transactionalId,
        maxInFlightRequests: this._config.maxInFlightRequests,
      });

      this._connectionState = 'connected';
    } catch (error) {
      this._connectionState = 'disconnected';
      throw new ConnectionError(
        `Failed to connect producer: ${error instanceof Error ? error.message : String(error)}`
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
        await this._rpcClient.$.producerDisconnect();
      }
    } finally {
      this._connectionState = 'disconnected';
      this._rpcClient = null;
    }
  }

  /**
   * Send messages to a topic
   *
   * @param record - Producer record with topic and messages
   * @returns Array of record metadata
   */
  async send(record: ProducerRecord): Promise<RecordMetadata[]> {
    this._ensureConnected();

    const { topic, messages, acks = -1, timeout, compression } = record;

    // Serialize messages
    const serializedMessages = messages.map((msg) => this._serializeMessage(msg));

    try {
      const result = await this._rpcClient!.$.producerSend({
        topic,
        messages: serializedMessages,
        acks,
        timeout,
        compression,
        allowAutoTopicCreation: this._config.allowAutoTopicCreation,
      });

      return result as RecordMetadata[];
    } catch (error) {
      throw new ProducerError(
        `Failed to send messages to topic '${topic}': ${error instanceof Error ? error.message : String(error)}`
      );
    }
  }

  /**
   * Send messages to multiple topics in a single batch
   *
   * @param batch - Batch configuration with topic messages
   * @returns Array of record metadata
   */
  async sendBatch(batch: ProducerBatch): Promise<RecordMetadata[]> {
    this._ensureConnected();

    const { topicMessages, acks = -1, timeout, compression } = batch;

    // Serialize all messages
    const serializedBatch = topicMessages.map((tm: TopicMessages) => ({
      topic: tm.topic,
      messages: tm.messages.map((msg) => this._serializeMessage(msg)),
    }));

    try {
      const result = await this._rpcClient!.$.producerSendBatch({
        topicMessages: serializedBatch,
        acks,
        timeout,
        compression,
        allowAutoTopicCreation: this._config.allowAutoTopicCreation,
      });

      return result as RecordMetadata[];
    } catch (error) {
      throw new ProducerError(
        `Failed to send batch: ${error instanceof Error ? error.message : String(error)}`
      );
    }
  }

  /**
   * Begin a transaction (for exactly-once semantics)
   *
   * @returns Transaction object
   */
  async transaction(): Promise<Transaction> {
    this._ensureConnected();

    if (!this._config.transactionalId) {
      throw new ProducerError('Transactions require a transactionalId in producer config');
    }

    if (this._activeTransaction) {
      throw new ProducerError('A transaction is already in progress');
    }

    try {
      await this._rpcClient!.$.producerBeginTransaction({
        transactionalId: this._config.transactionalId,
      });

      let isActive = true;

      const transaction: Transaction = {
        sendOffsets: async (options: { consumerGroupId: string; topics: TopicOffsets[] }) => {
          if (!isActive) {
            throw new ProducerError('Transaction is no longer active');
          }
          await this._rpcClient!.$.producerSendOffsetsToTransaction({
            transactionalId: this._config.transactionalId,
            consumerGroupId: options.consumerGroupId,
            topics: options.topics,
          });
        },

        commit: async () => {
          if (!isActive) {
            throw new ProducerError('Transaction is no longer active');
          }
          try {
            await this._rpcClient!.$.producerCommitTransaction({
              transactionalId: this._config.transactionalId,
            });
          } finally {
            isActive = false;
            this._activeTransaction = null;
          }
        },

        abort: async () => {
          if (!isActive) {
            throw new ProducerError('Transaction is no longer active');
          }
          try {
            await this._rpcClient!.$.producerAbortTransaction({
              transactionalId: this._config.transactionalId,
            });
          } finally {
            isActive = false;
            this._activeTransaction = null;
          }
        },

        isActive: () => isActive,
      };

      this._activeTransaction = transaction;
      return transaction;
    } catch (error) {
      throw new ProducerError(
        `Failed to begin transaction: ${error instanceof Error ? error.message : String(error)}`
      );
    }
  }

  /**
   * Serialize a message for sending
   */
  private _serializeMessage(message: Message): Record<string, unknown> {
    return {
      key: message.key !== null && message.key !== undefined
        ? (typeof message.key === 'string' ? message.key : message.key.toString('base64'))
        : null,
      value: message.value !== null
        ? (typeof message.value === 'string' ? message.value : message.value.toString('base64'))
        : null,
      partition: message.partition,
      headers: message.headers
        ? Object.fromEntries(
            Object.entries(message.headers).map(([k, v]) => [
              k,
              v === undefined
                ? undefined
                : Array.isArray(v)
                ? v.map((x) => (typeof x === 'string' ? x : x.toString('base64')))
                : typeof v === 'string'
                ? v
                : v.toString('base64'),
            ])
          )
        : undefined,
      timestamp: message.timestamp,
    };
  }

  /**
   * Ensure the producer is connected
   */
  private _ensureConnected(): void {
    if (this._connectionState !== 'connected' || !this._rpcClient) {
      throw new ConnectionError('Producer is not connected. Call connect() first.');
    }
  }
}
