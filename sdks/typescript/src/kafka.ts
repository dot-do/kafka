/**
 * kafka.do - Main Kafka class
 *
 * Provides a KafkaJS-compatible API that uses rpc.do for transport.
 * This is the main entry point for creating producers, consumers, and admin clients.
 */

import type { KafkaConfig, ProducerConfig, ConsumerConfig, AdminConfig } from './types.js';
import { Producer } from './producer.js';
import { Consumer } from './consumer.js';
import { Admin } from './admin.js';

/**
 * RPC client interface for the $ proxy pattern
 */
export interface RpcClient {
  $: {
    [method: string]: (...args: unknown[]) => Promise<unknown>;
  };
  close(): Promise<void>;
}

/**
 * RPC client factory type
 */
export type RpcClientFactory = (url: string) => RpcClient;

/**
 * Default RPC client factory (can be overridden for testing)
 */
let rpcClientFactory: RpcClientFactory | null = null;

/**
 * Set the RPC client factory (used for dependency injection in tests)
 */
export function setRpcClientFactory(factory: RpcClientFactory | null): void {
  rpcClientFactory = factory;
}

/**
 * Get the RPC client factory
 */
export function getRpcClientFactory(): RpcClientFactory | null {
  return rpcClientFactory;
}

/**
 * Create an RPC client for the given brokers
 */
export function createRpcClient(config: KafkaConfig): RpcClient {
  // Build the RPC URL from broker configuration
  const broker = config.brokers[0];
  const url = broker.startsWith('wss://') || broker.startsWith('ws://')
    ? broker
    : `wss://${broker.replace(':9092', '')}.kafka.do`;

  // Use injected factory or create a real client
  if (rpcClientFactory) {
    return rpcClientFactory(url);
  }

  // In production, this would use the real rpc.do connect function
  // For now, we throw an error if no factory is set
  throw new Error('RPC client factory not configured. Set it using setRpcClientFactory()');
}

/**
 * Kafka client - main entry point
 *
 * Provides a KafkaJS-compatible interface for creating producers, consumers,
 * and admin clients. Uses rpc.do under the hood for all Kafka operations.
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
 * // Create a producer
 * const producer = kafka.producer();
 *
 * // Create a consumer
 * const consumer = kafka.consumer({ groupId: 'my-group' });
 *
 * // Create an admin client
 * const admin = kafka.admin();
 * ```
 */
export class Kafka {
  private readonly _config: KafkaConfig;
  private _rpcClient: RpcClient | null = null;

  /**
   * Create a new Kafka client
   *
   * @param config - Kafka configuration
   */
  constructor(config: KafkaConfig) {
    if (!config.brokers || config.brokers.length === 0) {
      throw new Error('At least one broker is required');
    }
    this._config = {
      ...config,
      clientId: config.clientId ?? 'kafka.do-client',
      connectionTimeout: config.connectionTimeout ?? 30000,
      requestTimeout: config.requestTimeout ?? 30000,
    };
  }

  /**
   * Get the Kafka configuration
   */
  get config(): Readonly<KafkaConfig> {
    return this._config;
  }

  /**
   * Get or create the RPC client
   */
  getRpcClient(): RpcClient {
    if (!this._rpcClient) {
      this._rpcClient = createRpcClient(this._config);
    }
    return this._rpcClient;
  }

  /**
   * Set a custom RPC client (for testing)
   */
  setRpcClient(client: RpcClient): void {
    this._rpcClient = client;
  }

  /**
   * Create a producer instance
   *
   * @param config - Producer configuration
   * @returns Producer instance
   *
   * @example
   * ```typescript
   * const producer = kafka.producer();
   * await producer.connect();
   * await producer.send({
   *   topic: 'my-topic',
   *   messages: [{ value: 'Hello!' }],
   * });
   * await producer.disconnect();
   * ```
   */
  producer(config?: ProducerConfig): Producer {
    return new Producer(this, config);
  }

  /**
   * Create a consumer instance
   *
   * @param config - Consumer configuration (groupId required)
   * @returns Consumer instance
   *
   * @example
   * ```typescript
   * const consumer = kafka.consumer({ groupId: 'my-group' });
   * await consumer.connect();
   * await consumer.subscribe({ topic: 'my-topic' });
   * await consumer.run({
   *   eachMessage: async ({ topic, partition, message }) => {
   *     console.log(message.value?.toString());
   *   },
   * });
   * ```
   */
  consumer(config: ConsumerConfig): Consumer {
    if (!config.groupId) {
      throw new Error('Consumer groupId is required');
    }
    return new Consumer(this, config);
  }

  /**
   * Create an admin client instance
   *
   * @param config - Admin configuration
   * @returns Admin instance
   *
   * @example
   * ```typescript
   * const admin = kafka.admin();
   * await admin.connect();
   * await admin.createTopics({
   *   topics: [{ topic: 'new-topic', numPartitions: 3 }],
   * });
   * const topics = await admin.listTopics();
   * await admin.disconnect();
   * ```
   */
  admin(config?: AdminConfig): Admin {
    return new Admin(this, config);
  }

  /**
   * Close the Kafka client and all connections
   */
  async close(): Promise<void> {
    if (this._rpcClient) {
      await this._rpcClient.close();
      this._rpcClient = null;
    }
  }
}
