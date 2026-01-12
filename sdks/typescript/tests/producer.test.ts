/**
 * Tests for the Producer class
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { Kafka, setRpcClientFactory } from '../src/kafka.js';
import type { RpcClient } from '../src/kafka.js';
import { Producer } from '../src/producer.js';
import { ConnectionError, ProducerError } from '../src/types.js';

// Create mock RPC methods
function createMockRpcMethods() {
  return {
    producerConnect: vi.fn().mockResolvedValue({}),
    producerDisconnect: vi.fn().mockResolvedValue({}),
    producerSend: vi.fn().mockResolvedValue([
      { topicName: 'test-topic', partition: 0, errorCode: 0, baseOffset: '0' },
    ]),
    producerSendBatch: vi.fn().mockResolvedValue([
      { topicName: 'topic-1', partition: 0, errorCode: 0, baseOffset: '0' },
      { topicName: 'topic-2', partition: 0, errorCode: 0, baseOffset: '0' },
    ]),
    producerBeginTransaction: vi.fn().mockResolvedValue({}),
    producerSendOffsetsToTransaction: vi.fn().mockResolvedValue({}),
    producerCommitTransaction: vi.fn().mockResolvedValue({}),
    producerAbortTransaction: vi.fn().mockResolvedValue({}),
  };
}

function createMockRpcClient(methods = createMockRpcMethods()): RpcClient {
  return {
    $: new Proxy({} as Record<string, (...args: unknown[]) => Promise<unknown>>, {
      get: (_target, prop: string) => methods[prop as keyof typeof methods] || vi.fn().mockResolvedValue({}),
    }),
    close: vi.fn().mockResolvedValue(undefined),
  };
}

describe('Producer', () => {
  let kafka: Kafka;
  let mockMethods: ReturnType<typeof createMockRpcMethods>;
  let mockClient: RpcClient;

  beforeEach(() => {
    mockMethods = createMockRpcMethods();
    mockClient = createMockRpcClient(mockMethods);
    setRpcClientFactory(() => mockClient);
    kafka = new Kafka({ brokers: ['broker1:9092'], clientId: 'test-client' });
  });

  afterEach(() => {
    setRpcClientFactory(null);
  });

  describe('constructor', () => {
    it('should create a producer with default config', () => {
      const producer = new Producer(kafka);
      expect(producer).toBeInstanceOf(Producer);
      expect(producer.connectionState).toBe('disconnected');
    });

    it('should create a producer with custom config', () => {
      const producer = new Producer(kafka, {
        idempotent: true,
        transactionalId: 'tx-1',
        maxInFlightRequests: 1,
      });
      expect(producer).toBeInstanceOf(Producer);
    });
  });

  describe('isConnected', () => {
    it('should return false when not connected', () => {
      const producer = kafka.producer();
      expect(producer.isConnected()).toBe(false);
    });

    it('should return true when connected', async () => {
      const producer = kafka.producer();
      await producer.connect();
      expect(producer.isConnected()).toBe(true);
    });
  });

  describe('connect', () => {
    it('should connect successfully', async () => {
      const producer = kafka.producer();
      await producer.connect();
      expect(producer.connectionState).toBe('connected');
      expect(mockMethods.producerConnect).toHaveBeenCalled();
    });

    it('should not reconnect if already connected', async () => {
      const producer = kafka.producer();
      await producer.connect();
      await producer.connect();
      expect(mockMethods.producerConnect).toHaveBeenCalledTimes(1);
    });

    it('should throw ConnectionError on failure', async () => {
      mockMethods.producerConnect.mockRejectedValueOnce(new Error('Connection refused'));
      const producer = kafka.producer();
      await expect(producer.connect()).rejects.toThrow(ConnectionError);
      expect(producer.connectionState).toBe('disconnected');
    });

    it('should pass config to connect call', async () => {
      const producer = kafka.producer({ idempotent: true, transactionalId: 'tx-1' });
      await producer.connect();
      expect(mockMethods.producerConnect).toHaveBeenCalledWith({
        clientId: 'test-client',
        idempotent: true,
        transactionalId: 'tx-1',
        maxInFlightRequests: 5,
      });
    });
  });

  describe('disconnect', () => {
    it('should disconnect successfully', async () => {
      const producer = kafka.producer();
      await producer.connect();
      await producer.disconnect();
      expect(producer.connectionState).toBe('disconnected');
      expect(mockMethods.producerDisconnect).toHaveBeenCalled();
    });

    it('should handle disconnect when not connected', async () => {
      const producer = kafka.producer();
      await producer.disconnect();
      expect(mockMethods.producerDisconnect).not.toHaveBeenCalled();
    });

    it('should clear rpc client after disconnect', async () => {
      const producer = kafka.producer();
      await producer.connect();
      await producer.disconnect();
      expect(producer.isConnected()).toBe(false);
    });
  });

  describe('send', () => {
    it('should send messages successfully', async () => {
      const producer = kafka.producer();
      await producer.connect();

      const result = await producer.send({
        topic: 'test-topic',
        messages: [{ value: 'Hello!' }],
      });

      expect(result).toHaveLength(1);
      expect(result[0].topicName).toBe('test-topic');
      expect(mockMethods.producerSend).toHaveBeenCalled();
    });

    it('should throw when not connected', async () => {
      const producer = kafka.producer();
      await expect(
        producer.send({
          topic: 'test-topic',
          messages: [{ value: 'Hello!' }],
        })
      ).rejects.toThrow(ConnectionError);
    });

    it('should serialize message with key', async () => {
      const producer = kafka.producer();
      await producer.connect();

      await producer.send({
        topic: 'test-topic',
        messages: [{ key: 'key-1', value: 'Hello!' }],
      });

      const call = mockMethods.producerSend.mock.calls[0][0] as {
        messages: Array<{ key: string; value: string }>;
      };
      expect(call.messages[0].key).toBe('key-1');
    });

    it('should serialize message with Buffer key', async () => {
      const producer = kafka.producer();
      await producer.connect();

      await producer.send({
        topic: 'test-topic',
        messages: [{ key: Buffer.from('key-1'), value: 'Hello!' }],
      });

      const call = mockMethods.producerSend.mock.calls[0][0] as {
        messages: Array<{ key: string; value: string }>;
      };
      expect(call.messages[0].key).toBe(Buffer.from('key-1').toString('base64'));
    });

    it('should serialize message with Buffer value', async () => {
      const producer = kafka.producer();
      await producer.connect();

      await producer.send({
        topic: 'test-topic',
        messages: [{ value: Buffer.from('Hello!') }],
      });

      const call = mockMethods.producerSend.mock.calls[0][0] as {
        messages: Array<{ key: string | null; value: string }>;
      };
      expect(call.messages[0].value).toBe(Buffer.from('Hello!').toString('base64'));
    });

    it('should handle null key', async () => {
      const producer = kafka.producer();
      await producer.connect();

      await producer.send({
        topic: 'test-topic',
        messages: [{ key: null, value: 'Hello!' }],
      });

      const call = mockMethods.producerSend.mock.calls[0][0] as {
        messages: Array<{ key: string | null; value: string }>;
      };
      expect(call.messages[0].key).toBeNull();
    });

    it('should handle null value', async () => {
      const producer = kafka.producer();
      await producer.connect();

      await producer.send({
        topic: 'test-topic',
        messages: [{ value: null }],
      });

      const call = mockMethods.producerSend.mock.calls[0][0] as {
        messages: Array<{ key: string | null; value: string | null }>;
      };
      expect(call.messages[0].value).toBeNull();
    });

    it('should serialize headers', async () => {
      const producer = kafka.producer();
      await producer.connect();

      await producer.send({
        topic: 'test-topic',
        messages: [
          {
            value: 'Hello!',
            headers: {
              'correlation-id': 'abc-123',
              'source': Buffer.from('service-1'),
            },
          },
        ],
      });

      const call = mockMethods.producerSend.mock.calls[0][0] as {
        messages: Array<{ headers: Record<string, string> }>;
      };
      expect(call.messages[0].headers['correlation-id']).toBe('abc-123');
    });

    it('should serialize array headers', async () => {
      const producer = kafka.producer();
      await producer.connect();

      await producer.send({
        topic: 'test-topic',
        messages: [
          {
            value: 'Hello!',
            headers: {
              'tags': ['tag1', Buffer.from('tag2')],
            },
          },
        ],
      });

      expect(mockMethods.producerSend).toHaveBeenCalled();
    });

    it('should handle undefined header value', async () => {
      const producer = kafka.producer();
      await producer.connect();

      await producer.send({
        topic: 'test-topic',
        messages: [
          {
            value: 'Hello!',
            headers: {
              'optional': undefined,
            },
          },
        ],
      });

      expect(mockMethods.producerSend).toHaveBeenCalled();
    });

    it('should pass acks option', async () => {
      const producer = kafka.producer();
      await producer.connect();

      await producer.send({
        topic: 'test-topic',
        messages: [{ value: 'Hello!' }],
        acks: 1,
      });

      const call = mockMethods.producerSend.mock.calls[0][0] as { acks: number };
      expect(call.acks).toBe(1);
    });

    it('should pass timeout option', async () => {
      const producer = kafka.producer();
      await producer.connect();

      await producer.send({
        topic: 'test-topic',
        messages: [{ value: 'Hello!' }],
        timeout: 5000,
      });

      const call = mockMethods.producerSend.mock.calls[0][0] as { timeout: number };
      expect(call.timeout).toBe(5000);
    });

    it('should pass compression option', async () => {
      const producer = kafka.producer();
      await producer.connect();

      await producer.send({
        topic: 'test-topic',
        messages: [{ value: 'Hello!' }],
        compression: 1, // GZIP
      });

      const call = mockMethods.producerSend.mock.calls[0][0] as { compression: number };
      expect(call.compression).toBe(1);
    });

    it('should include partition in message', async () => {
      const producer = kafka.producer();
      await producer.connect();

      await producer.send({
        topic: 'test-topic',
        messages: [{ value: 'Hello!', partition: 2 }],
      });

      const call = mockMethods.producerSend.mock.calls[0][0] as {
        messages: Array<{ partition: number }>;
      };
      expect(call.messages[0].partition).toBe(2);
    });

    it('should include timestamp in message', async () => {
      const producer = kafka.producer();
      await producer.connect();

      await producer.send({
        topic: 'test-topic',
        messages: [{ value: 'Hello!', timestamp: '1234567890' }],
      });

      const call = mockMethods.producerSend.mock.calls[0][0] as {
        messages: Array<{ timestamp: string }>;
      };
      expect(call.messages[0].timestamp).toBe('1234567890');
    });

    it('should throw ProducerError on send failure', async () => {
      mockMethods.producerSend.mockRejectedValueOnce(new Error('Send failed'));
      const producer = kafka.producer();
      await producer.connect();

      await expect(
        producer.send({
          topic: 'test-topic',
          messages: [{ value: 'Hello!' }],
        })
      ).rejects.toThrow(ProducerError);
    });
  });

  describe('sendBatch', () => {
    it('should send batch successfully', async () => {
      const producer = kafka.producer();
      await producer.connect();

      const result = await producer.sendBatch({
        topicMessages: [
          { topic: 'topic-1', messages: [{ value: 'msg1' }] },
          { topic: 'topic-2', messages: [{ value: 'msg2' }] },
        ],
      });

      expect(result).toHaveLength(2);
      expect(mockMethods.producerSendBatch).toHaveBeenCalled();
    });

    it('should throw when not connected', async () => {
      const producer = kafka.producer();
      await expect(
        producer.sendBatch({
          topicMessages: [{ topic: 'topic-1', messages: [{ value: 'msg1' }] }],
        })
      ).rejects.toThrow(ConnectionError);
    });

    it('should pass batch options', async () => {
      const producer = kafka.producer();
      await producer.connect();

      await producer.sendBatch({
        topicMessages: [{ topic: 'topic-1', messages: [{ value: 'msg1' }] }],
        acks: 0,
        timeout: 10000,
        compression: 2,
      });

      const call = mockMethods.producerSendBatch.mock.calls[0][0] as {
        acks: number;
        timeout: number;
        compression: number;
      };
      expect(call.acks).toBe(0);
      expect(call.timeout).toBe(10000);
      expect(call.compression).toBe(2);
    });

    it('should throw ProducerError on batch failure', async () => {
      mockMethods.producerSendBatch.mockRejectedValueOnce(new Error('Batch failed'));
      const producer = kafka.producer();
      await producer.connect();

      await expect(
        producer.sendBatch({
          topicMessages: [{ topic: 'topic-1', messages: [{ value: 'msg1' }] }],
        })
      ).rejects.toThrow(ProducerError);
    });
  });

  describe('transaction', () => {
    it('should begin transaction successfully', async () => {
      const producer = kafka.producer({ transactionalId: 'tx-1' });
      await producer.connect();

      const transaction = await producer.transaction();
      expect(transaction).toBeDefined();
      expect(transaction.isActive()).toBe(true);
      expect(mockMethods.producerBeginTransaction).toHaveBeenCalled();
    });

    it('should throw when no transactionalId', async () => {
      const producer = kafka.producer();
      await producer.connect();

      await expect(producer.transaction()).rejects.toThrow(
        'Transactions require a transactionalId in producer config'
      );
    });

    it('should throw when not connected', async () => {
      const producer = kafka.producer({ transactionalId: 'tx-1' });
      await expect(producer.transaction()).rejects.toThrow(ConnectionError);
    });

    it('should throw when transaction already in progress', async () => {
      const producer = kafka.producer({ transactionalId: 'tx-1' });
      await producer.connect();

      await producer.transaction();
      await expect(producer.transaction()).rejects.toThrow('A transaction is already in progress');
    });

    it('should commit transaction', async () => {
      const producer = kafka.producer({ transactionalId: 'tx-1' });
      await producer.connect();

      const transaction = await producer.transaction();
      await transaction.commit();

      expect(mockMethods.producerCommitTransaction).toHaveBeenCalled();
      expect(transaction.isActive()).toBe(false);
    });

    it('should abort transaction', async () => {
      const producer = kafka.producer({ transactionalId: 'tx-1' });
      await producer.connect();

      const transaction = await producer.transaction();
      await transaction.abort();

      expect(mockMethods.producerAbortTransaction).toHaveBeenCalled();
      expect(transaction.isActive()).toBe(false);
    });

    it('should send offsets to transaction', async () => {
      const producer = kafka.producer({ transactionalId: 'tx-1' });
      await producer.connect();

      const transaction = await producer.transaction();
      await transaction.sendOffsets({
        consumerGroupId: 'my-group',
        topics: [{ topic: 'test', partitions: [{ partition: 0, offset: '10' }] }],
      });

      expect(mockMethods.producerSendOffsetsToTransaction).toHaveBeenCalled();
    });

    it('should throw when sending offsets after commit', async () => {
      const producer = kafka.producer({ transactionalId: 'tx-1' });
      await producer.connect();

      const transaction = await producer.transaction();
      await transaction.commit();

      await expect(
        transaction.sendOffsets({
          consumerGroupId: 'my-group',
          topics: [],
        })
      ).rejects.toThrow('Transaction is no longer active');
    });

    it('should throw when committing after abort', async () => {
      const producer = kafka.producer({ transactionalId: 'tx-1' });
      await producer.connect();

      const transaction = await producer.transaction();
      await transaction.abort();

      await expect(transaction.commit()).rejects.toThrow('Transaction is no longer active');
    });

    it('should throw when aborting after commit', async () => {
      const producer = kafka.producer({ transactionalId: 'tx-1' });
      await producer.connect();

      const transaction = await producer.transaction();
      await transaction.commit();

      await expect(transaction.abort()).rejects.toThrow('Transaction is no longer active');
    });

    it('should throw ProducerError on begin transaction failure', async () => {
      mockMethods.producerBeginTransaction.mockRejectedValueOnce(new Error('Failed'));
      const producer = kafka.producer({ transactionalId: 'tx-1' });
      await producer.connect();

      await expect(producer.transaction()).rejects.toThrow(ProducerError);
    });

    it('should allow new transaction after commit', async () => {
      const producer = kafka.producer({ transactionalId: 'tx-1' });
      await producer.connect();

      const tx1 = await producer.transaction();
      await tx1.commit();

      const tx2 = await producer.transaction();
      expect(tx2.isActive()).toBe(true);
    });

    it('should handle non-Error exceptions in transaction', async () => {
      mockMethods.producerBeginTransaction.mockRejectedValueOnce('string error');
      const producer = kafka.producer({ transactionalId: 'tx-1' });
      await producer.connect();

      await expect(producer.transaction()).rejects.toThrow('string error');
    });
  });

  describe('error handling with non-Error exceptions', () => {
    it('should handle non-Error in connect', async () => {
      mockMethods.producerConnect.mockRejectedValueOnce('connection string error');
      const producer = kafka.producer();
      await expect(producer.connect()).rejects.toThrow('connection string error');
    });

    it('should handle non-Error in send', async () => {
      mockMethods.producerSend.mockRejectedValueOnce('send string error');
      const producer = kafka.producer();
      await producer.connect();
      await expect(producer.send({ topic: 'test', messages: [{ value: 'x' }] })).rejects.toThrow(
        'send string error'
      );
    });

    it('should handle non-Error in sendBatch', async () => {
      mockMethods.producerSendBatch.mockRejectedValueOnce('batch string error');
      const producer = kafka.producer();
      await producer.connect();
      await expect(
        producer.sendBatch({ topicMessages: [{ topic: 'test', messages: [{ value: 'x' }] }] })
      ).rejects.toThrow('batch string error');
    });
  });
});
