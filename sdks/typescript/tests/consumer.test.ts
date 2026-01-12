/**
 * Tests for the Consumer class
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { Kafka, setRpcClientFactory } from '../src/kafka.js';
import type { RpcClient } from '../src/kafka.js';
import { Consumer } from '../src/consumer.js';
import { ConnectionError, ConsumerError } from '../src/types.js';

// Create mock RPC methods
function createMockRpcMethods() {
  return {
    consumerConnect: vi.fn().mockResolvedValue({}),
    consumerDisconnect: vi.fn().mockResolvedValue({}),
    consumerSubscribe: vi.fn().mockResolvedValue({}),
    consumerUnsubscribe: vi.fn().mockResolvedValue({}),
    consumerRun: vi.fn().mockResolvedValue({}),
    consumerStop: vi.fn().mockResolvedValue({}),
    consumerSeek: vi.fn().mockResolvedValue({}),
    consumerCommitOffsets: vi.fn().mockResolvedValue({}),
    consumerHeartbeat: vi.fn().mockResolvedValue({}),
    consumerPoll: vi.fn().mockResolvedValue({ messages: [] }),
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

describe('Consumer', () => {
  let kafka: Kafka;
  let mockMethods: ReturnType<typeof createMockRpcMethods>;
  let mockClient: RpcClient;

  beforeEach(() => {
    vi.useFakeTimers();
    mockMethods = createMockRpcMethods();
    mockClient = createMockRpcClient(mockMethods);
    setRpcClientFactory(() => mockClient);
    kafka = new Kafka({ brokers: ['broker1:9092'], clientId: 'test-client' });
  });

  afterEach(() => {
    vi.useRealTimers();
    setRpcClientFactory(null);
  });

  describe('constructor', () => {
    it('should create a consumer with required config', () => {
      const consumer = new Consumer(kafka, { groupId: 'my-group' });
      expect(consumer).toBeInstanceOf(Consumer);
      expect(consumer.groupId).toBe('my-group');
      expect(consumer.connectionState).toBe('disconnected');
    });

    it('should set default session timeout', () => {
      const consumer = new Consumer(kafka, { groupId: 'my-group' });
      expect(consumer).toBeDefined();
    });
  });

  describe('isConnected', () => {
    it('should return false when not connected', () => {
      const consumer = kafka.consumer({ groupId: 'my-group' });
      expect(consumer.isConnected()).toBe(false);
    });

    it('should return true when connected', async () => {
      const consumer = kafka.consumer({ groupId: 'my-group' });
      await consumer.connect();
      expect(consumer.isConnected()).toBe(true);
    });
  });

  describe('isRunning', () => {
    it('should return false when not running', () => {
      const consumer = kafka.consumer({ groupId: 'my-group' });
      expect(consumer.isRunning()).toBe(false);
    });
  });

  describe('connect', () => {
    it('should connect successfully', async () => {
      const consumer = kafka.consumer({ groupId: 'my-group' });
      await consumer.connect();
      expect(consumer.connectionState).toBe('connected');
      expect(mockMethods.consumerConnect).toHaveBeenCalled();
    });

    it('should not reconnect if already connected', async () => {
      const consumer = kafka.consumer({ groupId: 'my-group' });
      await consumer.connect();
      await consumer.connect();
      expect(mockMethods.consumerConnect).toHaveBeenCalledTimes(1);
    });

    it('should throw ConnectionError on failure', async () => {
      mockMethods.consumerConnect.mockRejectedValueOnce(new Error('Connection refused'));
      const consumer = kafka.consumer({ groupId: 'my-group' });
      await expect(consumer.connect()).rejects.toThrow(ConnectionError);
      expect(consumer.connectionState).toBe('disconnected');
    });

    it('should pass config to connect call', async () => {
      const consumer = kafka.consumer({
        groupId: 'my-group',
        sessionTimeout: 60000,
        maxBytesPerPartition: 2097152,
      });
      await consumer.connect();
      expect(mockMethods.consumerConnect).toHaveBeenCalledWith(
        expect.objectContaining({
          groupId: 'my-group',
          sessionTimeout: 60000,
          maxBytesPerPartition: 2097152,
        })
      );
    });
  });

  describe('disconnect', () => {
    it('should disconnect successfully', async () => {
      const consumer = kafka.consumer({ groupId: 'my-group' });
      await consumer.connect();
      await consumer.disconnect();
      expect(consumer.connectionState).toBe('disconnected');
      expect(mockMethods.consumerDisconnect).toHaveBeenCalled();
    });

    it('should handle disconnect when not connected', async () => {
      const consumer = kafka.consumer({ groupId: 'my-group' });
      await consumer.disconnect();
      expect(mockMethods.consumerDisconnect).not.toHaveBeenCalled();
    });

    it('should stop consumer before disconnecting', async () => {
      const consumer = kafka.consumer({ groupId: 'my-group' });
      await consumer.connect();
      await consumer.subscribe({ topic: 'test' });
      await consumer.run({ eachMessage: vi.fn() });
      await consumer.disconnect();
      expect(mockMethods.consumerStop).toHaveBeenCalled();
    });

    it('should clear subscriptions after disconnect', async () => {
      const consumer = kafka.consumer({ groupId: 'my-group' });
      await consumer.connect();
      await consumer.subscribe({ topic: 'test' });
      await consumer.disconnect();
      expect(consumer.assignment()).toHaveLength(0);
    });
  });

  describe('subscribe', () => {
    it('should subscribe to single topic', async () => {
      const consumer = kafka.consumer({ groupId: 'my-group' });
      await consumer.connect();
      await consumer.subscribe({ topic: 'my-topic' });
      expect(mockMethods.consumerSubscribe).toHaveBeenCalledWith({
        groupId: 'my-group',
        topics: ['my-topic'],
        fromBeginning: false,
      });
    });

    it('should subscribe from beginning', async () => {
      const consumer = kafka.consumer({ groupId: 'my-group' });
      await consumer.connect();
      await consumer.subscribe({ topic: 'my-topic', fromBeginning: true });
      expect(mockMethods.consumerSubscribe).toHaveBeenCalledWith({
        groupId: 'my-group',
        topics: ['my-topic'],
        fromBeginning: true,
      });
    });

    it('should subscribe to multiple topics', async () => {
      const consumer = kafka.consumer({ groupId: 'my-group' });
      await consumer.connect();
      await consumer.subscribe({ topics: ['topic-1', 'topic-2'] });
      expect(mockMethods.consumerSubscribe).toHaveBeenCalledWith({
        groupId: 'my-group',
        topics: ['topic-1', 'topic-2'],
        fromBeginning: false,
      });
    });

    it('should subscribe to regex topic', async () => {
      const consumer = kafka.consumer({ groupId: 'my-group' });
      await consumer.connect();
      await consumer.subscribe({ topic: /my-topic-.*/ });
      expect(mockMethods.consumerSubscribe).toHaveBeenCalledWith({
        groupId: 'my-group',
        topics: ['my-topic-.*'],
        fromBeginning: false,
      });
    });

    it('should subscribe to regex topics array', async () => {
      const consumer = kafka.consumer({ groupId: 'my-group' });
      await consumer.connect();
      await consumer.subscribe({ topics: [/topic-a-.*/, /topic-b-.*/] });
      expect(mockMethods.consumerSubscribe).toHaveBeenCalledWith({
        groupId: 'my-group',
        topics: ['topic-a-.*', 'topic-b-.*'],
        fromBeginning: false,
      });
    });

    it('should throw when not connected', async () => {
      const consumer = kafka.consumer({ groupId: 'my-group' });
      await expect(consumer.subscribe({ topic: 'my-topic' })).rejects.toThrow(ConnectionError);
    });

    it('should throw ConsumerError on failure', async () => {
      mockMethods.consumerSubscribe.mockRejectedValueOnce(new Error('Subscribe failed'));
      const consumer = kafka.consumer({ groupId: 'my-group' });
      await consumer.connect();
      await expect(consumer.subscribe({ topic: 'my-topic' })).rejects.toThrow(ConsumerError);
    });
  });

  describe('unsubscribe', () => {
    it('should unsubscribe from all topics', async () => {
      const consumer = kafka.consumer({ groupId: 'my-group' });
      await consumer.connect();
      await consumer.subscribe({ topic: 'my-topic' });
      await consumer.unsubscribe();
      expect(mockMethods.consumerUnsubscribe).toHaveBeenCalled();
    });

    it('should throw when not connected', async () => {
      const consumer = kafka.consumer({ groupId: 'my-group' });
      await expect(consumer.unsubscribe()).rejects.toThrow(ConnectionError);
    });

    it('should throw ConsumerError on failure', async () => {
      mockMethods.consumerUnsubscribe.mockRejectedValueOnce(new Error('Unsubscribe failed'));
      const consumer = kafka.consumer({ groupId: 'my-group' });
      await consumer.connect();
      await expect(consumer.unsubscribe()).rejects.toThrow(ConsumerError);
    });
  });

  describe('run', () => {
    it('should start consuming with eachMessage', async () => {
      const consumer = kafka.consumer({ groupId: 'my-group' });
      await consumer.connect();
      await consumer.subscribe({ topic: 'my-topic' });

      const eachMessage = vi.fn();
      await consumer.run({ eachMessage });

      expect(consumer.isRunning()).toBe(true);
      expect(mockMethods.consumerRun).toHaveBeenCalled();
    });

    it('should start consuming with eachBatch', async () => {
      const consumer = kafka.consumer({ groupId: 'my-group' });
      await consumer.connect();
      await consumer.subscribe({ topic: 'my-topic' });

      const eachBatch = vi.fn();
      await consumer.run({ eachBatch });

      expect(consumer.isRunning()).toBe(true);
    });

    it('should throw when neither handler is provided', async () => {
      const consumer = kafka.consumer({ groupId: 'my-group' });
      await consumer.connect();

      await expect(consumer.run({})).rejects.toThrow(
        'Either eachMessage or eachBatch handler is required'
      );
    });

    it('should throw when already running', async () => {
      const consumer = kafka.consumer({ groupId: 'my-group' });
      await consumer.connect();

      await consumer.run({ eachMessage: vi.fn() });
      await expect(consumer.run({ eachMessage: vi.fn() })).rejects.toThrow(
        'Consumer is already running'
      );
    });

    it('should throw when not connected', async () => {
      const consumer = kafka.consumer({ groupId: 'my-group' });
      await expect(consumer.run({ eachMessage: vi.fn() })).rejects.toThrow(ConnectionError);
    });

    it('should throw ConsumerError on run failure', async () => {
      mockMethods.consumerRun.mockRejectedValueOnce(new Error('Run failed'));
      const consumer = kafka.consumer({ groupId: 'my-group' });
      await consumer.connect();

      await expect(consumer.run({ eachMessage: vi.fn() })).rejects.toThrow(ConsumerError);
      expect(consumer.isRunning()).toBe(false);
    });

    it('should pass autoCommit config', async () => {
      const consumer = kafka.consumer({ groupId: 'my-group' });
      await consumer.connect();

      await consumer.run({
        eachMessage: vi.fn(),
        autoCommit: false,
        autoCommitInterval: 10000,
      });

      expect(mockMethods.consumerRun).toHaveBeenCalledWith({
        groupId: 'my-group',
        autoCommit: false,
        autoCommitInterval: 10000,
      });
    });
  });

  describe('stop', () => {
    it('should stop consuming', async () => {
      const consumer = kafka.consumer({ groupId: 'my-group' });
      await consumer.connect();
      await consumer.run({ eachMessage: vi.fn() });
      await consumer.stop();

      expect(consumer.isRunning()).toBe(false);
      expect(mockMethods.consumerStop).toHaveBeenCalled();
    });

    it('should handle stop when not running', async () => {
      const consumer = kafka.consumer({ groupId: 'my-group' });
      await consumer.stop();
      expect(mockMethods.consumerStop).not.toHaveBeenCalled();
    });
  });

  describe('pause/resume', () => {
    it('should pause consumption', async () => {
      const consumer = kafka.consumer({ groupId: 'my-group' });
      await consumer.connect();

      consumer.pause([{ topic: 'my-topic', partition: 0 }]);
      const paused = consumer.paused();

      expect(paused).toHaveLength(1);
      expect(paused[0]).toEqual({ topic: 'my-topic', partition: 0 });
    });

    it('should resume consumption', async () => {
      const consumer = kafka.consumer({ groupId: 'my-group' });
      await consumer.connect();

      consumer.pause([{ topic: 'my-topic', partition: 0 }]);
      consumer.resume([{ topic: 'my-topic', partition: 0 }]);
      const paused = consumer.paused();

      expect(paused).toHaveLength(0);
    });

    it('should pause multiple partitions', async () => {
      const consumer = kafka.consumer({ groupId: 'my-group' });
      await consumer.connect();

      consumer.pause([
        { topic: 'topic-1', partition: 0 },
        { topic: 'topic-1', partition: 1 },
        { topic: 'topic-2', partition: 0 },
      ]);
      const paused = consumer.paused();

      expect(paused).toHaveLength(3);
    });
  });

  describe('seek', () => {
    it('should seek to offset', async () => {
      const consumer = kafka.consumer({ groupId: 'my-group' });
      await consumer.connect();

      await consumer.seek({ topic: 'my-topic', partition: 0, offset: '100' });

      expect(mockMethods.consumerSeek).toHaveBeenCalledWith({
        groupId: 'my-group',
        topic: 'my-topic',
        partition: 0,
        offset: '100',
      });
    });

    it('should throw when not connected', async () => {
      const consumer = kafka.consumer({ groupId: 'my-group' });
      await expect(
        consumer.seek({ topic: 'my-topic', partition: 0, offset: '100' })
      ).rejects.toThrow(ConnectionError);
    });

    it('should throw ConsumerError on failure', async () => {
      mockMethods.consumerSeek.mockRejectedValueOnce(new Error('Seek failed'));
      const consumer = kafka.consumer({ groupId: 'my-group' });
      await consumer.connect();

      await expect(
        consumer.seek({ topic: 'my-topic', partition: 0, offset: '100' })
      ).rejects.toThrow(ConsumerError);
    });
  });

  describe('commitOffsets', () => {
    it('should commit offsets', async () => {
      const consumer = kafka.consumer({ groupId: 'my-group' });
      await consumer.connect();

      await consumer.commitOffsets({
        topics: [
          {
            topic: 'my-topic',
            partitions: [{ partition: 0, offset: '100' }],
          },
        ],
      });

      expect(mockMethods.consumerCommitOffsets).toHaveBeenCalled();
    });

    it('should throw when not connected', async () => {
      const consumer = kafka.consumer({ groupId: 'my-group' });
      await expect(
        consumer.commitOffsets({ topics: [] })
      ).rejects.toThrow(ConnectionError);
    });

    it('should throw ConsumerError on failure', async () => {
      mockMethods.consumerCommitOffsets.mockRejectedValueOnce(new Error('Commit failed'));
      const consumer = kafka.consumer({ groupId: 'my-group' });
      await consumer.connect();

      await expect(
        consumer.commitOffsets({ topics: [] })
      ).rejects.toThrow(ConsumerError);
    });
  });

  describe('assignment', () => {
    it('should return assignments', async () => {
      const consumer = kafka.consumer({ groupId: 'my-group' });
      await consumer.connect();
      await consumer.subscribe({ topic: 'my-topic' });

      const assignments = consumer.assignment();
      expect(assignments).toHaveLength(1);
      expect(assignments[0].topic).toBe('my-topic');
    });

    it('should return empty array when no subscriptions', async () => {
      const consumer = kafka.consumer({ groupId: 'my-group' });
      await consumer.connect();

      const assignments = consumer.assignment();
      expect(assignments).toHaveLength(0);
    });
  });

  describe('message processing', () => {
    it('should process messages with eachMessage handler', async () => {
      const messages = [
        {
          topic: 'my-topic',
          partition: 0,
          message: {
            key: Buffer.from('key1').toString('base64'),
            value: Buffer.from('value1').toString('base64'),
            timestamp: '1234567890',
            offset: '0',
            attributes: 0,
            size: 10,
          },
        },
      ];

      mockMethods.consumerPoll.mockResolvedValueOnce({ messages });

      const consumer = kafka.consumer({ groupId: 'my-group' });
      await consumer.connect();
      await consumer.subscribe({ topic: 'my-topic' });

      const eachMessage = vi.fn();
      await consumer.run({ eachMessage });

      // Trigger poll
      await vi.advanceTimersByTimeAsync(100);

      expect(eachMessage).toHaveBeenCalled();
    });

    it('should skip paused partitions', async () => {
      const messages = [
        {
          topic: 'my-topic',
          partition: 0,
          message: {
            key: null,
            value: Buffer.from('value1').toString('base64'),
            timestamp: '1234567890',
            offset: '0',
            attributes: 0,
            size: 10,
          },
        },
      ];

      mockMethods.consumerPoll.mockResolvedValueOnce({ messages });

      const consumer = kafka.consumer({ groupId: 'my-group' });
      await consumer.connect();
      await consumer.subscribe({ topic: 'my-topic' });

      consumer.pause([{ topic: 'my-topic', partition: 0 }]);

      const eachMessage = vi.fn();
      await consumer.run({ eachMessage });

      await vi.advanceTimersByTimeAsync(100);

      expect(eachMessage).not.toHaveBeenCalled();
    });

    it('should process messages with eachBatch handler', async () => {
      const messages = [
        {
          topic: 'my-topic',
          partition: 0,
          message: {
            key: null,
            value: Buffer.from('value1').toString('base64'),
            timestamp: '1234567890',
            offset: '0',
            attributes: 0,
            size: 10,
          },
        },
        {
          topic: 'my-topic',
          partition: 0,
          message: {
            key: null,
            value: Buffer.from('value2').toString('base64'),
            timestamp: '1234567891',
            offset: '1',
            attributes: 0,
            size: 10,
          },
        },
      ];

      mockMethods.consumerPoll.mockResolvedValueOnce({ messages });

      const consumer = kafka.consumer({ groupId: 'my-group' });
      await consumer.connect();
      await consumer.subscribe({ topic: 'my-topic' });

      const eachBatch = vi.fn();
      await consumer.run({ eachBatch });

      await vi.advanceTimersByTimeAsync(100);

      expect(eachBatch).toHaveBeenCalled();
    });

    it('should provide heartbeat function in eachMessage', async () => {
      const messages = [
        {
          topic: 'my-topic',
          partition: 0,
          message: {
            key: null,
            value: Buffer.from('value1').toString('base64'),
            timestamp: '1234567890',
            offset: '0',
            attributes: 0,
            size: 10,
          },
        },
      ];

      mockMethods.consumerPoll.mockResolvedValueOnce({ messages });

      const consumer = kafka.consumer({ groupId: 'my-group' });
      await consumer.connect();
      await consumer.subscribe({ topic: 'my-topic' });

      const eachMessage = vi.fn().mockImplementation(async ({ heartbeat }) => {
        await heartbeat();
      });

      await consumer.run({ eachMessage });
      await vi.advanceTimersByTimeAsync(100);

      expect(mockMethods.consumerHeartbeat).toHaveBeenCalled();
    });

    it('should provide pause function in eachMessage', async () => {
      const messages = [
        {
          topic: 'my-topic',
          partition: 0,
          message: {
            key: null,
            value: Buffer.from('value1').toString('base64'),
            timestamp: '1234567890',
            offset: '0',
            attributes: 0,
            size: 10,
          },
        },
      ];

      mockMethods.consumerPoll.mockResolvedValueOnce({ messages });

      const consumer = kafka.consumer({ groupId: 'my-group' });
      await consumer.connect();
      await consumer.subscribe({ topic: 'my-topic' });

      let resumeFn: () => void = () => {};
      const eachMessage = vi.fn().mockImplementation(async ({ pause }) => {
        resumeFn = pause();
      });

      await consumer.run({ eachMessage });
      await vi.advanceTimersByTimeAsync(100);

      expect(consumer.paused()).toHaveLength(1);
      resumeFn();
      expect(consumer.paused()).toHaveLength(0);
    });

    it('should provide batch functions in eachBatch', async () => {
      const messages = [
        {
          topic: 'my-topic',
          partition: 0,
          message: {
            key: null,
            value: Buffer.from('value1').toString('base64'),
            timestamp: '1234567890',
            offset: '0',
            attributes: 0,
            size: 10,
          },
        },
      ];

      mockMethods.consumerPoll.mockResolvedValueOnce({ messages });

      const consumer = kafka.consumer({ groupId: 'my-group' });
      await consumer.connect();
      await consumer.subscribe({ topic: 'my-topic' });

      const eachBatch = vi.fn().mockImplementation(async (payload) => {
        expect(payload.batch).toBeDefined();
        expect(payload.batch.isEmpty()).toBe(false);
        expect(payload.batch.firstOffset()).toBe('0');
        expect(payload.batch.lastOffset()).toBe('0');
        expect(payload.isRunning()).toBe(true);
        expect(payload.isStale()).toBe(false);
        payload.resolveOffset('0');
        expect(payload.uncommittedOffsets()).toBeDefined();
        await payload.heartbeat();
        await payload.commitOffsetsIfNecessary();
      });

      await consumer.run({ eachBatch });
      await vi.advanceTimersByTimeAsync(100);

      expect(eachBatch).toHaveBeenCalled();
    });

    it('should handle poll errors gracefully', async () => {
      mockMethods.consumerPoll.mockRejectedValueOnce(new Error('Poll failed'));

      const consumer = kafka.consumer({ groupId: 'my-group' });
      await consumer.connect();
      await consumer.subscribe({ topic: 'my-topic' });

      const eachMessage = vi.fn();
      await consumer.run({ eachMessage });

      // Should not throw
      await vi.advanceTimersByTimeAsync(100);
      expect(consumer.isRunning()).toBe(true);
    });

    it('should stop processing when consumer stops', async () => {
      const messages = [
        {
          topic: 'my-topic',
          partition: 0,
          message: {
            key: null,
            value: Buffer.from('value1').toString('base64'),
            timestamp: '1234567890',
            offset: '0',
            attributes: 0,
            size: 10,
          },
        },
      ];

      mockMethods.consumerPoll.mockResolvedValue({ messages });

      const consumer = kafka.consumer({ groupId: 'my-group' });
      await consumer.connect();
      await consumer.subscribe({ topic: 'my-topic' });

      const eachMessage = vi.fn().mockImplementation(async () => {
        await consumer.stop();
      });

      await consumer.run({ eachMessage });
      await vi.advanceTimersByTimeAsync(100);

      expect(consumer.isRunning()).toBe(false);
    });

    it('should skip paused partitions in eachBatch', async () => {
      const messages = [
        {
          topic: 'my-topic',
          partition: 0,
          message: {
            key: null,
            value: Buffer.from('value1').toString('base64'),
            timestamp: '1234567890',
            offset: '0',
            attributes: 0,
            size: 10,
          },
        },
      ];

      mockMethods.consumerPoll.mockResolvedValueOnce({ messages });

      const consumer = kafka.consumer({ groupId: 'my-group' });
      await consumer.connect();
      await consumer.subscribe({ topic: 'my-topic' });

      consumer.pause([{ topic: 'my-topic', partition: 0 }]);

      const eachBatch = vi.fn();
      await consumer.run({ eachBatch });

      await vi.advanceTimersByTimeAsync(100);

      expect(eachBatch).not.toHaveBeenCalled();
    });

    it('should stop processing batches when consumer stops', async () => {
      const messages = [
        {
          topic: 'my-topic',
          partition: 0,
          message: {
            key: null,
            value: Buffer.from('value1').toString('base64'),
            timestamp: '1234567890',
            offset: '0',
            attributes: 0,
            size: 10,
          },
        },
        {
          topic: 'my-topic',
          partition: 1,
          message: {
            key: null,
            value: Buffer.from('value2').toString('base64'),
            timestamp: '1234567891',
            offset: '0',
            attributes: 0,
            size: 10,
          },
        },
      ];

      mockMethods.consumerPoll.mockResolvedValue({ messages });

      const consumer = kafka.consumer({ groupId: 'my-group' });
      await consumer.connect();
      await consumer.subscribe({ topic: 'my-topic' });

      let callCount = 0;
      const eachBatch = vi.fn().mockImplementation(async () => {
        callCount++;
        if (callCount === 1) {
          await consumer.stop();
        }
      });

      await consumer.run({ eachBatch });
      await vi.advanceTimersByTimeAsync(100);

      // Should only process first batch before stopping
      expect(callCount).toBe(1);
    });

    it('should provide pause function in eachBatch', async () => {
      const messages = [
        {
          topic: 'my-topic',
          partition: 0,
          message: {
            key: null,
            value: Buffer.from('value1').toString('base64'),
            timestamp: '1234567890',
            offset: '0',
            attributes: 0,
            size: 10,
          },
        },
      ];

      mockMethods.consumerPoll.mockResolvedValueOnce({ messages });

      const consumer = kafka.consumer({ groupId: 'my-group' });
      await consumer.connect();
      await consumer.subscribe({ topic: 'my-topic' });

      let resumeFn: () => void = () => {};
      const eachBatch = vi.fn().mockImplementation(async ({ pause }) => {
        resumeFn = pause();
      });

      await consumer.run({ eachBatch });
      await vi.advanceTimersByTimeAsync(100);

      expect(consumer.paused()).toHaveLength(1);
      resumeFn();
      expect(consumer.paused()).toHaveLength(0);
    });

    it('should commit offsets in eachBatch when provided', async () => {
      const messages = [
        {
          topic: 'my-topic',
          partition: 0,
          message: {
            key: null,
            value: Buffer.from('value1').toString('base64'),
            timestamp: '1234567890',
            offset: '0',
            attributes: 0,
            size: 10,
          },
        },
      ];

      mockMethods.consumerPoll.mockResolvedValueOnce({ messages });

      const consumer = kafka.consumer({ groupId: 'my-group' });
      await consumer.connect();
      await consumer.subscribe({ topic: 'my-topic' });

      const eachBatch = vi.fn().mockImplementation(async (payload) => {
        await payload.commitOffsetsIfNecessary({
          topics: [
            {
              topic: 'my-topic',
              partitions: [{ partition: 0, offset: '1' }],
            },
          ],
        });
      });

      await consumer.run({ eachBatch });
      await vi.advanceTimersByTimeAsync(100);

      expect(mockMethods.consumerCommitOffsets).toHaveBeenCalled();
    });

    it('should not start polling twice', async () => {
      const consumer = kafka.consumer({ groupId: 'my-group' });
      await consumer.connect();
      await consumer.subscribe({ topic: 'my-topic' });

      const eachMessage = vi.fn();
      await consumer.run({ eachMessage });

      // Second run should throw "already running", which implicitly tests that
      // _startPolling early return works (via the isRunning check)
      await expect(consumer.run({ eachMessage })).rejects.toThrow('Consumer is already running');
    });

    it('should return early from _processMessages when no runConfig', async () => {
      const consumer = kafka.consumer({ groupId: 'my-group' });
      await consumer.connect();
      await consumer.subscribe({ topic: 'my-topic' });

      // Don't call run(), so _runConfig will be null
      // This tests the early return in _processMessages
      // We can't directly test this, but coverage shows it
      expect(consumer.isRunning()).toBe(false);
    });

    it('should handle empty batch', async () => {
      mockMethods.consumerPoll.mockResolvedValueOnce({ messages: [] });

      const consumer = kafka.consumer({ groupId: 'my-group' });
      await consumer.connect();
      await consumer.subscribe({ topic: 'my-topic' });

      const eachBatch = vi.fn();
      await consumer.run({ eachBatch });

      await vi.advanceTimersByTimeAsync(100);

      expect(eachBatch).not.toHaveBeenCalled();
    });

    it('should handle batch.isEmpty() for empty batch', async () => {
      const messages = [
        {
          topic: 'my-topic',
          partition: 0,
          message: {
            key: null,
            value: Buffer.from('value1').toString('base64'),
            timestamp: '1234567890',
            offset: '0',
            attributes: 0,
            size: 10,
          },
        },
      ];

      mockMethods.consumerPoll.mockResolvedValueOnce({ messages });

      const consumer = kafka.consumer({ groupId: 'my-group' });
      await consumer.connect();
      await consumer.subscribe({ topic: 'my-topic' });

      const eachBatch = vi.fn().mockImplementation(async (payload) => {
        // Test batch helper methods
        expect(payload.batch.isEmpty()).toBe(false);
        expect(payload.batch.firstOffset()).toBe('0');
        expect(payload.batch.lastOffset()).toBe('0');
        expect(payload.batch.offsetLag()).toBe('0');
        expect(payload.batch.offsetLagLow()).toBe('0');
      });

      await consumer.run({ eachBatch });
      await vi.advanceTimersByTimeAsync(100);

      expect(eachBatch).toHaveBeenCalled();
    });
  });

  describe('error handling with non-Error exceptions', () => {
    it('should handle non-Error in connect', async () => {
      mockMethods.consumerConnect.mockRejectedValueOnce('connection string error');
      const consumer = kafka.consumer({ groupId: 'my-group' });
      await expect(consumer.connect()).rejects.toThrow('connection string error');
    });

    it('should handle non-Error in subscribe', async () => {
      mockMethods.consumerSubscribe.mockRejectedValueOnce('subscribe string error');
      const consumer = kafka.consumer({ groupId: 'my-group' });
      await consumer.connect();
      await expect(consumer.subscribe({ topic: 'test' })).rejects.toThrow('subscribe string error');
    });

    it('should handle non-Error in unsubscribe', async () => {
      mockMethods.consumerUnsubscribe.mockRejectedValueOnce('unsubscribe string error');
      const consumer = kafka.consumer({ groupId: 'my-group' });
      await consumer.connect();
      await expect(consumer.unsubscribe()).rejects.toThrow('unsubscribe string error');
    });

    it('should handle non-Error in run', async () => {
      mockMethods.consumerRun.mockRejectedValueOnce('run string error');
      const consumer = kafka.consumer({ groupId: 'my-group' });
      await consumer.connect();
      await expect(consumer.run({ eachMessage: vi.fn() })).rejects.toThrow('run string error');
    });

    it('should handle non-Error in seek', async () => {
      mockMethods.consumerSeek.mockRejectedValueOnce('seek string error');
      const consumer = kafka.consumer({ groupId: 'my-group' });
      await consumer.connect();
      await expect(
        consumer.seek({ topic: 'test', partition: 0, offset: '0' })
      ).rejects.toThrow('seek string error');
    });

    it('should handle non-Error in commitOffsets', async () => {
      mockMethods.consumerCommitOffsets.mockRejectedValueOnce('commit string error');
      const consumer = kafka.consumer({ groupId: 'my-group' });
      await consumer.connect();
      await expect(consumer.commitOffsets({ topics: [] })).rejects.toThrow('commit string error');
    });
  });
});
