/**
 * Tests for the main Kafka class
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { Kafka, setRpcClientFactory, getRpcClientFactory, createRpcClient } from '../src/kafka.js';
import type { RpcClient } from '../src/kafka.js';

// Mock RPC client factory
function createMockRpcClient(): RpcClient {
  return {
    $: new Proxy({} as Record<string, (...args: unknown[]) => Promise<unknown>>, {
      get: () => vi.fn().mockResolvedValue({}),
    }),
    close: vi.fn().mockResolvedValue(undefined),
  };
}

describe('Kafka', () => {
  let mockFactory: ReturnType<typeof vi.fn>;

  beforeEach(() => {
    mockFactory = vi.fn().mockImplementation(() => createMockRpcClient());
    setRpcClientFactory(mockFactory);
  });

  afterEach(() => {
    setRpcClientFactory(null);
  });

  describe('constructor', () => {
    it('should create a Kafka instance with valid config', () => {
      const kafka = new Kafka({ brokers: ['broker1:9092'] });
      expect(kafka).toBeInstanceOf(Kafka);
      expect(kafka.config.brokers).toEqual(['broker1:9092']);
    });

    it('should set default clientId', () => {
      const kafka = new Kafka({ brokers: ['broker1:9092'] });
      expect(kafka.config.clientId).toBe('kafka.do-client');
    });

    it('should use provided clientId', () => {
      const kafka = new Kafka({ brokers: ['broker1:9092'], clientId: 'my-app' });
      expect(kafka.config.clientId).toBe('my-app');
    });

    it('should set default connection timeout', () => {
      const kafka = new Kafka({ brokers: ['broker1:9092'] });
      expect(kafka.config.connectionTimeout).toBe(30000);
    });

    it('should set default request timeout', () => {
      const kafka = new Kafka({ brokers: ['broker1:9092'] });
      expect(kafka.config.requestTimeout).toBe(30000);
    });

    it('should throw error when no brokers provided', () => {
      expect(() => new Kafka({ brokers: [] })).toThrow('At least one broker is required');
    });

    it('should throw error when brokers is undefined', () => {
      expect(() => new Kafka({ brokers: undefined as unknown as string[] })).toThrow(
        'At least one broker is required'
      );
    });

    it('should accept multiple brokers', () => {
      const kafka = new Kafka({ brokers: ['broker1:9092', 'broker2:9092', 'broker3:9092'] });
      expect(kafka.config.brokers).toHaveLength(3);
    });

    it('should accept auth configuration', () => {
      const kafka = new Kafka({
        brokers: ['broker1:9092'],
        auth: {
          mechanism: 'plain',
          username: 'user',
          password: 'pass',
        },
      });
      expect(kafka.config.auth?.mechanism).toBe('plain');
      expect(kafka.config.auth?.username).toBe('user');
    });

    it('should accept ssl configuration as boolean', () => {
      const kafka = new Kafka({
        brokers: ['broker1:9092'],
        ssl: true,
      });
      expect(kafka.config.ssl).toBe(true);
    });

    it('should accept ssl configuration as object', () => {
      const kafka = new Kafka({
        brokers: ['broker1:9092'],
        ssl: {
          rejectUnauthorized: false,
          ca: 'ca-cert',
        },
      });
      expect(kafka.config.ssl).toEqual({
        rejectUnauthorized: false,
        ca: 'ca-cert',
      });
    });

    it('should accept retry configuration', () => {
      const kafka = new Kafka({
        brokers: ['broker1:9092'],
        retry: {
          maxRetryTime: 60000,
          initialRetryTime: 500,
          factor: 2,
          retries: 10,
        },
      });
      expect(kafka.config.retry?.maxRetryTime).toBe(60000);
      expect(kafka.config.retry?.retries).toBe(10);
    });
  });

  describe('getRpcClient', () => {
    it('should create and return RPC client', () => {
      const kafka = new Kafka({ brokers: ['broker1:9092'] });
      const client = kafka.getRpcClient();
      expect(client).toBeDefined();
      expect(mockFactory).toHaveBeenCalled();
    });

    it('should reuse existing RPC client', () => {
      const kafka = new Kafka({ brokers: ['broker1:9092'] });
      const client1 = kafka.getRpcClient();
      const client2 = kafka.getRpcClient();
      expect(client1).toBe(client2);
      expect(mockFactory).toHaveBeenCalledTimes(1);
    });

    it('should build correct URL from broker', () => {
      const kafka = new Kafka({ brokers: ['mybroker:9092'] });
      kafka.getRpcClient();
      expect(mockFactory).toHaveBeenCalledWith('wss://mybroker.kafka.do');
    });

    it('should handle wss:// URL prefix', () => {
      const kafka = new Kafka({ brokers: ['wss://mybroker.kafka.do'] });
      kafka.getRpcClient();
      expect(mockFactory).toHaveBeenCalledWith('wss://mybroker.kafka.do');
    });

    it('should handle ws:// URL prefix', () => {
      const kafka = new Kafka({ brokers: ['ws://mybroker.kafka.do'] });
      kafka.getRpcClient();
      expect(mockFactory).toHaveBeenCalledWith('ws://mybroker.kafka.do');
    });
  });

  describe('setRpcClient', () => {
    it('should allow setting custom RPC client', () => {
      const kafka = new Kafka({ brokers: ['broker1:9092'] });
      const customClient = createMockRpcClient();
      kafka.setRpcClient(customClient);
      const client = kafka.getRpcClient();
      expect(client).toBe(customClient);
      expect(mockFactory).not.toHaveBeenCalled();
    });
  });

  describe('producer', () => {
    it('should create a producer instance', () => {
      const kafka = new Kafka({ brokers: ['broker1:9092'] });
      const producer = kafka.producer();
      expect(producer).toBeDefined();
    });

    it('should create producer with config', () => {
      const kafka = new Kafka({ brokers: ['broker1:9092'] });
      const producer = kafka.producer({ idempotent: true });
      expect(producer).toBeDefined();
    });
  });

  describe('consumer', () => {
    it('should create a consumer instance with groupId', () => {
      const kafka = new Kafka({ brokers: ['broker1:9092'] });
      const consumer = kafka.consumer({ groupId: 'my-group' });
      expect(consumer).toBeDefined();
      expect(consumer.groupId).toBe('my-group');
    });

    it('should throw error when groupId is missing', () => {
      const kafka = new Kafka({ brokers: ['broker1:9092'] });
      expect(() => kafka.consumer({ groupId: '' })).toThrow('Consumer groupId is required');
    });

    it('should throw error when groupId is not provided', () => {
      const kafka = new Kafka({ brokers: ['broker1:9092'] });
      expect(() => kafka.consumer({} as { groupId: string })).toThrow('Consumer groupId is required');
    });
  });

  describe('admin', () => {
    it('should create an admin instance', () => {
      const kafka = new Kafka({ brokers: ['broker1:9092'] });
      const admin = kafka.admin();
      expect(admin).toBeDefined();
    });

    it('should create admin with config', () => {
      const kafka = new Kafka({ brokers: ['broker1:9092'] });
      const admin = kafka.admin({
        retry: { retries: 3 },
      });
      expect(admin).toBeDefined();
    });
  });

  describe('close', () => {
    it('should close the RPC client', async () => {
      const kafka = new Kafka({ brokers: ['broker1:9092'] });
      const client = kafka.getRpcClient();
      await kafka.close();
      expect(client.close).toHaveBeenCalled();
    });

    it('should handle close when no client exists', async () => {
      const kafka = new Kafka({ brokers: ['broker1:9092'] });
      await expect(kafka.close()).resolves.toBeUndefined();
    });

    it('should clear client reference after close', async () => {
      const kafka = new Kafka({ brokers: ['broker1:9092'] });
      kafka.getRpcClient();
      await kafka.close();
      // Getting client again should create a new one
      kafka.getRpcClient();
      expect(mockFactory).toHaveBeenCalledTimes(2);
    });
  });
});

describe('setRpcClientFactory / getRpcClientFactory', () => {
  afterEach(() => {
    setRpcClientFactory(null);
  });

  it('should set and get factory', () => {
    const factory = vi.fn();
    setRpcClientFactory(factory);
    expect(getRpcClientFactory()).toBe(factory);
  });

  it('should clear factory with null', () => {
    const factory = vi.fn();
    setRpcClientFactory(factory);
    setRpcClientFactory(null);
    expect(getRpcClientFactory()).toBeNull();
  });
});

describe('createRpcClient', () => {
  afterEach(() => {
    setRpcClientFactory(null);
  });

  it('should throw error when no factory is set', () => {
    expect(() => createRpcClient({ brokers: ['broker:9092'] })).toThrow(
      'RPC client factory not configured'
    );
  });

  it('should use factory when set', () => {
    const mockClient = createMockRpcClient();
    const factory = vi.fn().mockReturnValue(mockClient);
    setRpcClientFactory(factory);

    const client = createRpcClient({ brokers: ['broker:9092'] });
    expect(client).toBe(mockClient);
    expect(factory).toHaveBeenCalledWith('wss://broker.kafka.do');
  });
});
