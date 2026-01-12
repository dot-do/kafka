/**
 * Tests for the Admin class
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { Kafka, setRpcClientFactory } from '../src/kafka.js';
import type { RpcClient } from '../src/kafka.js';
import { Admin } from '../src/admin.js';
import { ConnectionError, TopicError, KafkaError, ResourceTypes } from '../src/types.js';

// Create mock RPC methods
function createMockRpcMethods() {
  return {
    adminConnect: vi.fn().mockResolvedValue({}),
    adminDisconnect: vi.fn().mockResolvedValue({}),
    adminListTopics: vi.fn().mockResolvedValue(['topic-1', 'topic-2']),
    adminCreateTopics: vi.fn().mockResolvedValue(true),
    adminDeleteTopics: vi.fn().mockResolvedValue({}),
    adminFetchTopicMetadata: vi.fn().mockResolvedValue({
      topics: [
        {
          name: 'test-topic',
          partitions: [
            { partitionId: 0, leader: 1, replicas: [1, 2], isr: [1, 2], partitionErrorCode: 0 },
          ],
        },
      ],
    }),
    adminFetchTopicOffsets: vi.fn().mockResolvedValue([
      { partition: 0, offset: '100', high: '100', low: '0' },
    ]),
    adminFetchTopicOffsetsByTimestamp: vi.fn().mockResolvedValue([
      { partition: 0, offset: '50', high: '100', low: '0' },
    ]),
    adminCreatePartitions: vi.fn().mockResolvedValue(true),
    adminListGroups: vi.fn().mockResolvedValue({
      groups: [{ groupId: 'group-1', protocolType: 'consumer' }],
    }),
    adminDescribeGroups: vi.fn().mockResolvedValue({
      groups: [
        {
          errorCode: 0,
          groupId: 'group-1',
          protocolType: 'consumer',
          protocol: 'range',
          state: 'Stable',
          members: [],
        },
      ],
    }),
    adminDeleteGroups: vi.fn().mockResolvedValue({}),
    adminFetchOffsets: vi.fn().mockResolvedValue([
      { topic: 'test-topic', partitions: [{ partition: 0, offset: '100' }] },
    ]),
    adminResetOffsets: vi.fn().mockResolvedValue({}),
    adminSetOffsets: vi.fn().mockResolvedValue({}),
    adminDescribeCluster: vi.fn().mockResolvedValue({
      brokers: [{ nodeId: 1, host: 'broker1', port: 9092 }],
      controller: 1,
      clusterId: 'cluster-1',
    }),
    adminDescribeConfigs: vi.fn().mockResolvedValue({
      resources: [
        {
          resourceType: ResourceTypes.TOPIC,
          resourceName: 'test-topic',
          errorCode: 0,
          errorMessage: '',
          configEntries: [
            {
              configName: 'retention.ms',
              configValue: '86400000',
              isDefault: false,
              configSource: 5,
              isSensitive: false,
              readOnly: false,
            },
          ],
        },
      ],
    }),
    adminAlterConfigs: vi.fn().mockResolvedValue({}),
    adminAlterPartitionReassignments: vi.fn().mockResolvedValue({}),
    adminListPartitionReassignments: vi.fn().mockResolvedValue({ topics: [] }),
    adminDeleteRecords: vi.fn().mockResolvedValue({}),
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

describe('Admin', () => {
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
    it('should create an admin with default config', () => {
      const admin = new Admin(kafka);
      expect(admin).toBeInstanceOf(Admin);
      expect(admin.connectionState).toBe('disconnected');
    });

    it('should create an admin with custom config', () => {
      const admin = new Admin(kafka, {
        retry: { retries: 10, maxRetryTime: 60000 },
      });
      expect(admin).toBeInstanceOf(Admin);
    });
  });

  describe('isConnected', () => {
    it('should return false when not connected', () => {
      const admin = kafka.admin();
      expect(admin.isConnected()).toBe(false);
    });

    it('should return true when connected', async () => {
      const admin = kafka.admin();
      await admin.connect();
      expect(admin.isConnected()).toBe(true);
    });
  });

  describe('connect', () => {
    it('should connect successfully', async () => {
      const admin = kafka.admin();
      await admin.connect();
      expect(admin.connectionState).toBe('connected');
      expect(mockMethods.adminConnect).toHaveBeenCalled();
    });

    it('should not reconnect if already connected', async () => {
      const admin = kafka.admin();
      await admin.connect();
      await admin.connect();
      expect(mockMethods.adminConnect).toHaveBeenCalledTimes(1);
    });

    it('should throw ConnectionError on failure', async () => {
      mockMethods.adminConnect.mockRejectedValueOnce(new Error('Connection refused'));
      const admin = kafka.admin();
      await expect(admin.connect()).rejects.toThrow(ConnectionError);
      expect(admin.connectionState).toBe('disconnected');
    });
  });

  describe('disconnect', () => {
    it('should disconnect successfully', async () => {
      const admin = kafka.admin();
      await admin.connect();
      await admin.disconnect();
      expect(admin.connectionState).toBe('disconnected');
      expect(mockMethods.adminDisconnect).toHaveBeenCalled();
    });

    it('should handle disconnect when not connected', async () => {
      const admin = kafka.admin();
      await admin.disconnect();
      expect(mockMethods.adminDisconnect).not.toHaveBeenCalled();
    });
  });

  describe('listTopics', () => {
    it('should list topics', async () => {
      const admin = kafka.admin();
      await admin.connect();
      const topics = await admin.listTopics();
      expect(topics).toEqual(['topic-1', 'topic-2']);
    });

    it('should throw when not connected', async () => {
      const admin = kafka.admin();
      await expect(admin.listTopics()).rejects.toThrow(ConnectionError);
    });

    it('should throw KafkaError on failure', async () => {
      mockMethods.adminListTopics.mockRejectedValueOnce(new Error('Failed'));
      const admin = kafka.admin();
      await admin.connect();
      await expect(admin.listTopics()).rejects.toThrow(KafkaError);
    });
  });

  describe('createTopics', () => {
    it('should create topics', async () => {
      const admin = kafka.admin();
      await admin.connect();
      const result = await admin.createTopics({
        topics: [{ topic: 'new-topic', numPartitions: 3 }],
      });
      expect(result).toBe(true);
      expect(mockMethods.adminCreateTopics).toHaveBeenCalled();
    });

    it('should pass all options', async () => {
      const admin = kafka.admin();
      await admin.connect();

      await admin.createTopics({
        topics: [
          {
            topic: 'new-topic',
            numPartitions: 3,
            replicationFactor: 2,
            configEntries: [{ name: 'retention.ms', value: '86400000' }],
          },
        ],
        waitForLeaders: false,
        timeout: 60000,
        validateOnly: true,
      });

      expect(mockMethods.adminCreateTopics).toHaveBeenCalledWith({
        topics: [
          {
            topic: 'new-topic',
            numPartitions: 3,
            replicationFactor: 2,
            replicaAssignment: undefined,
            configEntries: [{ name: 'retention.ms', value: '86400000' }],
          },
        ],
        waitForLeaders: false,
        timeout: 60000,
        validateOnly: true,
      });
    });

    it('should throw when not connected', async () => {
      const admin = kafka.admin();
      await expect(
        admin.createTopics({ topics: [{ topic: 'new-topic' }] })
      ).rejects.toThrow(ConnectionError);
    });

    it('should throw TopicError on failure', async () => {
      mockMethods.adminCreateTopics.mockRejectedValueOnce(new Error('Failed'));
      const admin = kafka.admin();
      await admin.connect();
      await expect(
        admin.createTopics({ topics: [{ topic: 'new-topic' }] })
      ).rejects.toThrow(TopicError);
    });
  });

  describe('deleteTopics', () => {
    it('should delete topics', async () => {
      const admin = kafka.admin();
      await admin.connect();
      await admin.deleteTopics({ topics: ['topic-1'] });
      expect(mockMethods.adminDeleteTopics).toHaveBeenCalledWith({
        topics: ['topic-1'],
        timeout: 30000,
      });
    });

    it('should pass timeout option', async () => {
      const admin = kafka.admin();
      await admin.connect();
      await admin.deleteTopics({ topics: ['topic-1'], timeout: 60000 });
      expect(mockMethods.adminDeleteTopics).toHaveBeenCalledWith({
        topics: ['topic-1'],
        timeout: 60000,
      });
    });

    it('should throw when not connected', async () => {
      const admin = kafka.admin();
      await expect(admin.deleteTopics({ topics: ['topic-1'] })).rejects.toThrow(ConnectionError);
    });

    it('should throw TopicError on failure', async () => {
      mockMethods.adminDeleteTopics.mockRejectedValueOnce(new Error('Failed'));
      const admin = kafka.admin();
      await admin.connect();
      await expect(admin.deleteTopics({ topics: ['topic-1'] })).rejects.toThrow(TopicError);
    });
  });

  describe('fetchTopicMetadata', () => {
    it('should fetch metadata for all topics', async () => {
      const admin = kafka.admin();
      await admin.connect();
      const result = await admin.fetchTopicMetadata();
      expect(result.topics).toHaveLength(1);
    });

    it('should fetch metadata for specific topics', async () => {
      const admin = kafka.admin();
      await admin.connect();
      await admin.fetchTopicMetadata({ topics: ['test-topic'] });
      expect(mockMethods.adminFetchTopicMetadata).toHaveBeenCalledWith({
        topics: ['test-topic'],
      });
    });

    it('should throw when not connected', async () => {
      const admin = kafka.admin();
      await expect(admin.fetchTopicMetadata()).rejects.toThrow(ConnectionError);
    });

    it('should throw KafkaError on failure', async () => {
      mockMethods.adminFetchTopicMetadata.mockRejectedValueOnce(new Error('Failed'));
      const admin = kafka.admin();
      await admin.connect();
      await expect(admin.fetchTopicMetadata()).rejects.toThrow(KafkaError);
    });
  });

  describe('fetchTopicOffsets', () => {
    it('should fetch topic offsets', async () => {
      const admin = kafka.admin();
      await admin.connect();
      const result = await admin.fetchTopicOffsets('test-topic');
      expect(result).toHaveLength(1);
      expect(result[0].offset).toBe('100');
    });

    it('should throw when not connected', async () => {
      const admin = kafka.admin();
      await expect(admin.fetchTopicOffsets('test-topic')).rejects.toThrow(ConnectionError);
    });

    it('should throw TopicError on failure', async () => {
      mockMethods.adminFetchTopicOffsets.mockRejectedValueOnce(new Error('Failed'));
      const admin = kafka.admin();
      await admin.connect();
      await expect(admin.fetchTopicOffsets('test-topic')).rejects.toThrow(TopicError);
    });
  });

  describe('fetchTopicOffsetsByTimestamp', () => {
    it('should fetch offsets by timestamp', async () => {
      const admin = kafka.admin();
      await admin.connect();
      const result = await admin.fetchTopicOffsetsByTimestamp('test-topic', '1234567890');
      expect(result).toHaveLength(1);
      expect(result[0].offset).toBe('50');
    });

    it('should throw when not connected', async () => {
      const admin = kafka.admin();
      await expect(
        admin.fetchTopicOffsetsByTimestamp('test-topic', '1234567890')
      ).rejects.toThrow(ConnectionError);
    });

    it('should throw TopicError on failure', async () => {
      mockMethods.adminFetchTopicOffsetsByTimestamp.mockRejectedValueOnce(new Error('Failed'));
      const admin = kafka.admin();
      await admin.connect();
      await expect(
        admin.fetchTopicOffsetsByTimestamp('test-topic', '1234567890')
      ).rejects.toThrow(TopicError);
    });
  });

  describe('createPartitions', () => {
    it('should create partitions', async () => {
      const admin = kafka.admin();
      await admin.connect();
      const result = await admin.createPartitions({
        topicPartitions: [{ topic: 'test-topic', count: 6 }],
      });
      expect(result).toBe(true);
    });

    it('should pass all options', async () => {
      const admin = kafka.admin();
      await admin.connect();

      await admin.createPartitions({
        topicPartitions: [{ topic: 'test-topic', count: 6, assignments: [[1, 2]] }],
        timeout: 60000,
        validateOnly: true,
      });

      expect(mockMethods.adminCreatePartitions).toHaveBeenCalledWith({
        topicPartitions: [{ topic: 'test-topic', count: 6, assignments: [[1, 2]] }],
        timeout: 60000,
        validateOnly: true,
      });
    });

    it('should throw when not connected', async () => {
      const admin = kafka.admin();
      await expect(
        admin.createPartitions({ topicPartitions: [] })
      ).rejects.toThrow(ConnectionError);
    });

    it('should throw KafkaError on failure', async () => {
      mockMethods.adminCreatePartitions.mockRejectedValueOnce(new Error('Failed'));
      const admin = kafka.admin();
      await admin.connect();
      await expect(
        admin.createPartitions({ topicPartitions: [] })
      ).rejects.toThrow(KafkaError);
    });
  });

  describe('listGroups', () => {
    it('should list consumer groups', async () => {
      const admin = kafka.admin();
      await admin.connect();
      const result = await admin.listGroups();
      expect(result.groups).toHaveLength(1);
    });

    it('should throw when not connected', async () => {
      const admin = kafka.admin();
      await expect(admin.listGroups()).rejects.toThrow(ConnectionError);
    });

    it('should throw KafkaError on failure', async () => {
      mockMethods.adminListGroups.mockRejectedValueOnce(new Error('Failed'));
      const admin = kafka.admin();
      await admin.connect();
      await expect(admin.listGroups()).rejects.toThrow(KafkaError);
    });
  });

  describe('describeGroups', () => {
    it('should describe consumer groups', async () => {
      const admin = kafka.admin();
      await admin.connect();
      const result = await admin.describeGroups(['group-1']);
      expect(result.groups).toHaveLength(1);
      expect(result.groups[0].groupId).toBe('group-1');
    });

    it('should throw when not connected', async () => {
      const admin = kafka.admin();
      await expect(admin.describeGroups(['group-1'])).rejects.toThrow(ConnectionError);
    });

    it('should throw KafkaError on failure', async () => {
      mockMethods.adminDescribeGroups.mockRejectedValueOnce(new Error('Failed'));
      const admin = kafka.admin();
      await admin.connect();
      await expect(admin.describeGroups(['group-1'])).rejects.toThrow(KafkaError);
    });
  });

  describe('deleteGroups', () => {
    it('should delete consumer groups', async () => {
      const admin = kafka.admin();
      await admin.connect();
      await admin.deleteGroups({ groupIds: ['group-1'] });
      expect(mockMethods.adminDeleteGroups).toHaveBeenCalledWith({
        groupIds: ['group-1'],
      });
    });

    it('should throw when not connected', async () => {
      const admin = kafka.admin();
      await expect(admin.deleteGroups({ groupIds: ['group-1'] })).rejects.toThrow(ConnectionError);
    });

    it('should throw KafkaError on failure', async () => {
      mockMethods.adminDeleteGroups.mockRejectedValueOnce(new Error('Failed'));
      const admin = kafka.admin();
      await admin.connect();
      await expect(admin.deleteGroups({ groupIds: ['group-1'] })).rejects.toThrow(KafkaError);
    });
  });

  describe('fetchOffsets', () => {
    it('should fetch consumer group offsets', async () => {
      const admin = kafka.admin();
      await admin.connect();
      const result = await admin.fetchOffsets({ groupId: 'group-1' });
      expect(result).toHaveLength(1);
    });

    it('should pass options', async () => {
      const admin = kafka.admin();
      await admin.connect();
      await admin.fetchOffsets({
        groupId: 'group-1',
        topics: ['test-topic'],
        resolveOffsets: true,
      });
      expect(mockMethods.adminFetchOffsets).toHaveBeenCalledWith({
        groupId: 'group-1',
        topics: ['test-topic'],
        resolveOffsets: true,
      });
    });

    it('should throw when not connected', async () => {
      const admin = kafka.admin();
      await expect(admin.fetchOffsets({ groupId: 'group-1' })).rejects.toThrow(ConnectionError);
    });

    it('should throw KafkaError on failure', async () => {
      mockMethods.adminFetchOffsets.mockRejectedValueOnce(new Error('Failed'));
      const admin = kafka.admin();
      await admin.connect();
      await expect(admin.fetchOffsets({ groupId: 'group-1' })).rejects.toThrow(KafkaError);
    });
  });

  describe('resetOffsets', () => {
    it('should reset offsets', async () => {
      const admin = kafka.admin();
      await admin.connect();
      await admin.resetOffsets({
        groupId: 'group-1',
        topic: 'test-topic',
        earliest: true,
      });
      expect(mockMethods.adminResetOffsets).toHaveBeenCalledWith({
        groupId: 'group-1',
        topic: 'test-topic',
        earliest: true,
      });
    });

    it('should default earliest to false', async () => {
      const admin = kafka.admin();
      await admin.connect();
      await admin.resetOffsets({
        groupId: 'group-1',
        topic: 'test-topic',
      });
      expect(mockMethods.adminResetOffsets).toHaveBeenCalledWith({
        groupId: 'group-1',
        topic: 'test-topic',
        earliest: false,
      });
    });

    it('should throw when not connected', async () => {
      const admin = kafka.admin();
      await expect(
        admin.resetOffsets({ groupId: 'group-1', topic: 'test-topic' })
      ).rejects.toThrow(ConnectionError);
    });

    it('should throw KafkaError on failure', async () => {
      mockMethods.adminResetOffsets.mockRejectedValueOnce(new Error('Failed'));
      const admin = kafka.admin();
      await admin.connect();
      await expect(
        admin.resetOffsets({ groupId: 'group-1', topic: 'test-topic' })
      ).rejects.toThrow(KafkaError);
    });
  });

  describe('setOffsets', () => {
    it('should set offsets', async () => {
      const admin = kafka.admin();
      await admin.connect();
      await admin.setOffsets({
        groupId: 'group-1',
        topic: 'test-topic',
        partitions: [{ partition: 0, offset: '100' }],
      });
      expect(mockMethods.adminSetOffsets).toHaveBeenCalled();
    });

    it('should throw when not connected', async () => {
      const admin = kafka.admin();
      await expect(
        admin.setOffsets({
          groupId: 'group-1',
          topic: 'test-topic',
          partitions: [],
        })
      ).rejects.toThrow(ConnectionError);
    });

    it('should throw KafkaError on failure', async () => {
      mockMethods.adminSetOffsets.mockRejectedValueOnce(new Error('Failed'));
      const admin = kafka.admin();
      await admin.connect();
      await expect(
        admin.setOffsets({
          groupId: 'group-1',
          topic: 'test-topic',
          partitions: [],
        })
      ).rejects.toThrow(KafkaError);
    });
  });

  describe('describeCluster', () => {
    it('should describe cluster', async () => {
      const admin = kafka.admin();
      await admin.connect();
      const result = await admin.describeCluster();
      expect(result.brokers).toHaveLength(1);
      expect(result.controller).toBe(1);
      expect(result.clusterId).toBe('cluster-1');
    });

    it('should throw when not connected', async () => {
      const admin = kafka.admin();
      await expect(admin.describeCluster()).rejects.toThrow(ConnectionError);
    });

    it('should throw KafkaError on failure', async () => {
      mockMethods.adminDescribeCluster.mockRejectedValueOnce(new Error('Failed'));
      const admin = kafka.admin();
      await admin.connect();
      await expect(admin.describeCluster()).rejects.toThrow(KafkaError);
    });
  });

  describe('describeConfigs', () => {
    it('should describe configs', async () => {
      const admin = kafka.admin();
      await admin.connect();
      const result = await admin.describeConfigs({
        resources: [{ type: ResourceTypes.TOPIC, name: 'test-topic' }],
      });
      expect(result.resources).toHaveLength(1);
    });

    it('should pass includeSynonyms', async () => {
      const admin = kafka.admin();
      await admin.connect();
      await admin.describeConfigs({
        resources: [{ type: ResourceTypes.TOPIC, name: 'test-topic' }],
        includeSynonyms: true,
      });
      expect(mockMethods.adminDescribeConfigs).toHaveBeenCalledWith({
        resources: [{ type: ResourceTypes.TOPIC, name: 'test-topic' }],
        includeSynonyms: true,
      });
    });

    it('should throw when not connected', async () => {
      const admin = kafka.admin();
      await expect(
        admin.describeConfigs({ resources: [] })
      ).rejects.toThrow(ConnectionError);
    });

    it('should throw KafkaError on failure', async () => {
      mockMethods.adminDescribeConfigs.mockRejectedValueOnce(new Error('Failed'));
      const admin = kafka.admin();
      await admin.connect();
      await expect(admin.describeConfigs({ resources: [] })).rejects.toThrow(KafkaError);
    });
  });

  describe('alterConfigs', () => {
    it('should alter configs', async () => {
      const admin = kafka.admin();
      await admin.connect();
      await admin.alterConfigs({
        resources: [
          {
            type: ResourceTypes.TOPIC,
            name: 'test-topic',
            configEntries: [{ name: 'retention.ms', value: '172800000' }],
          },
        ],
      });
      expect(mockMethods.adminAlterConfigs).toHaveBeenCalled();
    });

    it('should pass validateOnly', async () => {
      const admin = kafka.admin();
      await admin.connect();
      await admin.alterConfigs({
        resources: [],
        validateOnly: true,
      });
      expect(mockMethods.adminAlterConfigs).toHaveBeenCalledWith({
        resources: [],
        validateOnly: true,
      });
    });

    it('should throw when not connected', async () => {
      const admin = kafka.admin();
      await expect(admin.alterConfigs({ resources: [] })).rejects.toThrow(ConnectionError);
    });

    it('should throw KafkaError on failure', async () => {
      mockMethods.adminAlterConfigs.mockRejectedValueOnce(new Error('Failed'));
      const admin = kafka.admin();
      await admin.connect();
      await expect(admin.alterConfigs({ resources: [] })).rejects.toThrow(KafkaError);
    });
  });

  describe('alterPartitionReassignments', () => {
    it('should alter partition reassignments', async () => {
      const admin = kafka.admin();
      await admin.connect();
      await admin.alterPartitionReassignments({
        topics: [
          {
            topic: 'test-topic',
            partitionAssignment: [{ partition: 0, replicas: [1, 2, 3] }],
          },
        ],
      });
      expect(mockMethods.adminAlterPartitionReassignments).toHaveBeenCalled();
    });

    it('should pass timeout', async () => {
      const admin = kafka.admin();
      await admin.connect();
      await admin.alterPartitionReassignments({
        topics: [],
        timeout: 60000,
      });
      expect(mockMethods.adminAlterPartitionReassignments).toHaveBeenCalledWith({
        topics: [],
        timeout: 60000,
      });
    });

    it('should throw when not connected', async () => {
      const admin = kafka.admin();
      await expect(
        admin.alterPartitionReassignments({ topics: [] })
      ).rejects.toThrow(ConnectionError);
    });

    it('should throw KafkaError on failure', async () => {
      mockMethods.adminAlterPartitionReassignments.mockRejectedValueOnce(new Error('Failed'));
      const admin = kafka.admin();
      await admin.connect();
      await expect(
        admin.alterPartitionReassignments({ topics: [] })
      ).rejects.toThrow(KafkaError);
    });
  });

  describe('listPartitionReassignments', () => {
    it('should list partition reassignments', async () => {
      const admin = kafka.admin();
      await admin.connect();
      const result = await admin.listPartitionReassignments();
      expect(result.topics).toBeDefined();
    });

    it('should pass options', async () => {
      const admin = kafka.admin();
      await admin.connect();
      await admin.listPartitionReassignments({
        topics: [{ topic: 'test-topic', partitions: [0, 1] }],
        timeout: 60000,
      });
      expect(mockMethods.adminListPartitionReassignments).toHaveBeenCalledWith({
        topics: [{ topic: 'test-topic', partitions: [0, 1] }],
        timeout: 60000,
      });
    });

    it('should throw when not connected', async () => {
      const admin = kafka.admin();
      await expect(admin.listPartitionReassignments()).rejects.toThrow(ConnectionError);
    });

    it('should throw KafkaError on failure', async () => {
      mockMethods.adminListPartitionReassignments.mockRejectedValueOnce(new Error('Failed'));
      const admin = kafka.admin();
      await admin.connect();
      await expect(admin.listPartitionReassignments()).rejects.toThrow(KafkaError);
    });
  });

  describe('deleteRecords', () => {
    it('should delete records', async () => {
      const admin = kafka.admin();
      await admin.connect();
      await admin.deleteRecords({
        topic: 'test-topic',
        partitions: [{ partition: 0, offset: '100' }],
      });
      expect(mockMethods.adminDeleteRecords).toHaveBeenCalledWith({
        topic: 'test-topic',
        partitions: [{ partition: 0, offset: '100' }],
      });
    });

    it('should throw when not connected', async () => {
      const admin = kafka.admin();
      await expect(
        admin.deleteRecords({ topic: 'test-topic', partitions: [] })
      ).rejects.toThrow(ConnectionError);
    });

    it('should throw KafkaError on failure', async () => {
      mockMethods.adminDeleteRecords.mockRejectedValueOnce(new Error('Failed'));
      const admin = kafka.admin();
      await admin.connect();
      await expect(
        admin.deleteRecords({ topic: 'test-topic', partitions: [] })
      ).rejects.toThrow(KafkaError);
    });
  });

  describe('error handling with non-Error exceptions', () => {
    it('should handle non-Error in connect', async () => {
      mockMethods.adminConnect.mockRejectedValueOnce('connection string error');
      const admin = kafka.admin();
      await expect(admin.connect()).rejects.toThrow('connection string error');
    });

    it('should handle non-Error in listTopics', async () => {
      mockMethods.adminListTopics.mockRejectedValueOnce('list string error');
      const admin = kafka.admin();
      await admin.connect();
      await expect(admin.listTopics()).rejects.toThrow('list string error');
    });

    it('should handle non-Error in createTopics', async () => {
      mockMethods.adminCreateTopics.mockRejectedValueOnce('create string error');
      const admin = kafka.admin();
      await admin.connect();
      await expect(
        admin.createTopics({ topics: [{ topic: 'test' }] })
      ).rejects.toThrow('create string error');
    });

    it('should handle non-Error in deleteTopics', async () => {
      mockMethods.adminDeleteTopics.mockRejectedValueOnce('delete string error');
      const admin = kafka.admin();
      await admin.connect();
      await expect(admin.deleteTopics({ topics: ['test'] })).rejects.toThrow('delete string error');
    });

    it('should handle non-Error in fetchTopicMetadata', async () => {
      mockMethods.adminFetchTopicMetadata.mockRejectedValueOnce('metadata string error');
      const admin = kafka.admin();
      await admin.connect();
      await expect(admin.fetchTopicMetadata()).rejects.toThrow('metadata string error');
    });

    it('should handle non-Error in fetchTopicOffsets', async () => {
      mockMethods.adminFetchTopicOffsets.mockRejectedValueOnce('offsets string error');
      const admin = kafka.admin();
      await admin.connect();
      await expect(admin.fetchTopicOffsets('test')).rejects.toThrow('offsets string error');
    });

    it('should handle non-Error in fetchTopicOffsetsByTimestamp', async () => {
      mockMethods.adminFetchTopicOffsetsByTimestamp.mockRejectedValueOnce('timestamp string error');
      const admin = kafka.admin();
      await admin.connect();
      await expect(admin.fetchTopicOffsetsByTimestamp('test', '123')).rejects.toThrow(
        'timestamp string error'
      );
    });

    it('should handle non-Error in createPartitions', async () => {
      mockMethods.adminCreatePartitions.mockRejectedValueOnce('partitions string error');
      const admin = kafka.admin();
      await admin.connect();
      await expect(
        admin.createPartitions({ topicPartitions: [] })
      ).rejects.toThrow('partitions string error');
    });

    it('should handle non-Error in listGroups', async () => {
      mockMethods.adminListGroups.mockRejectedValueOnce('groups string error');
      const admin = kafka.admin();
      await admin.connect();
      await expect(admin.listGroups()).rejects.toThrow('groups string error');
    });

    it('should handle non-Error in describeGroups', async () => {
      mockMethods.adminDescribeGroups.mockRejectedValueOnce('describe string error');
      const admin = kafka.admin();
      await admin.connect();
      await expect(admin.describeGroups(['group'])).rejects.toThrow('describe string error');
    });

    it('should handle non-Error in deleteGroups', async () => {
      mockMethods.adminDeleteGroups.mockRejectedValueOnce('delete groups string error');
      const admin = kafka.admin();
      await admin.connect();
      await expect(admin.deleteGroups({ groupIds: ['group'] })).rejects.toThrow(
        'delete groups string error'
      );
    });

    it('should handle non-Error in fetchOffsets', async () => {
      mockMethods.adminFetchOffsets.mockRejectedValueOnce('fetch offsets string error');
      const admin = kafka.admin();
      await admin.connect();
      await expect(admin.fetchOffsets({ groupId: 'group' })).rejects.toThrow(
        'fetch offsets string error'
      );
    });

    it('should handle non-Error in resetOffsets', async () => {
      mockMethods.adminResetOffsets.mockRejectedValueOnce('reset string error');
      const admin = kafka.admin();
      await admin.connect();
      await expect(
        admin.resetOffsets({ groupId: 'group', topic: 'test' })
      ).rejects.toThrow('reset string error');
    });

    it('should handle non-Error in setOffsets', async () => {
      mockMethods.adminSetOffsets.mockRejectedValueOnce('set string error');
      const admin = kafka.admin();
      await admin.connect();
      await expect(
        admin.setOffsets({ groupId: 'group', topic: 'test', partitions: [] })
      ).rejects.toThrow('set string error');
    });

    it('should handle non-Error in describeCluster', async () => {
      mockMethods.adminDescribeCluster.mockRejectedValueOnce('cluster string error');
      const admin = kafka.admin();
      await admin.connect();
      await expect(admin.describeCluster()).rejects.toThrow('cluster string error');
    });

    it('should handle non-Error in describeConfigs', async () => {
      mockMethods.adminDescribeConfigs.mockRejectedValueOnce('configs string error');
      const admin = kafka.admin();
      await admin.connect();
      await expect(admin.describeConfigs({ resources: [] })).rejects.toThrow('configs string error');
    });

    it('should handle non-Error in alterConfigs', async () => {
      mockMethods.adminAlterConfigs.mockRejectedValueOnce('alter string error');
      const admin = kafka.admin();
      await admin.connect();
      await expect(admin.alterConfigs({ resources: [] })).rejects.toThrow('alter string error');
    });

    it('should handle non-Error in alterPartitionReassignments', async () => {
      mockMethods.adminAlterPartitionReassignments.mockRejectedValueOnce('reassign string error');
      const admin = kafka.admin();
      await admin.connect();
      await expect(admin.alterPartitionReassignments({ topics: [] })).rejects.toThrow(
        'reassign string error'
      );
    });

    it('should handle non-Error in listPartitionReassignments', async () => {
      mockMethods.adminListPartitionReassignments.mockRejectedValueOnce(
        'list reassign string error'
      );
      const admin = kafka.admin();
      await admin.connect();
      await expect(admin.listPartitionReassignments()).rejects.toThrow('list reassign string error');
    });

    it('should handle non-Error in deleteRecords', async () => {
      mockMethods.adminDeleteRecords.mockRejectedValueOnce('delete records string error');
      const admin = kafka.admin();
      await admin.connect();
      await expect(
        admin.deleteRecords({ topic: 'test', partitions: [] })
      ).rejects.toThrow('delete records string error');
    });
  });
});
