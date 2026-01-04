import { describe, it, expect } from 'vitest'
import type {
  Admin,
  TopicConfig,
  TopicMetadata,
  GroupDescription,
} from './admin'

describe('Admin Types', () => {
  it('TopicConfig for creating topics', () => {
    const config: TopicConfig = {
      topic: 'my-topic',
      partitions: 4,
      replicationFactor: 1,
      config: {
        'retention.ms': '604800000',
        'cleanup.policy': 'delete',
      },
    }
    expect(config.partitions).toBe(4)
    expect(config.config?.['retention.ms']).toBe('604800000')
  })

  it('TopicMetadata describes existing topic', () => {
    const metadata: TopicMetadata = {
      topic: 'my-topic',
      partitions: [
        { partition: 0, leader: 'do-1', replicas: ['do-1'], isr: ['do-1'] },
        { partition: 1, leader: 'do-2', replicas: ['do-2'], isr: ['do-2'] },
      ],
      config: {},
    }
    expect(metadata.partitions.length).toBe(2)
  })

  it('GroupDescription shows consumer group state', () => {
    const desc: GroupDescription = {
      groupId: 'my-group',
      state: 'Stable',
      protocolType: 'consumer',
      protocol: 'range',
      members: [
        {
          memberId: 'consumer-1',
          clientId: 'app-1',
          clientHost: '/10.0.0.1',
          assignment: [{ topic: 'topic-a', partition: 0 }],
        },
      ],
    }
    expect(desc.state).toBe('Stable')
    expect(desc.members.length).toBe(1)
  })

  it('Admin interface has topic and group methods', () => {
    const mockAdmin: Admin = {
      createTopic: async () => {},
      deleteTopic: async () => {},
      listTopics: async () => [],
      describeTopic: async () => ({ topic: '', partitions: [], config: {} }),
      createPartitions: async () => {},
      listGroups: async () => [],
      describeGroup: async () => ({
        groupId: '',
        state: 'Empty',
        protocolType: '',
        protocol: '',
        members: [],
      }),
      deleteGroup: async () => {},
      listOffsets: async () => new Map(),
      close: async () => {},
    }
    expect(mockAdmin.createTopic).toBeDefined()
    expect(mockAdmin.describeGroup).toBeDefined()
  })
})
