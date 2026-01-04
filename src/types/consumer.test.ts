import { describe, it, expect } from 'vitest'
import type {
  ConsumerConfig,
  Consumer,
  ConsumerGroupState,
  PartitionAssignment,
} from './consumer'

describe('Consumer Types', () => {
  it('ConsumerConfig requires groupId', () => {
    const config: ConsumerConfig = {
      groupId: 'my-group',
    }
    expect(config.groupId).toBe('my-group')
  })

  it('ConsumerConfig supports all options', () => {
    const config: ConsumerConfig = {
      groupId: 'my-group',
      clientId: 'my-consumer',
      sessionTimeoutMs: 30000,
      heartbeatIntervalMs: 3000,
      maxPollRecords: 500,
      autoCommit: true,
      autoCommitIntervalMs: 5000,
      fromBeginning: false,
    }
    expect(config.sessionTimeoutMs).toBe(30000)
    expect(config.maxPollRecords).toBe(500)
  })

  it('Consumer interface has subscribe and poll', () => {
    const mockConsumer: Consumer = {
      subscribe: async () => {},
      unsubscribe: async () => {},
      poll: async () => [],
      commit: async () => {},
      commitSync: async () => {},
      seek: async () => {},
      pause: async () => {},
      resume: async () => {},
      close: async () => {},
      [Symbol.asyncIterator]: async function* () { yield* [] },
    }
    expect(mockConsumer.subscribe).toBeDefined()
    expect(mockConsumer.poll).toBeDefined()
    expect(mockConsumer.commit).toBeDefined()
  })

  it('ConsumerGroupState enum values', () => {
    const states: ConsumerGroupState[] = [
      'Empty',
      'Stable',
      'PreparingRebalance',
      'CompletingRebalance',
      'Dead',
    ]
    expect(states).toContain('Stable')
    expect(states).toContain('PreparingRebalance')
  })

  it('PartitionAssignment maps members to partitions', () => {
    const assignment: PartitionAssignment = {
      memberId: 'consumer-1',
      partitions: [
        { topic: 'topic-a', partition: 0 },
        { topic: 'topic-a', partition: 1 },
      ],
    }
    expect(assignment.partitions.length).toBe(2)
  })
})
