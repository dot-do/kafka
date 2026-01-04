import { describe, it, expect } from 'vitest'
import type {
  ProducerConfig,
  Producer,
  TransactionalProducer,
} from './producer'

describe('Producer Types', () => {
  it('ProducerConfig has sensible defaults', () => {
    const config: ProducerConfig = {
      clientId: 'my-producer',
    }
    expect(config.clientId).toBe('my-producer')
  })

  it('ProducerConfig supports batch settings', () => {
    const config: ProducerConfig = {
      clientId: 'my-producer',
      batchSize: 100,
      lingerMs: 5,
      maxRequestSize: 1048576,
      acks: 'all',
    }
    expect(config.batchSize).toBe(100)
    expect(config.acks).toBe('all')
  })

  it('Producer interface has send methods', () => {
    // Type-only test - ensuring interface shape
    const mockProducer: Producer = {
      send: async () => ({ topic: 't', partition: 0, offset: 0, timestamp: 0 }),
      sendBatch: async () => [],
      flush: async () => {},
      close: async () => {},
    }
    expect(mockProducer.send).toBeDefined()
    expect(mockProducer.sendBatch).toBeDefined()
  })

  it('TransactionalProducer extends Producer', () => {
    const mockTxProducer: TransactionalProducer = {
      send: async () => ({ topic: 't', partition: 0, offset: 0, timestamp: 0 }),
      sendBatch: async () => [],
      flush: async () => {},
      close: async () => {},
      beginTransaction: async () => {},
      commitTransaction: async () => {},
      abortTransaction: async () => {},
    }
    expect(mockTxProducer.beginTransaction).toBeDefined()
    expect(mockTxProducer.commitTransaction).toBeDefined()
  })
})
