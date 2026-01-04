import { describe, it, expect } from 'vitest'
import type {
  ProducerRecord,
  ConsumerRecord,
  RecordMetadata,
  TopicPartition,
  OffsetAndMetadata,
} from './records'

describe('Record Types', () => {
  it('ProducerRecord has required fields', () => {
    const record: ProducerRecord = {
      topic: 'test-topic',
      value: { message: 'hello' },
    }
    expect(record.topic).toBe('test-topic')
    expect(record.value).toEqual({ message: 'hello' })
  })

  it('ProducerRecord supports optional key and partition', () => {
    const record: ProducerRecord = {
      topic: 'test-topic',
      key: 'my-key',
      value: { data: 'test' },
      partition: 0,
      headers: { 'x-trace-id': 'abc123' },
      timestamp: Date.now(),
    }
    expect(record.key).toBe('my-key')
    expect(record.partition).toBe(0)
    expect(record.headers?.['x-trace-id']).toBe('abc123')
  })

  it('ConsumerRecord has all required fields', () => {
    const record: ConsumerRecord = {
      topic: 'test-topic',
      partition: 0,
      offset: 42,
      key: 'my-key',
      value: { data: 'test' },
      headers: {},
      timestamp: Date.now(),
    }
    expect(record.offset).toBe(42)
    expect(record.partition).toBe(0)
  })

  it('RecordMetadata contains send result', () => {
    const metadata: RecordMetadata = {
      topic: 'test-topic',
      partition: 0,
      offset: 100,
      timestamp: Date.now(),
    }
    expect(metadata.offset).toBe(100)
  })

  it('TopicPartition identifies a partition', () => {
    const tp: TopicPartition = {
      topic: 'test-topic',
      partition: 0,
    }
    expect(tp.topic).toBe('test-topic')
    expect(tp.partition).toBe(0)
  })

  it('OffsetAndMetadata for commits', () => {
    const offset: OffsetAndMetadata = {
      offset: 100,
      metadata: 'consumer-state',
    }
    expect(offset.offset).toBe(100)
  })
})
