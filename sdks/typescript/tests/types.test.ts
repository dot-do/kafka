/**
 * Tests for error types and enums
 */

import { describe, it, expect } from 'vitest';
import {
  KafkaError,
  ConnectionError,
  TopicError,
  ConsumerError,
  ProducerError,
  CompressionTypes,
  ResourceTypes,
} from '../src/types.js';

describe('Error types', () => {
  describe('KafkaError', () => {
    it('should create error with message and code', () => {
      const error = new KafkaError('Test error', 'TEST_ERROR');
      expect(error.message).toBe('Test error');
      expect(error.code).toBe('TEST_ERROR');
      expect(error.name).toBe('KafkaError');
      expect(error.retriable).toBe(false);
    });

    it('should create retriable error', () => {
      const error = new KafkaError('Test error', 'TEST_ERROR', true);
      expect(error.retriable).toBe(true);
    });

    it('should be instance of Error', () => {
      const error = new KafkaError('Test error', 'TEST_ERROR');
      expect(error).toBeInstanceOf(Error);
    });
  });

  describe('ConnectionError', () => {
    it('should create connection error', () => {
      const error = new ConnectionError('Connection failed');
      expect(error.message).toBe('Connection failed');
      expect(error.code).toBe('KAFKA_CONNECTION_ERROR');
      expect(error.name).toBe('ConnectionError');
      expect(error.retriable).toBe(true);
    });

    it('should be instance of KafkaError', () => {
      const error = new ConnectionError('Connection failed');
      expect(error).toBeInstanceOf(KafkaError);
    });
  });

  describe('TopicError', () => {
    it('should create topic error with topic name', () => {
      const error = new TopicError('Topic not found', 'my-topic');
      expect(error.message).toBe('Topic not found');
      expect(error.code).toBe('KAFKA_TOPIC_ERROR');
      expect(error.name).toBe('TopicError');
      expect(error.topic).toBe('my-topic');
      expect(error.retriable).toBe(false);
    });

    it('should be instance of KafkaError', () => {
      const error = new TopicError('Topic not found', 'my-topic');
      expect(error).toBeInstanceOf(KafkaError);
    });
  });

  describe('ConsumerError', () => {
    it('should create consumer error with group ID', () => {
      const error = new ConsumerError('Consumer failed', 'my-group');
      expect(error.message).toBe('Consumer failed');
      expect(error.code).toBe('KAFKA_CONSUMER_ERROR');
      expect(error.name).toBe('ConsumerError');
      expect(error.groupId).toBe('my-group');
      expect(error.retriable).toBe(false);
    });

    it('should be instance of KafkaError', () => {
      const error = new ConsumerError('Consumer failed', 'my-group');
      expect(error).toBeInstanceOf(KafkaError);
    });
  });

  describe('ProducerError', () => {
    it('should create producer error', () => {
      const error = new ProducerError('Producer failed');
      expect(error.message).toBe('Producer failed');
      expect(error.code).toBe('KAFKA_PRODUCER_ERROR');
      expect(error.name).toBe('ProducerError');
      expect(error.retriable).toBe(false);
    });

    it('should be instance of KafkaError', () => {
      const error = new ProducerError('Producer failed');
      expect(error).toBeInstanceOf(KafkaError);
    });
  });
});

describe('CompressionTypes enum', () => {
  it('should have correct values', () => {
    expect(CompressionTypes.None).toBe(0);
    expect(CompressionTypes.GZIP).toBe(1);
    expect(CompressionTypes.Snappy).toBe(2);
    expect(CompressionTypes.LZ4).toBe(3);
    expect(CompressionTypes.ZSTD).toBe(4);
  });
});

describe('ResourceTypes enum', () => {
  it('should have correct values', () => {
    expect(ResourceTypes.UNKNOWN).toBe(0);
    expect(ResourceTypes.ANY).toBe(1);
    expect(ResourceTypes.TOPIC).toBe(2);
    expect(ResourceTypes.GROUP).toBe(3);
    expect(ResourceTypes.CLUSTER).toBe(4);
    expect(ResourceTypes.TRANSACTIONAL_ID).toBe(5);
    expect(ResourceTypes.DELEGATION_TOKEN).toBe(6);
  });
});
