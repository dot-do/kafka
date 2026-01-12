/**
 * Tests for module exports
 */

import { describe, it, expect } from 'vitest';
import * as kafkaDo from '../src/index.js';

describe('Module exports', () => {
  describe('Classes', () => {
    it('should export Kafka class', () => {
      expect(kafkaDo.Kafka).toBeDefined();
    });

    it('should export Producer class', () => {
      expect(kafkaDo.Producer).toBeDefined();
    });

    it('should export Consumer class', () => {
      expect(kafkaDo.Consumer).toBeDefined();
    });

    it('should export Admin class', () => {
      expect(kafkaDo.Admin).toBeDefined();
    });
  });

  describe('Functions', () => {
    it('should export setRpcClientFactory', () => {
      expect(kafkaDo.setRpcClientFactory).toBeDefined();
      expect(typeof kafkaDo.setRpcClientFactory).toBe('function');
    });

    it('should export getRpcClientFactory', () => {
      expect(kafkaDo.getRpcClientFactory).toBeDefined();
      expect(typeof kafkaDo.getRpcClientFactory).toBe('function');
    });

    it('should export createRpcClient', () => {
      expect(kafkaDo.createRpcClient).toBeDefined();
      expect(typeof kafkaDo.createRpcClient).toBe('function');
    });
  });

  describe('Enums', () => {
    it('should export CompressionTypes', () => {
      expect(kafkaDo.CompressionTypes).toBeDefined();
      expect(kafkaDo.CompressionTypes.GZIP).toBe(1);
    });

    it('should export ResourceTypes', () => {
      expect(kafkaDo.ResourceTypes).toBeDefined();
      expect(kafkaDo.ResourceTypes.TOPIC).toBe(2);
    });
  });

  describe('Error classes', () => {
    it('should export KafkaError', () => {
      expect(kafkaDo.KafkaError).toBeDefined();
    });

    it('should export ConnectionError', () => {
      expect(kafkaDo.ConnectionError).toBeDefined();
    });

    it('should export TopicError', () => {
      expect(kafkaDo.TopicError).toBeDefined();
    });

    it('should export ConsumerError', () => {
      expect(kafkaDo.ConsumerError).toBeDefined();
    });

    it('should export ProducerError', () => {
      expect(kafkaDo.ProducerError).toBeDefined();
    });
  });

  describe('Type exports (compile-time check)', () => {
    it('should allow creating typed instances', () => {
      // This is a compile-time check - if types are not exported, this would fail to compile
      const config: kafkaDo.KafkaConfig = {
        brokers: ['localhost:9092'],
        clientId: 'test',
      };

      const producerConfig: kafkaDo.ProducerConfig = {
        idempotent: true,
      };

      const consumerConfig: kafkaDo.ConsumerConfig = {
        groupId: 'test-group',
      };

      const adminConfig: kafkaDo.AdminConfig = {
        retry: { retries: 5 },
      };

      expect(config.brokers).toBeDefined();
      expect(producerConfig.idempotent).toBe(true);
      expect(consumerConfig.groupId).toBe('test-group');
      expect(adminConfig.retry?.retries).toBe(5);
    });

    it('should export message types', () => {
      const message: kafkaDo.Message = {
        value: 'test',
        key: 'key1',
      };

      const kafkaMessage: kafkaDo.KafkaMessage = {
        key: Buffer.from('key'),
        value: Buffer.from('value'),
        timestamp: '123456',
        attributes: 0,
        offset: '0',
        size: 10,
      };

      expect(message.value).toBe('test');
      expect(kafkaMessage.offset).toBe('0');
    });

    it('should export producer types', () => {
      const record: kafkaDo.ProducerRecord = {
        topic: 'test',
        messages: [{ value: 'hello' }],
      };

      const batch: kafkaDo.ProducerBatch = {
        topicMessages: [
          { topic: 'test', messages: [{ value: 'hello' }] },
        ],
      };

      expect(record.topic).toBe('test');
      expect(batch.topicMessages).toHaveLength(1);
    });

    it('should export consumer types', () => {
      const subscription: kafkaDo.ConsumerSubscribeTopic = {
        topic: 'test',
        fromBeginning: true,
      };

      const runConfig: kafkaDo.ConsumerRunConfig = {
        autoCommit: true,
      };

      expect(subscription.fromBeginning).toBe(true);
      expect(runConfig.autoCommit).toBe(true);
    });

    it('should export admin types', () => {
      const createOptions: kafkaDo.CreateTopicsOptions = {
        topics: [{ topic: 'test', numPartitions: 3 }],
      };

      const deleteOptions: kafkaDo.DeleteTopicsOptions = {
        topics: ['test'],
      };

      expect(createOptions.topics).toHaveLength(1);
      expect(deleteOptions.topics).toHaveLength(1);
    });
  });
});
