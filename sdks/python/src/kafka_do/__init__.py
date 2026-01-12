"""
kafka-do - Async Kafka client SDK for .do services.

This package provides a Python Kafka client with kafka-python compatible API
using rpc-do for the underlying transport. Features include:

- Async/await native API
- Producer for sending messages
- Consumer with async iteration support
- Admin client for topic management
- Type-safe operations with dataclasses

Example usage:
    from kafka_do import Kafka

    async def main():
        # Create Kafka client
        kafka = Kafka(brokers=['localhost:9092'])

        # Producer
        producer = kafka.producer()
        await producer.connect()
        await producer.send('my-topic', value=b'Hello!')
        await producer.disconnect()

        # Consumer
        consumer = kafka.consumer(group_id='my-group')
        await consumer.connect()
        await consumer.subscribe(['my-topic'])
        async for message in consumer:
            print(message.value)
            break
        await consumer.disconnect()

        # Admin
        admin = kafka.admin()
        await admin.connect()
        topics = await admin.list_topics()
        print(topics)
        await admin.disconnect()

    import asyncio
    asyncio.run(main())
"""

from __future__ import annotations

__version__ = "0.1.0"

from .admin import Admin
from .consumer import Consumer
from .kafka import Kafka
from .producer import Producer
from .types import (
    BatchResult,
    CompressionType,
    ConsumerError,
    ConsumerGroupMetadata,
    ConsumerRecord,
    IsolationLevel,
    KafkaConnectionError,
    KafkaError,
    KafkaTimeoutError,
    OffsetResetPolicy,
    PartitionMetadata,
    ProducerError,
    ProducerRecord,
    RecordMetadata,
    TopicAlreadyExistsError,
    TopicConfig,
    TopicMetadata,
    TopicNotFoundError,
    TopicPartition,
)

__all__ = [
    # Main classes
    "Kafka",
    "Producer",
    "Consumer",
    "Admin",
    # Data types
    "TopicPartition",
    "RecordMetadata",
    "ConsumerRecord",
    "ProducerRecord",
    "TopicConfig",
    "TopicMetadata",
    "PartitionMetadata",
    "ConsumerGroupMetadata",
    "BatchResult",
    # Enums
    "CompressionType",
    "IsolationLevel",
    "OffsetResetPolicy",
    # Exceptions
    "KafkaError",
    "KafkaConnectionError",
    "KafkaTimeoutError",
    "TopicNotFoundError",
    "TopicAlreadyExistsError",
    "ProducerError",
    "ConsumerError",
    # Version
    "__version__",
]
