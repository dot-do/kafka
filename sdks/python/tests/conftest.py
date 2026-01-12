"""
Pytest fixtures for kafka-do SDK tests.
"""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from kafka_do import Kafka
from kafka_do.admin import Admin
from kafka_do.consumer import Consumer
from kafka_do.producer import Producer


class MockRpcClient:
    """Mock RPC client for testing."""

    def __init__(self) -> None:
        self.kafka = MockKafkaNamespace()
        self._closed = False

    async def close(self) -> None:
        self._closed = True


class MockKafkaNamespace:
    """Mock kafka namespace on the RPC client."""

    def __init__(self) -> None:
        self.producer = MockProducerNamespace()
        self.consumer = MockConsumerNamespace()
        self.admin = MockAdminNamespace()
        self.configure = AsyncMock(return_value=None)


class MockProducerNamespace:
    """Mock producer namespace."""

    def __init__(self) -> None:
        self.send = AsyncMock(
            return_value={
                "topic": "test-topic",
                "partition": 0,
                "offset": 42,
                "timestamp": 1234567890,
                "serialized_key_size": 0,
                "serialized_value_size": 5,
            }
        )
        self.sendBatch = AsyncMock(
            return_value=[
                {
                    "topic": "test-topic",
                    "partition": 0,
                    "offset": 42,
                    "timestamp": 1234567890,
                    "serialized_key_size": 0,
                    "serialized_value_size": 4,
                },
                {
                    "topic": "test-topic",
                    "partition": 0,
                    "offset": 43,
                    "timestamp": 1234567891,
                    "serialized_key_size": 0,
                    "serialized_value_size": 4,
                },
            ]
        )
        self.flush = AsyncMock(return_value=None)
        self.partitionsFor = AsyncMock(return_value=[0, 1, 2])


class MockConsumerNamespace:
    """Mock consumer namespace."""

    def __init__(self) -> None:
        self.subscribe = AsyncMock(return_value=None)
        self.unsubscribe = AsyncMock(return_value=None)
        self.assign = AsyncMock(return_value=None)
        self.poll = AsyncMock(return_value=[])
        self.commit = AsyncMock(return_value=None)
        self.committed = AsyncMock(
            return_value=[
                {"topic": "test-topic", "partition": 0, "offset": 100},
            ]
        )
        self.seek = AsyncMock(return_value=None)
        self.seekToBeginning = AsyncMock(return_value=None)
        self.seekToEnd = AsyncMock(return_value=None)
        self.position = AsyncMock(return_value=42)
        self.pause = AsyncMock(return_value=None)
        self.resume = AsyncMock(return_value=None)


class MockAdminNamespace:
    """Mock admin namespace."""

    def __init__(self) -> None:
        self.createTopics = AsyncMock(return_value=[])
        self.deleteTopics = AsyncMock(return_value=[])
        self.listTopics = AsyncMock(return_value=["topic1", "topic2"])
        self.describeTopics = AsyncMock(
            return_value=[
                {
                    "name": "test-topic",
                    "partitions": 3,
                    "replicationFactor": 1,
                    "config": {},
                },
            ]
        )
        self.describePartitions = AsyncMock(
            return_value={
                "partitions": [
                    {"partition": 0, "leader": 1, "replicas": [1], "isr": [1]},
                    {"partition": 1, "leader": 1, "replicas": [1], "isr": [1]},
                ],
            }
        )
        self.listConsumerGroups = AsyncMock(return_value=["group1", "group2"])
        self.describeConsumerGroups = AsyncMock(
            return_value=[
                {
                    "groupId": "test-group",
                    "state": "Stable",
                    "members": ["member1"],
                    "coordinator": 1,
                },
            ]
        )
        self.deleteConsumerGroups = AsyncMock(return_value=[])
        self.alterTopicConfig = AsyncMock(return_value={})


@pytest.fixture
def mock_rpc_client() -> MockRpcClient:
    """Create a mock RPC client."""
    return MockRpcClient()


@pytest.fixture
def kafka_instance(mock_rpc_client: MockRpcClient) -> Kafka:
    """Create a Kafka instance with mocked client."""
    kafka = Kafka(brokers=["localhost:9092"], client_id="test-client")
    kafka._client = mock_rpc_client
    return kafka


@pytest.fixture
async def connected_producer(kafka_instance: Kafka, mock_rpc_client: MockRpcClient) -> Producer:
    """Create a connected producer with mocked client."""
    producer = kafka_instance.producer()
    producer._client = mock_rpc_client
    producer._connected = True
    return producer


@pytest.fixture
async def connected_consumer(kafka_instance: Kafka, mock_rpc_client: MockRpcClient) -> Consumer:
    """Create a connected consumer with mocked client."""
    consumer = kafka_instance.consumer(group_id="test-group")
    consumer._client = mock_rpc_client
    consumer._connected = True
    return consumer


@pytest.fixture
async def connected_admin(kafka_instance: Kafka, mock_rpc_client: MockRpcClient) -> Admin:
    """Create a connected admin with mocked client."""
    admin = kafka_instance.admin()
    admin._client = mock_rpc_client
    admin._connected = True
    return admin
