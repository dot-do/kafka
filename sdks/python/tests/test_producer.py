"""
Tests for kafka-do Producer.
"""

from __future__ import annotations

import base64
from unittest.mock import AsyncMock, patch

import pytest

from kafka_do import (
    CompressionType,
    Kafka,
    KafkaConnectionError,
    KafkaError,
    ProducerError,
    ProducerRecord,
    RecordMetadata,
)
from kafka_do.producer import Producer
from kafka_do.types import BatchResult, serialize_record

from .conftest import MockRpcClient


class TestProducerInit:
    """Tests for Producer initialization."""

    def test_producer_init_defaults(self, kafka_instance: Kafka) -> None:
        """Test producer initialization with default settings."""
        producer = kafka_instance.producer()

        assert producer._kafka is kafka_instance
        assert producer._connected is False
        assert producer._acks == 1
        assert producer._compression_type == CompressionType.NONE
        assert producer._batch_size == 16384
        assert producer._linger_ms == 0

    def test_producer_init_custom_settings(self, kafka_instance: Kafka) -> None:
        """Test producer initialization with custom settings."""
        producer = kafka_instance.producer(
            acks="all",
            compression_type=CompressionType.GZIP,
            batch_size=32768,
            linger_ms=100,
            client_id="custom-client",
        )

        assert producer._acks == "all"
        assert producer._compression_type == CompressionType.GZIP
        assert producer._batch_size == 32768
        assert producer._linger_ms == 100
        assert producer._client_id == "custom-client"

    def test_producer_connected_property(self, kafka_instance: Kafka) -> None:
        """Test connected property."""
        producer = kafka_instance.producer()
        assert producer.connected is False

        producer._connected = True
        assert producer.connected is True


class TestProducerConnection:
    """Tests for Producer connection handling."""

    async def test_connect_success(self, mock_rpc_client: MockRpcClient) -> None:
        """Test successful producer connection."""
        kafka = Kafka(brokers=["localhost:9092"])
        kafka._client = mock_rpc_client
        producer = kafka.producer()

        await producer.connect()

        assert producer.connected is True
        assert producer._client is mock_rpc_client

    async def test_connect_already_connected(
        self, connected_producer: Producer, mock_rpc_client: MockRpcClient
    ) -> None:
        """Test that connect does nothing if already connected."""
        original_client = connected_producer._client
        await connected_producer.connect()

        assert connected_producer._client is original_client

    async def test_connect_failure(self) -> None:
        """Test connection failure raises KafkaConnectionError."""
        kafka = Kafka(brokers=["localhost:9092"])
        producer = kafka.producer()

        with pytest.raises(KafkaConnectionError, match="Failed to connect producer"):
            await producer.connect()

    async def test_disconnect(self, connected_producer: Producer) -> None:
        """Test producer disconnection."""
        await connected_producer.disconnect()

        assert connected_producer.connected is False
        assert connected_producer._client is None

    async def test_disconnect_not_connected(self, kafka_instance: Kafka) -> None:
        """Test disconnect when not connected does nothing."""
        producer = kafka_instance.producer()
        await producer.disconnect()  # Should not raise

        assert producer.connected is False

    async def test_context_manager(self, mock_rpc_client: MockRpcClient) -> None:
        """Test async context manager."""
        kafka = Kafka(brokers=["localhost:9092"])
        kafka._client = mock_rpc_client

        async with kafka.producer() as producer:
            assert producer.connected is True

        assert producer.connected is False


class TestProducerSend:
    """Tests for Producer send operations."""

    async def test_send_simple_message(
        self, connected_producer: Producer, mock_rpc_client: MockRpcClient
    ) -> None:
        """Test sending a simple message."""
        metadata = await connected_producer.send("test-topic", value=b"Hello!")

        assert isinstance(metadata, RecordMetadata)
        assert metadata.topic == "test-topic"
        assert metadata.partition == 0
        assert metadata.offset == 42

        # Verify the call was made with correct serialization
        mock_rpc_client.kafka.producer.send.assert_called_once()
        call_args = mock_rpc_client.kafka.producer.send.call_args[0][0]
        assert call_args["topic"] == "test-topic"
        assert base64.b64decode(call_args["value"]) == b"Hello!"

    async def test_send_with_key(
        self, connected_producer: Producer, mock_rpc_client: MockRpcClient
    ) -> None:
        """Test sending a message with key."""
        await connected_producer.send("test-topic", key=b"my-key", value=b"Hello!")

        call_args = mock_rpc_client.kafka.producer.send.call_args[0][0]
        assert base64.b64decode(call_args["key"]) == b"my-key"

    async def test_send_with_partition(
        self, connected_producer: Producer, mock_rpc_client: MockRpcClient
    ) -> None:
        """Test sending to a specific partition."""
        await connected_producer.send("test-topic", value=b"Hello!", partition=2)

        call_args = mock_rpc_client.kafka.producer.send.call_args[0][0]
        assert call_args["partition"] == 2

    async def test_send_with_timestamp(
        self, connected_producer: Producer, mock_rpc_client: MockRpcClient
    ) -> None:
        """Test sending with a timestamp."""
        await connected_producer.send("test-topic", value=b"Hello!", timestamp=1234567890)

        call_args = mock_rpc_client.kafka.producer.send.call_args[0][0]
        assert call_args["timestamp"] == 1234567890

    async def test_send_with_headers(
        self, connected_producer: Producer, mock_rpc_client: MockRpcClient
    ) -> None:
        """Test sending with headers."""
        headers = [("header1", b"value1"), ("header2", b"value2")]
        await connected_producer.send("test-topic", value=b"Hello!", headers=headers)

        call_args = mock_rpc_client.kafka.producer.send.call_args[0][0]
        assert len(call_args["headers"]) == 2
        assert call_args["headers"][0]["key"] == "header1"

    async def test_send_not_connected(self, kafka_instance: Kafka) -> None:
        """Test send raises error when not connected."""
        producer = kafka_instance.producer()

        with pytest.raises(KafkaConnectionError, match="Producer not connected"):
            await producer.send("test-topic", value=b"Hello!")

    async def test_send_failure(
        self, connected_producer: Producer, mock_rpc_client: MockRpcClient
    ) -> None:
        """Test send failure raises ProducerError."""
        mock_rpc_client.kafka.producer.send.side_effect = Exception("Send failed")

        with pytest.raises(ProducerError, match="Failed to send message"):
            await connected_producer.send("test-topic", value=b"Hello!")


class TestProducerBatch:
    """Tests for Producer batch operations."""

    async def test_send_batch_with_dicts(
        self, connected_producer: Producer, mock_rpc_client: MockRpcClient
    ) -> None:
        """Test sending a batch of messages as dicts."""
        records = [
            {"topic": "topic1", "value": b"msg1"},
            {"topic": "topic2", "value": b"msg2"},
        ]

        result = await connected_producer.send_batch(records)

        assert isinstance(result, BatchResult)
        assert len(result.successful) == 2
        assert len(result.failed) == 0

    async def test_send_batch_with_producer_records(
        self, connected_producer: Producer, mock_rpc_client: MockRpcClient
    ) -> None:
        """Test sending a batch of ProducerRecord objects."""
        records = [
            ProducerRecord(topic="topic1", value=b"msg1", key=b"key1"),
            ProducerRecord(topic="topic2", value=b"msg2"),
        ]

        result = await connected_producer.send_batch(records)

        assert len(result.successful) == 2

    async def test_send_batch_with_partial_failure(
        self, connected_producer: Producer, mock_rpc_client: MockRpcClient
    ) -> None:
        """Test batch with partial failure."""
        mock_rpc_client.kafka.producer.sendBatch.return_value = [
            {"topic": "topic1", "partition": 0, "offset": 42},
            {"error": {"code": "TIMEOUT", "message": "Request timed out"}},
        ]

        records = [
            {"topic": "topic1", "value": b"msg1"},
            {"topic": "topic2", "value": b"msg2"},
        ]

        result = await connected_producer.send_batch(records)

        assert len(result.successful) == 1
        assert len(result.failed) == 1
        assert result.failed[0][0].topic == "topic2"

    async def test_send_batch_total_failure(
        self, connected_producer: Producer, mock_rpc_client: MockRpcClient
    ) -> None:
        """Test batch with total failure."""
        mock_rpc_client.kafka.producer.sendBatch.side_effect = Exception("Batch failed")

        records = [
            {"topic": "topic1", "value": b"msg1"},
            {"topic": "topic2", "value": b"msg2"},
        ]

        result = await connected_producer.send_batch(records)

        assert len(result.successful) == 0
        assert len(result.failed) == 2

    async def test_send_batch_not_connected(self, kafka_instance: Kafka) -> None:
        """Test batch send raises error when not connected."""
        producer = kafka_instance.producer()

        with pytest.raises(KafkaConnectionError, match="Producer not connected"):
            await producer.send_batch([{"topic": "t", "value": b"v"}])


class TestProducerMisc:
    """Tests for miscellaneous Producer operations."""

    async def test_flush(
        self, connected_producer: Producer, mock_rpc_client: MockRpcClient
    ) -> None:
        """Test flush operation."""
        await connected_producer.flush()

        mock_rpc_client.kafka.producer.flush.assert_called_once()

    async def test_flush_with_timeout(
        self, connected_producer: Producer, mock_rpc_client: MockRpcClient
    ) -> None:
        """Test flush with timeout."""
        await connected_producer.flush(timeout=5.0)

        mock_rpc_client.kafka.producer.flush.assert_called_once_with(timeout=5.0)

    async def test_flush_not_connected(self, kafka_instance: Kafka) -> None:
        """Test flush when not connected does nothing."""
        producer = kafka_instance.producer()
        await producer.flush()  # Should not raise

    async def test_flush_error_ignored(
        self, connected_producer: Producer, mock_rpc_client: MockRpcClient
    ) -> None:
        """Test that flush errors are ignored."""
        mock_rpc_client.kafka.producer.flush.side_effect = Exception("Flush failed")

        await connected_producer.flush()  # Should not raise

    async def test_partitions_for(
        self, connected_producer: Producer, mock_rpc_client: MockRpcClient
    ) -> None:
        """Test getting partitions for a topic."""
        partitions = await connected_producer.partitions_for("test-topic")

        assert partitions == [0, 1, 2]
        mock_rpc_client.kafka.producer.partitionsFor.assert_called_once_with("test-topic")

    async def test_partitions_for_not_connected(self, kafka_instance: Kafka) -> None:
        """Test partitions_for raises error when not connected."""
        producer = kafka_instance.producer()

        with pytest.raises(KafkaConnectionError, match="Producer not connected"):
            await producer.partitions_for("test-topic")

    async def test_partitions_for_failure(
        self, connected_producer: Producer, mock_rpc_client: MockRpcClient
    ) -> None:
        """Test partitions_for failure."""
        mock_rpc_client.kafka.producer.partitionsFor.side_effect = Exception("Failed")

        with pytest.raises(KafkaError, match="Failed to get partitions"):
            await connected_producer.partitions_for("test-topic")


class TestSerializeRecord:
    """Tests for serialize_record helper function."""

    def test_serialize_basic_record(self) -> None:
        """Test serializing a basic record."""
        record = ProducerRecord(topic="test", value=b"Hello")
        result = serialize_record(record)

        assert result["topic"] == "test"
        assert base64.b64decode(result["value"]) == b"Hello"
        assert "key" not in result
        assert "partition" not in result

    def test_serialize_full_record(self) -> None:
        """Test serializing a record with all fields."""
        record = ProducerRecord(
            topic="test",
            value=b"Hello",
            key=b"key1",
            partition=2,
            timestamp=1234567890,
            headers=[("h1", b"v1")],
        )
        result = serialize_record(record)

        assert result["topic"] == "test"
        assert base64.b64decode(result["value"]) == b"Hello"
        assert base64.b64decode(result["key"]) == b"key1"
        assert result["partition"] == 2
        assert result["timestamp"] == 1234567890
        assert len(result["headers"]) == 1
