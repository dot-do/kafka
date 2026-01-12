"""
Tests for kafka-do Consumer.
"""

from __future__ import annotations

import asyncio
import base64
from unittest.mock import AsyncMock

import pytest

from kafka_do import (
    ConsumerError,
    ConsumerRecord,
    IsolationLevel,
    Kafka,
    KafkaConnectionError,
    OffsetResetPolicy,
    TopicPartition,
)
from kafka_do.consumer import Consumer
from kafka_do.types import deserialize_consumer_record

from typing import Any

from .conftest import MockRpcClient


class TestConsumerInit:
    """Tests for Consumer initialization."""

    def test_consumer_init_defaults(self, kafka_instance: Kafka) -> None:
        """Test consumer initialization with default settings."""
        consumer = kafka_instance.consumer()

        assert consumer._kafka is kafka_instance
        assert consumer._connected is False
        assert consumer._group_id is None
        assert consumer._auto_commit is True
        assert consumer._auto_commit_interval_ms == 5000
        assert consumer._max_poll_records == 500
        assert consumer._isolation_level == IsolationLevel.READ_UNCOMMITTED
        assert consumer._auto_offset_reset == OffsetResetPolicy.LATEST

    def test_consumer_init_with_group(self, kafka_instance: Kafka) -> None:
        """Test consumer initialization with group ID."""
        consumer = kafka_instance.consumer(group_id="my-group")

        assert consumer._group_id == "my-group"
        assert consumer.group_id == "my-group"

    def test_consumer_init_custom_settings(self, kafka_instance: Kafka) -> None:
        """Test consumer initialization with custom settings."""
        consumer = kafka_instance.consumer(
            group_id="custom-group",
            auto_commit=False,
            auto_commit_interval_ms=10000,
            session_timeout_ms=30000,
            heartbeat_interval_ms=5000,
            max_poll_records=100,
            max_poll_interval_ms=600000,
            isolation_level=IsolationLevel.READ_COMMITTED,
            auto_offset_reset=OffsetResetPolicy.EARLIEST,
            client_id="custom-client",
        )

        assert consumer._group_id == "custom-group"
        assert consumer._auto_commit is False
        assert consumer._auto_commit_interval_ms == 10000
        assert consumer._session_timeout_ms == 30000
        assert consumer._heartbeat_interval_ms == 5000
        assert consumer._max_poll_records == 100
        assert consumer._max_poll_interval_ms == 600000
        assert consumer._isolation_level == IsolationLevel.READ_COMMITTED
        assert consumer._auto_offset_reset == OffsetResetPolicy.EARLIEST
        assert consumer._client_id == "custom-client"

    def test_consumer_connected_property(self, kafka_instance: Kafka) -> None:
        """Test connected property."""
        consumer = kafka_instance.consumer()
        assert consumer.connected is False

        consumer._connected = True
        assert consumer.connected is True

    def test_consumer_subscriptions_property(self, kafka_instance: Kafka) -> None:
        """Test subscriptions property returns a copy."""
        consumer = kafka_instance.consumer()
        consumer._subscriptions = {"topic1", "topic2"}

        subs = consumer.subscriptions
        assert subs == {"topic1", "topic2"}

        # Verify it's a copy
        subs.add("topic3")
        assert "topic3" not in consumer._subscriptions

    def test_consumer_assignments_property(self, kafka_instance: Kafka) -> None:
        """Test assignments property returns a copy."""
        consumer = kafka_instance.consumer()
        tp = TopicPartition("test", 0)
        consumer._assignments = {tp}

        assignments = consumer.assignments
        assert tp in assignments

        # Verify it's a copy
        assignments.add(TopicPartition("test", 1))
        assert len(consumer._assignments) == 1


class TestConsumerConnection:
    """Tests for Consumer connection handling."""

    async def test_connect_success(self, mock_rpc_client: MockRpcClient) -> None:
        """Test successful consumer connection."""
        kafka = Kafka(brokers=["localhost:9092"])
        kafka._client = mock_rpc_client
        consumer = kafka.consumer(group_id="test-group")

        await consumer.connect()

        assert consumer.connected is True
        assert consumer._client is mock_rpc_client

    async def test_connect_already_connected(
        self, connected_consumer: Consumer, mock_rpc_client: MockRpcClient
    ) -> None:
        """Test that connect does nothing if already connected."""
        original_client = connected_consumer._client
        await connected_consumer.connect()

        assert connected_consumer._client is original_client

    async def test_connect_failure(self) -> None:
        """Test connection failure raises KafkaConnectionError."""
        kafka = Kafka(brokers=["localhost:9092"])
        consumer = kafka.consumer()

        with pytest.raises(KafkaConnectionError, match="Failed to connect consumer"):
            await consumer.connect()

    async def test_disconnect(
        self, connected_consumer: Consumer, mock_rpc_client: MockRpcClient
    ) -> None:
        """Test consumer disconnection."""
        connected_consumer._subscriptions = {"topic1"}
        connected_consumer._assignments = {TopicPartition("topic1", 0)}

        await connected_consumer.disconnect()

        assert connected_consumer.connected is False
        assert connected_consumer._client is None
        assert len(connected_consumer._subscriptions) == 0
        assert len(connected_consumer._assignments) == 0

    async def test_disconnect_not_connected(self, kafka_instance: Kafka) -> None:
        """Test disconnect when not connected does nothing."""
        consumer = kafka_instance.consumer()
        await consumer.disconnect()  # Should not raise

        assert consumer.connected is False

    async def test_disconnect_with_auto_commit(
        self, connected_consumer: Consumer, mock_rpc_client: MockRpcClient
    ) -> None:
        """Test disconnect commits offsets when auto-commit enabled."""
        await connected_consumer.disconnect()

        mock_rpc_client.kafka.consumer.commit.assert_called_once()

    async def test_disconnect_with_commit_error(
        self, connected_consumer: Consumer, mock_rpc_client: MockRpcClient
    ) -> None:
        """Test disconnect handles commit errors gracefully."""
        mock_rpc_client.kafka.consumer.commit.side_effect = Exception("Commit failed")

        # Should not raise even if commit fails
        await connected_consumer.disconnect()

        assert connected_consumer.connected is False

    async def test_disconnect_with_poll_task(
        self, connected_consumer: Consumer, mock_rpc_client: MockRpcClient
    ) -> None:
        """Test disconnect cancels poll task if running."""
        import asyncio

        # Create a mock poll task
        async def mock_poll() -> None:
            await asyncio.sleep(10)

        connected_consumer._poll_task = asyncio.create_task(mock_poll())

        await connected_consumer.disconnect()

        assert connected_consumer._poll_task is None
        assert connected_consumer.connected is False

    async def test_context_manager(self, mock_rpc_client: MockRpcClient) -> None:
        """Test async context manager."""
        kafka = Kafka(brokers=["localhost:9092"])
        kafka._client = mock_rpc_client

        async with kafka.consumer(group_id="test") as consumer:
            assert consumer.connected is True

        assert consumer.connected is False


class TestConsumerSubscription:
    """Tests for Consumer subscription operations."""

    async def test_subscribe(
        self, connected_consumer: Consumer, mock_rpc_client: MockRpcClient
    ) -> None:
        """Test subscribing to topics."""
        await connected_consumer.subscribe(["topic1", "topic2"])

        assert connected_consumer.subscriptions == {"topic1", "topic2"}
        mock_rpc_client.kafka.consumer.subscribe.assert_called_once()

    async def test_subscribe_with_pattern(
        self, connected_consumer: Consumer, mock_rpc_client: MockRpcClient
    ) -> None:
        """Test subscribing with pattern."""
        await connected_consumer.subscribe([], pattern="test-.*")

        mock_rpc_client.kafka.consumer.subscribe.assert_called_once()
        call_kwargs = mock_rpc_client.kafka.consumer.subscribe.call_args[1]
        assert call_kwargs["pattern"] == "test-.*"

    async def test_subscribe_not_connected(self, kafka_instance: Kafka) -> None:
        """Test subscribe raises error when not connected."""
        consumer = kafka_instance.consumer()

        with pytest.raises(KafkaConnectionError, match="Consumer not connected"):
            await consumer.subscribe(["topic1"])

    async def test_subscribe_failure(
        self, connected_consumer: Consumer, mock_rpc_client: MockRpcClient
    ) -> None:
        """Test subscribe failure."""
        mock_rpc_client.kafka.consumer.subscribe.side_effect = Exception("Failed")

        with pytest.raises(ConsumerError, match="Failed to subscribe"):
            await connected_consumer.subscribe(["topic1"])

    async def test_unsubscribe(
        self, connected_consumer: Consumer, mock_rpc_client: MockRpcClient
    ) -> None:
        """Test unsubscribing from all topics."""
        connected_consumer._subscriptions = {"topic1"}
        connected_consumer._assignments = {TopicPartition("topic1", 0)}

        await connected_consumer.unsubscribe()

        assert len(connected_consumer.subscriptions) == 0
        assert len(connected_consumer.assignments) == 0

    async def test_unsubscribe_not_connected(self, kafka_instance: Kafka) -> None:
        """Test unsubscribe raises error when not connected."""
        consumer = kafka_instance.consumer()

        with pytest.raises(KafkaConnectionError, match="Consumer not connected"):
            await consumer.unsubscribe()

    async def test_unsubscribe_failure(
        self, connected_consumer: Consumer, mock_rpc_client: MockRpcClient
    ) -> None:
        """Test unsubscribe failure."""
        mock_rpc_client.kafka.consumer.unsubscribe.side_effect = Exception("Failed")

        with pytest.raises(ConsumerError, match="Failed to unsubscribe"):
            await connected_consumer.unsubscribe()


class TestConsumerAssignment:
    """Tests for Consumer manual assignment."""

    async def test_assign(
        self, connected_consumer: Consumer, mock_rpc_client: MockRpcClient
    ) -> None:
        """Test manual partition assignment."""
        partitions = [TopicPartition("topic1", 0), TopicPartition("topic1", 1)]
        await connected_consumer.assign(partitions)

        assert connected_consumer.assignments == set(partitions)

    async def test_assign_not_connected(self, kafka_instance: Kafka) -> None:
        """Test assign raises error when not connected."""
        consumer = kafka_instance.consumer()

        with pytest.raises(KafkaConnectionError, match="Consumer not connected"):
            await consumer.assign([TopicPartition("topic", 0)])

    async def test_assign_failure(
        self, connected_consumer: Consumer, mock_rpc_client: MockRpcClient
    ) -> None:
        """Test assign failure."""
        mock_rpc_client.kafka.consumer.assign.side_effect = Exception("Failed")

        with pytest.raises(ConsumerError, match="Failed to assign partitions"):
            await connected_consumer.assign([TopicPartition("topic", 0)])


class TestConsumerPoll:
    """Tests for Consumer poll operations."""

    async def test_poll_empty(
        self, connected_consumer: Consumer, mock_rpc_client: MockRpcClient
    ) -> None:
        """Test polling with no messages."""
        records = await connected_consumer.poll()

        assert records == []

    async def test_poll_with_messages(
        self, connected_consumer: Consumer, mock_rpc_client: MockRpcClient
    ) -> None:
        """Test polling with messages."""
        mock_rpc_client.kafka.consumer.poll.return_value = [
            {
                "topic": "test-topic",
                "partition": 0,
                "offset": 42,
                "timestamp": 1234567890,
                "key": base64.b64encode(b"key1").decode(),
                "value": base64.b64encode(b"Hello!").decode(),
                "headers": [],
            },
        ]

        records = await connected_consumer.poll()

        assert len(records) == 1
        assert isinstance(records[0], ConsumerRecord)
        assert records[0].topic == "test-topic"
        assert records[0].value == b"Hello!"
        assert records[0].key == b"key1"

    async def test_poll_with_timeout(
        self, connected_consumer: Consumer, mock_rpc_client: MockRpcClient
    ) -> None:
        """Test polling with timeout."""
        await connected_consumer.poll(timeout_ms=5000)

        call_kwargs = mock_rpc_client.kafka.consumer.poll.call_args[1]
        assert call_kwargs["timeoutMs"] == 5000

    async def test_poll_with_max_records(
        self, connected_consumer: Consumer, mock_rpc_client: MockRpcClient
    ) -> None:
        """Test polling with max records."""
        await connected_consumer.poll(max_records=10)

        call_kwargs = mock_rpc_client.kafka.consumer.poll.call_args[1]
        assert call_kwargs["maxRecords"] == 10

    async def test_poll_not_connected(self, kafka_instance: Kafka) -> None:
        """Test poll raises error when not connected."""
        consumer = kafka_instance.consumer()

        with pytest.raises(KafkaConnectionError, match="Consumer not connected"):
            await consumer.poll()

    async def test_poll_failure(
        self, connected_consumer: Consumer, mock_rpc_client: MockRpcClient
    ) -> None:
        """Test poll failure."""
        mock_rpc_client.kafka.consumer.poll.side_effect = Exception("Failed")

        with pytest.raises(ConsumerError, match="Failed to poll"):
            await connected_consumer.poll()


class TestConsumerCommit:
    """Tests for Consumer commit operations."""

    async def test_commit_default(
        self, connected_consumer: Consumer, mock_rpc_client: MockRpcClient
    ) -> None:
        """Test commit without specific offsets."""
        await connected_consumer.commit()

        mock_rpc_client.kafka.consumer.commit.assert_called_once()

    async def test_commit_specific_offsets(
        self, connected_consumer: Consumer, mock_rpc_client: MockRpcClient
    ) -> None:
        """Test commit with specific offsets."""
        offsets = {TopicPartition("topic1", 0): 100, TopicPartition("topic1", 1): 200}
        await connected_consumer.commit(offsets)

        call_kwargs = mock_rpc_client.kafka.consumer.commit.call_args[1]
        assert len(call_kwargs["offsets"]) == 2

    async def test_commit_not_connected(self, kafka_instance: Kafka) -> None:
        """Test commit raises error when not connected."""
        consumer = kafka_instance.consumer()

        with pytest.raises(KafkaConnectionError, match="Consumer not connected"):
            await consumer.commit()

    async def test_commit_failure(
        self, connected_consumer: Consumer, mock_rpc_client: MockRpcClient
    ) -> None:
        """Test commit failure."""
        mock_rpc_client.kafka.consumer.commit.side_effect = Exception("Failed")

        with pytest.raises(ConsumerError, match="Failed to commit"):
            await connected_consumer.commit()

    async def test_committed(
        self, connected_consumer: Consumer, mock_rpc_client: MockRpcClient
    ) -> None:
        """Test getting committed offsets."""
        partitions = [TopicPartition("test-topic", 0)]
        result = await connected_consumer.committed(partitions)

        assert TopicPartition("test-topic", 0) in result
        assert result[TopicPartition("test-topic", 0)] == 100

    async def test_committed_not_connected(self, kafka_instance: Kafka) -> None:
        """Test committed raises error when not connected."""
        consumer = kafka_instance.consumer()

        with pytest.raises(KafkaConnectionError, match="Consumer not connected"):
            await consumer.committed([TopicPartition("topic", 0)])

    async def test_committed_failure(
        self, connected_consumer: Consumer, mock_rpc_client: MockRpcClient
    ) -> None:
        """Test committed failure."""
        mock_rpc_client.kafka.consumer.committed.side_effect = Exception("Failed")

        with pytest.raises(ConsumerError, match="Failed to get committed offsets"):
            await connected_consumer.committed([TopicPartition("topic", 0)])


class TestConsumerSeek:
    """Tests for Consumer seek operations."""

    async def test_seek(
        self, connected_consumer: Consumer, mock_rpc_client: MockRpcClient
    ) -> None:
        """Test seeking to a specific offset."""
        await connected_consumer.seek(TopicPartition("topic1", 0), 100)

        mock_rpc_client.kafka.consumer.seek.assert_called_once_with(
            topic="topic1", partition=0, offset=100
        )

    async def test_seek_not_connected(self, kafka_instance: Kafka) -> None:
        """Test seek raises error when not connected."""
        consumer = kafka_instance.consumer()

        with pytest.raises(KafkaConnectionError, match="Consumer not connected"):
            await consumer.seek(TopicPartition("topic", 0), 100)

    async def test_seek_failure(
        self, connected_consumer: Consumer, mock_rpc_client: MockRpcClient
    ) -> None:
        """Test seek failure."""
        mock_rpc_client.kafka.consumer.seek.side_effect = Exception("Failed")

        with pytest.raises(ConsumerError, match="Failed to seek"):
            await connected_consumer.seek(TopicPartition("topic", 0), 100)

    async def test_seek_to_beginning(
        self, connected_consumer: Consumer, mock_rpc_client: MockRpcClient
    ) -> None:
        """Test seeking to beginning."""
        partitions = [TopicPartition("topic1", 0)]
        await connected_consumer.seek_to_beginning(partitions)

        mock_rpc_client.kafka.consumer.seekToBeginning.assert_called_once()

    async def test_seek_to_beginning_all(
        self, connected_consumer: Consumer, mock_rpc_client: MockRpcClient
    ) -> None:
        """Test seeking to beginning for all partitions."""
        await connected_consumer.seek_to_beginning()

        mock_rpc_client.kafka.consumer.seekToBeginning.assert_called_once()

    async def test_seek_to_beginning_not_connected(self, kafka_instance: Kafka) -> None:
        """Test seek_to_beginning raises error when not connected."""
        consumer = kafka_instance.consumer()

        with pytest.raises(KafkaConnectionError, match="Consumer not connected"):
            await consumer.seek_to_beginning()

    async def test_seek_to_beginning_failure(
        self, connected_consumer: Consumer, mock_rpc_client: MockRpcClient
    ) -> None:
        """Test seek_to_beginning failure."""
        mock_rpc_client.kafka.consumer.seekToBeginning.side_effect = Exception("Failed")

        with pytest.raises(ConsumerError, match="Failed to seek to beginning"):
            await connected_consumer.seek_to_beginning()

    async def test_seek_to_end(
        self, connected_consumer: Consumer, mock_rpc_client: MockRpcClient
    ) -> None:
        """Test seeking to end."""
        partitions = [TopicPartition("topic1", 0)]
        await connected_consumer.seek_to_end(partitions)

        mock_rpc_client.kafka.consumer.seekToEnd.assert_called_once()

    async def test_seek_to_end_all(
        self, connected_consumer: Consumer, mock_rpc_client: MockRpcClient
    ) -> None:
        """Test seeking to end for all partitions."""
        await connected_consumer.seek_to_end()

        mock_rpc_client.kafka.consumer.seekToEnd.assert_called_once()

    async def test_seek_to_end_not_connected(self, kafka_instance: Kafka) -> None:
        """Test seek_to_end raises error when not connected."""
        consumer = kafka_instance.consumer()

        with pytest.raises(KafkaConnectionError, match="Consumer not connected"):
            await consumer.seek_to_end()

    async def test_seek_to_end_failure(
        self, connected_consumer: Consumer, mock_rpc_client: MockRpcClient
    ) -> None:
        """Test seek_to_end failure."""
        mock_rpc_client.kafka.consumer.seekToEnd.side_effect = Exception("Failed")

        with pytest.raises(ConsumerError, match="Failed to seek to end"):
            await connected_consumer.seek_to_end()


class TestConsumerPosition:
    """Tests for Consumer position operations."""

    async def test_position(
        self, connected_consumer: Consumer, mock_rpc_client: MockRpcClient
    ) -> None:
        """Test getting current position."""
        position = await connected_consumer.position(TopicPartition("topic1", 0))

        assert position == 42

    async def test_position_not_connected(self, kafka_instance: Kafka) -> None:
        """Test position raises error when not connected."""
        consumer = kafka_instance.consumer()

        with pytest.raises(KafkaConnectionError, match="Consumer not connected"):
            await consumer.position(TopicPartition("topic", 0))

    async def test_position_failure(
        self, connected_consumer: Consumer, mock_rpc_client: MockRpcClient
    ) -> None:
        """Test position failure."""
        mock_rpc_client.kafka.consumer.position.side_effect = Exception("Failed")

        with pytest.raises(ConsumerError, match="Failed to get position"):
            await connected_consumer.position(TopicPartition("topic", 0))


class TestConsumerPauseResume:
    """Tests for Consumer pause/resume operations."""

    async def test_pause(
        self, connected_consumer: Consumer, mock_rpc_client: MockRpcClient
    ) -> None:
        """Test pausing partitions."""
        partitions = [TopicPartition("topic1", 0), TopicPartition("topic1", 1)]
        await connected_consumer.pause(partitions)

        assert connected_consumer.paused() == set(partitions)

    async def test_pause_not_connected(self, kafka_instance: Kafka) -> None:
        """Test pause raises error when not connected."""
        consumer = kafka_instance.consumer()

        with pytest.raises(KafkaConnectionError, match="Consumer not connected"):
            await consumer.pause([TopicPartition("topic", 0)])

    async def test_pause_failure(
        self, connected_consumer: Consumer, mock_rpc_client: MockRpcClient
    ) -> None:
        """Test pause failure."""
        mock_rpc_client.kafka.consumer.pause.side_effect = Exception("Failed")

        with pytest.raises(ConsumerError, match="Failed to pause"):
            await connected_consumer.pause([TopicPartition("topic", 0)])

    async def test_resume(
        self, connected_consumer: Consumer, mock_rpc_client: MockRpcClient
    ) -> None:
        """Test resuming partitions."""
        tp = TopicPartition("topic1", 0)
        connected_consumer._paused = {tp}

        await connected_consumer.resume([tp])

        assert connected_consumer.paused() == set()

    async def test_resume_not_connected(self, kafka_instance: Kafka) -> None:
        """Test resume raises error when not connected."""
        consumer = kafka_instance.consumer()

        with pytest.raises(KafkaConnectionError, match="Consumer not connected"):
            await consumer.resume([TopicPartition("topic", 0)])

    async def test_resume_failure(
        self, connected_consumer: Consumer, mock_rpc_client: MockRpcClient
    ) -> None:
        """Test resume failure."""
        mock_rpc_client.kafka.consumer.resume.side_effect = Exception("Failed")

        with pytest.raises(ConsumerError, match="Failed to resume"):
            await connected_consumer.resume([TopicPartition("topic", 0)])


class TestConsumerIteration:
    """Tests for Consumer async iteration."""

    async def test_async_iteration(
        self, connected_consumer: Consumer, mock_rpc_client: MockRpcClient
    ) -> None:
        """Test async iteration over messages."""
        mock_rpc_client.kafka.consumer.poll.return_value = [
            {
                "topic": "test-topic",
                "partition": 0,
                "offset": 42,
                "timestamp": 1234567890,
                "key": None,
                "value": base64.b64encode(b"Hello!").decode(),
                "headers": [],
            },
        ]

        messages = []
        count = 0
        async for message in connected_consumer:
            messages.append(message)
            count += 1
            if count >= 1:
                connected_consumer._closed = True
                break

        assert len(messages) == 1
        assert messages[0].value == b"Hello!"

    async def test_iteration_stops_when_closed(
        self, connected_consumer: Consumer, mock_rpc_client: MockRpcClient
    ) -> None:
        """Test that iteration stops when consumer is closed."""
        connected_consumer._closed = True

        messages = []
        async for message in connected_consumer:
            messages.append(message)

        assert len(messages) == 0

    async def test_iteration_stops_when_disconnected(
        self, kafka_instance: Kafka, mock_rpc_client: MockRpcClient
    ) -> None:
        """Test that iteration stops when consumer is disconnected."""
        consumer = kafka_instance.consumer()  # Not connected

        messages = []
        async for message in consumer:
            messages.append(message)

        assert len(messages) == 0

    async def test_iteration_error(
        self, connected_consumer: Consumer, mock_rpc_client: MockRpcClient
    ) -> None:
        """Test iteration error handling."""
        mock_rpc_client.kafka.consumer.poll.side_effect = Exception("Poll failed")

        with pytest.raises(ConsumerError, match="Iteration failed"):
            async for _ in connected_consumer:
                pass

    async def test_iteration_closes_during_poll(
        self, connected_consumer: Consumer, mock_rpc_client: MockRpcClient
    ) -> None:
        """Test iteration stops when consumer closes during poll."""
        call_count = 0

        async def poll_and_close(*args: Any, **kwargs: Any) -> list[Any]:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                # Return empty on first call to trigger while loop
                return []
            # Close consumer on subsequent calls
            connected_consumer._closed = True
            return []

        mock_rpc_client.kafka.consumer.poll.side_effect = poll_and_close

        messages = []
        async for message in connected_consumer:
            messages.append(message)

        assert len(messages) == 0

    async def test_iteration_closes_during_error(
        self, connected_consumer: Consumer, mock_rpc_client: MockRpcClient
    ) -> None:
        """Test iteration stops gracefully when closed during error handling."""

        async def poll_raise_after_close(*args: Any, **kwargs: Any) -> list[Any]:
            connected_consumer._closed = True
            raise Exception("Poll failed but consumer is closed")

        mock_rpc_client.kafka.consumer.poll.side_effect = poll_raise_after_close

        # Should stop iteration gracefully instead of raising
        messages = []
        async for message in connected_consumer:
            messages.append(message)

        assert len(messages) == 0


class TestDeserializeConsumerRecord:
    """Tests for deserialize_consumer_record helper function."""

    def test_deserialize_basic_record(self) -> None:
        """Test deserializing a basic record."""
        data = {
            "topic": "test",
            "partition": 0,
            "offset": 42,
            "timestamp": 1234567890,
            "key": None,
            "value": base64.b64encode(b"Hello").decode(),
        }

        record = deserialize_consumer_record(data)

        assert record.topic == "test"
        assert record.partition == 0
        assert record.offset == 42
        assert record.timestamp == 1234567890
        assert record.key is None
        assert record.value == b"Hello"
        assert record.headers == []

    def test_deserialize_record_with_key(self) -> None:
        """Test deserializing a record with key."""
        data = {
            "topic": "test",
            "partition": 0,
            "offset": 42,
            "timestamp": None,
            "key": base64.b64encode(b"my-key").decode(),
            "value": base64.b64encode(b"Hello").decode(),
        }

        record = deserialize_consumer_record(data)

        assert record.key == b"my-key"

    def test_deserialize_record_with_headers(self) -> None:
        """Test deserializing a record with headers."""
        data = {
            "topic": "test",
            "partition": 0,
            "offset": 42,
            "timestamp": None,
            "key": None,
            "value": base64.b64encode(b"Hello").decode(),
            "headers": [
                {"key": "h1", "value": base64.b64encode(b"v1").decode()},
                {"key": "h2", "value": base64.b64encode(b"v2").decode()},
            ],
        }

        record = deserialize_consumer_record(data)

        assert len(record.headers) == 2
        assert record.headers[0] == ("h1", b"v1")
        assert record.headers[1] == ("h2", b"v2")
