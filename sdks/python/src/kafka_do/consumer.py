"""
Kafka Consumer implementation for kafka-do SDK.

Provides an async consumer with kafka-python compatible API
using rpc-do for the underlying transport.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any, AsyncIterator

from .types import (
    ConsumerError,
    ConsumerRecord,
    IsolationLevel,
    KafkaConnectionError,
    KafkaError,
    OffsetResetPolicy,
    TopicPartition,
    deserialize_consumer_record,
)

if TYPE_CHECKING:
    from .kafka import Kafka


class Consumer:
    """
    Async Kafka consumer for consuming messages from Kafka topics.

    The consumer connects to a Kafka service via rpc-do and provides
    methods for subscribing to topics and consuming messages.

    Supports async iteration for convenient message consumption:

    Example:
        kafka = Kafka(brokers=['localhost:9092'])
        consumer = kafka.consumer(group_id='my-group')
        await consumer.connect()
        await consumer.subscribe(['my-topic'])

        async for message in consumer:
            print(f"Received: {message.value}")
            await consumer.commit()

        await consumer.disconnect()
    """

    __slots__ = (
        "_kafka",
        "_client",
        "_connected",
        "_group_id",
        "_auto_commit",
        "_auto_commit_interval_ms",
        "_session_timeout_ms",
        "_heartbeat_interval_ms",
        "_max_poll_records",
        "_max_poll_interval_ms",
        "_isolation_level",
        "_auto_offset_reset",
        "_client_id",
        "_subscriptions",
        "_assignments",
        "_paused",
        "_message_queue",
        "_poll_task",
        "_closed",
    )

    def __init__(
        self,
        kafka: Kafka,
        *,
        group_id: str | None = None,
        auto_commit: bool = True,
        auto_commit_interval_ms: int = 5000,
        session_timeout_ms: int = 10000,
        heartbeat_interval_ms: int = 3000,
        max_poll_records: int = 500,
        max_poll_interval_ms: int = 300000,
        isolation_level: IsolationLevel = IsolationLevel.READ_UNCOMMITTED,
        auto_offset_reset: OffsetResetPolicy = OffsetResetPolicy.LATEST,
        client_id: str | None = None,
    ) -> None:
        """
        Initialize the consumer.

        Args:
            kafka: The parent Kafka instance
            group_id: Consumer group ID (required for group coordination)
            auto_commit: Enable auto-commit of offsets
            auto_commit_interval_ms: Interval for auto-commit in milliseconds
            session_timeout_ms: Session timeout for group membership
            heartbeat_interval_ms: Heartbeat interval for group coordination
            max_poll_records: Maximum records per poll
            max_poll_interval_ms: Maximum interval between polls
            isolation_level: Transaction isolation level
            auto_offset_reset: Offset reset policy for new consumers
            client_id: Optional client identifier
        """
        self._kafka = kafka
        self._client: Any = None
        self._connected = False
        self._group_id = group_id
        self._auto_commit = auto_commit
        self._auto_commit_interval_ms = auto_commit_interval_ms
        self._session_timeout_ms = session_timeout_ms
        self._heartbeat_interval_ms = heartbeat_interval_ms
        self._max_poll_records = max_poll_records
        self._max_poll_interval_ms = max_poll_interval_ms
        self._isolation_level = isolation_level
        self._auto_offset_reset = auto_offset_reset
        self._client_id = client_id
        self._subscriptions: set[str] = set()
        self._assignments: set[TopicPartition] = set()
        self._paused: set[TopicPartition] = set()
        self._message_queue: asyncio.Queue[ConsumerRecord] = asyncio.Queue()
        self._poll_task: asyncio.Task[None] | None = None
        self._closed = False

    @property
    def connected(self) -> bool:
        """Return whether the consumer is connected."""
        return self._connected

    @property
    def group_id(self) -> str | None:
        """Return the consumer group ID."""
        return self._group_id

    @property
    def subscriptions(self) -> set[str]:
        """Return the set of subscribed topics."""
        return self._subscriptions.copy()

    @property
    def assignments(self) -> set[TopicPartition]:
        """Return the set of assigned partitions."""
        return self._assignments.copy()

    async def connect(self) -> None:
        """
        Connect to the Kafka service.

        Raises:
            KafkaConnectionError: If connection fails
        """
        if self._connected:
            return

        try:
            self._client = await self._kafka._get_client()
            self._connected = True
            self._closed = False
        except Exception as e:
            raise KafkaConnectionError(f"Failed to connect consumer: {e}") from e

    async def disconnect(self) -> None:
        """
        Disconnect from the Kafka service.

        This will commit any pending offsets if auto-commit is enabled.
        """
        if not self._connected:
            return

        self._closed = True

        # Stop polling
        if self._poll_task is not None:
            self._poll_task.cancel()
            try:
                await self._poll_task
            except asyncio.CancelledError:
                pass
            self._poll_task = None

        # Commit final offsets if auto-commit
        if self._auto_commit and self._group_id:
            try:
                await self.commit()
            except Exception:
                pass

        self._connected = False
        self._client = None
        self._subscriptions.clear()
        self._assignments.clear()
        self._paused.clear()

    async def subscribe(
        self,
        topics: list[str],
        *,
        pattern: str | None = None,
    ) -> None:
        """
        Subscribe to topics.

        Args:
            topics: List of topic names to subscribe to
            pattern: Optional regex pattern for topic matching

        Raises:
            KafkaConnectionError: If not connected
            ConsumerError: If subscription fails
        """
        if not self._connected:
            raise KafkaConnectionError("Consumer not connected")

        try:
            await self._client.kafka.consumer.subscribe(
                topics=topics,
                pattern=pattern,
                groupId=self._group_id,
                autoOffsetReset=self._auto_offset_reset.value,
            )
            self._subscriptions = set(topics)
        except Exception as e:
            raise ConsumerError(f"Failed to subscribe: {e}") from e

    async def unsubscribe(self) -> None:
        """
        Unsubscribe from all topics.

        Raises:
            KafkaConnectionError: If not connected
        """
        if not self._connected:
            raise KafkaConnectionError("Consumer not connected")

        try:
            await self._client.kafka.consumer.unsubscribe()
            self._subscriptions.clear()
            self._assignments.clear()
        except Exception as e:
            raise ConsumerError(f"Failed to unsubscribe: {e}") from e

    async def assign(self, partitions: list[TopicPartition]) -> None:
        """
        Manually assign partitions to this consumer.

        Args:
            partitions: List of TopicPartition to assign

        Raises:
            KafkaConnectionError: If not connected
            ConsumerError: If assignment fails
        """
        if not self._connected:
            raise KafkaConnectionError("Consumer not connected")

        try:
            partition_data = [{"topic": p.topic, "partition": p.partition} for p in partitions]
            await self._client.kafka.consumer.assign(partitions=partition_data)
            self._assignments = set(partitions)
        except Exception as e:
            raise ConsumerError(f"Failed to assign partitions: {e}") from e

    async def poll(
        self,
        timeout_ms: int = 0,
        max_records: int | None = None,
    ) -> list[ConsumerRecord]:
        """
        Poll for messages.

        Args:
            timeout_ms: Maximum time to wait for messages (0 for no wait)
            max_records: Maximum number of records to return

        Returns:
            List of ConsumerRecord objects

        Raises:
            KafkaConnectionError: If not connected
            ConsumerError: If poll fails
        """
        if not self._connected:
            raise KafkaConnectionError("Consumer not connected")

        try:
            result = await self._client.kafka.consumer.poll(
                timeoutMs=timeout_ms,
                maxRecords=max_records or self._max_poll_records,
            )

            records = []
            for record_data in result:
                records.append(deserialize_consumer_record(record_data))

            return records
        except Exception as e:
            raise ConsumerError(f"Failed to poll: {e}") from e

    async def commit(
        self,
        offsets: dict[TopicPartition, int] | None = None,
    ) -> None:
        """
        Commit offsets.

        Args:
            offsets: Optional map of TopicPartition to offset to commit.
                     If None, commits current positions for all assigned partitions.

        Raises:
            KafkaConnectionError: If not connected
            ConsumerError: If commit fails
        """
        if not self._connected:
            raise KafkaConnectionError("Consumer not connected")

        try:
            if offsets:
                offset_data = [
                    {"topic": tp.topic, "partition": tp.partition, "offset": offset}
                    for tp, offset in offsets.items()
                ]
                await self._client.kafka.consumer.commit(offsets=offset_data)
            else:
                await self._client.kafka.consumer.commit()
        except Exception as e:
            raise ConsumerError(f"Failed to commit: {e}") from e

    async def committed(
        self,
        partitions: list[TopicPartition],
    ) -> dict[TopicPartition, int | None]:
        """
        Get committed offsets for partitions.

        Args:
            partitions: List of partitions to query

        Returns:
            Map of TopicPartition to committed offset (or None if not committed)

        Raises:
            KafkaConnectionError: If not connected
        """
        if not self._connected:
            raise KafkaConnectionError("Consumer not connected")

        try:
            partition_data = [{"topic": p.topic, "partition": p.partition} for p in partitions]
            result = await self._client.kafka.consumer.committed(partitions=partition_data)

            committed_offsets: dict[TopicPartition, int | None] = {}
            for item in result:
                tp = TopicPartition(topic=item["topic"], partition=item["partition"])
                committed_offsets[tp] = item.get("offset")

            return committed_offsets
        except Exception as e:
            raise ConsumerError(f"Failed to get committed offsets: {e}") from e

    async def seek(self, partition: TopicPartition, offset: int) -> None:
        """
        Seek to a specific offset.

        Args:
            partition: The partition to seek
            offset: The offset to seek to

        Raises:
            KafkaConnectionError: If not connected
            ConsumerError: If seek fails
        """
        if not self._connected:
            raise KafkaConnectionError("Consumer not connected")

        try:
            await self._client.kafka.consumer.seek(
                topic=partition.topic,
                partition=partition.partition,
                offset=offset,
            )
        except Exception as e:
            raise ConsumerError(f"Failed to seek: {e}") from e

    async def seek_to_beginning(
        self,
        partitions: list[TopicPartition] | None = None,
    ) -> None:
        """
        Seek to the beginning of partitions.

        Args:
            partitions: List of partitions (or None for all assigned)

        Raises:
            KafkaConnectionError: If not connected
        """
        if not self._connected:
            raise KafkaConnectionError("Consumer not connected")

        try:
            if partitions:
                partition_data = [{"topic": p.topic, "partition": p.partition} for p in partitions]
                await self._client.kafka.consumer.seekToBeginning(partitions=partition_data)
            else:
                await self._client.kafka.consumer.seekToBeginning()
        except Exception as e:
            raise ConsumerError(f"Failed to seek to beginning: {e}") from e

    async def seek_to_end(
        self,
        partitions: list[TopicPartition] | None = None,
    ) -> None:
        """
        Seek to the end of partitions.

        Args:
            partitions: List of partitions (or None for all assigned)

        Raises:
            KafkaConnectionError: If not connected
        """
        if not self._connected:
            raise KafkaConnectionError("Consumer not connected")

        try:
            if partitions:
                partition_data = [{"topic": p.topic, "partition": p.partition} for p in partitions]
                await self._client.kafka.consumer.seekToEnd(partitions=partition_data)
            else:
                await self._client.kafka.consumer.seekToEnd()
        except Exception as e:
            raise ConsumerError(f"Failed to seek to end: {e}") from e

    async def position(self, partition: TopicPartition) -> int:
        """
        Get the current position (offset) for a partition.

        Args:
            partition: The partition to query

        Returns:
            Current offset position

        Raises:
            KafkaConnectionError: If not connected
        """
        if not self._connected:
            raise KafkaConnectionError("Consumer not connected")

        try:
            result = await self._client.kafka.consumer.position(
                topic=partition.topic,
                partition=partition.partition,
            )
            return int(result)
        except Exception as e:
            raise ConsumerError(f"Failed to get position: {e}") from e

    async def pause(self, partitions: list[TopicPartition]) -> None:
        """
        Pause consumption from partitions.

        Args:
            partitions: List of partitions to pause

        Raises:
            KafkaConnectionError: If not connected
        """
        if not self._connected:
            raise KafkaConnectionError("Consumer not connected")

        try:
            partition_data = [{"topic": p.topic, "partition": p.partition} for p in partitions]
            await self._client.kafka.consumer.pause(partitions=partition_data)
            self._paused.update(partitions)
        except Exception as e:
            raise ConsumerError(f"Failed to pause: {e}") from e

    async def resume(self, partitions: list[TopicPartition]) -> None:
        """
        Resume consumption from paused partitions.

        Args:
            partitions: List of partitions to resume

        Raises:
            KafkaConnectionError: If not connected
        """
        if not self._connected:
            raise KafkaConnectionError("Consumer not connected")

        try:
            partition_data = [{"topic": p.topic, "partition": p.partition} for p in partitions]
            await self._client.kafka.consumer.resume(partitions=partition_data)
            self._paused.difference_update(partitions)
        except Exception as e:
            raise ConsumerError(f"Failed to resume: {e}") from e

    def paused(self) -> set[TopicPartition]:
        """
        Get the set of paused partitions.

        Returns:
            Set of paused TopicPartition objects
        """
        return self._paused.copy()

    async def __aenter__(self) -> Consumer:
        """Async context manager entry."""
        await self.connect()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        """Async context manager exit."""
        await self.disconnect()

    def __aiter__(self) -> AsyncIterator[ConsumerRecord]:
        """Return async iterator for message consumption."""
        return self

    async def __anext__(self) -> ConsumerRecord:
        """
        Get the next message from the subscribed topics.

        Returns:
            Next ConsumerRecord

        Raises:
            StopAsyncIteration: When consumer is closed
            ConsumerError: If polling fails
        """
        if self._closed or not self._connected:
            raise StopAsyncIteration

        # Poll for messages if queue is empty
        while self._message_queue.empty():
            if self._closed or not self._connected:
                raise StopAsyncIteration

            try:
                records = await self.poll(timeout_ms=1000)
                for record in records:
                    await self._message_queue.put(record)
            except Exception as e:
                if self._closed:
                    raise StopAsyncIteration
                raise ConsumerError(f"Iteration failed: {e}") from e

        return await self._message_queue.get()
