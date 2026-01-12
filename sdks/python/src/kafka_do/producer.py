"""
Kafka Producer implementation for kafka-do SDK.

Provides an async producer with kafka-python compatible API
using rpc-do for the underlying transport.
"""

from __future__ import annotations

import base64
from typing import TYPE_CHECKING, Any

from .types import (
    BatchResult,
    CompressionType,
    KafkaConnectionError,
    KafkaError,
    ProducerError,
    ProducerRecord,
    RecordMetadata,
    deserialize_record_metadata,
    serialize_record,
)

if TYPE_CHECKING:
    from .kafka import Kafka


class Producer:
    """
    Async Kafka producer for sending messages to Kafka topics.

    The producer connects to a Kafka service via rpc-do and provides
    methods for sending individual messages or batches.

    Example:
        kafka = Kafka(brokers=['localhost:9092'])
        producer = kafka.producer()
        await producer.connect()

        # Send a single message
        metadata = await producer.send('my-topic', value=b'Hello!')

        # Send with key
        metadata = await producer.send('my-topic', key=b'key1', value=b'message')

        # Send batch
        result = await producer.send_batch([
            {'topic': 'topic1', 'value': b'msg1'},
            {'topic': 'topic2', 'value': b'msg2'}
        ])

        await producer.disconnect()
    """

    __slots__ = (
        "_kafka",
        "_client",
        "_connected",
        "_acks",
        "_compression_type",
        "_batch_size",
        "_linger_ms",
        "_client_id",
    )

    def __init__(
        self,
        kafka: Kafka,
        *,
        acks: int | str = 1,
        compression_type: CompressionType = CompressionType.NONE,
        batch_size: int = 16384,
        linger_ms: int = 0,
        client_id: str | None = None,
    ) -> None:
        """
        Initialize the producer.

        Args:
            kafka: The parent Kafka instance
            acks: Number of acknowledgments required (0, 1, or 'all')
            compression_type: Compression type for messages
            batch_size: Maximum batch size in bytes
            linger_ms: Time to wait before sending batched messages
            client_id: Optional client identifier
        """
        self._kafka = kafka
        self._client: Any = None
        self._connected = False
        self._acks = acks
        self._compression_type = compression_type
        self._batch_size = batch_size
        self._linger_ms = linger_ms
        self._client_id = client_id

    @property
    def connected(self) -> bool:
        """Return whether the producer is connected."""
        return self._connected

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
        except Exception as e:
            raise KafkaConnectionError(f"Failed to connect producer: {e}") from e

    async def disconnect(self) -> None:
        """
        Disconnect from the Kafka service.

        This will flush any pending messages before disconnecting.
        """
        if not self._connected:
            return

        try:
            await self.flush()
        finally:
            self._connected = False
            self._client = None

    async def send(
        self,
        topic: str,
        value: bytes,
        key: bytes | None = None,
        partition: int | None = None,
        timestamp: int | None = None,
        headers: list[tuple[str, bytes]] | None = None,
    ) -> RecordMetadata:
        """
        Send a single message to a Kafka topic.

        Args:
            topic: The topic to send to
            value: The message value
            key: Optional message key (used for partitioning)
            partition: Optional specific partition to send to
            timestamp: Optional message timestamp (milliseconds since epoch)
            headers: Optional message headers

        Returns:
            RecordMetadata with information about the sent message

        Raises:
            KafkaConnectionError: If not connected
            ProducerError: If send fails
        """
        if not self._connected:
            raise KafkaConnectionError("Producer not connected")

        record = ProducerRecord(
            topic=topic,
            value=value,
            key=key,
            partition=partition,
            timestamp=timestamp,
            headers=headers or [],
        )

        try:
            result = await self._client.kafka.producer.send(serialize_record(record))
            return deserialize_record_metadata(result)
        except Exception as e:
            raise ProducerError(f"Failed to send message: {e}") from e

    async def send_batch(
        self,
        records: list[dict[str, Any] | ProducerRecord],
    ) -> BatchResult:
        """
        Send a batch of messages.

        Args:
            records: List of records to send. Each record can be either:
                - A ProducerRecord instance
                - A dict with keys: topic, value, and optionally key, partition, timestamp, headers

        Returns:
            BatchResult with successful and failed records

        Raises:
            KafkaConnectionError: If not connected
        """
        if not self._connected:
            raise KafkaConnectionError("Producer not connected")

        # Convert dicts to ProducerRecords
        producer_records: list[ProducerRecord] = []
        for record in records:
            if isinstance(record, ProducerRecord):
                producer_records.append(record)
            else:
                producer_records.append(
                    ProducerRecord(
                        topic=record["topic"],
                        value=record["value"],
                        key=record.get("key"),
                        partition=record.get("partition"),
                        timestamp=record.get("timestamp"),
                        headers=record.get("headers", []),
                    )
                )

        # Serialize all records
        serialized = [serialize_record(r) for r in producer_records]

        try:
            results = await self._client.kafka.producer.sendBatch(serialized)
            batch_result = BatchResult()

            for i, result in enumerate(results):
                if result.get("error"):
                    error = KafkaError(
                        result["error"].get("message", "Unknown error"),
                        code=result["error"].get("code"),
                    )
                    batch_result.failed.append((producer_records[i], error))
                else:
                    batch_result.successful.append(deserialize_record_metadata(result))

            return batch_result
        except Exception as e:
            # If the entire batch fails, mark all as failed
            batch_result = BatchResult()
            error = ProducerError(f"Batch send failed: {e}")
            for record in producer_records:
                batch_result.failed.append((record, error))
            return batch_result

    async def flush(self, timeout: float | None = None) -> None:
        """
        Flush any pending messages.

        Args:
            timeout: Maximum time to wait for flush (None for default)
        """
        if not self._connected:
            return

        try:
            await self._client.kafka.producer.flush(timeout=timeout)
        except Exception:
            # Ignore flush errors on disconnect
            pass

    async def partitions_for(self, topic: str) -> list[int]:
        """
        Get the list of partitions for a topic.

        Args:
            topic: The topic name

        Returns:
            List of partition numbers

        Raises:
            KafkaConnectionError: If not connected
            KafkaError: If the operation fails
        """
        if not self._connected:
            raise KafkaConnectionError("Producer not connected")

        try:
            result = await self._client.kafka.producer.partitionsFor(topic)
            return list(result)
        except Exception as e:
            raise KafkaError(f"Failed to get partitions: {e}") from e

    async def __aenter__(self) -> Producer:
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
