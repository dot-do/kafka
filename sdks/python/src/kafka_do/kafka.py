"""
Main Kafka class for kafka-do SDK.

Provides the entry point for creating producers, consumers, and admin clients.
"""

from __future__ import annotations

from typing import Any

from .admin import Admin
from .consumer import Consumer
from .producer import Producer
from .types import (
    CompressionType,
    IsolationLevel,
    KafkaConnectionError,
    OffsetResetPolicy,
)


class Kafka:
    """
    Main Kafka client class.

    The Kafka class serves as the entry point for creating producers,
    consumers, and admin clients that connect to Kafka via rpc-do.

    Example:
        # Create Kafka client
        kafka = Kafka(brokers=['localhost:9092'])

        # Create producer
        producer = kafka.producer()
        await producer.connect()
        await producer.send('my-topic', value=b'Hello!')
        await producer.disconnect()

        # Create consumer
        consumer = kafka.consumer(group_id='my-group')
        await consumer.connect()
        await consumer.subscribe(['my-topic'])
        async for message in consumer:
            print(message.value)
        await consumer.disconnect()

        # Create admin
        admin = kafka.admin()
        await admin.connect()
        topics = await admin.list_topics()
        await admin.disconnect()
    """

    __slots__ = (
        "_brokers",
        "_client_id",
        "_timeout",
        "_client",
        "_connection_url",
    )

    def __init__(
        self,
        brokers: list[str] | None = None,
        *,
        client_id: str | None = None,
        timeout: float = 30.0,
        connection_url: str | None = None,
    ) -> None:
        """
        Initialize the Kafka client.

        Args:
            brokers: List of broker addresses (e.g., ['localhost:9092'])
            client_id: Optional client identifier
            timeout: Connection timeout in seconds
            connection_url: Optional direct connection URL for rpc-do
                            (defaults to kafka.do)
        """
        self._brokers = brokers or ["localhost:9092"]
        self._client_id = client_id
        self._timeout = timeout
        self._client: Any = None
        self._connection_url = connection_url or "kafka.do"

    @property
    def brokers(self) -> list[str]:
        """Return the configured broker addresses."""
        return self._brokers.copy()

    @property
    def client_id(self) -> str | None:
        """Return the client ID."""
        return self._client_id

    async def _get_client(self) -> Any:
        """
        Get or create the rpc-do client.

        Returns:
            Connected rpc-do client

        Raises:
            KafkaConnectionError: If connection fails
        """
        if self._client is not None:
            return self._client

        try:
            from rpc_do import connect  # pragma: no cover

            self._client = await connect(  # pragma: no cover
                self._connection_url,
                timeout=self._timeout,
            )

            # Configure the Kafka service with brokers
            await self._client.kafka.configure(  # pragma: no cover
                brokers=self._brokers,
                clientId=self._client_id,
            )

            return self._client  # pragma: no cover
        except ImportError as e:
            raise KafkaConnectionError(
                "rpc-do package not installed. Install with: pip install rpc-do"
            ) from e
        except Exception as e:  # pragma: no cover
            raise KafkaConnectionError(f"Failed to connect to Kafka service: {e}") from e  # pragma: no cover

    async def close(self) -> None:
        """
        Close the Kafka client and release resources.
        """
        if self._client is not None:
            try:
                await self._client.close()
            except Exception:
                pass
            self._client = None

    def producer(
        self,
        *,
        acks: int | str = 1,
        compression_type: CompressionType = CompressionType.NONE,
        batch_size: int = 16384,
        linger_ms: int = 0,
        client_id: str | None = None,
    ) -> Producer:
        """
        Create a new producer.

        Args:
            acks: Number of acknowledgments required (0, 1, or 'all')
            compression_type: Compression type for messages
            batch_size: Maximum batch size in bytes
            linger_ms: Time to wait before sending batched messages
            client_id: Optional client identifier (overrides Kafka client_id)

        Returns:
            New Producer instance
        """
        return Producer(
            self,
            acks=acks,
            compression_type=compression_type,
            batch_size=batch_size,
            linger_ms=linger_ms,
            client_id=client_id or self._client_id,
        )

    def consumer(
        self,
        group_id: str | None = None,
        *,
        auto_commit: bool = True,
        auto_commit_interval_ms: int = 5000,
        session_timeout_ms: int = 10000,
        heartbeat_interval_ms: int = 3000,
        max_poll_records: int = 500,
        max_poll_interval_ms: int = 300000,
        isolation_level: IsolationLevel = IsolationLevel.READ_UNCOMMITTED,
        auto_offset_reset: OffsetResetPolicy = OffsetResetPolicy.LATEST,
        client_id: str | None = None,
    ) -> Consumer:
        """
        Create a new consumer.

        Args:
            group_id: Consumer group ID (required for group coordination)
            auto_commit: Enable auto-commit of offsets
            auto_commit_interval_ms: Interval for auto-commit in milliseconds
            session_timeout_ms: Session timeout for group membership
            heartbeat_interval_ms: Heartbeat interval for group coordination
            max_poll_records: Maximum records per poll
            max_poll_interval_ms: Maximum interval between polls
            isolation_level: Transaction isolation level
            auto_offset_reset: Offset reset policy for new consumers
            client_id: Optional client identifier (overrides Kafka client_id)

        Returns:
            New Consumer instance
        """
        return Consumer(
            self,
            group_id=group_id,
            auto_commit=auto_commit,
            auto_commit_interval_ms=auto_commit_interval_ms,
            session_timeout_ms=session_timeout_ms,
            heartbeat_interval_ms=heartbeat_interval_ms,
            max_poll_records=max_poll_records,
            max_poll_interval_ms=max_poll_interval_ms,
            isolation_level=isolation_level,
            auto_offset_reset=auto_offset_reset,
            client_id=client_id or self._client_id,
        )

    def admin(
        self,
        *,
        request_timeout_ms: int = 30000,
    ) -> Admin:
        """
        Create a new admin client.

        Args:
            request_timeout_ms: Request timeout in milliseconds

        Returns:
            New Admin instance
        """
        return Admin(
            self,
            request_timeout_ms=request_timeout_ms,
        )

    async def __aenter__(self) -> Kafka:
        """Async context manager entry."""
        await self._get_client()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        """Async context manager exit."""
        await self.close()
