"""
Type definitions for kafka-do SDK.

This module provides type definitions compatible with kafka-python
while supporting async/await patterns.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class CompressionType(Enum):
    """Compression types for Kafka messages."""

    NONE = "none"
    GZIP = "gzip"
    SNAPPY = "snappy"
    LZ4 = "lz4"
    ZSTD = "zstd"


class IsolationLevel(Enum):
    """Isolation levels for consumer reads."""

    READ_UNCOMMITTED = "read_uncommitted"
    READ_COMMITTED = "read_committed"


class OffsetResetPolicy(Enum):
    """Offset reset policies for consumers."""

    EARLIEST = "earliest"
    LATEST = "latest"
    NONE = "none"


@dataclass
class TopicPartition:
    """
    Represents a topic and partition assignment.

    Attributes:
        topic: The topic name
        partition: The partition number
    """

    topic: str
    partition: int

    def __hash__(self) -> int:
        return hash((self.topic, self.partition))

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, TopicPartition):
            return NotImplemented
        return self.topic == other.topic and self.partition == other.partition


@dataclass
class RecordMetadata:
    """
    Metadata about a successfully sent record.

    Attributes:
        topic: The topic the record was sent to
        partition: The partition the record was sent to
        offset: The offset of the record in the partition
        timestamp: The timestamp of the record (milliseconds since epoch)
        serialized_key_size: Size of the serialized key in bytes
        serialized_value_size: Size of the serialized value in bytes
    """

    topic: str
    partition: int
    offset: int
    timestamp: int | None = None
    serialized_key_size: int = 0
    serialized_value_size: int = 0


@dataclass
class ConsumerRecord:
    """
    A record consumed from a Kafka topic.

    Attributes:
        topic: The topic this record was received from
        partition: The partition from which the record was received
        offset: The position of this record in the partition
        timestamp: The timestamp of this record (milliseconds since epoch)
        key: The key of the record (may be None)
        value: The value of the record
        headers: Optional headers associated with the record
    """

    topic: str
    partition: int
    offset: int
    timestamp: int | None
    key: bytes | None
    value: bytes
    headers: list[tuple[str, bytes]] = field(default_factory=list)


@dataclass
class ProducerRecord:
    """
    A record to be sent to Kafka.

    Attributes:
        topic: The topic to send the record to
        value: The value of the record
        key: Optional key for the record
        partition: Optional partition to send to
        timestamp: Optional timestamp (milliseconds since epoch)
        headers: Optional headers
    """

    topic: str
    value: bytes
    key: bytes | None = None
    partition: int | None = None
    timestamp: int | None = None
    headers: list[tuple[str, bytes]] = field(default_factory=list)


@dataclass
class TopicConfig:
    """
    Configuration for creating a new topic.

    Attributes:
        name: The topic name
        num_partitions: Number of partitions (default: 1)
        replication_factor: Replication factor (default: 1)
        config: Additional topic-level configuration
    """

    name: str
    num_partitions: int = 1
    replication_factor: int = 1
    config: dict[str, str] = field(default_factory=dict)


@dataclass
class TopicMetadata:
    """
    Metadata about a Kafka topic.

    Attributes:
        name: The topic name
        partitions: Number of partitions
        replication_factor: Replication factor
        config: Topic configuration
    """

    name: str
    partitions: int
    replication_factor: int
    config: dict[str, str] = field(default_factory=dict)


@dataclass
class PartitionMetadata:
    """
    Metadata about a specific partition.

    Attributes:
        topic: The topic name
        partition: The partition number
        leader: The broker ID of the leader
        replicas: List of replica broker IDs
        isr: List of in-sync replica broker IDs
    """

    topic: str
    partition: int
    leader: int
    replicas: list[int] = field(default_factory=list)
    isr: list[int] = field(default_factory=list)


@dataclass
class ConsumerGroupMetadata:
    """
    Metadata about a consumer group.

    Attributes:
        group_id: The consumer group ID
        state: The current state of the group
        members: List of member IDs
        coordinator: The broker ID of the group coordinator
    """

    group_id: str
    state: str
    members: list[str] = field(default_factory=list)
    coordinator: int | None = None


class KafkaError(Exception):
    """
    Base exception for Kafka errors.

    Attributes:
        message: Error message
        code: Error code (if available)
        retriable: Whether the error is retriable
    """

    def __init__(
        self,
        message: str,
        code: str | None = None,
        retriable: bool = False,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.code = code
        self.retriable = retriable

    def __str__(self) -> str:
        if self.code:
            return f"[{self.code}] {self.message}"
        return self.message


class KafkaConnectionError(KafkaError):
    """Raised when there is a connection error."""

    def __init__(self, message: str) -> None:
        super().__init__(message, code="CONNECTION_ERROR", retriable=True)


class KafkaTimeoutError(KafkaError):
    """Raised when an operation times out."""

    def __init__(self, message: str) -> None:
        super().__init__(message, code="TIMEOUT", retriable=True)


class TopicNotFoundError(KafkaError):
    """Raised when a topic is not found."""

    def __init__(self, topic: str) -> None:
        super().__init__(f"Topic not found: {topic}", code="TOPIC_NOT_FOUND", retriable=False)
        self.topic = topic


class TopicAlreadyExistsError(KafkaError):
    """Raised when attempting to create a topic that already exists."""

    def __init__(self, topic: str) -> None:
        super().__init__(
            f"Topic already exists: {topic}", code="TOPIC_ALREADY_EXISTS", retriable=False
        )
        self.topic = topic


class ProducerError(KafkaError):
    """Raised when a producer operation fails."""

    pass


class ConsumerError(KafkaError):
    """Raised when a consumer operation fails."""

    pass


@dataclass
class BatchResult:
    """
    Result of a batch send operation.

    Attributes:
        successful: List of successfully sent record metadata
        failed: List of (record, error) tuples for failed sends
    """

    successful: list[RecordMetadata] = field(default_factory=list)
    failed: list[tuple[ProducerRecord, KafkaError]] = field(default_factory=list)


def serialize_record(record: ProducerRecord) -> dict[str, Any]:
    """
    Serialize a ProducerRecord for RPC transmission.

    Args:
        record: The record to serialize

    Returns:
        Dictionary representation suitable for RPC
    """
    import base64

    data: dict[str, Any] = {
        "topic": record.topic,
        "value": base64.b64encode(record.value).decode("ascii"),
    }

    if record.key is not None:
        data["key"] = base64.b64encode(record.key).decode("ascii")

    if record.partition is not None:
        data["partition"] = record.partition

    if record.timestamp is not None:
        data["timestamp"] = record.timestamp

    if record.headers:
        data["headers"] = [
            {"key": k, "value": base64.b64encode(v).decode("ascii")} for k, v in record.headers
        ]

    return data


def deserialize_consumer_record(data: dict[str, Any]) -> ConsumerRecord:
    """
    Deserialize a ConsumerRecord from RPC response.

    Args:
        data: Dictionary from RPC response

    Returns:
        Deserialized ConsumerRecord
    """
    import base64

    headers: list[tuple[str, bytes]] = []
    if "headers" in data and data["headers"]:
        headers = [(h["key"], base64.b64decode(h["value"])) for h in data["headers"]]

    return ConsumerRecord(
        topic=data["topic"],
        partition=data["partition"],
        offset=data["offset"],
        timestamp=data.get("timestamp"),
        key=base64.b64decode(data["key"]) if data.get("key") else None,
        value=base64.b64decode(data["value"]),
        headers=headers,
    )


def deserialize_record_metadata(data: dict[str, Any]) -> RecordMetadata:
    """
    Deserialize RecordMetadata from RPC response.

    Args:
        data: Dictionary from RPC response

    Returns:
        Deserialized RecordMetadata
    """
    return RecordMetadata(
        topic=data["topic"],
        partition=data["partition"],
        offset=data["offset"],
        timestamp=data.get("timestamp"),
        serialized_key_size=data.get("serialized_key_size", 0),
        serialized_value_size=data.get("serialized_value_size", 0),
    )
