"""
Tests for kafka-do Kafka class and types.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from kafka_do import (
    Admin,
    CompressionType,
    Consumer,
    ConsumerError,
    ConsumerGroupMetadata,
    ConsumerRecord,
    IsolationLevel,
    Kafka,
    KafkaConnectionError,
    KafkaError,
    KafkaTimeoutError,
    OffsetResetPolicy,
    PartitionMetadata,
    Producer,
    ProducerError,
    ProducerRecord,
    RecordMetadata,
    TopicAlreadyExistsError,
    TopicConfig,
    TopicMetadata,
    TopicNotFoundError,
    TopicPartition,
)
from kafka_do.types import BatchResult, deserialize_record_metadata

from .conftest import MockRpcClient


class TestKafkaInit:
    """Tests for Kafka initialization."""

    def test_kafka_init_defaults(self) -> None:
        """Test Kafka initialization with default settings."""
        kafka = Kafka()

        assert kafka._brokers == ["localhost:9092"]
        assert kafka._client_id is None
        assert kafka._timeout == 30.0
        assert kafka._connection_url == "kafka.do"
        assert kafka._client is None

    def test_kafka_init_with_brokers(self) -> None:
        """Test Kafka initialization with custom brokers."""
        kafka = Kafka(brokers=["broker1:9092", "broker2:9092"])

        assert kafka._brokers == ["broker1:9092", "broker2:9092"]

    def test_kafka_init_with_all_options(self) -> None:
        """Test Kafka initialization with all options."""
        kafka = Kafka(
            brokers=["broker1:9092"],
            client_id="my-client",
            timeout=60.0,
            connection_url="custom.kafka.do",
        )

        assert kafka._brokers == ["broker1:9092"]
        assert kafka._client_id == "my-client"
        assert kafka._timeout == 60.0
        assert kafka._connection_url == "custom.kafka.do"

    def test_kafka_brokers_property(self) -> None:
        """Test brokers property returns a copy."""
        kafka = Kafka(brokers=["broker1:9092"])
        brokers = kafka.brokers

        assert brokers == ["broker1:9092"]

        # Verify it's a copy
        brokers.append("broker2:9092")
        assert len(kafka._brokers) == 1

    def test_kafka_client_id_property(self) -> None:
        """Test client_id property."""
        kafka = Kafka(client_id="my-client")
        assert kafka.client_id == "my-client"

        kafka2 = Kafka()
        assert kafka2.client_id is None


class TestKafkaConnection:
    """Tests for Kafka connection handling."""

    async def test_get_client_reuses_connection(self, mock_rpc_client: MockRpcClient) -> None:
        """Test _get_client reuses existing connection."""
        kafka = Kafka()
        kafka._client = mock_rpc_client

        client = await kafka._get_client()

        assert client is mock_rpc_client

    async def test_get_client_connection_error(self) -> None:
        """Test _get_client raises KafkaConnectionError when rpc-do not available."""
        kafka = Kafka()

        # Without a mock, this will fail to import/connect rpc_do
        with pytest.raises(KafkaConnectionError, match="rpc-do"):
            await kafka._get_client()

    async def test_close(self, mock_rpc_client: MockRpcClient) -> None:
        """Test close releases resources."""
        kafka = Kafka()
        kafka._client = mock_rpc_client
        mock_rpc_client.close = AsyncMock()

        await kafka.close()

        assert kafka._client is None
        mock_rpc_client.close.assert_called_once()

    async def test_close_error_ignored(self, mock_rpc_client: MockRpcClient) -> None:
        """Test close ignores errors."""
        kafka = Kafka()
        kafka._client = mock_rpc_client
        mock_rpc_client.close = AsyncMock(side_effect=Exception("Close failed"))

        await kafka.close()  # Should not raise

        assert kafka._client is None

    async def test_close_not_connected(self) -> None:
        """Test close when not connected does nothing."""
        kafka = Kafka()
        await kafka.close()  # Should not raise

    async def test_context_manager(self, mock_rpc_client: MockRpcClient) -> None:
        """Test async context manager with pre-set client."""
        kafka = Kafka()
        kafka._client = mock_rpc_client
        mock_rpc_client.close = AsyncMock()

        async with kafka:
            assert kafka._client is mock_rpc_client

        # close() was called but we check client is None
        assert kafka._client is None


class TestKafkaFactoryMethods:
    """Tests for Kafka factory methods."""

    def test_producer_factory(self) -> None:
        """Test producer factory method."""
        kafka = Kafka(client_id="kafka-client")
        producer = kafka.producer()

        assert isinstance(producer, Producer)
        assert producer._kafka is kafka
        assert producer._client_id == "kafka-client"

    def test_producer_factory_with_options(self) -> None:
        """Test producer factory with custom options."""
        kafka = Kafka()
        producer = kafka.producer(
            acks="all",
            compression_type=CompressionType.GZIP,
            batch_size=32768,
            linger_ms=100,
            client_id="producer-client",
        )

        assert producer._acks == "all"
        assert producer._compression_type == CompressionType.GZIP
        assert producer._batch_size == 32768
        assert producer._linger_ms == 100
        assert producer._client_id == "producer-client"

    def test_consumer_factory(self) -> None:
        """Test consumer factory method."""
        kafka = Kafka(client_id="kafka-client")
        consumer = kafka.consumer(group_id="my-group")

        assert isinstance(consumer, Consumer)
        assert consumer._kafka is kafka
        assert consumer._group_id == "my-group"
        assert consumer._client_id == "kafka-client"

    def test_consumer_factory_with_options(self) -> None:
        """Test consumer factory with custom options."""
        kafka = Kafka()
        consumer = kafka.consumer(
            group_id="my-group",
            auto_commit=False,
            auto_commit_interval_ms=10000,
            session_timeout_ms=30000,
            heartbeat_interval_ms=5000,
            max_poll_records=100,
            max_poll_interval_ms=600000,
            isolation_level=IsolationLevel.READ_COMMITTED,
            auto_offset_reset=OffsetResetPolicy.EARLIEST,
            client_id="consumer-client",
        )

        assert consumer._group_id == "my-group"
        assert consumer._auto_commit is False
        assert consumer._isolation_level == IsolationLevel.READ_COMMITTED
        assert consumer._auto_offset_reset == OffsetResetPolicy.EARLIEST
        assert consumer._client_id == "consumer-client"

    def test_admin_factory(self) -> None:
        """Test admin factory method."""
        kafka = Kafka()
        admin = kafka.admin()

        assert isinstance(admin, Admin)
        assert admin._kafka is kafka

    def test_admin_factory_with_options(self) -> None:
        """Test admin factory with custom options."""
        kafka = Kafka()
        admin = kafka.admin(request_timeout_ms=60000)

        assert admin._request_timeout_ms == 60000


class TestTypes:
    """Tests for type definitions."""

    def test_topic_partition_hash(self) -> None:
        """Test TopicPartition hash."""
        tp1 = TopicPartition("test", 0)
        tp2 = TopicPartition("test", 0)
        tp3 = TopicPartition("test", 1)

        assert hash(tp1) == hash(tp2)
        assert hash(tp1) != hash(tp3)

    def test_topic_partition_equality(self) -> None:
        """Test TopicPartition equality."""
        tp1 = TopicPartition("test", 0)
        tp2 = TopicPartition("test", 0)
        tp3 = TopicPartition("test", 1)

        assert tp1 == tp2
        assert tp1 != tp3
        assert tp1 != "not a topic partition"

    def test_record_metadata(self) -> None:
        """Test RecordMetadata creation."""
        metadata = RecordMetadata(
            topic="test",
            partition=0,
            offset=42,
            timestamp=1234567890,
            serialized_key_size=4,
            serialized_value_size=10,
        )

        assert metadata.topic == "test"
        assert metadata.partition == 0
        assert metadata.offset == 42
        assert metadata.timestamp == 1234567890
        assert metadata.serialized_key_size == 4
        assert metadata.serialized_value_size == 10

    def test_consumer_record(self) -> None:
        """Test ConsumerRecord creation."""
        record = ConsumerRecord(
            topic="test",
            partition=0,
            offset=42,
            timestamp=1234567890,
            key=b"key",
            value=b"value",
            headers=[("h1", b"v1")],
        )

        assert record.topic == "test"
        assert record.key == b"key"
        assert record.value == b"value"
        assert record.headers == [("h1", b"v1")]

    def test_producer_record(self) -> None:
        """Test ProducerRecord creation."""
        record = ProducerRecord(
            topic="test",
            value=b"value",
            key=b"key",
            partition=2,
            timestamp=1234567890,
            headers=[("h1", b"v1")],
        )

        assert record.topic == "test"
        assert record.value == b"value"
        assert record.key == b"key"
        assert record.partition == 2

    def test_topic_config(self) -> None:
        """Test TopicConfig creation."""
        config = TopicConfig(
            name="test",
            num_partitions=3,
            replication_factor=2,
            config={"retention.ms": "86400000"},
        )

        assert config.name == "test"
        assert config.num_partitions == 3
        assert config.replication_factor == 2
        assert config.config["retention.ms"] == "86400000"

    def test_topic_metadata(self) -> None:
        """Test TopicMetadata creation."""
        metadata = TopicMetadata(
            name="test",
            partitions=3,
            replication_factor=2,
            config={"retention.ms": "86400000"},
        )

        assert metadata.name == "test"
        assert metadata.partitions == 3
        assert metadata.replication_factor == 2

    def test_partition_metadata(self) -> None:
        """Test PartitionMetadata creation."""
        metadata = PartitionMetadata(
            topic="test",
            partition=0,
            leader=1,
            replicas=[1, 2],
            isr=[1, 2],
        )

        assert metadata.topic == "test"
        assert metadata.partition == 0
        assert metadata.leader == 1
        assert metadata.replicas == [1, 2]
        assert metadata.isr == [1, 2]

    def test_consumer_group_metadata(self) -> None:
        """Test ConsumerGroupMetadata creation."""
        metadata = ConsumerGroupMetadata(
            group_id="test-group",
            state="Stable",
            members=["member1", "member2"],
            coordinator=1,
        )

        assert metadata.group_id == "test-group"
        assert metadata.state == "Stable"
        assert metadata.members == ["member1", "member2"]
        assert metadata.coordinator == 1

    def test_batch_result(self) -> None:
        """Test BatchResult creation."""
        result = BatchResult(
            successful=[RecordMetadata(topic="t", partition=0, offset=1)],
            failed=[],
        )

        assert len(result.successful) == 1
        assert len(result.failed) == 0


class TestExceptions:
    """Tests for exception classes."""

    def test_kafka_error(self) -> None:
        """Test KafkaError."""
        error = KafkaError("Test error", code="TEST", retriable=True)

        assert str(error) == "[TEST] Test error"
        assert error.message == "Test error"
        assert error.code == "TEST"
        assert error.retriable is True

    def test_kafka_error_no_code(self) -> None:
        """Test KafkaError without code."""
        error = KafkaError("Test error")

        assert str(error) == "Test error"
        assert error.code is None
        assert error.retriable is False

    def test_kafka_connection_error(self) -> None:
        """Test KafkaConnectionError."""
        error = KafkaConnectionError("Connection failed")

        assert str(error) == "[CONNECTION_ERROR] Connection failed"
        assert error.code == "CONNECTION_ERROR"
        assert error.retriable is True

    def test_kafka_timeout_error(self) -> None:
        """Test KafkaTimeoutError."""
        error = KafkaTimeoutError("Request timed out")

        assert str(error) == "[TIMEOUT] Request timed out"
        assert error.code == "TIMEOUT"
        assert error.retriable is True

    def test_topic_not_found_error(self) -> None:
        """Test TopicNotFoundError."""
        error = TopicNotFoundError("my-topic")

        assert "my-topic" in str(error)
        assert error.topic == "my-topic"
        assert error.code == "TOPIC_NOT_FOUND"
        assert error.retriable is False

    def test_topic_already_exists_error(self) -> None:
        """Test TopicAlreadyExistsError."""
        error = TopicAlreadyExistsError("my-topic")

        assert "my-topic" in str(error)
        assert error.topic == "my-topic"
        assert error.code == "TOPIC_ALREADY_EXISTS"
        assert error.retriable is False

    def test_producer_error(self) -> None:
        """Test ProducerError."""
        error = ProducerError("Send failed")

        assert str(error) == "Send failed"

    def test_consumer_error(self) -> None:
        """Test ConsumerError."""
        error = ConsumerError("Poll failed")

        assert str(error) == "Poll failed"


class TestEnums:
    """Tests for enum types."""

    def test_compression_type(self) -> None:
        """Test CompressionType enum."""
        assert CompressionType.NONE.value == "none"
        assert CompressionType.GZIP.value == "gzip"
        assert CompressionType.SNAPPY.value == "snappy"
        assert CompressionType.LZ4.value == "lz4"
        assert CompressionType.ZSTD.value == "zstd"

    def test_isolation_level(self) -> None:
        """Test IsolationLevel enum."""
        assert IsolationLevel.READ_UNCOMMITTED.value == "read_uncommitted"
        assert IsolationLevel.READ_COMMITTED.value == "read_committed"

    def test_offset_reset_policy(self) -> None:
        """Test OffsetResetPolicy enum."""
        assert OffsetResetPolicy.EARLIEST.value == "earliest"
        assert OffsetResetPolicy.LATEST.value == "latest"
        assert OffsetResetPolicy.NONE.value == "none"


class TestDeserializeRecordMetadata:
    """Tests for deserialize_record_metadata helper."""

    def test_deserialize_basic(self) -> None:
        """Test basic deserialization."""
        data = {
            "topic": "test",
            "partition": 0,
            "offset": 42,
        }

        metadata = deserialize_record_metadata(data)

        assert metadata.topic == "test"
        assert metadata.partition == 0
        assert metadata.offset == 42
        assert metadata.timestamp is None
        assert metadata.serialized_key_size == 0
        assert metadata.serialized_value_size == 0

    def test_deserialize_full(self) -> None:
        """Test full deserialization."""
        data = {
            "topic": "test",
            "partition": 0,
            "offset": 42,
            "timestamp": 1234567890,
            "serialized_key_size": 4,
            "serialized_value_size": 10,
        }

        metadata = deserialize_record_metadata(data)

        assert metadata.timestamp == 1234567890
        assert metadata.serialized_key_size == 4
        assert metadata.serialized_value_size == 10


class TestImports:
    """Tests for package imports."""

    def test_all_exports_available(self) -> None:
        """Test that all exports are available."""
        import kafka_do

        assert hasattr(kafka_do, "__version__")
        assert hasattr(kafka_do, "Kafka")
        assert hasattr(kafka_do, "Producer")
        assert hasattr(kafka_do, "Consumer")
        assert hasattr(kafka_do, "Admin")
        assert hasattr(kafka_do, "TopicPartition")
        assert hasattr(kafka_do, "RecordMetadata")
        assert hasattr(kafka_do, "ConsumerRecord")
        assert hasattr(kafka_do, "ProducerRecord")
        assert hasattr(kafka_do, "TopicConfig")
        assert hasattr(kafka_do, "TopicMetadata")
        assert hasattr(kafka_do, "PartitionMetadata")
        assert hasattr(kafka_do, "ConsumerGroupMetadata")
        assert hasattr(kafka_do, "BatchResult")
        assert hasattr(kafka_do, "CompressionType")
        assert hasattr(kafka_do, "IsolationLevel")
        assert hasattr(kafka_do, "OffsetResetPolicy")
        assert hasattr(kafka_do, "KafkaError")
        assert hasattr(kafka_do, "KafkaConnectionError")
        assert hasattr(kafka_do, "KafkaTimeoutError")
        assert hasattr(kafka_do, "TopicNotFoundError")
        assert hasattr(kafka_do, "TopicAlreadyExistsError")
        assert hasattr(kafka_do, "ProducerError")
        assert hasattr(kafka_do, "ConsumerError")
