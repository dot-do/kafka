"""
Tests for kafka-do Admin.
"""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from kafka_do import (
    ConsumerGroupMetadata,
    Kafka,
    KafkaConnectionError,
    KafkaError,
    PartitionMetadata,
    TopicAlreadyExistsError,
    TopicConfig,
    TopicMetadata,
    TopicNotFoundError,
)
from kafka_do.admin import Admin

from .conftest import MockRpcClient


class TestAdminInit:
    """Tests for Admin initialization."""

    def test_admin_init_defaults(self, kafka_instance: Kafka) -> None:
        """Test admin initialization with default settings."""
        admin = kafka_instance.admin()

        assert admin._kafka is kafka_instance
        assert admin._connected is False
        assert admin._request_timeout_ms == 30000

    def test_admin_init_custom_settings(self, kafka_instance: Kafka) -> None:
        """Test admin initialization with custom settings."""
        admin = kafka_instance.admin(request_timeout_ms=60000)

        assert admin._request_timeout_ms == 60000

    def test_admin_connected_property(self, kafka_instance: Kafka) -> None:
        """Test connected property."""
        admin = kafka_instance.admin()
        assert admin.connected is False

        admin._connected = True
        assert admin.connected is True


class TestAdminConnection:
    """Tests for Admin connection handling."""

    async def test_connect_success(self, mock_rpc_client: MockRpcClient) -> None:
        """Test successful admin connection."""
        kafka = Kafka(brokers=["localhost:9092"])
        kafka._client = mock_rpc_client
        admin = kafka.admin()

        await admin.connect()

        assert admin.connected is True
        assert admin._client is mock_rpc_client

    async def test_connect_already_connected(
        self, connected_admin: Admin, mock_rpc_client: MockRpcClient
    ) -> None:
        """Test that connect does nothing if already connected."""
        original_client = connected_admin._client
        await connected_admin.connect()

        assert connected_admin._client is original_client

    async def test_connect_failure(self) -> None:
        """Test connection failure raises KafkaConnectionError."""
        kafka = Kafka(brokers=["localhost:9092"])
        admin = kafka.admin()

        with pytest.raises(KafkaConnectionError, match="Failed to connect admin"):
            await admin.connect()

    async def test_disconnect(self, connected_admin: Admin) -> None:
        """Test admin disconnection."""
        await connected_admin.disconnect()

        assert connected_admin.connected is False
        assert connected_admin._client is None

    async def test_disconnect_not_connected(self, kafka_instance: Kafka) -> None:
        """Test disconnect when not connected does nothing."""
        admin = kafka_instance.admin()
        await admin.disconnect()  # Should not raise

        assert admin.connected is False

    async def test_context_manager(self, mock_rpc_client: MockRpcClient) -> None:
        """Test async context manager."""
        kafka = Kafka(brokers=["localhost:9092"])
        kafka._client = mock_rpc_client

        async with kafka.admin() as admin:
            assert admin.connected is True

        assert admin.connected is False


class TestAdminTopicOperations:
    """Tests for Admin topic operations."""

    async def test_create_topics_with_strings(
        self, connected_admin: Admin, mock_rpc_client: MockRpcClient
    ) -> None:
        """Test creating topics with string names."""
        await connected_admin.create_topics(["topic1", "topic2"])

        mock_rpc_client.kafka.admin.createTopics.assert_called_once()
        call_kwargs = mock_rpc_client.kafka.admin.createTopics.call_args[1]
        assert len(call_kwargs["topics"]) == 2
        assert call_kwargs["topics"][0]["name"] == "topic1"

    async def test_create_topics_with_config(
        self, connected_admin: Admin, mock_rpc_client: MockRpcClient
    ) -> None:
        """Test creating topics with TopicConfig."""
        config = TopicConfig(
            name="my-topic",
            num_partitions=3,
            replication_factor=2,
            config={"retention.ms": "86400000"},
        )
        await connected_admin.create_topics([config])

        call_kwargs = mock_rpc_client.kafka.admin.createTopics.call_args[1]
        topic_config = call_kwargs["topics"][0]
        assert topic_config["name"] == "my-topic"
        assert topic_config["numPartitions"] == 3
        assert topic_config["replicationFactor"] == 2
        assert topic_config["config"]["retention.ms"] == "86400000"

    async def test_create_topics_validate_only(
        self, connected_admin: Admin, mock_rpc_client: MockRpcClient
    ) -> None:
        """Test creating topics with validate_only flag."""
        await connected_admin.create_topics(["topic1"], validate_only=True)

        call_kwargs = mock_rpc_client.kafka.admin.createTopics.call_args[1]
        assert call_kwargs["validateOnly"] is True

    async def test_create_topics_already_exists(
        self, connected_admin: Admin, mock_rpc_client: MockRpcClient
    ) -> None:
        """Test creating topics that already exist."""
        mock_rpc_client.kafka.admin.createTopics.return_value = [
            {"name": "topic1", "error": {"code": "TOPIC_ALREADY_EXISTS", "message": "Exists"}}
        ]

        with pytest.raises(TopicAlreadyExistsError, match="topic1"):
            await connected_admin.create_topics(["topic1"])

    async def test_create_topics_error(
        self, connected_admin: Admin, mock_rpc_client: MockRpcClient
    ) -> None:
        """Test creating topics with generic error."""
        mock_rpc_client.kafka.admin.createTopics.return_value = [
            {"name": "topic1", "error": {"code": "UNKNOWN", "message": "Unknown error"}}
        ]

        with pytest.raises(KafkaError, match="Unknown error"):
            await connected_admin.create_topics(["topic1"])

    async def test_create_topics_not_connected(self, kafka_instance: Kafka) -> None:
        """Test create_topics raises error when not connected."""
        admin = kafka_instance.admin()

        with pytest.raises(KafkaConnectionError, match="Admin not connected"):
            await admin.create_topics(["topic1"])

    async def test_create_topics_failure(
        self, connected_admin: Admin, mock_rpc_client: MockRpcClient
    ) -> None:
        """Test create_topics failure."""
        mock_rpc_client.kafka.admin.createTopics.side_effect = Exception("Failed")

        with pytest.raises(KafkaError, match="Failed to create topics"):
            await connected_admin.create_topics(["topic1"])

    async def test_delete_topics(
        self, connected_admin: Admin, mock_rpc_client: MockRpcClient
    ) -> None:
        """Test deleting topics."""
        await connected_admin.delete_topics(["topic1", "topic2"])

        mock_rpc_client.kafka.admin.deleteTopics.assert_called_once()

    async def test_delete_topics_not_found(
        self, connected_admin: Admin, mock_rpc_client: MockRpcClient
    ) -> None:
        """Test deleting topics that don't exist."""
        mock_rpc_client.kafka.admin.deleteTopics.return_value = [
            {"name": "topic1", "error": {"code": "TOPIC_NOT_FOUND", "message": "Not found"}}
        ]

        with pytest.raises(TopicNotFoundError, match="topic1"):
            await connected_admin.delete_topics(["topic1"])

    async def test_delete_topics_error(
        self, connected_admin: Admin, mock_rpc_client: MockRpcClient
    ) -> None:
        """Test delete topics with generic error."""
        mock_rpc_client.kafka.admin.deleteTopics.return_value = [
            {"name": "topic1", "error": {"code": "UNKNOWN", "message": "Unknown error"}}
        ]

        with pytest.raises(KafkaError, match="Unknown error"):
            await connected_admin.delete_topics(["topic1"])

    async def test_delete_topics_not_connected(self, kafka_instance: Kafka) -> None:
        """Test delete_topics raises error when not connected."""
        admin = kafka_instance.admin()

        with pytest.raises(KafkaConnectionError, match="Admin not connected"):
            await admin.delete_topics(["topic1"])

    async def test_delete_topics_failure(
        self, connected_admin: Admin, mock_rpc_client: MockRpcClient
    ) -> None:
        """Test delete_topics failure."""
        mock_rpc_client.kafka.admin.deleteTopics.side_effect = Exception("Failed")

        with pytest.raises(KafkaError, match="Failed to delete topics"):
            await connected_admin.delete_topics(["topic1"])

    async def test_list_topics(
        self, connected_admin: Admin, mock_rpc_client: MockRpcClient
    ) -> None:
        """Test listing topics."""
        topics = await connected_admin.list_topics()

        assert topics == ["topic1", "topic2"]

    async def test_list_topics_include_internal(
        self, connected_admin: Admin, mock_rpc_client: MockRpcClient
    ) -> None:
        """Test listing topics including internal."""
        await connected_admin.list_topics(include_internal=True)

        call_kwargs = mock_rpc_client.kafka.admin.listTopics.call_args[1]
        assert call_kwargs["includeInternal"] is True

    async def test_list_topics_not_connected(self, kafka_instance: Kafka) -> None:
        """Test list_topics raises error when not connected."""
        admin = kafka_instance.admin()

        with pytest.raises(KafkaConnectionError, match="Admin not connected"):
            await admin.list_topics()

    async def test_list_topics_failure(
        self, connected_admin: Admin, mock_rpc_client: MockRpcClient
    ) -> None:
        """Test list_topics failure."""
        mock_rpc_client.kafka.admin.listTopics.side_effect = Exception("Failed")

        with pytest.raises(KafkaError, match="Failed to list topics"):
            await connected_admin.list_topics()


class TestAdminDescribeTopics:
    """Tests for Admin describe topic operations."""

    async def test_describe_topics(
        self, connected_admin: Admin, mock_rpc_client: MockRpcClient
    ) -> None:
        """Test describing topics."""
        result = await connected_admin.describe_topics(["test-topic"])

        assert len(result) == 1
        assert isinstance(result[0], TopicMetadata)
        assert result[0].name == "test-topic"
        assert result[0].partitions == 3
        assert result[0].replication_factor == 1

    async def test_describe_topics_not_found(
        self, connected_admin: Admin, mock_rpc_client: MockRpcClient
    ) -> None:
        """Test describing topics that don't exist."""
        mock_rpc_client.kafka.admin.describeTopics.return_value = [
            {"name": "unknown", "error": {"code": "TOPIC_NOT_FOUND", "message": "Not found"}}
        ]

        with pytest.raises(TopicNotFoundError):
            await connected_admin.describe_topics(["unknown"])

    async def test_describe_topics_error(
        self, connected_admin: Admin, mock_rpc_client: MockRpcClient
    ) -> None:
        """Test describe topics with generic error."""
        mock_rpc_client.kafka.admin.describeTopics.return_value = [
            {"name": "topic1", "error": {"code": "UNKNOWN", "message": "Unknown error"}}
        ]

        with pytest.raises(KafkaError, match="Unknown error"):
            await connected_admin.describe_topics(["topic1"])

    async def test_describe_topics_not_connected(self, kafka_instance: Kafka) -> None:
        """Test describe_topics raises error when not connected."""
        admin = kafka_instance.admin()

        with pytest.raises(KafkaConnectionError, match="Admin not connected"):
            await admin.describe_topics(["topic1"])

    async def test_describe_topics_failure(
        self, connected_admin: Admin, mock_rpc_client: MockRpcClient
    ) -> None:
        """Test describe_topics failure."""
        mock_rpc_client.kafka.admin.describeTopics.side_effect = Exception("Failed")

        with pytest.raises(KafkaError, match="Failed to describe topics"):
            await connected_admin.describe_topics(["topic1"])

    async def test_describe_partitions(
        self, connected_admin: Admin, mock_rpc_client: MockRpcClient
    ) -> None:
        """Test describing partitions."""
        result = await connected_admin.describe_partitions("test-topic")

        assert len(result) == 2
        assert isinstance(result[0], PartitionMetadata)
        assert result[0].topic == "test-topic"
        assert result[0].partition == 0
        assert result[0].leader == 1

    async def test_describe_partitions_not_found(
        self, connected_admin: Admin, mock_rpc_client: MockRpcClient
    ) -> None:
        """Test describing partitions for topic that doesn't exist."""
        mock_rpc_client.kafka.admin.describePartitions.return_value = {
            "error": {"code": "TOPIC_NOT_FOUND", "message": "Not found"}
        }

        with pytest.raises(TopicNotFoundError):
            await connected_admin.describe_partitions("unknown")

    async def test_describe_partitions_error(
        self, connected_admin: Admin, mock_rpc_client: MockRpcClient
    ) -> None:
        """Test describe partitions with generic error."""
        mock_rpc_client.kafka.admin.describePartitions.return_value = {
            "error": {"code": "UNKNOWN", "message": "Unknown error"}
        }

        with pytest.raises(KafkaError, match="Unknown error"):
            await connected_admin.describe_partitions("topic1")

    async def test_describe_partitions_not_connected(self, kafka_instance: Kafka) -> None:
        """Test describe_partitions raises error when not connected."""
        admin = kafka_instance.admin()

        with pytest.raises(KafkaConnectionError, match="Admin not connected"):
            await admin.describe_partitions("topic1")

    async def test_describe_partitions_failure(
        self, connected_admin: Admin, mock_rpc_client: MockRpcClient
    ) -> None:
        """Test describe_partitions failure."""
        mock_rpc_client.kafka.admin.describePartitions.side_effect = Exception("Failed")

        with pytest.raises(KafkaError, match="Failed to describe partitions"):
            await connected_admin.describe_partitions("topic1")


class TestAdminConsumerGroups:
    """Tests for Admin consumer group operations."""

    async def test_list_consumer_groups(
        self, connected_admin: Admin, mock_rpc_client: MockRpcClient
    ) -> None:
        """Test listing consumer groups."""
        result = await connected_admin.list_consumer_groups()

        assert result == ["group1", "group2"]

    async def test_list_consumer_groups_not_connected(self, kafka_instance: Kafka) -> None:
        """Test list_consumer_groups raises error when not connected."""
        admin = kafka_instance.admin()

        with pytest.raises(KafkaConnectionError, match="Admin not connected"):
            await admin.list_consumer_groups()

    async def test_list_consumer_groups_failure(
        self, connected_admin: Admin, mock_rpc_client: MockRpcClient
    ) -> None:
        """Test list_consumer_groups failure."""
        mock_rpc_client.kafka.admin.listConsumerGroups.side_effect = Exception("Failed")

        with pytest.raises(KafkaError, match="Failed to list consumer groups"):
            await connected_admin.list_consumer_groups()

    async def test_describe_consumer_groups(
        self, connected_admin: Admin, mock_rpc_client: MockRpcClient
    ) -> None:
        """Test describing consumer groups."""
        result = await connected_admin.describe_consumer_groups(["test-group"])

        assert len(result) == 1
        assert isinstance(result[0], ConsumerGroupMetadata)
        assert result[0].group_id == "test-group"
        assert result[0].state == "Stable"
        assert result[0].members == ["member1"]
        assert result[0].coordinator == 1

    async def test_describe_consumer_groups_error(
        self, connected_admin: Admin, mock_rpc_client: MockRpcClient
    ) -> None:
        """Test describe consumer groups with error."""
        mock_rpc_client.kafka.admin.describeConsumerGroups.return_value = [
            {"groupId": "group1", "error": {"code": "UNKNOWN", "message": "Unknown error"}}
        ]

        with pytest.raises(KafkaError, match="Unknown error"):
            await connected_admin.describe_consumer_groups(["group1"])

    async def test_describe_consumer_groups_not_connected(self, kafka_instance: Kafka) -> None:
        """Test describe_consumer_groups raises error when not connected."""
        admin = kafka_instance.admin()

        with pytest.raises(KafkaConnectionError, match="Admin not connected"):
            await admin.describe_consumer_groups(["group1"])

    async def test_describe_consumer_groups_failure(
        self, connected_admin: Admin, mock_rpc_client: MockRpcClient
    ) -> None:
        """Test describe_consumer_groups failure."""
        mock_rpc_client.kafka.admin.describeConsumerGroups.side_effect = Exception("Failed")

        with pytest.raises(KafkaError, match="Failed to describe consumer groups"):
            await connected_admin.describe_consumer_groups(["group1"])

    async def test_delete_consumer_groups(
        self, connected_admin: Admin, mock_rpc_client: MockRpcClient
    ) -> None:
        """Test deleting consumer groups."""
        await connected_admin.delete_consumer_groups(["group1", "group2"])

        mock_rpc_client.kafka.admin.deleteConsumerGroups.assert_called_once()

    async def test_delete_consumer_groups_error(
        self, connected_admin: Admin, mock_rpc_client: MockRpcClient
    ) -> None:
        """Test delete consumer groups with error."""
        mock_rpc_client.kafka.admin.deleteConsumerGroups.return_value = [
            {"groupId": "group1", "error": {"code": "UNKNOWN", "message": "Unknown error"}}
        ]

        with pytest.raises(KafkaError, match="Unknown error"):
            await connected_admin.delete_consumer_groups(["group1"])

    async def test_delete_consumer_groups_not_connected(self, kafka_instance: Kafka) -> None:
        """Test delete_consumer_groups raises error when not connected."""
        admin = kafka_instance.admin()

        with pytest.raises(KafkaConnectionError, match="Admin not connected"):
            await admin.delete_consumer_groups(["group1"])

    async def test_delete_consumer_groups_failure(
        self, connected_admin: Admin, mock_rpc_client: MockRpcClient
    ) -> None:
        """Test delete_consumer_groups failure."""
        mock_rpc_client.kafka.admin.deleteConsumerGroups.side_effect = Exception("Failed")

        with pytest.raises(KafkaError, match="Failed to delete consumer groups"):
            await connected_admin.delete_consumer_groups(["group1"])


class TestAdminAlterConfig:
    """Tests for Admin alter config operations."""

    async def test_alter_topic_config(
        self, connected_admin: Admin, mock_rpc_client: MockRpcClient
    ) -> None:
        """Test altering topic configuration."""
        await connected_admin.alter_topic_config(
            "test-topic", {"retention.ms": "86400000", "cleanup.policy": "compact"}
        )

        mock_rpc_client.kafka.admin.alterTopicConfig.assert_called_once_with(
            topic="test-topic",
            config={"retention.ms": "86400000", "cleanup.policy": "compact"},
        )

    async def test_alter_topic_config_not_found(
        self, connected_admin: Admin, mock_rpc_client: MockRpcClient
    ) -> None:
        """Test altering config for topic that doesn't exist."""
        mock_rpc_client.kafka.admin.alterTopicConfig.return_value = {
            "error": {"code": "TOPIC_NOT_FOUND", "message": "Not found"}
        }

        with pytest.raises(TopicNotFoundError):
            await connected_admin.alter_topic_config("unknown", {"retention.ms": "86400000"})

    async def test_alter_topic_config_error(
        self, connected_admin: Admin, mock_rpc_client: MockRpcClient
    ) -> None:
        """Test alter topic config with generic error."""
        mock_rpc_client.kafka.admin.alterTopicConfig.return_value = {
            "error": {"code": "UNKNOWN", "message": "Unknown error"}
        }

        with pytest.raises(KafkaError, match="Unknown error"):
            await connected_admin.alter_topic_config("topic1", {"retention.ms": "86400000"})

    async def test_alter_topic_config_not_connected(self, kafka_instance: Kafka) -> None:
        """Test alter_topic_config raises error when not connected."""
        admin = kafka_instance.admin()

        with pytest.raises(KafkaConnectionError, match="Admin not connected"):
            await admin.alter_topic_config("topic1", {"retention.ms": "86400000"})

    async def test_alter_topic_config_failure(
        self, connected_admin: Admin, mock_rpc_client: MockRpcClient
    ) -> None:
        """Test alter_topic_config failure."""
        mock_rpc_client.kafka.admin.alterTopicConfig.side_effect = Exception("Failed")

        with pytest.raises(KafkaError, match="Failed to alter topic config"):
            await connected_admin.alter_topic_config("topic1", {"retention.ms": "86400000"})
