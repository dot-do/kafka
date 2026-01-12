"""
Kafka Admin implementation for kafka-do SDK.

Provides async admin operations for managing Kafka topics and groups
using rpc-do for the underlying transport.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from .types import (
    ConsumerGroupMetadata,
    KafkaConnectionError,
    KafkaError,
    PartitionMetadata,
    TopicAlreadyExistsError,
    TopicConfig,
    TopicMetadata,
    TopicNotFoundError,
)

if TYPE_CHECKING:
    from .kafka import Kafka


class Admin:
    """
    Async Kafka admin client for managing topics and groups.

    The admin client connects to a Kafka service via rpc-do and provides
    methods for topic and consumer group management.

    Example:
        kafka = Kafka(brokers=['localhost:9092'])
        admin = kafka.admin()
        await admin.connect()

        # Create topics
        await admin.create_topics(['new-topic'])

        # List topics
        topics = await admin.list_topics()

        # Delete topics
        await admin.delete_topics(['old-topic'])

        await admin.disconnect()
    """

    __slots__ = (
        "_kafka",
        "_client",
        "_connected",
        "_request_timeout_ms",
    )

    def __init__(
        self,
        kafka: Kafka,
        *,
        request_timeout_ms: int = 30000,
    ) -> None:
        """
        Initialize the admin client.

        Args:
            kafka: The parent Kafka instance
            request_timeout_ms: Request timeout in milliseconds
        """
        self._kafka = kafka
        self._client: Any = None
        self._connected = False
        self._request_timeout_ms = request_timeout_ms

    @property
    def connected(self) -> bool:
        """Return whether the admin client is connected."""
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
            raise KafkaConnectionError(f"Failed to connect admin: {e}") from e

    async def disconnect(self) -> None:
        """Disconnect from the Kafka service."""
        if not self._connected:
            return

        self._connected = False
        self._client = None

    async def create_topics(
        self,
        topics: list[str | TopicConfig],
        *,
        validate_only: bool = False,
    ) -> None:
        """
        Create new topics.

        Args:
            topics: List of topic names or TopicConfig objects
            validate_only: If True, only validate without creating

        Raises:
            KafkaConnectionError: If not connected
            TopicAlreadyExistsError: If a topic already exists
            KafkaError: If creation fails
        """
        if not self._connected:
            raise KafkaConnectionError("Admin not connected")

        topic_configs = []
        for topic in topics:
            if isinstance(topic, str):
                topic_configs.append(
                    {
                        "name": topic,
                        "numPartitions": 1,
                        "replicationFactor": 1,
                        "config": {},
                    }
                )
            else:
                topic_configs.append(
                    {
                        "name": topic.name,
                        "numPartitions": topic.num_partitions,
                        "replicationFactor": topic.replication_factor,
                        "config": topic.config,
                    }
                )

        try:
            result = await self._client.kafka.admin.createTopics(
                topics=topic_configs,
                validateOnly=validate_only,
            )

            # Check for errors in result
            if isinstance(result, list):
                for item in result:
                    if item.get("error"):
                        error_code = item["error"].get("code", "")
                        if error_code == "TOPIC_ALREADY_EXISTS":
                            raise TopicAlreadyExistsError(item.get("name", "unknown"))
                        raise KafkaError(
                            item["error"].get("message", "Unknown error"),
                            code=error_code,
                        )
        except TopicAlreadyExistsError:
            raise
        except KafkaError:
            raise
        except Exception as e:
            raise KafkaError(f"Failed to create topics: {e}") from e

    async def delete_topics(self, topics: list[str]) -> None:
        """
        Delete topics.

        Args:
            topics: List of topic names to delete

        Raises:
            KafkaConnectionError: If not connected
            TopicNotFoundError: If a topic is not found
            KafkaError: If deletion fails
        """
        if not self._connected:
            raise KafkaConnectionError("Admin not connected")

        try:
            result = await self._client.kafka.admin.deleteTopics(topics=topics)

            # Check for errors in result
            if isinstance(result, list):
                for item in result:
                    if item.get("error"):
                        error_code = item["error"].get("code", "")
                        if error_code == "TOPIC_NOT_FOUND":
                            raise TopicNotFoundError(item.get("name", "unknown"))
                        raise KafkaError(
                            item["error"].get("message", "Unknown error"),
                            code=error_code,
                        )
        except TopicNotFoundError:
            raise
        except KafkaError:
            raise
        except Exception as e:
            raise KafkaError(f"Failed to delete topics: {e}") from e

    async def list_topics(
        self,
        *,
        include_internal: bool = False,
    ) -> list[str]:
        """
        List all topics.

        Args:
            include_internal: Include internal topics (starting with __)

        Returns:
            List of topic names

        Raises:
            KafkaConnectionError: If not connected
            KafkaError: If listing fails
        """
        if not self._connected:
            raise KafkaConnectionError("Admin not connected")

        try:
            result = await self._client.kafka.admin.listTopics(
                includeInternal=include_internal,
            )
            return list(result)
        except Exception as e:
            raise KafkaError(f"Failed to list topics: {e}") from e

    async def describe_topics(
        self,
        topics: list[str],
    ) -> list[TopicMetadata]:
        """
        Get metadata about topics.

        Args:
            topics: List of topic names to describe

        Returns:
            List of TopicMetadata objects

        Raises:
            KafkaConnectionError: If not connected
            TopicNotFoundError: If a topic is not found
            KafkaError: If describe fails
        """
        if not self._connected:
            raise KafkaConnectionError("Admin not connected")

        try:
            result = await self._client.kafka.admin.describeTopics(topics=topics)

            metadata_list = []
            for item in result:
                if item.get("error"):
                    error_code = item["error"].get("code", "")
                    if error_code == "TOPIC_NOT_FOUND":
                        raise TopicNotFoundError(item.get("name", "unknown"))
                    raise KafkaError(
                        item["error"].get("message", "Unknown error"),
                        code=error_code,
                    )

                metadata_list.append(
                    TopicMetadata(
                        name=item["name"],
                        partitions=item.get("partitions", 1),
                        replication_factor=item.get("replicationFactor", 1),
                        config=item.get("config", {}),
                    )
                )

            return metadata_list
        except TopicNotFoundError:
            raise
        except KafkaError:
            raise
        except Exception as e:
            raise KafkaError(f"Failed to describe topics: {e}") from e

    async def describe_partitions(
        self,
        topic: str,
    ) -> list[PartitionMetadata]:
        """
        Get partition metadata for a topic.

        Args:
            topic: The topic name

        Returns:
            List of PartitionMetadata objects

        Raises:
            KafkaConnectionError: If not connected
            TopicNotFoundError: If topic is not found
        """
        if not self._connected:
            raise KafkaConnectionError("Admin not connected")

        try:
            result = await self._client.kafka.admin.describePartitions(topic=topic)

            if result.get("error"):
                error_code = result["error"].get("code", "")
                if error_code == "TOPIC_NOT_FOUND":
                    raise TopicNotFoundError(topic)
                raise KafkaError(
                    result["error"].get("message", "Unknown error"),
                    code=error_code,
                )

            partitions = []
            for item in result.get("partitions", []):
                partitions.append(
                    PartitionMetadata(
                        topic=topic,
                        partition=item["partition"],
                        leader=item.get("leader", -1),
                        replicas=item.get("replicas", []),
                        isr=item.get("isr", []),
                    )
                )

            return partitions
        except TopicNotFoundError:
            raise
        except KafkaError:
            raise
        except Exception as e:
            raise KafkaError(f"Failed to describe partitions: {e}") from e

    async def list_consumer_groups(self) -> list[str]:
        """
        List all consumer groups.

        Returns:
            List of consumer group IDs

        Raises:
            KafkaConnectionError: If not connected
            KafkaError: If listing fails
        """
        if not self._connected:
            raise KafkaConnectionError("Admin not connected")

        try:
            result = await self._client.kafka.admin.listConsumerGroups()
            return list(result)
        except Exception as e:
            raise KafkaError(f"Failed to list consumer groups: {e}") from e

    async def describe_consumer_groups(
        self,
        group_ids: list[str],
    ) -> list[ConsumerGroupMetadata]:
        """
        Get metadata about consumer groups.

        Args:
            group_ids: List of group IDs to describe

        Returns:
            List of ConsumerGroupMetadata objects

        Raises:
            KafkaConnectionError: If not connected
            KafkaError: If describe fails
        """
        if not self._connected:
            raise KafkaConnectionError("Admin not connected")

        try:
            result = await self._client.kafka.admin.describeConsumerGroups(
                groupIds=group_ids,
            )

            groups = []
            for item in result:
                if item.get("error"):
                    raise KafkaError(
                        item["error"].get("message", "Unknown error"),
                        code=item["error"].get("code"),
                    )

                groups.append(
                    ConsumerGroupMetadata(
                        group_id=item["groupId"],
                        state=item.get("state", "Unknown"),
                        members=item.get("members", []),
                        coordinator=item.get("coordinator"),
                    )
                )

            return groups
        except KafkaError:
            raise
        except Exception as e:
            raise KafkaError(f"Failed to describe consumer groups: {e}") from e

    async def delete_consumer_groups(self, group_ids: list[str]) -> None:
        """
        Delete consumer groups.

        Args:
            group_ids: List of group IDs to delete

        Raises:
            KafkaConnectionError: If not connected
            KafkaError: If deletion fails
        """
        if not self._connected:
            raise KafkaConnectionError("Admin not connected")

        try:
            result = await self._client.kafka.admin.deleteConsumerGroups(
                groupIds=group_ids,
            )

            # Check for errors in result
            if isinstance(result, list):
                for item in result:
                    if item.get("error"):
                        raise KafkaError(
                            item["error"].get("message", "Unknown error"),
                            code=item["error"].get("code"),
                        )
        except KafkaError:
            raise
        except Exception as e:
            raise KafkaError(f"Failed to delete consumer groups: {e}") from e

    async def alter_topic_config(
        self,
        topic: str,
        config: dict[str, str],
    ) -> None:
        """
        Alter topic configuration.

        Args:
            topic: The topic name
            config: Configuration key-value pairs to set

        Raises:
            KafkaConnectionError: If not connected
            TopicNotFoundError: If topic is not found
            KafkaError: If alter fails
        """
        if not self._connected:
            raise KafkaConnectionError("Admin not connected")

        try:
            result = await self._client.kafka.admin.alterTopicConfig(
                topic=topic,
                config=config,
            )

            if result.get("error"):
                error_code = result["error"].get("code", "")
                if error_code == "TOPIC_NOT_FOUND":
                    raise TopicNotFoundError(topic)
                raise KafkaError(
                    result["error"].get("message", "Unknown error"),
                    code=error_code,
                )
        except TopicNotFoundError:
            raise
        except KafkaError:
            raise
        except Exception as e:
            raise KafkaError(f"Failed to alter topic config: {e}") from e

    async def __aenter__(self) -> Admin:
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
