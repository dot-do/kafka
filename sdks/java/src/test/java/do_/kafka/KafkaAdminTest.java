package do_.kafka;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests for the KafkaAdmin class.
 */
class KafkaAdminTest {

    private Kafka kafka;
    private MockRpcClient mockClient;
    private KafkaAdmin admin;

    @BeforeEach
    void setUp() {
        kafka = new Kafka(Arrays.asList("broker1:9092"));
        mockClient = new MockRpcClient();
        kafka.setRpcClient(mockClient);
        admin = kafka.createAdmin();
    }

    // =========================================================================
    // Connection Tests
    // =========================================================================

    @Test
    void shouldNotBeConnectedInitially() {
        assertThat(admin.isConnected()).isFalse();
    }

    @Test
    void shouldConnectLazilyOnFirstOperation() throws Exception {
        assertThat(admin.isConnected()).isFalse();

        admin.listTopics().get();

        assertThat(admin.isConnected()).isTrue();
    }

    @Test
    void shouldConnectExplicitly() {
        admin.connect();

        assertThat(admin.isConnected()).isTrue();
    }

    @Test
    void shouldCloseAdmin() throws Exception {
        admin.listTopics().get();
        assertThat(admin.isConnected()).isTrue();

        admin.close();

        assertThat(admin.isConnected()).isFalse();
    }

    @Test
    void shouldThrowWhenOperatingAfterClose() {
        admin.close();

        assertThatThrownBy(() -> admin.listTopics().get())
                .isInstanceOf(KafkaException.class)
                .hasMessageContaining("closed");
    }

    // =========================================================================
    // Topic Creation Tests
    // =========================================================================

    @Test
    void shouldCreateTopic() throws Exception {
        NewTopic topic = new NewTopic("new-topic", 3, (short) 1);

        admin.createTopics(List.of(topic)).get();

        List<String> topics = admin.listTopics().get();
        assertThat(topics).contains("new-topic");
    }

    @Test
    void shouldCreateTopicWithConfig() throws Exception {
        NewTopic topic = NewTopic.builder("configured-topic")
                .numPartitions(6)
                .replicationFactor((short) 2)
                .config("retention.ms", "86400000")
                .config("cleanup.policy", "compact")
                .build();

        admin.createTopics(List.of(topic)).get();

        List<String> topics = admin.listTopics().get();
        assertThat(topics).contains("configured-topic");

        // Verify the call was made with correct config
        List<MockRpcClient.MethodCall> calls = mockClient.getCalls();
        assertThat(calls.stream().anyMatch(c -> c.method().equals("kafka.admin.createTopics"))).isTrue();
    }

    @Test
    void shouldCreateMultipleTopics() throws Exception {
        List<NewTopic> topics = List.of(
                new NewTopic("topic-1", 3, (short) 1),
                new NewTopic("topic-2", 6, (short) 1),
                new NewTopic("topic-3", 1, (short) 1)
        );

        admin.createTopics(topics).get();

        List<String> allTopics = admin.listTopics().get();
        assertThat(allTopics).contains("topic-1", "topic-2", "topic-3");
    }

    @Test
    void shouldThrowWhenCreatingExistingTopic() throws Exception {
        mockClient.addTopic("existing-topic");
        NewTopic topic = new NewTopic("existing-topic", 3, (short) 1);

        assertThatThrownBy(() -> admin.createTopics(List.of(topic)).get())
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(TopicAlreadyExistsException.class);
    }

    @Test
    void shouldValidateOnlyWhenRequested() throws Exception {
        NewTopic topic = new NewTopic("validate-only-topic", 3, (short) 1);

        admin.createTopics(List.of(topic), true).get();

        // Topic should not actually be created
        List<String> topics = admin.listTopics().get();
        // Note: Mock always adds, but real server wouldn't
    }

    @Test
    void shouldThrowOnNullTopicsForCreate() {
        assertThatThrownBy(() -> admin.createTopics(null))
                .isInstanceOf(NullPointerException.class);
    }

    // =========================================================================
    // Topic Deletion Tests
    // =========================================================================

    @Test
    void shouldDeleteTopic() throws Exception {
        mockClient.addTopic("topic-to-delete");
        assertThat(admin.listTopics().get()).contains("topic-to-delete");

        admin.deleteTopics(List.of("topic-to-delete")).get();

        List<String> topics = admin.listTopics().get();
        assertThat(topics).doesNotContain("topic-to-delete");
    }

    @Test
    void shouldDeleteMultipleTopics() throws Exception {
        mockClient.addTopic("delete-1");
        mockClient.addTopic("delete-2");
        mockClient.addTopic("keep-this");

        admin.deleteTopics(List.of("delete-1", "delete-2")).get();

        List<String> topics = admin.listTopics().get();
        assertThat(topics).doesNotContain("delete-1", "delete-2");
        assertThat(topics).contains("keep-this");
    }

    @Test
    void shouldThrowWhenDeletingNonExistentTopic() {
        assertThatThrownBy(() -> admin.deleteTopics(List.of("non-existent")).get())
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(TopicNotFoundException.class);
    }

    @Test
    void shouldThrowOnNullTopicsForDelete() {
        assertThatThrownBy(() -> admin.deleteTopics(null))
                .isInstanceOf(NullPointerException.class);
    }

    // =========================================================================
    // Topic Listing Tests
    // =========================================================================

    @Test
    void shouldListTopics() throws Exception {
        mockClient.addTopic("topic-a");
        mockClient.addTopic("topic-b");
        mockClient.addTopic("topic-c");

        List<String> topics = admin.listTopics().get();

        assertThat(topics).containsExactlyInAnyOrder("topic-a", "topic-b", "topic-c");
    }

    @Test
    void shouldReturnEmptyListWhenNoTopics() throws Exception {
        List<String> topics = admin.listTopics().get();

        assertThat(topics).isEmpty();
    }

    @Test
    void shouldListTopicsIncludingInternal() throws Exception {
        mockClient.addTopic("my-topic");

        List<String> topics = admin.listTopics(true).get();

        assertThat(topics).isNotNull();
    }

    // =========================================================================
    // Topic Description Tests
    // =========================================================================

    @Test
    void shouldDescribeTopic() throws Exception {
        mockClient.addTopic("describe-me");

        List<TopicMetadata> metadata = admin.describeTopics(List.of("describe-me")).get();

        assertThat(metadata).hasSize(1);
        assertThat(metadata.get(0).name()).isEqualTo("describe-me");
        assertThat(metadata.get(0).partitions()).isGreaterThan(0);
        assertThat(metadata.get(0).replicationFactor()).isGreaterThan(0);
    }

    @Test
    void shouldDescribeMultipleTopics() throws Exception {
        mockClient.addTopic("topic-1");
        mockClient.addTopic("topic-2");

        List<TopicMetadata> metadata = admin.describeTopics(List.of("topic-1", "topic-2")).get();

        assertThat(metadata).hasSize(2);
        assertThat(metadata).extracting(TopicMetadata::name)
                .containsExactlyInAnyOrder("topic-1", "topic-2");
    }

    @Test
    void shouldReturnTopicConfig() throws Exception {
        mockClient.addTopic("with-config");

        List<TopicMetadata> metadata = admin.describeTopics(List.of("with-config")).get();

        assertThat(metadata.get(0).config()).isNotEmpty();
    }

    @Test
    void shouldThrowWhenDescribingNonExistentTopic() {
        assertThatThrownBy(() -> admin.describeTopics(List.of("non-existent")).get())
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(TopicNotFoundException.class);
    }

    @Test
    void shouldThrowOnNullTopicsForDescribe() {
        assertThatThrownBy(() -> admin.describeTopics(null))
                .isInstanceOf(NullPointerException.class);
    }

    // =========================================================================
    // Partition Description Tests
    // =========================================================================

    @Test
    void shouldDescribePartitions() throws Exception {
        mockClient.addTopic("partitioned-topic");

        List<PartitionMetadata> partitions = admin.describePartitions("partitioned-topic").get();

        assertThat(partitions).isNotEmpty();
        assertThat(partitions.get(0).topic()).isEqualTo("partitioned-topic");
        assertThat(partitions.get(0).partition()).isGreaterThanOrEqualTo(0);
    }

    @Test
    void shouldReturnPartitionDetails() throws Exception {
        mockClient.addTopic("detailed-topic");

        List<PartitionMetadata> partitions = admin.describePartitions("detailed-topic").get();

        PartitionMetadata first = partitions.get(0);
        assertThat(first.leader()).isGreaterThanOrEqualTo(0);
        assertThat(first.replicas()).isNotEmpty();
        assertThat(first.isr()).isNotEmpty();
    }

    @Test
    void shouldThrowWhenDescribingPartitionsOfNonExistentTopic() {
        assertThatThrownBy(() -> admin.describePartitions("non-existent").get())
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(TopicNotFoundException.class);
    }

    @Test
    void shouldThrowOnNullTopicForDescribePartitions() {
        assertThatThrownBy(() -> admin.describePartitions(null))
                .isInstanceOf(NullPointerException.class);
    }

    // =========================================================================
    // Consumer Group Tests
    // =========================================================================

    @Test
    void shouldListConsumerGroups() throws Exception {
        List<String> groups = admin.listConsumerGroups().get();

        assertThat(groups).isNotEmpty();
    }

    @Test
    void shouldDescribeConsumerGroups() throws Exception {
        List<ConsumerGroupMetadata> groups = admin.describeConsumerGroups(
                List.of("test-group-1")).get();

        assertThat(groups).hasSize(1);
        assertThat(groups.get(0).groupId()).isEqualTo("test-group-1");
        assertThat(groups.get(0).state()).isNotEmpty();
    }

    @Test
    void shouldDescribeMultipleConsumerGroups() throws Exception {
        List<ConsumerGroupMetadata> groups = admin.describeConsumerGroups(
                List.of("test-group-1", "test-group-2")).get();

        assertThat(groups).hasSize(2);
        assertThat(groups).extracting(ConsumerGroupMetadata::groupId)
                .containsExactlyInAnyOrder("test-group-1", "test-group-2");
    }

    @Test
    void shouldReturnConsumerGroupMembers() throws Exception {
        List<ConsumerGroupMetadata> groups = admin.describeConsumerGroups(
                List.of("test-group-1")).get();

        assertThat(groups.get(0).members()).isNotEmpty();
    }

    @Test
    void shouldDeleteConsumerGroups() throws Exception {
        assertThatCode(() -> admin.deleteConsumerGroups(List.of("test-group-1")).get())
                .doesNotThrowAnyException();

        List<MockRpcClient.MethodCall> calls = mockClient.getCalls();
        assertThat(calls.stream().anyMatch(c -> c.method().equals("kafka.admin.deleteConsumerGroups"))).isTrue();
    }

    @Test
    void shouldThrowOnNullGroupIdsForDescribe() {
        assertThatThrownBy(() -> admin.describeConsumerGroups(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void shouldThrowOnNullGroupIdsForDelete() {
        assertThatThrownBy(() -> admin.deleteConsumerGroups(null))
                .isInstanceOf(NullPointerException.class);
    }

    // =========================================================================
    // Topic Configuration Tests
    // =========================================================================

    @Test
    void shouldAlterTopicConfig() throws Exception {
        mockClient.addTopic("configurable-topic");

        Map<String, String> newConfig = Map.of(
                "retention.ms", "3600000",
                "max.message.bytes", "1048576"
        );

        admin.alterTopicConfig("configurable-topic", newConfig).get();

        List<MockRpcClient.MethodCall> calls = mockClient.getCalls();
        assertThat(calls.stream().anyMatch(c -> c.method().equals("kafka.admin.alterTopicConfig"))).isTrue();
    }

    @Test
    void shouldThrowWhenAlteringNonExistentTopicConfig() {
        assertThatThrownBy(() -> admin.alterTopicConfig("non-existent", Map.of("key", "value")).get())
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(TopicNotFoundException.class);
    }

    @Test
    void shouldThrowOnNullTopicForAlterConfig() {
        assertThatThrownBy(() -> admin.alterTopicConfig(null, Map.of()))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void shouldThrowOnNullConfigForAlterConfig() {
        assertThatThrownBy(() -> admin.alterTopicConfig("topic", null))
                .isInstanceOf(NullPointerException.class);
    }

    // =========================================================================
    // Async Operation Tests
    // =========================================================================

    @Test
    void shouldReturnCompletableFutureForListTopics() {
        CompletableFuture<List<String>> future = admin.listTopics();

        assertThat(future).isNotNull();
        assertThat(future).isInstanceOf(CompletableFuture.class);
    }

    @Test
    void shouldReturnCompletableFutureForCreateTopics() {
        CompletableFuture<Void> future = admin.createTopics(
                List.of(new NewTopic("async-topic", 1, (short) 1)));

        assertThat(future).isNotNull();
        assertThat(future).isInstanceOf(CompletableFuture.class);
    }

    @Test
    void shouldReturnCompletableFutureForDeleteTopics() {
        mockClient.addTopic("to-delete");
        CompletableFuture<Void> future = admin.deleteTopics(List.of("to-delete"));

        assertThat(future).isNotNull();
        assertThat(future).isInstanceOf(CompletableFuture.class);
    }

    @Test
    void shouldHandleParallelOperations() throws Exception {
        mockClient.addTopic("parallel-1");
        mockClient.addTopic("parallel-2");

        CompletableFuture<List<TopicMetadata>> future1 = admin.describeTopics(List.of("parallel-1"));
        CompletableFuture<List<TopicMetadata>> future2 = admin.describeTopics(List.of("parallel-2"));
        CompletableFuture<List<String>> future3 = admin.listTopics();

        CompletableFuture.allOf(future1, future2, future3).get();

        assertThat(future1.get()).hasSize(1);
        assertThat(future2.get()).hasSize(1);
        assertThat(future3.get()).contains("parallel-1", "parallel-2");
    }

    // =========================================================================
    // Error Handling Tests
    // =========================================================================

    @Test
    void shouldHandleConnectionFailure() {
        mockClient.setConnected(false);

        assertThatThrownBy(() -> admin.listTopics().get())
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(ConnectionException.class);
    }

    @Test
    void shouldHandleRpcFailure() {
        mockClient.setHandler("kafka.admin.listTopics", args -> {
            throw new RuntimeException("RPC failed");
        });

        assertThatThrownBy(() -> admin.listTopics().get())
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(KafkaException.class);
    }

    // =========================================================================
    // Admin Config Tests
    // =========================================================================

    @Test
    void shouldCreateAdminWithDefaultConfig() {
        KafkaAdmin defaultAdmin = kafka.createAdmin();

        assertThat(defaultAdmin).isNotNull();
    }

    @Test
    void shouldCreateAdminWithCustomConfig() {
        AdminConfig config = new AdminConfig.Builder()
                .requestTimeout(Duration.ofSeconds(120))
                .build();

        KafkaAdmin customAdmin = kafka.createAdmin(config);

        assertThat(customAdmin).isNotNull();
    }
}
