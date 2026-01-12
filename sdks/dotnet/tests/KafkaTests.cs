// ============================================================================
// Kafka.Do Tests
// Comprehensive xUnit tests with mocked RPC client
// ============================================================================

using System.Text.Json;
using System.Text.Json.Nodes;
using Xunit;

namespace Kafka.Do.Tests;

// ============================================================================
// Mock RPC Client
// ============================================================================

/// <summary>
/// Mock RPC client for testing Kafka.Do operations.
/// </summary>
public class MockRpcClient : IRpcClient
{
    private readonly Dictionary<string, Func<object?, JsonNode?>> _handlers = new();
    private readonly List<(string Method, object? Args)> _calls = new();
    private bool _disposed;

    /// <summary>
    /// Gets the list of method calls made to this client.
    /// </summary>
    public IReadOnlyList<(string Method, object? Args)> Calls => _calls;

    /// <summary>
    /// Registers a handler for the specified method.
    /// </summary>
    public void On(string method, Func<object?, JsonNode?> handler)
    {
        _handlers[method] = handler;
    }

    /// <summary>
    /// Registers a handler that returns the specified result.
    /// </summary>
    public void OnReturn(string method, JsonNode? result)
    {
        _handlers[method] = _ => result;
    }

    /// <summary>
    /// Registers a handler that throws the specified exception.
    /// </summary>
    public void OnThrow(string method, Exception exception)
    {
        _handlers[method] = _ => throw exception;
    }

    /// <inheritdoc/>
    public Task<JsonNode?> CallAsync(string method, object? args = null, CancellationToken cancellationToken = default)
    {
        if (_disposed)
        {
            throw new ObjectDisposedException(nameof(MockRpcClient));
        }

        _calls.Add((method, args));

        if (_handlers.TryGetValue(method, out var handler))
        {
            return Task.FromResult(handler(args));
        }

        throw new InvalidOperationException($"No handler registered for method: {method}");
    }

    /// <inheritdoc/>
    public ValueTask DisposeAsync()
    {
        _disposed = true;
        return ValueTask.CompletedTask;
    }

    /// <summary>
    /// Clears recorded calls.
    /// </summary>
    public void ClearCalls() => _calls.Clear();
}

// ============================================================================
// Kafka Client Tests
// ============================================================================

public class KafkaClientTests
{
    [Fact]
    public void NewKafka_WithBrokers_StoresBrokers()
    {
        var kafka = new Kafka(new[] { "broker1:9092", "broker2:9092" });

        Assert.Equal(2, kafka.Brokers.Count);
        Assert.Equal("broker1:9092", kafka.Brokers[0]);
        Assert.Equal("broker2:9092", kafka.Brokers[1]);
    }

    [Fact]
    public void NewKafka_WithConfig_StoresConfig()
    {
        var config = new KafkaConfig
        {
            Brokers = new[] { "broker1:9092" },
            ClientId = "test-client",
            Timeout = TimeSpan.FromSeconds(60),
            ConnectionUrl = "custom.kafka.do"
        };

        var kafka = new Kafka(config);

        Assert.Equal("test-client", kafka.ClientId);
        Assert.Equal("custom.kafka.do", kafka.ConnectionUrl);
    }

    [Fact]
    public void Brokers_ReturnsDefensiveCopy()
    {
        var kafka = new Kafka(new[] { "broker1:9092", "broker2:9092" });

        var brokers1 = kafka.Brokers.ToList();
        var brokers2 = kafka.Brokers.ToList();

        brokers1[0] = "modified";
        Assert.Equal("broker1:9092", brokers2[0]);
    }

    [Fact]
    public void SetRpcClient_SetsClient()
    {
        var kafka = new Kafka(new[] { "broker1:9092" });
        var mockClient = new MockRpcClient();

        kafka.SetRpcClient(mockClient);

        // Creating a producer should not throw
        var producer = kafka.CreateProducer<string, string>();
        Assert.NotNull(producer);
    }

    [Fact]
    public async Task GetRpcClient_WithoutClient_ThrowsConnectionException()
    {
        var kafka = new Kafka(new[] { "broker1:9092" });

        var ex = await Assert.ThrowsAsync<KafkaConnectionException>(() =>
            kafka.GetRpcClientAsync());

        Assert.Contains("RPC client not configured", ex.Message);
    }

    [Fact]
    public void CreateProducer_ReturnsProducer()
    {
        var kafka = new Kafka(new[] { "broker1:9092" });

        var producer = kafka.CreateProducer<string, string>();

        Assert.NotNull(producer);
    }

    [Fact]
    public void CreateConsumer_ReturnsConsumer()
    {
        var kafka = new Kafka(new[] { "broker1:9092" });

        var consumer = kafka.CreateConsumer<string, string>("test-group");

        Assert.NotNull(consumer);
        Assert.Equal("test-group", consumer.GroupId);
    }

    [Fact]
    public void CreateAdmin_ReturnsAdmin()
    {
        var kafka = new Kafka(new[] { "broker1:9092" });

        var admin = kafka.CreateAdmin();

        Assert.NotNull(admin);
    }

    [Fact]
    public async Task Dispose_DisposesRpcClient()
    {
        var kafka = new Kafka(new[] { "broker1:9092" });
        var mockClient = new MockRpcClient();
        kafka.SetRpcClient(mockClient);

        await kafka.DisposeAsync();

        // Should not throw when accessing disposed state
        await Assert.ThrowsAsync<ObjectDisposedException>(
            () => mockClient.CallAsync("test"));
    }

    [Fact]
    public async Task Dispose_AfterDisposed_DoesNotThrow()
    {
        var kafka = new Kafka(new[] { "broker1:9092" });

        await kafka.DisposeAsync();
        await kafka.DisposeAsync(); // Should not throw
    }
}

// ============================================================================
// Producer Tests
// ============================================================================

public class ProducerTests
{
    private static (Kafka Kafka, MockRpcClient Client, Producer<string, string> Producer) CreateProducer()
    {
        var kafka = new Kafka(new[] { "broker1:9092" });
        var mockClient = new MockRpcClient();
        kafka.SetRpcClient(mockClient);
        var producer = kafka.CreateProducer<string, string>();
        return (kafka, mockClient, producer);
    }

    [Fact]
    public async Task ConnectAsync_SetsConnectedState()
    {
        var (kafka, client, producer) = CreateProducer();

        await producer.ConnectAsync();

        Assert.True(producer.IsConnected);
    }

    [Fact]
    public async Task ConnectAsync_WhenAlreadyConnected_DoesNotThrow()
    {
        var (kafka, client, producer) = CreateProducer();

        await producer.ConnectAsync();
        await producer.ConnectAsync(); // Should not throw

        Assert.True(producer.IsConnected);
    }

    [Fact]
    public async Task ProduceAsync_SendsMessage()
    {
        var (kafka, client, producer) = CreateProducer();
        client.OnReturn("kafka.producer.send", JsonNode.Parse(@"{
            ""topic"": ""test-topic"",
            ""partition"": 0,
            ""offset"": 42,
            ""timestamp"": 1704067200000
        }"));

        await producer.ConnectAsync();
        var result = await producer.ProduceAsync("test-topic", new Message<string, string>
        {
            Key = "key1",
            Value = "value1"
        });

        Assert.Equal("test-topic", result.Topic);
        Assert.Equal(0, result.Partition);
        Assert.Equal(42, result.Offset);
        Assert.Single(client.Calls);
        Assert.Equal("kafka.producer.send", client.Calls[0].Method);
    }

    [Fact]
    public async Task ProduceAsync_WithPartition_IncludesPartition()
    {
        var (kafka, client, producer) = CreateProducer();
        client.OnReturn("kafka.producer.send", JsonNode.Parse(@"{
            ""topic"": ""test-topic"",
            ""partition"": 5,
            ""offset"": 100
        }"));

        await producer.ConnectAsync();
        await producer.ProduceAsync("test-topic", new Message<string, string>
        {
            Value = "value1"
        }, partition: 5);

        var args = client.Calls[0].Args as Dictionary<string, object?>;
        Assert.NotNull(args);
        Assert.Equal(5, args["partition"]);
    }

    [Fact]
    public async Task ProduceAsync_WithHeaders_IncludesHeaders()
    {
        var (kafka, client, producer) = CreateProducer();
        client.OnReturn("kafka.producer.send", JsonNode.Parse(@"{
            ""topic"": ""test-topic"",
            ""partition"": 0,
            ""offset"": 0
        }"));

        var headers = new Headers();
        headers.Add("trace-id", System.Text.Encoding.UTF8.GetBytes("abc123"));

        await producer.ConnectAsync();
        await producer.ProduceAsync("test-topic", new Message<string, string>
        {
            Value = "value1",
            Headers = headers
        });

        var args = client.Calls[0].Args as Dictionary<string, object?>;
        Assert.NotNull(args);
        Assert.NotNull(args["headers"]);
    }

    [Fact]
    public async Task ProduceAsync_WhenNotConnected_ThrowsConnectionException()
    {
        var (kafka, client, producer) = CreateProducer();

        await Assert.ThrowsAsync<KafkaConnectionException>(() =>
            producer.ProduceAsync("test-topic", new Message<string, string> { Value = "value" }));
    }

    [Fact]
    public async Task ProduceAsync_WithError_ThrowsProducerException()
    {
        var (kafka, client, producer) = CreateProducer();
        client.OnReturn("kafka.producer.send", JsonNode.Parse(@"{
            ""error"": {
                ""code"": ""MESSAGE_TOO_LARGE"",
                ""message"": ""Message too large""
            }
        }"));

        await producer.ConnectAsync();

        var ex = await Assert.ThrowsAsync<ProducerException>(() =>
            producer.ProduceAsync("test-topic", new Message<string, string> { Value = "value" }));

        Assert.Equal("Message too large", ex.Message);
    }

    [Fact]
    public async Task ProduceBatchAsync_SendsMultipleMessages()
    {
        var (kafka, client, producer) = CreateProducer();
        client.OnReturn("kafka.producer.sendBatch", JsonNode.Parse(@"[
            {""topic"": ""topic1"", ""partition"": 0, ""offset"": 1},
            {""topic"": ""topic2"", ""partition"": 0, ""offset"": 2}
        ]"));

        await producer.ConnectAsync();
        var results = await producer.ProduceBatchAsync(new[]
        {
            ("topic1", new Message<string, string> { Value = "value1" }),
            ("topic2", new Message<string, string> { Value = "value2" })
        });

        Assert.Equal(2, results.Count);
        Assert.Equal(1, results[0].Offset);
        Assert.Equal(2, results[1].Offset);
    }

    [Fact]
    public async Task FlushAsync_CallsFlush()
    {
        var (kafka, client, producer) = CreateProducer();
        client.OnReturn("kafka.producer.flush", null);

        await producer.ConnectAsync();
        await producer.FlushAsync();

        Assert.Contains(client.Calls, c => c.Method == "kafka.producer.flush");
    }

    [Fact]
    public async Task PartitionsForAsync_ReturnsPartitions()
    {
        var (kafka, client, producer) = CreateProducer();
        client.OnReturn("kafka.producer.partitionsFor", JsonNode.Parse("[0, 1, 2]"));

        await producer.ConnectAsync();
        var partitions = await producer.PartitionsForAsync("test-topic");

        Assert.Equal(3, partitions.Count);
        Assert.Equal(new[] { 0, 1, 2 }, partitions);
    }

    [Fact]
    public async Task Dispose_FlushesAndDisconnects()
    {
        var (kafka, client, producer) = CreateProducer();
        client.OnReturn("kafka.producer.flush", null);

        await producer.ConnectAsync();
        await producer.DisposeAsync();

        Assert.False(producer.IsConnected);
    }
}

// ============================================================================
// Consumer Tests
// ============================================================================

public class ConsumerTests
{
    private static (Kafka Kafka, MockRpcClient Client, Consumer<string, string> Consumer) CreateConsumer(string groupId = "test-group")
    {
        var kafka = new Kafka(new[] { "broker1:9092" });
        var mockClient = new MockRpcClient();
        kafka.SetRpcClient(mockClient);
        var consumer = kafka.CreateConsumer<string, string>(groupId);
        return (kafka, mockClient, consumer);
    }

    [Fact]
    public void GroupId_ReturnsConfiguredGroupId()
    {
        var (kafka, client, consumer) = CreateConsumer("my-group");

        Assert.Equal("my-group", consumer.GroupId);
    }

    [Fact]
    public async Task ConnectAsync_SetsConnectedState()
    {
        var (kafka, client, consumer) = CreateConsumer();

        await consumer.ConnectAsync();

        Assert.True(consumer.IsConnected);
    }

    [Fact]
    public async Task SubscribeAsync_SubscribesToTopics()
    {
        var (kafka, client, consumer) = CreateConsumer();
        client.OnReturn("kafka.consumer.subscribe", null);

        await consumer.ConnectAsync();
        await consumer.SubscribeAsync(new[] { "topic1", "topic2" });

        Assert.Contains("topic1", consumer.Subscriptions);
        Assert.Contains("topic2", consumer.Subscriptions);

        var args = client.Calls.First(c => c.Method == "kafka.consumer.subscribe").Args as Dictionary<string, object>;
        Assert.NotNull(args);
        Assert.Equal("test-group", args["groupId"]);
    }

    [Fact]
    public async Task UnsubscribeAsync_ClearsSubscriptions()
    {
        var (kafka, client, consumer) = CreateConsumer();
        client.OnReturn("kafka.consumer.subscribe", null);
        client.OnReturn("kafka.consumer.unsubscribe", null);

        await consumer.ConnectAsync();
        await consumer.SubscribeAsync(new[] { "topic1" });
        await consumer.UnsubscribeAsync();

        Assert.Empty(consumer.Subscriptions);
    }

    [Fact]
    public async Task AssignAsync_AssignsPartitions()
    {
        var (kafka, client, consumer) = CreateConsumer();
        client.OnReturn("kafka.consumer.assign", null);

        await consumer.ConnectAsync();
        await consumer.AssignAsync(new[]
        {
            new TopicPartition("topic1", 0),
            new TopicPartition("topic1", 1)
        });

        Assert.Equal(2, consumer.Assignments.Count);
        Assert.Contains(new TopicPartition("topic1", 0), consumer.Assignments);
        Assert.Contains(new TopicPartition("topic1", 1), consumer.Assignments);
    }

    [Fact]
    public async Task PollAsync_ReturnsMessages()
    {
        var (kafka, client, consumer) = CreateConsumer();
        var keyBase64 = Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes("key1"));
        var valueBase64 = Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes("value1"));

        client.OnReturn("kafka.consumer.poll", JsonNode.Parse($@"[
            {{
                ""topic"": ""test-topic"",
                ""partition"": 0,
                ""offset"": 100,
                ""key"": ""{keyBase64}"",
                ""value"": ""{valueBase64}"",
                ""timestamp"": 1704067200000
            }}
        ]"));

        await consumer.ConnectAsync();
        var messages = await consumer.PollAsync(1000);

        Assert.Single(messages);
        Assert.Equal("test-topic", messages[0].Topic);
        Assert.Equal(0, messages[0].Partition);
        Assert.Equal(100, messages[0].Offset);
        Assert.Equal("key1", messages[0].Message.Key);
        Assert.Equal("value1", messages[0].Message.Value);
    }

    [Fact]
    public async Task ConsumeAsync_YieldsMessages()
    {
        var (kafka, client, consumer) = CreateConsumer();
        var valueBase64 = Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes("value1"));

        var callCount = 0;
        client.On("kafka.consumer.poll", _ =>
        {
            callCount++;
            if (callCount == 1)
            {
                return JsonNode.Parse($@"[
                    {{""topic"": ""test"", ""partition"": 0, ""offset"": 1, ""value"": ""{valueBase64}""}}
                ]");
            }
            throw new OperationCanceledException();
        });

        await consumer.ConnectAsync();

        var messages = new List<ConsumeResult<string, string>>();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(1));

        try
        {
            await foreach (var msg in consumer.ConsumeAsync(cts.Token))
            {
                messages.Add(msg);
                break; // Only consume one message
            }
        }
        catch (OperationCanceledException)
        {
            // Expected
        }

        Assert.Single(messages);
        Assert.Equal("value1", messages[0].Message.Value);
    }

    [Fact]
    public async Task CommitAsync_CommitsOffsets()
    {
        var (kafka, client, consumer) = CreateConsumer();
        client.OnReturn("kafka.consumer.commit", null);

        await consumer.ConnectAsync();
        await consumer.CommitAsync(new Dictionary<TopicPartition, long>
        {
            [new TopicPartition("topic1", 0)] = 100
        });

        var args = client.Calls.First(c => c.Method == "kafka.consumer.commit").Args as Dictionary<string, object>;
        Assert.NotNull(args);
        Assert.NotNull(args["offsets"]);
    }

    [Fact]
    public async Task CommittedAsync_ReturnsCommittedOffsets()
    {
        var (kafka, client, consumer) = CreateConsumer();
        client.OnReturn("kafka.consumer.committed", JsonNode.Parse(@"[
            {""topic"": ""topic1"", ""partition"": 0, ""offset"": 100}
        ]"));

        await consumer.ConnectAsync();
        var offsets = await consumer.CommittedAsync(new[]
        {
            new TopicPartition("topic1", 0)
        });

        Assert.Single(offsets);
        Assert.Equal(100, offsets[new TopicPartition("topic1", 0)]);
    }

    [Fact]
    public async Task SeekAsync_SeeksToOffset()
    {
        var (kafka, client, consumer) = CreateConsumer();
        client.OnReturn("kafka.consumer.seek", null);

        await consumer.ConnectAsync();
        await consumer.SeekAsync(new TopicPartition("topic1", 0), 500);

        var args = client.Calls.First(c => c.Method == "kafka.consumer.seek").Args as Dictionary<string, object>;
        Assert.NotNull(args);
        Assert.Equal("topic1", args["topic"]);
        Assert.Equal(0, args["partition"]);
        Assert.Equal(500L, args["offset"]);
    }

    [Fact]
    public async Task SeekToBeginningAsync_SeeksToBeginning()
    {
        var (kafka, client, consumer) = CreateConsumer();
        client.OnReturn("kafka.consumer.seekToBeginning", null);

        await consumer.ConnectAsync();
        await consumer.SeekToBeginningAsync(new[] { new TopicPartition("topic1", 0) });

        Assert.Contains(client.Calls, c => c.Method == "kafka.consumer.seekToBeginning");
    }

    [Fact]
    public async Task SeekToEndAsync_SeeksToEnd()
    {
        var (kafka, client, consumer) = CreateConsumer();
        client.OnReturn("kafka.consumer.seekToEnd", null);

        await consumer.ConnectAsync();
        await consumer.SeekToEndAsync(new[] { new TopicPartition("topic1", 0) });

        Assert.Contains(client.Calls, c => c.Method == "kafka.consumer.seekToEnd");
    }

    [Fact]
    public async Task PositionAsync_ReturnsPosition()
    {
        var (kafka, client, consumer) = CreateConsumer();
        client.OnReturn("kafka.consumer.position", JsonValue.Create(250L));

        await consumer.ConnectAsync();
        var position = await consumer.PositionAsync(new TopicPartition("topic1", 0));

        Assert.Equal(250, position);
    }

    [Fact]
    public async Task PauseAsync_PausesPartitions()
    {
        var (kafka, client, consumer) = CreateConsumer();
        client.OnReturn("kafka.consumer.pause", null);

        await consumer.ConnectAsync();
        await consumer.PauseAsync(new[] { new TopicPartition("topic1", 0) });

        Assert.Contains(new TopicPartition("topic1", 0), consumer.Paused);
    }

    [Fact]
    public async Task ResumeAsync_ResumesPartitions()
    {
        var (kafka, client, consumer) = CreateConsumer();
        client.OnReturn("kafka.consumer.pause", null);
        client.OnReturn("kafka.consumer.resume", null);

        await consumer.ConnectAsync();
        await consumer.PauseAsync(new[] { new TopicPartition("topic1", 0) });
        await consumer.ResumeAsync(new[] { new TopicPartition("topic1", 0) });

        Assert.DoesNotContain(new TopicPartition("topic1", 0), consumer.Paused);
    }

    [Fact]
    public async Task Dispose_CommitsIfAutoCommitEnabled()
    {
        var (kafka, client, consumer) = CreateConsumer();
        client.OnReturn("kafka.consumer.commit", null);

        await consumer.ConnectAsync();
        await consumer.DisposeAsync();

        Assert.Contains(client.Calls, c => c.Method == "kafka.consumer.commit");
    }
}

// ============================================================================
// Admin Tests
// ============================================================================

public class AdminTests
{
    private static (Kafka Kafka, MockRpcClient Client, Admin Admin) CreateAdmin()
    {
        var kafka = new Kafka(new[] { "broker1:9092" });
        var mockClient = new MockRpcClient();
        kafka.SetRpcClient(mockClient);
        var admin = kafka.CreateAdmin();
        return (kafka, mockClient, admin);
    }

    [Fact]
    public async Task ConnectAsync_SetsConnectedState()
    {
        var (kafka, client, admin) = CreateAdmin();

        await admin.ConnectAsync();

        Assert.True(admin.IsConnected);
    }

    [Fact]
    public async Task CreateTopicAsync_CreatesTopic()
    {
        var (kafka, client, admin) = CreateAdmin();
        client.OnReturn("kafka.admin.createTopics", JsonNode.Parse(@"[
            {""name"": ""new-topic""}
        ]"));

        await admin.ConnectAsync();
        await admin.CreateTopicAsync("new-topic");

        var args = client.Calls.First(c => c.Method == "kafka.admin.createTopics").Args as Dictionary<string, object>;
        Assert.NotNull(args);
    }

    [Fact]
    public async Task CreateTopicsAsync_CreatesMultipleTopics()
    {
        var (kafka, client, admin) = CreateAdmin();
        client.OnReturn("kafka.admin.createTopics", JsonNode.Parse(@"[
            {""name"": ""topic1""},
            {""name"": ""topic2""}
        ]"));

        await admin.ConnectAsync();
        await admin.CreateTopicsAsync(new[]
        {
            new TopicConfig { Name = "topic1", NumPartitions = 3, ReplicationFactor = 2 },
            new TopicConfig { Name = "topic2", NumPartitions = 1, ReplicationFactor = 1 }
        });

        Assert.Contains(client.Calls, c => c.Method == "kafka.admin.createTopics");
    }

    [Fact]
    public async Task CreateTopicsAsync_WithExistingTopic_ThrowsTopicAlreadyExistsException()
    {
        var (kafka, client, admin) = CreateAdmin();
        client.OnReturn("kafka.admin.createTopics", JsonNode.Parse(@"[
            {""name"": ""existing-topic"", ""error"": {""code"": ""TOPIC_ALREADY_EXISTS"", ""message"": ""Topic already exists""}}
        ]"));

        await admin.ConnectAsync();

        var ex = await Assert.ThrowsAsync<TopicAlreadyExistsException>(() =>
            admin.CreateTopicAsync("existing-topic"));

        Assert.Equal("existing-topic", ex.Topic);
    }

    [Fact]
    public async Task DeleteTopicsAsync_DeletesTopics()
    {
        var (kafka, client, admin) = CreateAdmin();
        client.OnReturn("kafka.admin.deleteTopics", JsonNode.Parse(@"[
            {""name"": ""topic1""}
        ]"));

        await admin.ConnectAsync();
        await admin.DeleteTopicsAsync(new[] { "topic1" });

        Assert.Contains(client.Calls, c => c.Method == "kafka.admin.deleteTopics");
    }

    [Fact]
    public async Task DeleteTopicsAsync_WithNonExistentTopic_ThrowsTopicNotFoundException()
    {
        var (kafka, client, admin) = CreateAdmin();
        client.OnReturn("kafka.admin.deleteTopics", JsonNode.Parse(@"[
            {""name"": ""missing-topic"", ""error"": {""code"": ""TOPIC_NOT_FOUND"", ""message"": ""Topic not found""}}
        ]"));

        await admin.ConnectAsync();

        var ex = await Assert.ThrowsAsync<TopicNotFoundException>(() =>
            admin.DeleteTopicsAsync(new[] { "missing-topic" }));

        Assert.Equal("missing-topic", ex.Topic);
    }

    [Fact]
    public async Task ListTopicsAsync_ReturnsTopics()
    {
        var (kafka, client, admin) = CreateAdmin();
        client.OnReturn("kafka.admin.listTopics", JsonNode.Parse(@"[""topic1"", ""topic2"", ""topic3""]"));

        await admin.ConnectAsync();
        var topics = await admin.ListTopicsAsync();

        Assert.Equal(3, topics.Count);
        Assert.Contains("topic1", topics);
        Assert.Contains("topic2", topics);
        Assert.Contains("topic3", topics);
    }

    [Fact]
    public async Task DescribeTopicsAsync_ReturnsMetadata()
    {
        var (kafka, client, admin) = CreateAdmin();
        client.OnReturn("kafka.admin.describeTopics", JsonNode.Parse(@"[
            {
                ""name"": ""topic1"",
                ""partitions"": 3,
                ""replicationFactor"": 2,
                ""config"": {""retention.ms"": ""86400000""}
            }
        ]"));

        await admin.ConnectAsync();
        var metadata = await admin.DescribeTopicsAsync(new[] { "topic1" });

        Assert.Single(metadata);
        Assert.Equal("topic1", metadata[0].Name);
        Assert.Equal(3, metadata[0].Partitions);
        Assert.Equal(2, metadata[0].ReplicationFactor);
        Assert.Equal("86400000", metadata[0].Config["retention.ms"]);
    }

    [Fact]
    public async Task DescribeTopicsAsync_WithNonExistentTopic_ThrowsTopicNotFoundException()
    {
        var (kafka, client, admin) = CreateAdmin();
        client.OnReturn("kafka.admin.describeTopics", JsonNode.Parse(@"[
            {""name"": ""missing"", ""error"": {""code"": ""TOPIC_NOT_FOUND"", ""message"": ""Topic not found""}}
        ]"));

        await admin.ConnectAsync();

        var ex = await Assert.ThrowsAsync<TopicNotFoundException>(() =>
            admin.DescribeTopicsAsync(new[] { "missing" }));

        Assert.Equal("missing", ex.Topic);
    }

    [Fact]
    public async Task DescribePartitionsAsync_ReturnsPartitionMetadata()
    {
        var (kafka, client, admin) = CreateAdmin();
        client.OnReturn("kafka.admin.describePartitions", JsonNode.Parse(@"{
            ""partitions"": [
                {""partition"": 0, ""leader"": 1, ""replicas"": [1, 2], ""isr"": [1, 2]},
                {""partition"": 1, ""leader"": 2, ""replicas"": [2, 3], ""isr"": [2, 3]}
            ]
        }"));

        await admin.ConnectAsync();
        var partitions = await admin.DescribePartitionsAsync("topic1");

        Assert.Equal(2, partitions.Count);
        Assert.Equal(0, partitions[0].Partition);
        Assert.Equal(1, partitions[0].Leader);
        Assert.Equal(new[] { 1, 2 }, partitions[0].Replicas);
        Assert.Equal(new[] { 1, 2 }, partitions[0].Isr);
    }

    [Fact]
    public async Task ListConsumerGroupsAsync_ReturnsGroups()
    {
        var (kafka, client, admin) = CreateAdmin();
        client.OnReturn("kafka.admin.listConsumerGroups", JsonNode.Parse(@"[""group1"", ""group2""]"));

        await admin.ConnectAsync();
        var groups = await admin.ListConsumerGroupsAsync();

        Assert.Equal(2, groups.Count);
        Assert.Contains("group1", groups);
        Assert.Contains("group2", groups);
    }

    [Fact]
    public async Task DescribeConsumerGroupsAsync_ReturnsMetadata()
    {
        var (kafka, client, admin) = CreateAdmin();
        client.OnReturn("kafka.admin.describeConsumerGroups", JsonNode.Parse(@"[
            {
                ""groupId"": ""group1"",
                ""state"": ""Stable"",
                ""members"": [""member1"", ""member2""],
                ""coordinator"": 1
            }
        ]"));

        await admin.ConnectAsync();
        var metadata = await admin.DescribeConsumerGroupsAsync(new[] { "group1" });

        Assert.Single(metadata);
        Assert.Equal("group1", metadata[0].GroupId);
        Assert.Equal("Stable", metadata[0].State);
        Assert.Equal(2, metadata[0].Members.Count);
        Assert.Equal(1, metadata[0].Coordinator);
    }

    [Fact]
    public async Task DeleteConsumerGroupsAsync_DeletesGroups()
    {
        var (kafka, client, admin) = CreateAdmin();
        client.OnReturn("kafka.admin.deleteConsumerGroups", JsonNode.Parse(@"[
            {""groupId"": ""group1""}
        ]"));

        await admin.ConnectAsync();
        await admin.DeleteConsumerGroupsAsync(new[] { "group1" });

        Assert.Contains(client.Calls, c => c.Method == "kafka.admin.deleteConsumerGroups");
    }

    [Fact]
    public async Task AlterTopicConfigAsync_AltersConfig()
    {
        var (kafka, client, admin) = CreateAdmin();
        client.OnReturn("kafka.admin.alterTopicConfig", JsonNode.Parse(@"{}"));

        await admin.ConnectAsync();
        await admin.AlterTopicConfigAsync("topic1", new Dictionary<string, string>
        {
            ["retention.ms"] = "172800000"
        });

        var args = client.Calls.First(c => c.Method == "kafka.admin.alterTopicConfig").Args as Dictionary<string, object>;
        Assert.NotNull(args);
        Assert.Equal("topic1", args["topic"]);
    }
}

// ============================================================================
// Message and Type Tests
// ============================================================================

public class MessageTests
{
    [Fact]
    public void Message_CanSetKeyAndValue()
    {
        var message = new Message<string, string>
        {
            Key = "key1",
            Value = "value1"
        };

        Assert.Equal("key1", message.Key);
        Assert.Equal("value1", message.Value);
    }

    [Fact]
    public void Message_WithHeaders_CanAddHeaders()
    {
        var headers = new Headers();
        headers.Add("trace-id", new byte[] { 1, 2, 3 });
        headers.Add("correlation-id", new byte[] { 4, 5, 6 });

        var message = new Message<string, string>
        {
            Value = "test",
            Headers = headers
        };

        Assert.Equal(2, message.Headers?.Count);
    }

    [Fact]
    public void Headers_GetLastOrNull_ReturnsLastHeader()
    {
        var headers = new Headers();
        headers.Add("key", new byte[] { 1 });
        headers.Add("key", new byte[] { 2 });
        headers.Add("key", new byte[] { 3 });

        var last = headers.GetLastOrNull("key");

        Assert.NotNull(last);
        Assert.Equal(new byte[] { 3 }, last.Value.Value);
    }

    [Fact]
    public void Headers_GetAll_ReturnsAllMatchingHeaders()
    {
        var headers = new Headers();
        headers.Add("key1", new byte[] { 1 });
        headers.Add("key2", new byte[] { 2 });
        headers.Add("key1", new byte[] { 3 });

        var all = headers.GetAll("key1").ToList();

        Assert.Equal(2, all.Count);
    }

    [Fact]
    public void TopicPartition_Equality()
    {
        var tp1 = new TopicPartition("topic", 0);
        var tp2 = new TopicPartition("topic", 0);
        var tp3 = new TopicPartition("topic", 1);

        Assert.Equal(tp1, tp2);
        Assert.NotEqual(tp1, tp3);
    }

    [Fact]
    public void TopicPartitionOffset_Equality()
    {
        var tpo1 = new TopicPartitionOffset("topic", 0, 100);
        var tpo2 = new TopicPartitionOffset("topic", 0, 100);
        var tpo3 = new TopicPartitionOffset("topic", 0, 200);

        Assert.Equal(tpo1, tpo2);
        Assert.NotEqual(tpo1, tpo3);
    }

    [Fact]
    public void ConsumeResult_TopicPartition_ReturnsCorrectValue()
    {
        var result = new ConsumeResult<string, string>
        {
            Topic = "topic",
            Partition = 5,
            Offset = 100
        };

        Assert.Equal(new TopicPartition("topic", 5), result.TopicPartition);
    }

    [Fact]
    public void ConsumeResult_TopicPartitionOffset_ReturnsCorrectValue()
    {
        var result = new ConsumeResult<string, string>
        {
            Topic = "topic",
            Partition = 5,
            Offset = 100
        };

        Assert.Equal(new TopicPartitionOffset("topic", 5, 100), result.TopicPartitionOffset);
    }

    [Fact]
    public void DeliveryResult_TopicPartition_ReturnsCorrectValue()
    {
        var result = new DeliveryResult<string, string>
        {
            Topic = "topic",
            Partition = 3,
            Offset = 50
        };

        Assert.Equal(new TopicPartition("topic", 3), result.TopicPartition);
    }
}

// ============================================================================
// Exception Tests
// ============================================================================

public class ExceptionTests
{
    [Fact]
    public void KafkaException_ContainsCodeAndRetriable()
    {
        var ex = new KafkaException("Test error", "TEST_CODE", isRetriable: true);

        Assert.Equal("Test error", ex.Message);
        Assert.Equal("TEST_CODE", ex.Code);
        Assert.True(ex.IsRetriable);
    }

    [Fact]
    public void KafkaConnectionException_IsRetriable()
    {
        var ex = new KafkaConnectionException("Connection failed");

        Assert.True(ex.IsRetriable);
        Assert.Equal("CONNECTION_ERROR", ex.Code);
    }

    [Fact]
    public void KafkaTimeoutException_IsRetriable()
    {
        var ex = new KafkaTimeoutException("Timeout");

        Assert.True(ex.IsRetriable);
        Assert.Equal("TIMEOUT", ex.Code);
    }

    [Fact]
    public void TopicNotFoundException_ContainsTopic()
    {
        var ex = new TopicNotFoundException("my-topic");

        Assert.Equal("my-topic", ex.Topic);
        Assert.Contains("my-topic", ex.Message);
        Assert.False(ex.IsRetriable);
    }

    [Fact]
    public void TopicAlreadyExistsException_ContainsTopic()
    {
        var ex = new TopicAlreadyExistsException("existing-topic");

        Assert.Equal("existing-topic", ex.Topic);
        Assert.Contains("existing-topic", ex.Message);
        Assert.False(ex.IsRetriable);
    }

    [Fact]
    public void ProducerException_IsNotRetriable()
    {
        var ex = new ProducerException("Producer error");

        Assert.False(ex.IsRetriable);
        Assert.Equal("PRODUCER_ERROR", ex.Code);
    }

    [Fact]
    public void ConsumerException_IsNotRetriable()
    {
        var ex = new ConsumerException("Consumer error");

        Assert.False(ex.IsRetriable);
        Assert.Equal("CONSUMER_ERROR", ex.Code);
    }

    [Fact]
    public void AdminException_IsNotRetriable()
    {
        var ex = new AdminException("Admin error");

        Assert.False(ex.IsRetriable);
        Assert.Equal("ADMIN_ERROR", ex.Code);
    }
}

// ============================================================================
// Serialization Tests
// ============================================================================

public class SerializationTests
{
    [Fact]
    public void SerializeKey_WithString_ReturnsBase64()
    {
        var result = KafkaHelpers.SerializeKey("test-key");

        Assert.NotEmpty(result);
        var decoded = System.Text.Encoding.UTF8.GetString(Convert.FromBase64String(result));
        Assert.Equal("test-key", decoded);
    }

    [Fact]
    public void SerializeValue_WithBytes_ReturnsBase64()
    {
        var bytes = new byte[] { 1, 2, 3, 4, 5 };
        var result = KafkaHelpers.SerializeValue(bytes);

        Assert.NotEmpty(result);
        var decoded = Convert.FromBase64String(result);
        Assert.Equal(bytes, decoded);
    }

    [Fact]
    public void DeserializeKey_WithBase64String_ReturnsOriginal()
    {
        var original = "test-key";
        var base64 = Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes(original));

        var result = KafkaHelpers.DeserializeKey<string>(base64);

        Assert.Equal(original, result);
    }

    [Fact]
    public void DeserializeValue_WithBase64Bytes_ReturnsOriginal()
    {
        var original = new byte[] { 1, 2, 3 };
        var base64 = Convert.ToBase64String(original);

        var result = KafkaHelpers.DeserializeValue<byte[]>(base64);

        Assert.Equal(original, result);
    }

    [Fact]
    public void SerializeHeaders_WithHeaders_ReturnsSerializedList()
    {
        var headers = new Headers();
        headers.Add("key1", new byte[] { 1, 2 });
        headers.Add("key2", new byte[] { 3, 4 });

        var result = KafkaHelpers.SerializeHeaders(headers);

        Assert.NotNull(result);
        Assert.Equal(2, result.Count);
        Assert.Equal("key1", result[0]["key"]);
        Assert.Equal("key2", result[1]["key"]);
    }

    [Fact]
    public void ParseHeaders_WithJsonArray_ReturnsHeaders()
    {
        var json = JsonNode.Parse(@"[
            {""key"": ""header1"", ""value"": ""YWJj""},
            {""key"": ""header2"", ""value"": ""eHl6""}
        ]");

        var result = KafkaHelpers.ParseHeaders(json);

        Assert.NotNull(result);
        Assert.Equal(2, result.Count);
    }
}

// ============================================================================
// Config Tests
// ============================================================================

public class ConfigTests
{
    [Fact]
    public void ProducerConfig_HasDefaults()
    {
        var config = new ProducerConfig();

        Assert.Equal(Acks.Leader, config.Acks);
        Assert.Equal(CompressionType.None, config.CompressionType);
        Assert.Equal(16384, config.BatchSize);
        Assert.Equal(0, config.LingerMs);
    }

    [Fact]
    public void ConsumerConfig_HasDefaults()
    {
        var config = new ConsumerConfig();

        Assert.True(config.EnableAutoCommit);
        Assert.Equal(5000, config.AutoCommitIntervalMs);
        Assert.Equal(500, config.MaxPollRecords);
        Assert.Equal(AutoOffsetReset.Latest, config.AutoOffsetReset);
    }

    [Fact]
    public void AdminConfig_HasDefaults()
    {
        var config = new AdminConfig();

        Assert.Equal(30000, config.RequestTimeoutMs);
    }

    [Fact]
    public void TopicConfig_HasDefaults()
    {
        var config = new TopicConfig();

        Assert.Equal(1, config.NumPartitions);
        Assert.Equal(1, config.ReplicationFactor);
        Assert.NotNull(config.Config);
    }
}

// ============================================================================
// Enum Tests
// ============================================================================

public class EnumTests
{
    [Fact]
    public void CompressionType_HasExpectedValues()
    {
        Assert.Equal(0, (int)CompressionType.None);
        Assert.Equal(1, (int)CompressionType.Gzip);
        Assert.Equal(2, (int)CompressionType.Snappy);
        Assert.Equal(3, (int)CompressionType.Lz4);
        Assert.Equal(4, (int)CompressionType.Zstd);
    }

    [Fact]
    public void Acks_HasExpectedValues()
    {
        Assert.Equal(0, (int)Acks.None);
        Assert.Equal(1, (int)Acks.Leader);
        Assert.Equal(-1, (int)Acks.All);
    }

    [Fact]
    public void AutoOffsetReset_HasExpectedValues()
    {
        Assert.Equal(0, (int)AutoOffsetReset.Earliest);
        Assert.Equal(1, (int)AutoOffsetReset.Latest);
        Assert.Equal(2, (int)AutoOffsetReset.Error);
    }

    [Fact]
    public void IsolationLevel_HasExpectedValues()
    {
        Assert.Equal(0, (int)IsolationLevel.ReadUncommitted);
        Assert.Equal(1, (int)IsolationLevel.ReadCommitted);
    }
}
