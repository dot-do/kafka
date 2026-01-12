// ============================================================================
// Kafka.Do - Admin Client
// Confluent.Kafka compatible admin operations with RPC transport
// ============================================================================

using System.Text.Json.Nodes;

namespace Kafka.Do;

/// <summary>
/// Configuration for a Kafka admin client.
/// </summary>
public class AdminConfig
{
    /// <summary>
    /// Gets or sets the request timeout in milliseconds.
    /// </summary>
    public int RequestTimeoutMs { get; set; } = 30000;

    /// <summary>
    /// Gets or sets an optional client identifier.
    /// </summary>
    public string? ClientId { get; set; }
}

/// <summary>
/// A Kafka admin client for managing topics and consumer groups.
/// </summary>
public class Admin : IAsyncDisposable, IDisposable
{
    private readonly Kafka _kafka;
    private readonly AdminConfig _config;
    private IRpcClient? _client;
    private bool _connected;
    private bool _disposed;
    private readonly SemaphoreSlim _lock = new(1, 1);

    internal Admin(Kafka kafka, AdminConfig config)
    {
        _kafka = kafka ?? throw new ArgumentNullException(nameof(kafka));
        _config = config ?? throw new ArgumentNullException(nameof(config));
    }

    /// <summary>
    /// Gets whether the admin client is connected.
    /// </summary>
    public bool IsConnected
    {
        get
        {
            _lock.Wait();
            try
            {
                return _connected && !_disposed;
            }
            finally
            {
                _lock.Release();
            }
        }
    }

    /// <summary>
    /// Gets the admin configuration.
    /// </summary>
    public AdminConfig Config => _config;

    /// <summary>
    /// Connects the admin client to the Kafka service.
    /// </summary>
    public async Task ConnectAsync(CancellationToken cancellationToken = default)
    {
        await _lock.WaitAsync(cancellationToken);
        try
        {
            if (_connected) return;
            ObjectDisposedException.ThrowIf(_disposed, this);

            _client = await _kafka.GetRpcClientAsync(cancellationToken);
            _connected = true;
        }
        catch (Exception ex) when (ex is not KafkaException)
        {
            throw new KafkaConnectionException("Failed to connect admin", ex);
        }
        finally
        {
            _lock.Release();
        }
    }

    /// <summary>
    /// Creates a single topic with default settings.
    /// </summary>
    /// <param name="topic">The topic name</param>
    /// <param name="cancellationToken">Cancellation token</param>
    public Task CreateTopicAsync(string topic, CancellationToken cancellationToken = default)
    {
        return CreateTopicsAsync(new[] { topic }, cancellationToken);
    }

    /// <summary>
    /// Creates topics with default settings.
    /// </summary>
    /// <param name="topics">The topic names</param>
    /// <param name="cancellationToken">Cancellation token</param>
    public Task CreateTopicsAsync(IEnumerable<string> topics, CancellationToken cancellationToken = default)
    {
        var configs = topics.Select(t => new TopicConfig
        {
            Name = t,
            NumPartitions = 1,
            ReplicationFactor = 1
        });

        return CreateTopicsAsync(configs, validateOnly: false, cancellationToken);
    }

    /// <summary>
    /// Creates topics with configuration.
    /// </summary>
    /// <param name="topics">The topic configurations</param>
    /// <param name="validateOnly">Whether to only validate without creating</param>
    /// <param name="cancellationToken">Cancellation token</param>
    public async Task CreateTopicsAsync(
        IEnumerable<TopicConfig> topics,
        bool validateOnly = false,
        CancellationToken cancellationToken = default)
    {
        var client = await EnsureConnectedAsync(cancellationToken);
        var topicList = topics.ToList();

        var topicConfigs = topicList.Select(t => new Dictionary<string, object>
        {
            ["name"] = t.Name,
            ["numPartitions"] = t.NumPartitions,
            ["replicationFactor"] = t.ReplicationFactor,
            ["config"] = t.Config
        }).ToList();

        var result = await client.CallAsync("kafka.admin.createTopics", new Dictionary<string, object>
        {
            ["topics"] = topicConfigs,
            ["validateOnly"] = validateOnly
        }, cancellationToken);

        CheckResultErrors(result, topicList.Select(t => t.Name).ToList());
    }

    /// <summary>
    /// Deletes topics.
    /// </summary>
    /// <param name="topics">The topic names to delete</param>
    /// <param name="cancellationToken">Cancellation token</param>
    public async Task DeleteTopicsAsync(IEnumerable<string> topics, CancellationToken cancellationToken = default)
    {
        var client = await EnsureConnectedAsync(cancellationToken);
        var topicList = topics.ToList();

        var result = await client.CallAsync("kafka.admin.deleteTopics", new Dictionary<string, object>
        {
            ["topics"] = topicList
        }, cancellationToken);

        CheckResultErrors(result, topicList);
    }

    /// <summary>
    /// Lists all topics.
    /// </summary>
    /// <param name="includeInternal">Whether to include internal topics</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>The list of topic names</returns>
    public async Task<IReadOnlyList<string>> ListTopicsAsync(
        bool includeInternal = false,
        CancellationToken cancellationToken = default)
    {
        var client = await EnsureConnectedAsync(cancellationToken);

        var result = await client.CallAsync("kafka.admin.listTopics", new Dictionary<string, object>
        {
            ["includeInternal"] = includeInternal
        }, cancellationToken);

        if (result is not JsonArray topics)
        {
            throw new AdminException("Invalid list topics response format");
        }

        return topics
            .Select(t => t?.GetValue<string>())
            .Where(t => t != null)
            .Cast<string>()
            .ToList();
    }

    /// <summary>
    /// Gets metadata about topics.
    /// </summary>
    /// <param name="topics">The topic names</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>The topic metadata</returns>
    public async Task<IReadOnlyList<TopicMetadata>> DescribeTopicsAsync(
        IEnumerable<string> topics,
        CancellationToken cancellationToken = default)
    {
        var client = await EnsureConnectedAsync(cancellationToken);
        var topicList = topics.ToList();

        var result = await client.CallAsync("kafka.admin.describeTopics", new Dictionary<string, object>
        {
            ["topics"] = topicList
        }, cancellationToken);

        if (result is not JsonArray results)
        {
            throw new AdminException("Invalid describe topics response format");
        }

        var metadata = new List<TopicMetadata>();
        foreach (var item in results.OfType<JsonObject>())
        {
            // Check for error
            if (item["error"] is JsonObject errorObj)
            {
                var code = errorObj["code"]?.GetValue<string>();
                var topicName = item["name"]?.GetValue<string>() ?? string.Empty;

                if (code == "TOPIC_NOT_FOUND")
                {
                    throw new TopicNotFoundException(topicName);
                }

                var message = errorObj["message"]?.GetValue<string>() ?? "Unknown error";
                throw new KafkaException(message, code);
            }

            metadata.Add(ParseTopicMetadata(item));
        }

        return metadata;
    }

    /// <summary>
    /// Gets partition metadata for a topic.
    /// </summary>
    /// <param name="topic">The topic name</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>The partition metadata</returns>
    public async Task<IReadOnlyList<PartitionMetadata>> DescribePartitionsAsync(
        string topic,
        CancellationToken cancellationToken = default)
    {
        var client = await EnsureConnectedAsync(cancellationToken);

        var result = await client.CallAsync("kafka.admin.describePartitions", new Dictionary<string, object>
        {
            ["topic"] = topic
        }, cancellationToken);

        if (result is not JsonObject obj)
        {
            throw new AdminException("Invalid describe partitions response format");
        }

        // Check for error
        if (obj["error"] is JsonObject errorObj)
        {
            var code = errorObj["code"]?.GetValue<string>();
            if (code == "TOPIC_NOT_FOUND")
            {
                throw new TopicNotFoundException(topic);
            }

            var message = errorObj["message"]?.GetValue<string>() ?? "Unknown error";
            throw new KafkaException(message, code);
        }

        if (obj["partitions"] is not JsonArray partitions)
        {
            throw new AdminException("Invalid partitions format");
        }

        return partitions
            .OfType<JsonObject>()
            .Select(p => ParsePartitionMetadata(p, topic))
            .ToList();
    }

    /// <summary>
    /// Lists all consumer groups.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>The list of group IDs</returns>
    public async Task<IReadOnlyList<string>> ListConsumerGroupsAsync(CancellationToken cancellationToken = default)
    {
        var client = await EnsureConnectedAsync(cancellationToken);

        var result = await client.CallAsync("kafka.admin.listConsumerGroups", null, cancellationToken);

        if (result is not JsonArray groups)
        {
            throw new AdminException("Invalid list consumer groups response format");
        }

        return groups
            .Select(g => g?.GetValue<string>())
            .Where(g => g != null)
            .Cast<string>()
            .ToList();
    }

    /// <summary>
    /// Gets metadata about consumer groups.
    /// </summary>
    /// <param name="groupIds">The group IDs</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>The consumer group metadata</returns>
    public async Task<IReadOnlyList<ConsumerGroupMetadata>> DescribeConsumerGroupsAsync(
        IEnumerable<string> groupIds,
        CancellationToken cancellationToken = default)
    {
        var client = await EnsureConnectedAsync(cancellationToken);

        var result = await client.CallAsync("kafka.admin.describeConsumerGroups", new Dictionary<string, object>
        {
            ["groupIds"] = groupIds.ToList()
        }, cancellationToken);

        if (result is not JsonArray results)
        {
            throw new AdminException("Invalid describe consumer groups response format");
        }

        var metadata = new List<ConsumerGroupMetadata>();
        foreach (var item in results.OfType<JsonObject>())
        {
            // Check for error
            if (item["error"] is JsonObject errorObj)
            {
                var code = errorObj["code"]?.GetValue<string>();
                var message = errorObj["message"]?.GetValue<string>() ?? "Unknown error";
                throw new KafkaException(message, code);
            }

            metadata.Add(ParseConsumerGroupMetadata(item));
        }

        return metadata;
    }

    /// <summary>
    /// Deletes consumer groups.
    /// </summary>
    /// <param name="groupIds">The group IDs to delete</param>
    /// <param name="cancellationToken">Cancellation token</param>
    public async Task DeleteConsumerGroupsAsync(
        IEnumerable<string> groupIds,
        CancellationToken cancellationToken = default)
    {
        var client = await EnsureConnectedAsync(cancellationToken);

        var result = await client.CallAsync("kafka.admin.deleteConsumerGroups", new Dictionary<string, object>
        {
            ["groupIds"] = groupIds.ToList()
        }, cancellationToken);

        CheckResultErrors(result);
    }

    /// <summary>
    /// Alters topic configuration.
    /// </summary>
    /// <param name="topic">The topic name</param>
    /// <param name="config">The configuration to set</param>
    /// <param name="cancellationToken">Cancellation token</param>
    public async Task AlterTopicConfigAsync(
        string topic,
        Dictionary<string, string> config,
        CancellationToken cancellationToken = default)
    {
        var client = await EnsureConnectedAsync(cancellationToken);

        var result = await client.CallAsync("kafka.admin.alterTopicConfig", new Dictionary<string, object>
        {
            ["topic"] = topic,
            ["config"] = config
        }, cancellationToken);

        if (result is JsonObject obj && obj["error"] is JsonObject errorObj)
        {
            var code = errorObj["code"]?.GetValue<string>();
            if (code == "TOPIC_NOT_FOUND")
            {
                throw new TopicNotFoundException(topic);
            }

            var message = errorObj["message"]?.GetValue<string>() ?? "Unknown error";
            throw new KafkaException(message, code);
        }
    }

    private async Task<IRpcClient> EnsureConnectedAsync(CancellationToken cancellationToken)
    {
        await _lock.WaitAsync(cancellationToken);
        try
        {
            ObjectDisposedException.ThrowIf(_disposed, this);

            if (!_connected)
            {
                throw new KafkaConnectionException("Admin not connected. Call ConnectAsync() first.");
            }

            return _client!;
        }
        finally
        {
            _lock.Release();
        }
    }

    private static void CheckResultErrors(JsonNode? result, IReadOnlyList<string>? topicNames = null)
    {
        if (result is not JsonArray results) return;

        var index = 0;
        foreach (var item in results.OfType<JsonObject>())
        {
            if (item["error"] is JsonObject errorObj)
            {
                var code = errorObj["code"]?.GetValue<string>();
                var topicName = item["name"]?.GetValue<string>()
                    ?? (topicNames != null && index < topicNames.Count ? topicNames[index] : null)
                    ?? string.Empty;

                if (code == "TOPIC_ALREADY_EXISTS")
                {
                    throw new TopicAlreadyExistsException(topicName);
                }

                if (code == "TOPIC_NOT_FOUND")
                {
                    throw new TopicNotFoundException(topicName);
                }

                var message = errorObj["message"]?.GetValue<string>() ?? "Unknown error";
                throw new KafkaException(message, code);
            }
            index++;
        }
    }

    private static TopicMetadata ParseTopicMetadata(JsonObject obj)
    {
        var config = new Dictionary<string, string>();
        if (obj["config"] is JsonObject configObj)
        {
            foreach (var prop in configObj)
            {
                if (prop.Value?.GetValue<string>() is string value)
                {
                    config[prop.Key] = value;
                }
            }
        }

        return new TopicMetadata
        {
            Name = obj["name"]?.GetValue<string>() ?? string.Empty,
            Partitions = obj["partitions"]?.GetValue<int>() ?? 0,
            ReplicationFactor = obj["replicationFactor"]?.GetValue<int>() ?? 0,
            Config = config
        };
    }

    private static PartitionMetadata ParsePartitionMetadata(JsonObject obj, string topic)
    {
        var replicas = new List<int>();
        if (obj["replicas"] is JsonArray replicasArray)
        {
            replicas.AddRange(replicasArray.Select(r => r?.GetValue<int>() ?? 0));
        }

        var isr = new List<int>();
        if (obj["isr"] is JsonArray isrArray)
        {
            isr.AddRange(isrArray.Select(r => r?.GetValue<int>() ?? 0));
        }

        return new PartitionMetadata
        {
            Topic = topic,
            Partition = obj["partition"]?.GetValue<int>() ?? 0,
            Leader = obj["leader"]?.GetValue<int>() ?? 0,
            Replicas = replicas,
            Isr = isr
        };
    }

    private static ConsumerGroupMetadata ParseConsumerGroupMetadata(JsonObject obj)
    {
        var members = new List<string>();
        if (obj["members"] is JsonArray membersArray)
        {
            members.AddRange(membersArray
                .Select(m => m?.GetValue<string>())
                .Where(m => m != null)
                .Cast<string>());
        }

        return new ConsumerGroupMetadata
        {
            GroupId = obj["groupId"]?.GetValue<string>() ?? string.Empty,
            State = obj["state"]?.GetValue<string>() ?? string.Empty,
            Members = members,
            Coordinator = obj["coordinator"]?.GetValue<int>() ?? 0
        };
    }

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;

        await _lock.WaitAsync();
        try
        {
            if (_disposed) return;
            _disposed = true;
            _connected = false;
            _client = null;
        }
        finally
        {
            _lock.Release();
        }

        _lock.Dispose();
        GC.SuppressFinalize(this);
    }

    /// <inheritdoc/>
    public void Dispose()
    {
        DisposeAsync().AsTask().GetAwaiter().GetResult();
    }
}
