// ============================================================================
// Kafka.Do - Consumer
// Confluent.Kafka compatible consumer with RPC transport and IAsyncEnumerable
// ============================================================================

using System.Runtime.CompilerServices;
using System.Text.Json.Nodes;

namespace Kafka.Do;

/// <summary>
/// Configuration for a Kafka consumer.
/// </summary>
public class ConsumerConfig
{
    /// <summary>
    /// Gets or sets the consumer group ID.
    /// </summary>
    public string GroupId { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets whether auto-commit is enabled.
    /// </summary>
    public bool EnableAutoCommit { get; set; } = true;

    /// <summary>
    /// Gets or sets the auto-commit interval in milliseconds.
    /// </summary>
    public int AutoCommitIntervalMs { get; set; } = 5000;

    /// <summary>
    /// Gets or sets the session timeout in milliseconds.
    /// </summary>
    public int SessionTimeoutMs { get; set; } = 10000;

    /// <summary>
    /// Gets or sets the heartbeat interval in milliseconds.
    /// </summary>
    public int HeartbeatIntervalMs { get; set; } = 3000;

    /// <summary>
    /// Gets or sets the maximum records per poll.
    /// </summary>
    public int MaxPollRecords { get; set; } = 500;

    /// <summary>
    /// Gets or sets the maximum poll interval in milliseconds.
    /// </summary>
    public int MaxPollIntervalMs { get; set; } = 300000;

    /// <summary>
    /// Gets or sets the isolation level.
    /// </summary>
    public IsolationLevel IsolationLevel { get; set; } = IsolationLevel.ReadUncommitted;

    /// <summary>
    /// Gets or sets the auto offset reset policy.
    /// </summary>
    public AutoOffsetReset AutoOffsetReset { get; set; } = AutoOffsetReset.Latest;

    /// <summary>
    /// Gets or sets an optional client identifier.
    /// </summary>
    public string? ClientId { get; set; }

    /// <summary>
    /// Gets or sets the fetch minimum bytes.
    /// </summary>
    public int FetchMinBytes { get; set; } = 1;

    /// <summary>
    /// Gets or sets the fetch maximum bytes.
    /// </summary>
    public int FetchMaxBytes { get; set; } = 52428800;
}

/// <summary>
/// A Kafka message consumer with typed key and value.
/// Supports IAsyncEnumerable for modern async iteration.
/// </summary>
/// <typeparam name="TKey">The type of the message key</typeparam>
/// <typeparam name="TValue">The type of the message value</typeparam>
public class Consumer<TKey, TValue> : IAsyncDisposable, IDisposable
{
    private readonly Kafka _kafka;
    private readonly ConsumerConfig _config;
    private IRpcClient? _client;
    private bool _connected;
    private bool _disposed;
    private readonly SemaphoreSlim _lock = new(1, 1);
    private readonly HashSet<string> _subscriptions = new();
    private readonly HashSet<TopicPartition> _assignments = new();
    private readonly HashSet<TopicPartition> _paused = new();
    private CancellationTokenSource? _consumeCts;

    internal Consumer(Kafka kafka, ConsumerConfig config)
    {
        _kafka = kafka ?? throw new ArgumentNullException(nameof(kafka));
        _config = config ?? throw new ArgumentNullException(nameof(config));
    }

    /// <summary>
    /// Gets the consumer group ID.
    /// </summary>
    public string GroupId => _config.GroupId;

    /// <summary>
    /// Gets whether the consumer is connected.
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
    /// Gets the consumer configuration.
    /// </summary>
    public ConsumerConfig Config => _config;

    /// <summary>
    /// Gets the currently subscribed topics.
    /// </summary>
    public IReadOnlySet<string> Subscriptions
    {
        get
        {
            _lock.Wait();
            try
            {
                return _subscriptions.ToHashSet();
            }
            finally
            {
                _lock.Release();
            }
        }
    }

    /// <summary>
    /// Gets the currently assigned partitions.
    /// </summary>
    public IReadOnlySet<TopicPartition> Assignments
    {
        get
        {
            _lock.Wait();
            try
            {
                return _assignments.ToHashSet();
            }
            finally
            {
                _lock.Release();
            }
        }
    }

    /// <summary>
    /// Gets the currently paused partitions.
    /// </summary>
    public IReadOnlySet<TopicPartition> Paused
    {
        get
        {
            _lock.Wait();
            try
            {
                return _paused.ToHashSet();
            }
            finally
            {
                _lock.Release();
            }
        }
    }

    /// <summary>
    /// Connects the consumer to the Kafka service.
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
            throw new KafkaConnectionException("Failed to connect consumer", ex);
        }
        finally
        {
            _lock.Release();
        }
    }

    /// <summary>
    /// Subscribes to the specified topic.
    /// </summary>
    /// <param name="topic">The topic to subscribe to</param>
    public void Subscribe(string topic) => Subscribe(new[] { topic });

    /// <summary>
    /// Subscribes to the specified topics.
    /// </summary>
    /// <param name="topics">The topics to subscribe to</param>
    public void Subscribe(IEnumerable<string> topics)
    {
        SubscribeAsync(topics).GetAwaiter().GetResult();
    }

    /// <summary>
    /// Subscribes to the specified topics asynchronously.
    /// </summary>
    /// <param name="topics">The topics to subscribe to</param>
    /// <param name="cancellationToken">Cancellation token</param>
    public async Task SubscribeAsync(IEnumerable<string> topics, CancellationToken cancellationToken = default)
    {
        var client = await EnsureConnectedAsync(cancellationToken);
        var topicList = topics.ToList();

        var request = new Dictionary<string, object>
        {
            ["topics"] = topicList,
            ["groupId"] = _config.GroupId,
            ["autoOffsetReset"] = _config.AutoOffsetReset.ToString().ToLowerInvariant()
        };

        await client.CallAsync("kafka.consumer.subscribe", request, cancellationToken);

        await _lock.WaitAsync(cancellationToken);
        try
        {
            _subscriptions.Clear();
            foreach (var topic in topicList)
            {
                _subscriptions.Add(topic);
            }
        }
        finally
        {
            _lock.Release();
        }
    }

    /// <summary>
    /// Unsubscribes from all topics.
    /// </summary>
    public void Unsubscribe()
    {
        UnsubscribeAsync().GetAwaiter().GetResult();
    }

    /// <summary>
    /// Unsubscribes from all topics asynchronously.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token</param>
    public async Task UnsubscribeAsync(CancellationToken cancellationToken = default)
    {
        var client = await EnsureConnectedAsync(cancellationToken);

        await client.CallAsync("kafka.consumer.unsubscribe", null, cancellationToken);

        await _lock.WaitAsync(cancellationToken);
        try
        {
            _subscriptions.Clear();
            _assignments.Clear();
        }
        finally
        {
            _lock.Release();
        }
    }

    /// <summary>
    /// Manually assigns partitions to this consumer.
    /// </summary>
    /// <param name="partitions">The partitions to assign</param>
    /// <param name="cancellationToken">Cancellation token</param>
    public async Task AssignAsync(IEnumerable<TopicPartition> partitions, CancellationToken cancellationToken = default)
    {
        var client = await EnsureConnectedAsync(cancellationToken);
        var partitionList = partitions.ToList();

        var partitionData = partitionList.Select(p => new Dictionary<string, object>
        {
            ["topic"] = p.Topic,
            ["partition"] = p.Partition
        }).ToList();

        await client.CallAsync("kafka.consumer.assign", new Dictionary<string, object>
        {
            ["partitions"] = partitionData
        }, cancellationToken);

        await _lock.WaitAsync(cancellationToken);
        try
        {
            _assignments.Clear();
            foreach (var partition in partitionList)
            {
                _assignments.Add(partition);
            }
        }
        finally
        {
            _lock.Release();
        }
    }

    /// <summary>
    /// Consumes messages asynchronously as an IAsyncEnumerable.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>An async enumerable of consume results</returns>
    public async IAsyncEnumerable<ConsumeResult<TKey, TValue>> ConsumeAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        _consumeCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var token = _consumeCts.Token;

        while (!token.IsCancellationRequested)
        {
            IReadOnlyList<ConsumeResult<TKey, TValue>> messages;

            try
            {
                messages = await PollAsync(1000, token);
            }
            catch (OperationCanceledException)
            {
                yield break;
            }
            catch (KafkaClientClosedException)
            {
                yield break;
            }

            foreach (var message in messages)
            {
                yield return message;
            }
        }
    }

    /// <summary>
    /// Polls for messages with the specified timeout.
    /// </summary>
    /// <param name="timeoutMs">The timeout in milliseconds</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>The consumed messages</returns>
    public async Task<IReadOnlyList<ConsumeResult<TKey, TValue>>> PollAsync(
        int timeoutMs = 1000,
        CancellationToken cancellationToken = default)
    {
        var client = await EnsureConnectedAsync(cancellationToken);

        var request = new Dictionary<string, object>
        {
            ["timeoutMs"] = timeoutMs,
            ["maxRecords"] = _config.MaxPollRecords
        };

        var result = await client.CallAsync("kafka.consumer.poll", request, cancellationToken);

        if (result == null)
        {
            return Array.Empty<ConsumeResult<TKey, TValue>>();
        }

        if (result is not JsonArray messages)
        {
            throw new ConsumerException("Invalid poll response format");
        }

        return messages
            .OfType<JsonObject>()
            .Select(ParseConsumeResult)
            .ToList();
    }

    /// <summary>
    /// Commits offsets for the specified partitions.
    /// </summary>
    /// <param name="offsets">The offsets to commit (null for current positions)</param>
    /// <param name="cancellationToken">Cancellation token</param>
    public async Task CommitAsync(
        IReadOnlyDictionary<TopicPartition, long>? offsets = null,
        CancellationToken cancellationToken = default)
    {
        var client = await EnsureConnectedAsync(cancellationToken);

        object? args = null;
        if (offsets != null)
        {
            var offsetData = offsets.Select(kv => new Dictionary<string, object>
            {
                ["topic"] = kv.Key.Topic,
                ["partition"] = kv.Key.Partition,
                ["offset"] = kv.Value
            }).ToList();

            args = new Dictionary<string, object> { ["offsets"] = offsetData };
        }

        await client.CallAsync("kafka.consumer.commit", args, cancellationToken);
    }

    /// <summary>
    /// Gets committed offsets for the specified partitions.
    /// </summary>
    /// <param name="partitions">The partitions to get offsets for</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>The committed offsets</returns>
    public async Task<IReadOnlyDictionary<TopicPartition, long>> CommittedAsync(
        IEnumerable<TopicPartition> partitions,
        CancellationToken cancellationToken = default)
    {
        var client = await EnsureConnectedAsync(cancellationToken);

        var partitionData = partitions.Select(p => new Dictionary<string, object>
        {
            ["topic"] = p.Topic,
            ["partition"] = p.Partition
        }).ToList();

        var result = await client.CallAsync("kafka.consumer.committed", new Dictionary<string, object>
        {
            ["partitions"] = partitionData
        }, cancellationToken);

        if (result is not JsonArray results)
        {
            throw new ConsumerException("Invalid committed response format");
        }

        var offsets = new Dictionary<TopicPartition, long>();
        foreach (var item in results.OfType<JsonObject>())
        {
            var topic = item["topic"]?.GetValue<string>() ?? string.Empty;
            var partition = item["partition"]?.GetValue<int>() ?? 0;
            if (item["offset"]?.GetValue<long>() is long offset)
            {
                offsets[new TopicPartition(topic, partition)] = offset;
            }
        }

        return offsets;
    }

    /// <summary>
    /// Seeks to a specific offset.
    /// </summary>
    /// <param name="partition">The partition to seek</param>
    /// <param name="offset">The offset to seek to</param>
    /// <param name="cancellationToken">Cancellation token</param>
    public async Task SeekAsync(
        TopicPartition partition,
        long offset,
        CancellationToken cancellationToken = default)
    {
        var client = await EnsureConnectedAsync(cancellationToken);

        await client.CallAsync("kafka.consumer.seek", new Dictionary<string, object>
        {
            ["topic"] = partition.Topic,
            ["partition"] = partition.Partition,
            ["offset"] = offset
        }, cancellationToken);
    }

    /// <summary>
    /// Seeks to the beginning of partitions.
    /// </summary>
    /// <param name="partitions">The partitions to seek (null for all assigned)</param>
    /// <param name="cancellationToken">Cancellation token</param>
    public async Task SeekToBeginningAsync(
        IEnumerable<TopicPartition>? partitions = null,
        CancellationToken cancellationToken = default)
    {
        var client = await EnsureConnectedAsync(cancellationToken);

        object? args = null;
        if (partitions != null)
        {
            var partitionData = partitions.Select(p => new Dictionary<string, object>
            {
                ["topic"] = p.Topic,
                ["partition"] = p.Partition
            }).ToList();

            args = new Dictionary<string, object> { ["partitions"] = partitionData };
        }

        await client.CallAsync("kafka.consumer.seekToBeginning", args, cancellationToken);
    }

    /// <summary>
    /// Seeks to the end of partitions.
    /// </summary>
    /// <param name="partitions">The partitions to seek (null for all assigned)</param>
    /// <param name="cancellationToken">Cancellation token</param>
    public async Task SeekToEndAsync(
        IEnumerable<TopicPartition>? partitions = null,
        CancellationToken cancellationToken = default)
    {
        var client = await EnsureConnectedAsync(cancellationToken);

        object? args = null;
        if (partitions != null)
        {
            var partitionData = partitions.Select(p => new Dictionary<string, object>
            {
                ["topic"] = p.Topic,
                ["partition"] = p.Partition
            }).ToList();

            args = new Dictionary<string, object> { ["partitions"] = partitionData };
        }

        await client.CallAsync("kafka.consumer.seekToEnd", args, cancellationToken);
    }

    /// <summary>
    /// Gets the current position (offset) for a partition.
    /// </summary>
    /// <param name="partition">The partition</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>The current offset</returns>
    public async Task<long> PositionAsync(
        TopicPartition partition,
        CancellationToken cancellationToken = default)
    {
        var client = await EnsureConnectedAsync(cancellationToken);

        var result = await client.CallAsync("kafka.consumer.position", new Dictionary<string, object>
        {
            ["topic"] = partition.Topic,
            ["partition"] = partition.Partition
        }, cancellationToken);

        return result?.GetValue<long>() ?? 0;
    }

    /// <summary>
    /// Pauses consumption from partitions.
    /// </summary>
    /// <param name="partitions">The partitions to pause</param>
    /// <param name="cancellationToken">Cancellation token</param>
    public async Task PauseAsync(
        IEnumerable<TopicPartition> partitions,
        CancellationToken cancellationToken = default)
    {
        var client = await EnsureConnectedAsync(cancellationToken);
        var partitionList = partitions.ToList();

        var partitionData = partitionList.Select(p => new Dictionary<string, object>
        {
            ["topic"] = p.Topic,
            ["partition"] = p.Partition
        }).ToList();

        await client.CallAsync("kafka.consumer.pause", new Dictionary<string, object>
        {
            ["partitions"] = partitionData
        }, cancellationToken);

        await _lock.WaitAsync(cancellationToken);
        try
        {
            foreach (var partition in partitionList)
            {
                _paused.Add(partition);
            }
        }
        finally
        {
            _lock.Release();
        }
    }

    /// <summary>
    /// Resumes consumption from paused partitions.
    /// </summary>
    /// <param name="partitions">The partitions to resume</param>
    /// <param name="cancellationToken">Cancellation token</param>
    public async Task ResumeAsync(
        IEnumerable<TopicPartition> partitions,
        CancellationToken cancellationToken = default)
    {
        var client = await EnsureConnectedAsync(cancellationToken);
        var partitionList = partitions.ToList();

        var partitionData = partitionList.Select(p => new Dictionary<string, object>
        {
            ["topic"] = p.Topic,
            ["partition"] = p.Partition
        }).ToList();

        await client.CallAsync("kafka.consumer.resume", new Dictionary<string, object>
        {
            ["partitions"] = partitionData
        }, cancellationToken);

        await _lock.WaitAsync(cancellationToken);
        try
        {
            foreach (var partition in partitionList)
            {
                _paused.Remove(partition);
            }
        }
        finally
        {
            _lock.Release();
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
                throw new KafkaConnectionException("Consumer not connected. Call ConnectAsync() first.");
            }

            return _client!;
        }
        finally
        {
            _lock.Release();
        }
    }

    private ConsumeResult<TKey, TValue> ParseConsumeResult(JsonObject obj)
    {
        var message = new Message<TKey, TValue>
        {
            Key = KafkaHelpers.DeserializeKey<TKey>(obj["key"]?.GetValue<string>()),
            Value = KafkaHelpers.DeserializeValue<TValue>(obj["value"]?.GetValue<string>()),
            Headers = KafkaHelpers.ParseHeaders(obj["headers"])
        };

        if (obj["timestamp"]?.GetValue<long>() is long ts)
        {
            message.Timestamp = DateTimeOffset.FromUnixTimeMilliseconds(ts);
        }

        return new ConsumeResult<TKey, TValue>
        {
            Topic = obj["topic"]?.GetValue<string>() ?? string.Empty,
            Partition = obj["partition"]?.GetValue<int>() ?? 0,
            Offset = obj["offset"]?.GetValue<long>() ?? 0,
            Message = message
        };
    }

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;

        _consumeCts?.Cancel();

        await _lock.WaitAsync();
        try
        {
            if (_disposed) return;
            _disposed = true;

            if (_connected && _config.EnableAutoCommit && !string.IsNullOrEmpty(_config.GroupId))
            {
                try
                {
                    await CommitAsync();
                }
                catch
                {
                    // Ignore commit errors on dispose
                }
            }

            _connected = false;
            _client = null;
            _subscriptions.Clear();
            _assignments.Clear();
            _paused.Clear();
        }
        finally
        {
            _lock.Release();
        }

        _consumeCts?.Dispose();
        _lock.Dispose();
        GC.SuppressFinalize(this);
    }

    /// <inheritdoc/>
    public void Dispose()
    {
        DisposeAsync().AsTask().GetAwaiter().GetResult();
    }
}
