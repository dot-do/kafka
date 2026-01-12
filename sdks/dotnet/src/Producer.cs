// ============================================================================
// Kafka.Do - Producer
// Confluent.Kafka compatible producer with RPC transport
// ============================================================================

using System.Text.Json.Nodes;

namespace Kafka.Do;

/// <summary>
/// Configuration for a Kafka producer.
/// </summary>
public class ProducerConfig
{
    /// <summary>
    /// Gets or sets the acknowledgment level.
    /// </summary>
    public Acks Acks { get; set; } = Acks.Leader;

    /// <summary>
    /// Gets or sets the compression type.
    /// </summary>
    public CompressionType CompressionType { get; set; } = CompressionType.None;

    /// <summary>
    /// Gets or sets the batch size in bytes.
    /// </summary>
    public int BatchSize { get; set; } = 16384;

    /// <summary>
    /// Gets or sets the linger time in milliseconds.
    /// </summary>
    public int LingerMs { get; set; } = 0;

    /// <summary>
    /// Gets or sets an optional client identifier.
    /// </summary>
    public string? ClientId { get; set; }

    /// <summary>
    /// Gets or sets whether to enable idempotent producer.
    /// </summary>
    public bool EnableIdempotence { get; set; } = false;

    /// <summary>
    /// Gets or sets the maximum number of in-flight requests.
    /// </summary>
    public int MaxInFlight { get; set; } = 5;

    /// <summary>
    /// Gets or sets the request timeout in milliseconds.
    /// </summary>
    public int RequestTimeoutMs { get; set; } = 30000;
}

/// <summary>
/// A Kafka message producer with typed key and value.
/// </summary>
/// <typeparam name="TKey">The type of the message key</typeparam>
/// <typeparam name="TValue">The type of the message value</typeparam>
public class Producer<TKey, TValue> : IAsyncDisposable, IDisposable
{
    private readonly Kafka _kafka;
    private readonly ProducerConfig _config;
    private IRpcClient? _client;
    private bool _connected;
    private bool _disposed;
    private readonly SemaphoreSlim _lock = new(1, 1);

    internal Producer(Kafka kafka, ProducerConfig config)
    {
        _kafka = kafka ?? throw new ArgumentNullException(nameof(kafka));
        _config = config ?? throw new ArgumentNullException(nameof(config));
    }

    /// <summary>
    /// Gets whether the producer is connected.
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
    /// Gets the producer configuration.
    /// </summary>
    public ProducerConfig Config => _config;

    /// <summary>
    /// Connects the producer to the Kafka service.
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
            throw new KafkaConnectionException("Failed to connect producer", ex);
        }
        finally
        {
            _lock.Release();
        }
    }

    /// <summary>
    /// Produces a message to the specified topic.
    /// </summary>
    /// <param name="topic">The topic to produce to</param>
    /// <param name="message">The message to produce</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>The delivery result</returns>
    public async Task<DeliveryResult<TKey, TValue>> ProduceAsync(
        string topic,
        Message<TKey, TValue> message,
        CancellationToken cancellationToken = default)
    {
        return await ProduceAsync(topic, message, partition: null, cancellationToken);
    }

    /// <summary>
    /// Produces a message to the specified topic and partition.
    /// </summary>
    /// <param name="topic">The topic to produce to</param>
    /// <param name="message">The message to produce</param>
    /// <param name="partition">The specific partition to send to (null for auto)</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>The delivery result</returns>
    public async Task<DeliveryResult<TKey, TValue>> ProduceAsync(
        string topic,
        Message<TKey, TValue> message,
        int? partition,
        CancellationToken cancellationToken = default)
    {
        var client = await EnsureConnectedAsync(cancellationToken);

        var request = new Dictionary<string, object?>
        {
            ["topic"] = topic,
            ["value"] = KafkaHelpers.SerializeValue(message.Value)
        };

        if (message.Key != null)
        {
            request["key"] = KafkaHelpers.SerializeKey(message.Key);
        }

        if (partition.HasValue)
        {
            request["partition"] = partition.Value;
        }

        if (message.Timestamp.HasValue)
        {
            request["timestamp"] = message.Timestamp.Value.ToUnixTimeMilliseconds();
        }

        var headers = KafkaHelpers.SerializeHeaders(message.Headers);
        if (headers != null)
        {
            request["headers"] = headers;
        }

        var result = await client.CallAsync("kafka.producer.send", request, cancellationToken);

        if (result is not JsonObject obj)
        {
            throw new ProducerException("Invalid response format from server");
        }

        return ParseDeliveryResult(obj, message);
    }

    /// <summary>
    /// Produces a batch of messages to Kafka.
    /// </summary>
    /// <param name="messages">The messages to produce, keyed by topic</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>The delivery results</returns>
    public async Task<IReadOnlyList<DeliveryResult<TKey, TValue>>> ProduceBatchAsync(
        IEnumerable<(string Topic, Message<TKey, TValue> Message)> messages,
        CancellationToken cancellationToken = default)
    {
        var client = await EnsureConnectedAsync(cancellationToken);
        var messageList = messages.ToList();

        var records = messageList.Select(m => new Dictionary<string, object?>
        {
            ["topic"] = m.Topic,
            ["key"] = KafkaHelpers.SerializeKey(m.Message.Key),
            ["value"] = KafkaHelpers.SerializeValue(m.Message.Value),
            ["headers"] = KafkaHelpers.SerializeHeaders(m.Message.Headers)
        }).ToList();

        var result = await client.CallAsync("kafka.producer.sendBatch", records, cancellationToken);

        if (result is not JsonArray results)
        {
            throw new ProducerException("Invalid batch response format from server");
        }

        var deliveryResults = new List<DeliveryResult<TKey, TValue>>();
        for (var i = 0; i < results.Count && i < messageList.Count; i++)
        {
            if (results[i] is JsonObject obj)
            {
                deliveryResults.Add(ParseDeliveryResult(obj, messageList[i].Message));
            }
        }

        return deliveryResults;
    }

    /// <summary>
    /// Flushes any pending messages.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token</param>
    public async Task FlushAsync(CancellationToken cancellationToken = default)
    {
        await _lock.WaitAsync(cancellationToken);
        try
        {
            if (!_connected || _client == null) return;
        }
        finally
        {
            _lock.Release();
        }

        try
        {
            await _client.CallAsync("kafka.producer.flush", null, cancellationToken);
        }
        catch
        {
            // Ignore flush errors
        }
    }

    /// <summary>
    /// Gets the partitions for a topic.
    /// </summary>
    /// <param name="topic">The topic name</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>The list of partition numbers</returns>
    public async Task<IReadOnlyList<int>> PartitionsForAsync(string topic, CancellationToken cancellationToken = default)
    {
        var client = await EnsureConnectedAsync(cancellationToken);

        var result = await client.CallAsync("kafka.producer.partitionsFor", topic, cancellationToken);

        if (result is not JsonArray partitions)
        {
            throw new ProducerException("Invalid partitions response format");
        }

        return partitions.Select(p => p?.GetValue<int>() ?? 0).ToList();
    }

    private async Task<IRpcClient> EnsureConnectedAsync(CancellationToken cancellationToken)
    {
        await _lock.WaitAsync(cancellationToken);
        try
        {
            ObjectDisposedException.ThrowIf(_disposed, this);

            if (!_connected)
            {
                throw new KafkaConnectionException("Producer not connected. Call ConnectAsync() first.");
            }

            return _client!;
        }
        finally
        {
            _lock.Release();
        }
    }

    private DeliveryResult<TKey, TValue> ParseDeliveryResult(JsonObject obj, Message<TKey, TValue>? message)
    {
        // Check for error
        if (obj["error"] is JsonObject errorObj)
        {
            var errorMessage = errorObj["message"]?.GetValue<string>() ?? "Unknown error";
            throw new ProducerException(errorMessage);
        }

        return new DeliveryResult<TKey, TValue>
        {
            Topic = obj["topic"]?.GetValue<string>() ?? string.Empty,
            Partition = obj["partition"]?.GetValue<int>() ?? 0,
            Offset = obj["offset"]?.GetValue<long>() ?? 0,
            Timestamp = obj["timestamp"]?.GetValue<long>() is long ts
                ? DateTimeOffset.FromUnixTimeMilliseconds(ts)
                : DateTimeOffset.UtcNow,
            Message = message
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

            if (_connected)
            {
                await FlushAsync();
            }

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
