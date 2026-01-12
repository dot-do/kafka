// ============================================================================
// Kafka.Do - Main Entry Point
// Confluent.Kafka compatible API with RPC transport for serverless operations
// ============================================================================
//
// Usage:
//   var kafka = new Kafka(new[] { "broker1:9092" });
//
//   // Producer
//   using var producer = kafka.CreateProducer<string, string>();
//   await producer.ProduceAsync("topic", new Message<string, string> { Key = "key", Value = "value" });
//
//   // Consumer
//   using var consumer = kafka.CreateConsumer<string, string>("group");
//   consumer.Subscribe("topic");
//   await foreach (var msg in consumer.ConsumeAsync()) {
//       Console.WriteLine(msg.Message.Value);
//   }
//
//   // Admin
//   using var admin = kafka.CreateAdmin();
//   await admin.CreateTopicAsync("new-topic");
//
// ============================================================================

using System.Text.Json;
using System.Text.Json.Nodes;

namespace Kafka.Do;

/// <summary>
/// Interface for the underlying RPC transport.
/// This can be mocked for testing.
/// </summary>
public interface IRpcClient : IAsyncDisposable
{
    /// <summary>
    /// Calls an RPC method with the specified arguments.
    /// </summary>
    Task<JsonNode?> CallAsync(string method, object? args = null, CancellationToken cancellationToken = default);
}

/// <summary>
/// Configuration for the Kafka client.
/// </summary>
public class KafkaConfig
{
    /// <summary>
    /// Gets or sets the list of Kafka broker addresses.
    /// </summary>
    public IReadOnlyList<string> Brokers { get; set; } = Array.Empty<string>();

    /// <summary>
    /// Gets or sets an optional client identifier.
    /// </summary>
    public string? ClientId { get; set; }

    /// <summary>
    /// Gets or sets the connection timeout.
    /// </summary>
    public TimeSpan Timeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Gets or sets the URL for the kafka.do RPC service.
    /// </summary>
    public string ConnectionUrl { get; set; } = "kafka.do";
}

/// <summary>
/// The main Kafka client. Entry point for creating producers, consumers, and admin clients.
/// </summary>
public class Kafka : IAsyncDisposable
{
    private readonly KafkaConfig _config;
    private IRpcClient? _rpcClient;
    private readonly SemaphoreSlim _clientLock = new(1, 1);
    private bool _disposed;

    /// <summary>
    /// Creates a new Kafka client with the specified brokers.
    /// </summary>
    /// <param name="brokers">The list of broker addresses</param>
    public Kafka(IEnumerable<string> brokers)
        : this(new KafkaConfig { Brokers = brokers.ToList() })
    {
    }

    /// <summary>
    /// Creates a new Kafka client with the specified configuration.
    /// </summary>
    /// <param name="config">The client configuration</param>
    public Kafka(KafkaConfig config)
    {
        _config = config ?? throw new ArgumentNullException(nameof(config));
    }

    /// <summary>
    /// Gets the configured broker addresses.
    /// </summary>
    public IReadOnlyList<string> Brokers => _config.Brokers.ToList();

    /// <summary>
    /// Gets the configured client ID.
    /// </summary>
    public string? ClientId => _config.ClientId;

    /// <summary>
    /// Gets the connection URL.
    /// </summary>
    public string ConnectionUrl => _config.ConnectionUrl;

    /// <summary>
    /// Sets the RPC client. Primarily used for testing.
    /// </summary>
    /// <param name="client">The RPC client to use</param>
    public void SetRpcClient(IRpcClient client)
    {
        _rpcClient = client;
    }

    /// <summary>
    /// Gets the RPC client, creating one if necessary.
    /// </summary>
    internal async Task<IRpcClient> GetRpcClientAsync(CancellationToken cancellationToken = default)
    {
        if (_rpcClient != null)
        {
            return _rpcClient;
        }

        await _clientLock.WaitAsync(cancellationToken);
        try
        {
            if (_rpcClient != null)
            {
                return _rpcClient;
            }

            // In a real implementation, this would connect via rpc.do
            // For now, throw an error if no client is set
            throw new KafkaConnectionException("RPC client not configured. Call SetRpcClient() or configure the connection.");
        }
        finally
        {
            _clientLock.Release();
        }
    }

    /// <summary>
    /// Creates a new producer with the specified configuration.
    /// </summary>
    /// <typeparam name="TKey">The type of the message key</typeparam>
    /// <typeparam name="TValue">The type of the message value</typeparam>
    /// <param name="config">Optional producer configuration</param>
    /// <returns>A new producer instance</returns>
    public Producer<TKey, TValue> CreateProducer<TKey, TValue>(ProducerConfig? config = null)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        return new Producer<TKey, TValue>(this, config ?? new ProducerConfig());
    }

    /// <summary>
    /// Creates a new consumer with the specified group ID and configuration.
    /// </summary>
    /// <typeparam name="TKey">The type of the message key</typeparam>
    /// <typeparam name="TValue">The type of the message value</typeparam>
    /// <param name="groupId">The consumer group ID</param>
    /// <param name="config">Optional consumer configuration</param>
    /// <returns>A new consumer instance</returns>
    public Consumer<TKey, TValue> CreateConsumer<TKey, TValue>(string groupId, ConsumerConfig? config = null)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        var consumerConfig = config ?? new ConsumerConfig();
        consumerConfig.GroupId = groupId;
        return new Consumer<TKey, TValue>(this, consumerConfig);
    }

    /// <summary>
    /// Creates a new admin client with the specified configuration.
    /// </summary>
    /// <param name="config">Optional admin configuration</param>
    /// <returns>A new admin client instance</returns>
    public Admin CreateAdmin(AdminConfig? config = null)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        return new Admin(this, config ?? new AdminConfig());
    }

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;

        if (_rpcClient != null)
        {
            await _rpcClient.DisposeAsync();
        }

        _clientLock.Dispose();
        GC.SuppressFinalize(this);
    }
}

// ============================================================================
// Exceptions
// ============================================================================

/// <summary>
/// Base exception for all Kafka.Do errors.
/// </summary>
public class KafkaException : Exception
{
    /// <summary>
    /// Gets the error code, if any.
    /// </summary>
    public string? Code { get; }

    /// <summary>
    /// Gets whether this error is retriable.
    /// </summary>
    public bool IsRetriable { get; }

    /// <summary>
    /// Creates a new KafkaException.
    /// </summary>
    public KafkaException(string message, string? code = null, bool isRetriable = false, Exception? innerException = null)
        : base(message, innerException)
    {
        Code = code;
        IsRetriable = isRetriable;
    }
}

/// <summary>
/// Thrown when a connection error occurs.
/// </summary>
public class KafkaConnectionException : KafkaException
{
    /// <summary>
    /// Creates a new KafkaConnectionException.
    /// </summary>
    public KafkaConnectionException(string message, Exception? innerException = null)
        : base(message, "CONNECTION_ERROR", isRetriable: true, innerException)
    {
    }
}

/// <summary>
/// Thrown when an operation times out.
/// </summary>
public class KafkaTimeoutException : KafkaException
{
    /// <summary>
    /// Creates a new KafkaTimeoutException.
    /// </summary>
    public KafkaTimeoutException(string message, Exception? innerException = null)
        : base(message, "TIMEOUT", isRetriable: true, innerException)
    {
    }
}

/// <summary>
/// Thrown when a topic is not found.
/// </summary>
public class TopicNotFoundException : KafkaException
{
    /// <summary>
    /// Gets the topic name.
    /// </summary>
    public string Topic { get; }

    /// <summary>
    /// Creates a new TopicNotFoundException.
    /// </summary>
    public TopicNotFoundException(string topic)
        : base($"Topic not found: {topic}", "TOPIC_NOT_FOUND", isRetriable: false)
    {
        Topic = topic;
    }
}

/// <summary>
/// Thrown when attempting to create a topic that already exists.
/// </summary>
public class TopicAlreadyExistsException : KafkaException
{
    /// <summary>
    /// Gets the topic name.
    /// </summary>
    public string Topic { get; }

    /// <summary>
    /// Creates a new TopicAlreadyExistsException.
    /// </summary>
    public TopicAlreadyExistsException(string topic)
        : base($"Topic already exists: {topic}", "TOPIC_ALREADY_EXISTS", isRetriable: false)
    {
        Topic = topic;
    }
}

/// <summary>
/// Thrown when a producer operation fails.
/// </summary>
public class ProducerException : KafkaException
{
    /// <summary>
    /// Creates a new ProducerException.
    /// </summary>
    public ProducerException(string message, Exception? innerException = null)
        : base(message, "PRODUCER_ERROR", isRetriable: false, innerException)
    {
    }
}

/// <summary>
/// Thrown when a consumer operation fails.
/// </summary>
public class ConsumerException : KafkaException
{
    /// <summary>
    /// Creates a new ConsumerException.
    /// </summary>
    public ConsumerException(string message, Exception? innerException = null)
        : base(message, "CONSUMER_ERROR", isRetriable: false, innerException)
    {
    }
}

/// <summary>
/// Thrown when an admin operation fails.
/// </summary>
public class AdminException : KafkaException
{
    /// <summary>
    /// Creates a new AdminException.
    /// </summary>
    public AdminException(string message, Exception? innerException = null)
        : base(message, "ADMIN_ERROR", isRetriable: false, innerException)
    {
    }
}

/// <summary>
/// Thrown when a client has been closed.
/// </summary>
public class KafkaClientClosedException : KafkaException
{
    /// <summary>
    /// Creates a new KafkaClientClosedException.
    /// </summary>
    public KafkaClientClosedException()
        : base("Client has been closed", "CLIENT_CLOSED", isRetriable: false)
    {
    }
}

// ============================================================================
// Internal Utilities
// ============================================================================

internal static class KafkaHelpers
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        WriteIndented = false
    };

    public static string SerializeKey<TKey>(TKey? key)
    {
        if (key == null) return string.Empty;
        if (key is string s) return Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes(s));
        if (key is byte[] bytes) return Convert.ToBase64String(bytes);
        return Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes(JsonSerializer.Serialize(key, JsonOptions)));
    }

    public static string SerializeValue<TValue>(TValue? value)
    {
        if (value == null) return string.Empty;
        if (value is string s) return Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes(s));
        if (value is byte[] bytes) return Convert.ToBase64String(bytes);
        return Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes(JsonSerializer.Serialize(value, JsonOptions)));
    }

    public static TKey? DeserializeKey<TKey>(string? base64)
    {
        if (string.IsNullOrEmpty(base64)) return default;
        var bytes = Convert.FromBase64String(base64);
        if (typeof(TKey) == typeof(string)) return (TKey)(object)System.Text.Encoding.UTF8.GetString(bytes);
        if (typeof(TKey) == typeof(byte[])) return (TKey)(object)bytes;
        return JsonSerializer.Deserialize<TKey>(bytes, JsonOptions);
    }

    public static TValue? DeserializeValue<TValue>(string? base64)
    {
        if (string.IsNullOrEmpty(base64)) return default;
        var bytes = Convert.FromBase64String(base64);
        if (typeof(TValue) == typeof(string)) return (TValue)(object)System.Text.Encoding.UTF8.GetString(bytes);
        if (typeof(TValue) == typeof(byte[])) return (TValue)(object)bytes;
        return JsonSerializer.Deserialize<TValue>(bytes, JsonOptions);
    }

    public static Headers? ParseHeaders(JsonNode? headersNode)
    {
        if (headersNode is not JsonArray headersArray || headersArray.Count == 0)
        {
            return null;
        }

        var headers = new Headers();
        foreach (var header in headersArray)
        {
            if (header is JsonObject obj &&
                obj["key"]?.GetValue<string>() is string key &&
                obj["value"]?.GetValue<string>() is string value)
            {
                headers.Add(key, Convert.FromBase64String(value));
            }
        }
        return headers;
    }

    public static List<Dictionary<string, string>>? SerializeHeaders(Headers? headers)
    {
        if (headers == null || headers.Count == 0) return null;

        return headers.Select(h => new Dictionary<string, string>
        {
            ["key"] = h.Key,
            ["value"] = Convert.ToBase64String(h.Value)
        }).ToList();
    }
}
