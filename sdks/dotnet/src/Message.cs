// ============================================================================
// Kafka.Do - Message Types
// Confluent.Kafka compatible message types for RPC-based Kafka operations
// ============================================================================

namespace Kafka.Do;

/// <summary>
/// Represents a Kafka message with typed key and value.
/// Compatible with Confluent.Kafka Message&lt;TKey, TValue&gt;.
/// </summary>
/// <typeparam name="TKey">The type of the message key</typeparam>
/// <typeparam name="TValue">The type of the message value</typeparam>
public class Message<TKey, TValue>
{
    /// <summary>
    /// Gets or sets the message key.
    /// </summary>
    public TKey? Key { get; set; }

    /// <summary>
    /// Gets or sets the message value.
    /// </summary>
    public TValue? Value { get; set; }

    /// <summary>
    /// Gets or sets the message timestamp.
    /// </summary>
    public DateTimeOffset? Timestamp { get; set; }

    /// <summary>
    /// Gets or sets the message headers.
    /// </summary>
    public Headers? Headers { get; set; }
}

/// <summary>
/// Represents a consumed Kafka message with metadata.
/// </summary>
/// <typeparam name="TKey">The type of the message key</typeparam>
/// <typeparam name="TValue">The type of the message value</typeparam>
public class ConsumeResult<TKey, TValue>
{
    /// <summary>
    /// Gets or sets the topic the message was consumed from.
    /// </summary>
    public string Topic { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets the partition the message was consumed from.
    /// </summary>
    public int Partition { get; set; }

    /// <summary>
    /// Gets or sets the offset of the message within the partition.
    /// </summary>
    public long Offset { get; set; }

    /// <summary>
    /// Gets or sets the message.
    /// </summary>
    public Message<TKey, TValue> Message { get; set; } = new();

    /// <summary>
    /// Gets the topic partition for this result.
    /// </summary>
    public TopicPartition TopicPartition => new(Topic, Partition);

    /// <summary>
    /// Gets the topic partition offset for this result.
    /// </summary>
    public TopicPartitionOffset TopicPartitionOffset => new(Topic, Partition, Offset);
}

/// <summary>
/// Represents metadata about a successfully produced record.
/// </summary>
public class DeliveryResult<TKey, TValue>
{
    /// <summary>
    /// Gets or sets the topic the record was sent to.
    /// </summary>
    public string Topic { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets the partition the record was sent to.
    /// </summary>
    public int Partition { get; set; }

    /// <summary>
    /// Gets or sets the offset of the record in the partition.
    /// </summary>
    public long Offset { get; set; }

    /// <summary>
    /// Gets or sets the timestamp of the record.
    /// </summary>
    public DateTimeOffset Timestamp { get; set; }

    /// <summary>
    /// Gets or sets the original message that was sent.
    /// </summary>
    public Message<TKey, TValue>? Message { get; set; }

    /// <summary>
    /// Gets the topic partition for this result.
    /// </summary>
    public TopicPartition TopicPartition => new(Topic, Partition);

    /// <summary>
    /// Gets the topic partition offset for this result.
    /// </summary>
    public TopicPartitionOffset TopicPartitionOffset => new(Topic, Partition, Offset);
}

/// <summary>
/// Represents a topic and partition pair.
/// </summary>
public readonly record struct TopicPartition(string Topic, int Partition);

/// <summary>
/// Represents a topic, partition, and offset tuple.
/// </summary>
public readonly record struct TopicPartitionOffset(string Topic, int Partition, long Offset);

/// <summary>
/// Represents a collection of message headers.
/// </summary>
public class Headers : IEnumerable<Header>
{
    private readonly List<Header> _headers = new();

    /// <summary>
    /// Adds a header to the collection.
    /// </summary>
    public void Add(string key, byte[] value)
    {
        _headers.Add(new Header(key, value));
    }

    /// <summary>
    /// Adds a header to the collection.
    /// </summary>
    public void Add(Header header)
    {
        _headers.Add(header);
    }

    /// <summary>
    /// Gets all headers with the specified key.
    /// </summary>
    public IEnumerable<Header> GetAll(string key) =>
        _headers.Where(h => h.Key == key);

    /// <summary>
    /// Gets the last header with the specified key, or null if not found.
    /// </summary>
    public Header? GetLastOrNull(string key) =>
        _headers.LastOrDefault(h => h.Key == key);

    /// <summary>
    /// Gets the number of headers in the collection.
    /// </summary>
    public int Count => _headers.Count;

    /// <inheritdoc/>
    public IEnumerator<Header> GetEnumerator() => _headers.GetEnumerator();

    System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => GetEnumerator();
}

/// <summary>
/// Represents a single message header.
/// </summary>
public readonly record struct Header(string Key, byte[] Value);

/// <summary>
/// Configuration for creating a new topic.
/// </summary>
public class TopicConfig
{
    /// <summary>
    /// Gets or sets the topic name.
    /// </summary>
    public string Name { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets the number of partitions.
    /// </summary>
    public int NumPartitions { get; set; } = 1;

    /// <summary>
    /// Gets or sets the replication factor.
    /// </summary>
    public int ReplicationFactor { get; set; } = 1;

    /// <summary>
    /// Gets or sets additional topic configuration.
    /// </summary>
    public Dictionary<string, string> Config { get; set; } = new();
}

/// <summary>
/// Metadata about a topic.
/// </summary>
public class TopicMetadata
{
    /// <summary>
    /// Gets or sets the topic name.
    /// </summary>
    public string Name { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets the number of partitions.
    /// </summary>
    public int Partitions { get; set; }

    /// <summary>
    /// Gets or sets the replication factor.
    /// </summary>
    public int ReplicationFactor { get; set; }

    /// <summary>
    /// Gets or sets the topic configuration.
    /// </summary>
    public Dictionary<string, string> Config { get; set; } = new();
}

/// <summary>
/// Metadata about a partition.
/// </summary>
public class PartitionMetadata
{
    /// <summary>
    /// Gets or sets the topic name.
    /// </summary>
    public string Topic { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets the partition number.
    /// </summary>
    public int Partition { get; set; }

    /// <summary>
    /// Gets or sets the broker ID of the leader.
    /// </summary>
    public int Leader { get; set; }

    /// <summary>
    /// Gets or sets the list of replica broker IDs.
    /// </summary>
    public List<int> Replicas { get; set; } = new();

    /// <summary>
    /// Gets or sets the list of in-sync replica broker IDs.
    /// </summary>
    public List<int> Isr { get; set; } = new();
}

/// <summary>
/// Metadata about a consumer group.
/// </summary>
public class ConsumerGroupMetadata
{
    /// <summary>
    /// Gets or sets the consumer group ID.
    /// </summary>
    public string GroupId { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets the current state of the group.
    /// </summary>
    public string State { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets the list of member IDs.
    /// </summary>
    public List<string> Members { get; set; } = new();

    /// <summary>
    /// Gets or sets the broker ID of the group coordinator.
    /// </summary>
    public int Coordinator { get; set; }
}

/// <summary>
/// Compression types for messages.
/// </summary>
public enum CompressionType
{
    /// <summary>No compression</summary>
    None,
    /// <summary>Gzip compression</summary>
    Gzip,
    /// <summary>Snappy compression</summary>
    Snappy,
    /// <summary>LZ4 compression</summary>
    Lz4,
    /// <summary>Zstd compression</summary>
    Zstd
}

/// <summary>
/// Acknowledgment levels for producers.
/// </summary>
public enum Acks
{
    /// <summary>No acknowledgment required</summary>
    None = 0,
    /// <summary>Leader acknowledgment only</summary>
    Leader = 1,
    /// <summary>All in-sync replicas must acknowledge</summary>
    All = -1
}

/// <summary>
/// Offset reset policies for consumers.
/// </summary>
public enum AutoOffsetReset
{
    /// <summary>Start from the earliest offset</summary>
    Earliest,
    /// <summary>Start from the latest offset</summary>
    Latest,
    /// <summary>Throw an error if no offset is found</summary>
    Error
}

/// <summary>
/// Transaction isolation levels for consumers.
/// </summary>
public enum IsolationLevel
{
    /// <summary>Read uncommitted messages</summary>
    ReadUncommitted,
    /// <summary>Read only committed messages</summary>
    ReadCommitted
}
