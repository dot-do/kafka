/// Kafka type definitions for kafka.do
///
/// KafkaJS-compatible type definitions for the Dart Kafka SDK.
library;

/// Kafka client configuration
class KafkaConfig {
  /// Broker addresses (e.g., ['broker1:9092', 'broker2:9092'])
  final List<String> brokers;

  /// Client identifier
  final String? clientId;

  /// Connection timeout in milliseconds
  final int? connectionTimeout;

  /// Request timeout in milliseconds
  final int? requestTimeout;

  /// API key for authentication
  final String? apiKey;

  /// Retry configuration
  final RetryConfig? retry;

  const KafkaConfig({
    required this.brokers,
    this.clientId,
    this.connectionTimeout,
    this.requestTimeout,
    this.apiKey,
    this.retry,
  });
}

/// Retry configuration
class RetryConfig {
  /// Maximum retry time in milliseconds
  final int? maxRetryTime;

  /// Initial retry time in milliseconds
  final int? initialRetryTime;

  /// Retry factor
  final double? factor;

  /// Maximum number of retries
  final int? retries;

  const RetryConfig({
    this.maxRetryTime,
    this.initialRetryTime,
    this.factor,
    this.retries,
  });
}

/// Producer configuration
class ProducerConfig {
  /// Whether to create topics if they don't exist
  final bool? allowAutoTopicCreation;

  /// Transaction ID for exactly-once semantics
  final String? transactionalId;

  /// Transaction timeout in milliseconds
  final int? transactionTimeout;

  /// Idempotent producer (enables exactly-once delivery)
  final bool? idempotent;

  /// Maximum number of in-flight requests
  final int? maxInFlightRequests;

  /// Batch size in bytes
  final int? batchSize;

  /// Linger time in milliseconds
  final int? lingerMs;

  /// Compression type
  final CompressionTypes? compression;

  /// Required acknowledgments (-1, 0, 1)
  final Acks? acks;

  const ProducerConfig({
    this.allowAutoTopicCreation,
    this.transactionalId,
    this.transactionTimeout,
    this.idempotent,
    this.maxInFlightRequests,
    this.batchSize,
    this.lingerMs,
    this.compression,
    this.acks,
  });
}

/// Consumer configuration
class ConsumerConfig {
  /// Consumer group ID
  final String groupId;

  /// Session timeout in milliseconds
  final int? sessionTimeout;

  /// Rebalance timeout in milliseconds
  final int? rebalanceTimeout;

  /// Heartbeat interval in milliseconds
  final int? heartbeatInterval;

  /// Metadata max age in milliseconds
  final int? metadataMaxAge;

  /// Allow auto topic creation
  final bool? allowAutoTopicCreation;

  /// Max bytes per partition
  final int? maxBytesPerPartition;

  /// Minimum bytes to fetch
  final int? minBytes;

  /// Maximum bytes to fetch
  final int? maxBytes;

  /// Maximum wait time in milliseconds
  final int? maxWaitTimeInMs;

  /// Read uncommitted messages
  final bool? readUncommitted;

  /// Starting offset configuration
  final Offset? offset;

  /// Auto commit enabled
  final bool? autoCommit;

  /// Maximum poll records
  final int? maxPollRecords;

  const ConsumerConfig({
    required this.groupId,
    this.sessionTimeout,
    this.rebalanceTimeout,
    this.heartbeatInterval,
    this.metadataMaxAge,
    this.allowAutoTopicCreation,
    this.maxBytesPerPartition,
    this.minBytes,
    this.maxBytes,
    this.maxWaitTimeInMs,
    this.readUncommitted,
    this.offset,
    this.autoCommit,
    this.maxPollRecords,
  });
}

/// Batch consumer configuration
class BatchConsumerConfig extends ConsumerConfig {
  /// Batch size
  final int batchSize;

  /// Batch timeout
  final Duration batchTimeout;

  const BatchConsumerConfig({
    required super.groupId,
    this.batchSize = 100,
    this.batchTimeout = const Duration(seconds: 5),
    super.sessionTimeout,
    super.rebalanceTimeout,
    super.heartbeatInterval,
    super.metadataMaxAge,
    super.allowAutoTopicCreation,
    super.maxBytesPerPartition,
    super.minBytes,
    super.maxBytes,
    super.maxWaitTimeInMs,
    super.readUncommitted,
    super.offset,
    super.autoCommit,
    super.maxPollRecords,
  });
}

/// Admin configuration
class AdminConfig {
  /// Retry configuration
  final RetryConfig? retry;

  const AdminConfig({this.retry});
}

/// Offset specification
sealed class Offset {
  const Offset._();

  /// Start from the earliest offset
  static const Offset earliest = _EarliestOffset();

  /// Start from the latest offset
  static const Offset latest = _LatestOffset();

  /// Start from a specific timestamp
  static Offset timestamp(DateTime timestamp) => _TimestampOffset(timestamp);

  /// Start from a specific offset
  static Offset offset(int offset) => _SpecificOffset(offset);
}

class _EarliestOffset extends Offset {
  const _EarliestOffset() : super._();
}

class _LatestOffset extends Offset {
  const _LatestOffset() : super._();
}

class _TimestampOffset extends Offset {
  final DateTime timestamp;
  const _TimestampOffset(this.timestamp) : super._();
}

class _SpecificOffset extends Offset {
  final int offset;
  const _SpecificOffset(this.offset) : super._();
}

/// Compression types
enum CompressionTypes {
  none(0),
  gzip(1),
  snappy(2),
  lz4(3),
  zstd(4);

  final int value;
  const CompressionTypes(this.value);
}

/// Acknowledgment types
enum Acks {
  /// No acknowledgment
  none(0),

  /// Leader acknowledgment
  leader(1),

  /// All replicas acknowledgment
  all(-1);

  final int value;
  const Acks(this.value);
}

/// Message to send
class Message<T> {
  /// Message key (for partitioning)
  final String? key;

  /// Message value
  final T value;

  /// Partition to send to (optional)
  final int? partition;

  /// Message headers
  final Map<String, String>? headers;

  /// Message timestamp
  final DateTime? timestamp;

  const Message({
    required this.value,
    this.key,
    this.partition,
    this.headers,
    this.timestamp,
  });
}

/// Record metadata returned after send
class RecordMetadata {
  /// Topic name
  final String topic;

  /// Partition
  final int partition;

  /// Error code (0 for success)
  final int errorCode;

  /// Base offset
  final int? offset;

  /// Log append time
  final DateTime? logAppendTime;

  /// Log start offset
  final int? logStartOffset;

  const RecordMetadata({
    required this.topic,
    required this.partition,
    this.errorCode = 0,
    this.offset,
    this.logAppendTime,
    this.logStartOffset,
  });

  factory RecordMetadata.fromJson(Map<String, dynamic> json) {
    return RecordMetadata(
      topic: json['topicName'] as String? ?? json['topic'] as String,
      partition: json['partition'] as int,
      errorCode: json['errorCode'] as int? ?? 0,
      offset: json['baseOffset'] != null
          ? int.tryParse(json['baseOffset'].toString())
          : json['offset'] as int?,
      logAppendTime: json['logAppendTime'] != null
          ? DateTime.fromMillisecondsSinceEpoch(
              int.parse(json['logAppendTime'].toString()))
          : null,
      logStartOffset: json['logStartOffset'] != null
          ? int.tryParse(json['logStartOffset'].toString())
          : null,
    );
  }
}

/// Kafka record received by consumer
class KafkaRecord<T> {
  /// Topic name
  final String topic;

  /// Partition number
  final int partition;

  /// Message offset
  final int offset;

  /// Message key
  final String? key;

  /// Message value
  final T value;

  /// Message timestamp
  final DateTime timestamp;

  /// Message headers
  final Map<String, String> headers;

  /// Commit function
  final Future<void> Function() _commit;

  KafkaRecord({
    required this.topic,
    required this.partition,
    required this.offset,
    required this.value,
    required this.timestamp,
    required Future<void> Function() commit,
    this.key,
    this.headers = const {},
  }) : _commit = commit;

  /// Commit this record's offset
  Future<void> commit() => _commit();
}

/// Batch of records
class Batch<T> {
  final List<KafkaRecord<T>> _records;
  final Future<void> Function() _commit;

  Batch(this._records, this._commit);

  /// Get all records in the batch
  List<KafkaRecord<T>> get records => List.unmodifiable(_records);

  /// Iterate over records
  Iterator<KafkaRecord<T>> get iterator => _records.iterator;

  /// Number of records in the batch
  int get length => _records.length;

  /// Whether the batch is empty
  bool get isEmpty => _records.isEmpty;

  /// Commit all records in the batch
  Future<void> commit() => _commit();
}

/// Topic configuration for creation
class TopicConfig {
  /// Number of partitions
  final int partitions;

  /// Replication factor
  final int? replicationFactor;

  /// Retention time in milliseconds
  final int? retentionMs;

  /// Configuration entries
  final Map<String, String>? configEntries;

  const TopicConfig({
    this.partitions = 1,
    this.replicationFactor,
    this.retentionMs,
    this.configEntries,
  });

  Map<String, dynamic> toJson() => {
        'partitions': partitions,
        if (replicationFactor != null) 'replicationFactor': replicationFactor,
        if (retentionMs != null) 'retentionMs': retentionMs,
        if (configEntries != null) 'configEntries': configEntries,
      };
}

/// Topic information
class TopicInfo {
  /// Topic name
  final String name;

  /// Number of partitions
  final int partitions;

  /// Replication factor
  final int? replicationFactor;

  /// Retention time in milliseconds
  final int? retentionMs;

  const TopicInfo({
    required this.name,
    required this.partitions,
    this.replicationFactor,
    this.retentionMs,
  });

  factory TopicInfo.fromJson(Map<String, dynamic> json) {
    return TopicInfo(
      name: json['name'] as String,
      partitions: json['partitions'] as int? ?? 1,
      replicationFactor: json['replicationFactor'] as int?,
      retentionMs: json['retentionMs'] as int?,
    );
  }
}

/// Consumer group information
class GroupInfo {
  /// Group ID
  final String id;

  /// Protocol type
  final String? protocolType;

  /// Group state
  final String? state;

  /// Number of members
  final int memberCount;

  /// Total lag across all partitions
  final int? totalLag;

  /// Members
  final List<GroupMemberInfo> members;

  const GroupInfo({
    required this.id,
    this.protocolType,
    this.state,
    this.memberCount = 0,
    this.totalLag,
    this.members = const [],
  });

  factory GroupInfo.fromJson(Map<String, dynamic> json) {
    return GroupInfo(
      id: json['groupId'] as String? ?? json['id'] as String,
      protocolType: json['protocolType'] as String?,
      state: json['state'] as String?,
      memberCount: json['memberCount'] as int? ?? 0,
      totalLag: json['totalLag'] as int?,
      members: (json['members'] as List<dynamic>?)
              ?.map((m) => GroupMemberInfo.fromJson(m as Map<String, dynamic>))
              .toList() ??
          [],
    );
  }
}

/// Consumer group member information
class GroupMemberInfo {
  /// Member ID
  final String memberId;

  /// Client ID
  final String? clientId;

  /// Client host
  final String? clientHost;

  const GroupMemberInfo({
    required this.memberId,
    this.clientId,
    this.clientHost,
  });

  factory GroupMemberInfo.fromJson(Map<String, dynamic> json) {
    return GroupMemberInfo(
      memberId: json['memberId'] as String,
      clientId: json['clientId'] as String?,
      clientHost: json['clientHost'] as String?,
    );
  }
}

/// Connection state
enum ConnectionState {
  disconnected,
  connecting,
  connected,
}

/// RPC transport interface
abstract class RpcTransport {
  Future<dynamic> call(String method, List<dynamic> args);
  Future<void> close();
}

/// Dead letter queue record
class DlqRecord<T> {
  /// Original record
  final T originalRecord;

  /// Error message
  final String error;

  /// Timestamp
  final DateTime timestamp;

  const DlqRecord({
    required this.originalRecord,
    required this.error,
    required this.timestamp,
  });
}
