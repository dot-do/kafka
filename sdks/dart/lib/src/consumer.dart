/// Kafka consumer for kafka.do
library;

import 'dart:async';
import 'dart:convert';

import 'types.dart';
import 'kafka.dart';
import 'exceptions.dart';

/// Typed Kafka consumer
///
/// Consume messages from Kafka topics with type safety and automatic deserialization.
///
/// ```dart
/// final consumer = kafka.consumer<Order>('orders', group: 'order-processor');
///
/// await for (final record in consumer) {
///   print('Topic: ${record.topic}');
///   print('Partition: ${record.partition}');
///   print('Offset: ${record.offset}');
///   print('Value: ${record.value}');
///
///   await processOrder(record.value);
///   await record.commit();
/// }
/// ```
class Consumer<T> {
  final Kafka _kafka;
  final String _topic;
  final ConsumerConfig _config;
  final T Function(String)? _fromJson;

  ConnectionState _connectionState = ConnectionState.disconnected;
  bool _isRunning = false;
  final Set<String> _paused = {};

  /// Create a new consumer
  Consumer(
    this._kafka,
    this._topic, {
    required ConsumerConfig config,
    T Function(String)? fromJson,
  })  : _config = config,
        _fromJson = fromJson;

  /// Get the topic name
  String get topic => _topic;

  /// Get the consumer group ID
  String get groupId => _config.groupId;

  /// Get the current connection state
  ConnectionState get connectionState => _connectionState;

  /// Check if the consumer is connected
  bool get isConnected => _connectionState == ConnectionState.connected;

  /// Check if the consumer is running
  bool get isRunning => _isRunning;

  /// Connect the consumer
  Future<void> connect() async {
    if (_connectionState == ConnectionState.connected) return;

    _connectionState = ConnectionState.connecting;

    try {
      await _kafka.getTransport().call('consumerConnect', [
        {
          'clientId': _kafka.config.clientId,
          'groupId': _config.groupId,
          'sessionTimeout': _config.sessionTimeout ?? 30000,
          'rebalanceTimeout': _config.rebalanceTimeout ?? 60000,
          'heartbeatInterval': _config.heartbeatInterval ?? 3000,
          'maxBytesPerPartition': _config.maxBytesPerPartition ?? 1048576,
          'minBytes': _config.minBytes ?? 1,
          'maxBytes': _config.maxBytes ?? 10485760,
          'maxWaitTimeInMs': _config.maxWaitTimeInMs ?? 5000,
        }
      ]);

      _connectionState = ConnectionState.connected;
    } catch (e) {
      _connectionState = ConnectionState.disconnected;
      throw ConnectionException('Failed to connect consumer: $e');
    }
  }

  /// Disconnect the consumer
  Future<void> disconnect() async {
    if (_connectionState == ConnectionState.disconnected) return;

    await stop();

    try {
      await _kafka.getTransport().call('consumerDisconnect', [
        {'groupId': _config.groupId}
      ]);
    } finally {
      _connectionState = ConnectionState.disconnected;
    }
  }

  /// Subscribe to the topic
  Future<void> subscribe({bool fromBeginning = false}) async {
    await _ensureConnected();

    try {
      await _kafka.getTransport().call('consumerSubscribe', [
        {
          'groupId': _config.groupId,
          'topics': [_topic],
          'fromBeginning': fromBeginning,
        }
      ]);
    } catch (e) {
      throw ConsumerException('Failed to subscribe: $e', groupId: _config.groupId);
    }
  }

  /// Unsubscribe from all topics
  Future<void> unsubscribe() async {
    await _ensureConnected();

    try {
      await _kafka.getTransport().call('consumerUnsubscribe', [
        {'groupId': _config.groupId}
      ]);
    } catch (e) {
      throw ConsumerException('Failed to unsubscribe: $e', groupId: _config.groupId);
    }
  }

  /// Start consuming and return a stream of records
  ///
  /// ```dart
  /// await for (final record in consumer.stream()) {
  ///   print('Received: ${record.value}');
  ///   await record.commit();
  /// }
  /// ```
  Stream<KafkaRecord<T>> stream() async* {
    await _ensureConnected();
    await subscribe(fromBeginning: _config.offset == Offset.earliest);

    _isRunning = true;

    try {
      await _kafka.getTransport().call('consumerRun', [
        {
          'groupId': _config.groupId,
          'autoCommit': _config.autoCommit ?? false,
        }
      ]);

      final pollInterval = Duration(milliseconds: _config.maxWaitTimeInMs ?? 5000);

      while (_isRunning) {
        final result = await _kafka.getTransport().call('consumerPoll', [
          _config.groupId,
          [_topic],
          _config.maxPollRecords ?? 100,
        ]);

        final messages = (result as Map<String, dynamic>)['messages'] as List<dynamic>?;

        if (messages != null && messages.isNotEmpty) {
          for (final msg in messages) {
            final msgMap = msg as Map<String, dynamic>;
            final topicName = msgMap['topic'] as String;
            final partition = msgMap['partition'] as int;
            final offset = msgMap['offset'] as int;
            final pauseKey = '$topicName:$partition';

            // Skip if paused
            if (_paused.contains(pauseKey)) continue;

            yield KafkaRecord<T>(
              topic: topicName,
              partition: partition,
              offset: offset,
              key: msgMap['key'] as String?,
              value: _deserialize(msgMap['value'] as String),
              timestamp: DateTime.fromMillisecondsSinceEpoch(
                msgMap['timestamp'] as int? ?? DateTime.now().millisecondsSinceEpoch,
              ),
              headers: (msgMap['headers'] as Map<String, dynamic>?)
                      ?.map((k, v) => MapEntry(k, v.toString())) ??
                  {},
              commit: () => _commitOffset(topicName, partition, offset),
            );
          }
        }

        // Wait before next poll if no messages
        if (messages == null || messages.isEmpty) {
          await Future<void>.delayed(pollInterval);
        }
      }
    } finally {
      _isRunning = false;
    }
  }

  /// Start consuming and return a stream of batches
  ///
  /// ```dart
  /// await for (final batch in consumer.batchStream()) {
  ///   for (final record in batch) {
  ///     await processOrder(record.value);
  ///   }
  ///   await batch.commit();
  /// }
  /// ```
  Stream<Batch<T>> batchStream({
    int batchSize = 100,
    Duration batchTimeout = const Duration(seconds: 5),
  }) async* {
    await _ensureConnected();
    await subscribe(fromBeginning: _config.offset == Offset.earliest);

    _isRunning = true;

    try {
      await _kafka.getTransport().call('consumerRun', [
        {
          'groupId': _config.groupId,
          'autoCommit': false,
        }
      ]);

      final pollInterval = Duration(milliseconds: _config.maxWaitTimeInMs ?? 5000);
      var records = <KafkaRecord<T>>[];
      var batchStartTime = DateTime.now();

      while (_isRunning) {
        final result = await _kafka.getTransport().call('consumerPoll', [
          _config.groupId,
          [_topic],
          batchSize - records.length,
        ]);

        final messages = (result as Map<String, dynamic>)['messages'] as List<dynamic>?;

        if (messages != null && messages.isNotEmpty) {
          for (final msg in messages) {
            final msgMap = msg as Map<String, dynamic>;
            final topicName = msgMap['topic'] as String;
            final partition = msgMap['partition'] as int;
            final offset = msgMap['offset'] as int;
            final pauseKey = '$topicName:$partition';

            // Skip if paused
            if (_paused.contains(pauseKey)) continue;

            records.add(KafkaRecord<T>(
              topic: topicName,
              partition: partition,
              offset: offset,
              key: msgMap['key'] as String?,
              value: _deserialize(msgMap['value'] as String),
              timestamp: DateTime.fromMillisecondsSinceEpoch(
                msgMap['timestamp'] as int? ?? DateTime.now().millisecondsSinceEpoch,
              ),
              headers: (msgMap['headers'] as Map<String, dynamic>?)
                      ?.map((k, v) => MapEntry(k, v.toString())) ??
                  {},
              commit: () => _commitOffset(topicName, partition, offset),
            ));
          }
        }

        // Yield batch if size reached or timeout exceeded
        final timeoutExceeded = DateTime.now().difference(batchStartTime) >= batchTimeout;
        if (records.length >= batchSize || (records.isNotEmpty && timeoutExceeded)) {
          yield Batch<T>(
            List.from(records),
            () => _commitBatch(records),
          );
          records = [];
          batchStartTime = DateTime.now();
        }

        // Wait before next poll if batch is empty
        if (records.isEmpty) {
          await Future<void>.delayed(pollInterval);
        }
      }

      // Yield remaining records
      if (records.isNotEmpty) {
        yield Batch<T>(
          records,
          () => _commitBatch(records),
        );
      }
    } finally {
      _isRunning = false;
    }
  }

  /// Stop consuming
  Future<void> stop() async {
    if (!_isRunning) return;

    _isRunning = false;

    try {
      await _kafka.getTransport().call('consumerStop', [
        {'groupId': _config.groupId}
      ]);
    } catch (_) {
      // Ignore errors during stop
    }
  }

  /// Pause consumption for specific partitions
  void pause(List<({String topic, int partition})> topicPartitions) {
    for (final tp in topicPartitions) {
      _paused.add('${tp.topic}:${tp.partition}');
    }
  }

  /// Resume consumption for specific partitions
  void resume(List<({String topic, int partition})> topicPartitions) {
    for (final tp in topicPartitions) {
      _paused.remove('${tp.topic}:${tp.partition}');
    }
  }

  /// Get paused partitions
  List<({String topic, int partition})> paused() {
    return _paused.map((key) {
      final parts = key.split(':');
      return (topic: parts[0], partition: int.parse(parts[1]));
    }).toList();
  }

  /// Seek to a specific offset
  Future<void> seek(int partition, int offset) async {
    await _ensureConnected();

    try {
      await _kafka.getTransport().call('consumerSeek', [
        {
          'groupId': _config.groupId,
          'topic': _topic,
          'partition': partition,
          'offset': offset,
        }
      ]);
    } catch (e) {
      throw ConsumerException('Failed to seek: $e', groupId: _config.groupId);
    }
  }

  /// Commit a specific offset
  Future<void> _commitOffset(String topic, int partition, int offset) async {
    try {
      await _kafka.getTransport().call('consumerCommitOffsets', [
        _config.groupId,
        [
          {'topic': topic, 'partition': partition, 'offset': offset}
        ],
      ]);
    } catch (e) {
      throw ConsumerException('Failed to commit offset: $e', groupId: _config.groupId);
    }
  }

  /// Commit offsets for a batch of records
  Future<void> _commitBatch(List<KafkaRecord<T>> records) async {
    if (records.isEmpty) return;

    // Group by topic-partition and get max offset
    final offsets = <String, Map<String, dynamic>>{};
    for (final record in records) {
      final key = '${record.topic}:${record.partition}';
      final existing = offsets[key];
      if (existing == null || (existing['offset'] as int) < record.offset) {
        offsets[key] = {
          'topic': record.topic,
          'partition': record.partition,
          'offset': record.offset,
        };
      }
    }

    try {
      await _kafka.getTransport().call('consumerCommitOffsets', [
        _config.groupId,
        offsets.values.toList(),
      ]);
    } catch (e) {
      throw ConsumerException('Failed to commit batch: $e', groupId: _config.groupId);
    }
  }

  /// Send heartbeat
  Future<void> heartbeat() async {
    await _ensureConnected();

    try {
      await _kafka.getTransport().call('consumerHeartbeat', [
        {'groupId': _config.groupId}
      ]);
    } catch (e) {
      throw ConsumerException('Heartbeat failed: $e', groupId: _config.groupId);
    }
  }

  /// Deserialize a message value
  T _deserialize(String value) {
    if (_fromJson != null) {
      return _fromJson(value);
    }
    // If T is String, return as-is
    if (T == String) {
      return value as T;
    }
    // Try to decode as JSON
    try {
      return jsonDecode(value) as T;
    } catch (_) {
      return value as T;
    }
  }

  /// Ensure the consumer is connected
  Future<void> _ensureConnected() async {
    if (_connectionState != ConnectionState.connected) {
      await connect();
    }
  }
}
