/// Kafka admin client for kafka.do
library;

import 'dart:async';

import 'types.dart';
import 'kafka.dart';
import 'exceptions.dart';

/// Kafka admin client for managing topics and consumer groups
///
/// ```dart
/// final admin = kafka.admin;
///
/// // Create topic
/// await admin.createTopic('orders', TopicConfig(
///   partitions: 3,
///   retentionMs: Duration(days: 7).inMilliseconds,
/// ));
///
/// // List topics
/// await for (final topic in admin.listTopics()) {
///   print('${topic.name}: ${topic.partitions} partitions');
/// }
///
/// // Delete topic
/// await admin.deleteTopic('old-events');
/// ```
class Admin {
  final Kafka _kafka;
  ConnectionState _connectionState = ConnectionState.disconnected;

  /// Create a new admin client
  Admin(this._kafka);

  /// Get the current connection state
  ConnectionState get connectionState => _connectionState;

  /// Check if the admin client is connected
  bool get isConnected => _connectionState == ConnectionState.connected;

  /// Connect the admin client
  Future<void> connect() async {
    if (_connectionState == ConnectionState.connected) return;

    _connectionState = ConnectionState.connecting;

    try {
      await _kafka.getTransport().call('adminConnect', [
        {'clientId': _kafka.config.clientId}
      ]);

      _connectionState = ConnectionState.connected;
    } catch (e) {
      _connectionState = ConnectionState.disconnected;
      throw ConnectionException('Failed to connect admin: $e');
    }
  }

  /// Disconnect the admin client
  Future<void> disconnect() async {
    if (_connectionState == ConnectionState.disconnected) return;

    try {
      await _kafka.getTransport().call('adminDisconnect', []);
    } finally {
      _connectionState = ConnectionState.disconnected;
    }
  }

  /// Create a topic
  ///
  /// ```dart
  /// await admin.createTopic('orders', TopicConfig(
  ///   partitions: 3,
  ///   retentionMs: Duration(days: 7).inMilliseconds,
  /// ));
  /// ```
  Future<void> createTopic(String name, [TopicConfig? config]) async {
    await _ensureConnected();

    try {
      await _kafka.getTransport().call('adminCreateTopics', [
        [
          {
            'topic': name,
            'numPartitions': config?.partitions ?? 1,
            'replicationFactor': config?.replicationFactor ?? 1,
            if (config?.retentionMs != null)
              'configEntries': [
                {'name': 'retention.ms', 'value': config!.retentionMs.toString()}
              ],
          }
        ]
      ]);
    } catch (e) {
      throw AdminException('Failed to create topic "$name": $e');
    }
  }

  /// Delete a topic
  ///
  /// ```dart
  /// await admin.deleteTopic('old-events');
  /// ```
  Future<void> deleteTopic(String name) async {
    await _ensureConnected();

    try {
      await _kafka.getTransport().call('adminDeleteTopics', [
        [name]
      ]);
    } catch (e) {
      throw AdminException('Failed to delete topic "$name": $e');
    }
  }

  /// List all topics
  ///
  /// ```dart
  /// await for (final topic in admin.listTopics()) {
  ///   print('${topic.name}: ${topic.partitions} partitions');
  /// }
  /// ```
  Stream<TopicInfo> listTopics() async* {
    await _ensureConnected();

    try {
      final result = await _kafka.getTransport().call('adminListTopics', []);
      final topics = result as List<dynamic>;

      for (final topic in topics) {
        if (topic is String) {
          yield TopicInfo(name: topic, partitions: 1);
        } else if (topic is Map<String, dynamic>) {
          yield TopicInfo.fromJson(topic);
        }
      }
    } catch (e) {
      throw AdminException('Failed to list topics: $e');
    }
  }

  /// Get topic names as a list
  Future<List<String>> listTopicNames() async {
    await _ensureConnected();

    try {
      final result = await _kafka.getTransport().call('adminListTopics', []);
      final topics = result as List<dynamic>;
      return topics.map((t) => t is String ? t : (t as Map)['name'] as String).toList();
    } catch (e) {
      throw AdminException('Failed to list topic names: $e');
    }
  }

  /// Describe a topic
  ///
  /// ```dart
  /// final info = await admin.describeTopic('orders');
  /// print('Partitions: ${info.partitions}');
  /// print('Retention: ${info.retentionMs}ms');
  /// ```
  Future<TopicInfo> describeTopic(String name) async {
    await _ensureConnected();

    try {
      final result = await _kafka.getTransport().call('adminDescribeTopic', [name]);
      return TopicInfo.fromJson(result as Map<String, dynamic>);
    } catch (e) {
      throw AdminException('Failed to describe topic "$name": $e');
    }
  }

  /// Alter topic configuration
  ///
  /// ```dart
  /// await admin.alterTopic('orders', TopicConfig(
  ///   retentionMs: Duration(days: 30).inMilliseconds,
  /// ));
  /// ```
  Future<void> alterTopic(String name, TopicConfig config) async {
    await _ensureConnected();

    try {
      await _kafka.getTransport().call('adminAlterTopics', [
        {
          'topic': name,
          ...config.toJson(),
        }
      ]);
    } catch (e) {
      throw AdminException('Failed to alter topic "$name": $e');
    }
  }

  /// List consumer groups
  ///
  /// ```dart
  /// await for (final group in admin.listGroups()) {
  ///   print('Group: ${group.id}, Members: ${group.memberCount}');
  /// }
  /// ```
  Stream<GroupInfo> listGroups() async* {
    await _ensureConnected();

    try {
      final result = await _kafka.getTransport().call('adminListGroups', []);
      final groups = result as List<dynamic>;

      for (final group in groups) {
        yield GroupInfo.fromJson(group as Map<String, dynamic>);
      }
    } catch (e) {
      throw AdminException('Failed to list groups: $e');
    }
  }

  /// Describe a consumer group
  ///
  /// ```dart
  /// final info = await admin.describeGroup('order-processor');
  /// print('State: ${info.state}');
  /// print('Members: ${info.members.length}');
  /// print('Total Lag: ${info.totalLag}');
  /// ```
  Future<GroupInfo> describeGroup(String groupId) async {
    await _ensureConnected();

    try {
      final result = await _kafka.getTransport().call('adminDescribeGroup', [groupId]);
      return GroupInfo.fromJson(result as Map<String, dynamic>);
    } catch (e) {
      throw AdminException('Failed to describe group "$groupId": $e');
    }
  }

  /// Delete a consumer group
  Future<void> deleteGroup(String groupId) async {
    await _ensureConnected();

    try {
      await _kafka.getTransport().call('adminDeleteGroups', [
        [groupId]
      ]);
    } catch (e) {
      throw AdminException('Failed to delete group "$groupId": $e');
    }
  }

  /// Reset consumer group offsets
  ///
  /// ```dart
  /// // Reset to earliest
  /// await admin.resetOffsets('order-processor', 'orders', Offset.earliest);
  ///
  /// // Reset to timestamp
  /// final yesterday = DateTime.now().subtract(Duration(days: 1));
  /// await admin.resetOffsets('order-processor', 'orders', Offset.timestamp(yesterday));
  /// ```
  Future<void> resetOffsets(String groupId, String topic, Offset offset) async {
    await _ensureConnected();

    try {
      bool earliest = false;
      int? timestamp;

      switch (offset) {
        case _EarliestOffset():
          earliest = true;
        case _LatestOffset():
          earliest = false;
        case _TimestampOffset(timestamp: final ts):
          timestamp = ts.millisecondsSinceEpoch;
        case _SpecificOffset():
          throw const AdminException('Specific offset not supported for resetOffsets');
      }

      await _kafka.getTransport().call('adminResetOffsets', [
        groupId,
        topic,
        earliest,
        if (timestamp != null) timestamp,
      ]);
    } catch (e) {
      if (e is KafkaException) rethrow;
      throw AdminException('Failed to reset offsets: $e');
    }
  }

  /// Get consumer group lag for a topic
  Future<Map<int, int>> getGroupLag(String groupId, String topic) async {
    await _ensureConnected();

    try {
      final result = await _kafka.getTransport().call('adminGetGroupLag', [
        groupId,
        topic,
      ]);

      final lag = result as Map<String, dynamic>;
      return lag.map((k, v) => MapEntry(int.parse(k), v as int));
    } catch (e) {
      throw AdminException('Failed to get group lag: $e');
    }
  }

  /// Ensure the admin client is connected
  Future<void> _ensureConnected() async {
    if (_connectionState != ConnectionState.connected) {
      await connect();
    }
  }
}

// These are needed for the switch statement pattern matching
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
