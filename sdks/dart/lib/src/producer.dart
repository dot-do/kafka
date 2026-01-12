/// Kafka producer for kafka.do
library;

import 'dart:async';
import 'dart:convert';

import 'types.dart';
import 'kafka.dart';
import 'exceptions.dart';

/// Typed Kafka producer
///
/// Send messages to Kafka topics with type safety and automatic serialization.
///
/// ```dart
/// final producer = kafka.producer<Order>('orders');
///
/// // Send single message
/// await producer.send(Order(orderId: '123', amount: 99.99));
///
/// // Send with key for partitioning
/// await producer.send(
///   Order(orderId: '123', amount: 99.99),
///   key: 'customer-456',
/// );
///
/// // Send with headers
/// await producer.send(
///   Order(orderId: '123', amount: 99.99),
///   key: 'customer-456',
///   headers: {'correlation-id': 'abc-123'},
/// );
/// ```
class Producer<T> {
  final Kafka _kafka;
  final String _topic;
  final ProducerConfig? _config;
  final String Function(T)? _toJson;

  ConnectionState _connectionState = ConnectionState.disconnected;
  bool _inTransaction = false;

  /// Create a new producer
  Producer(
    this._kafka,
    this._topic, {
    ProducerConfig? config,
    String Function(T)? toJson,
  })  : _config = config,
        _toJson = toJson;

  /// Get the topic name
  String get topic => _topic;

  /// Get the current connection state
  ConnectionState get connectionState => _connectionState;

  /// Check if the producer is connected
  bool get isConnected => _connectionState == ConnectionState.connected;

  /// Connect the producer
  Future<void> connect() async {
    if (_connectionState == ConnectionState.connected) return;

    _connectionState = ConnectionState.connecting;

    try {
      await _kafka.getTransport().call('producerConnect', [
        {
          'clientId': _kafka.config.clientId,
          'idempotent': _config?.idempotent ?? false,
          'transactionalId': _config?.transactionalId,
          'maxInFlightRequests': _config?.maxInFlightRequests ?? 5,
        }
      ]);

      _connectionState = ConnectionState.connected;
    } catch (e) {
      _connectionState = ConnectionState.disconnected;
      throw ConnectionException('Failed to connect producer: $e');
    }
  }

  /// Disconnect the producer
  Future<void> disconnect() async {
    if (_connectionState == ConnectionState.disconnected) return;

    try {
      await _kafka.getTransport().call('producerDisconnect', []);
    } finally {
      _connectionState = ConnectionState.disconnected;
    }
  }

  /// Send a single message
  ///
  /// ```dart
  /// await producer.send(Order(orderId: '123', amount: 99.99));
  ///
  /// // With key and headers
  /// await producer.send(
  ///   Order(orderId: '123', amount: 99.99),
  ///   key: 'customer-456',
  ///   headers: {'correlation-id': 'abc-123'},
  /// );
  /// ```
  Future<RecordMetadata> send(
    T value, {
    String? key,
    Map<String, String>? headers,
    int? partition,
  }) async {
    await _ensureConnected();

    final serialized = _serialize(value);
    final message = {
      'key': key,
      'value': serialized,
      'partition': partition,
      'headers': headers,
      'timestamp': DateTime.now().millisecondsSinceEpoch,
    };

    try {
      final results = await _kafka.getTransport().call('producerSend', [
        _topic,
        [message],
        {
          'acks': _config?.acks?.value ?? -1,
          'compression': _config?.compression?.value ?? 0,
          'allowAutoTopicCreation': _config?.allowAutoTopicCreation ?? true,
        }
      ]);

      final resultList = results as List<dynamic>;
      if (resultList.isEmpty) {
        throw const ProducerException('No result returned from send');
      }

      return RecordMetadata.fromJson(resultList.first as Map<String, dynamic>);
    } catch (e) {
      if (e is KafkaException) rethrow;
      throw ProducerException('Failed to send message to topic "$_topic": $e');
    }
  }

  /// Send a batch of messages
  ///
  /// ```dart
  /// await producer.sendBatch([
  ///   Order(orderId: '124', amount: 49.99),
  ///   Order(orderId: '125', amount: 149.99),
  ///   Order(orderId: '126', amount: 29.99),
  /// ]);
  /// ```
  Future<List<RecordMetadata>> sendBatch(List<T> values) async {
    await _ensureConnected();

    final messages = values.map((value) => {
          'key': null,
          'value': _serialize(value),
          'partition': null,
          'headers': null,
          'timestamp': DateTime.now().millisecondsSinceEpoch,
        }).toList();

    try {
      final results = await _kafka.getTransport().call('producerSend', [
        _topic,
        messages,
        {
          'acks': _config?.acks?.value ?? -1,
          'compression': _config?.compression?.value ?? 0,
          'allowAutoTopicCreation': _config?.allowAutoTopicCreation ?? true,
        }
      ]);

      final resultList = results as List<dynamic>;
      return resultList
          .map((r) => RecordMetadata.fromJson(r as Map<String, dynamic>))
          .toList();
    } catch (e) {
      if (e is KafkaException) rethrow;
      throw ProducerException('Failed to send batch to topic "$_topic": $e');
    }
  }

  /// Send a batch of messages with keys
  ///
  /// ```dart
  /// await producer.sendBatchWithKeys([
  ///   Message(key: 'cust-1', value: Order(orderId: '124', amount: 49.99)),
  ///   Message(key: 'cust-2', value: Order(orderId: '125', amount: 149.99)),
  /// ]);
  /// ```
  Future<List<RecordMetadata>> sendBatchWithKeys(List<Message<T>> messages) async {
    await _ensureConnected();

    final serializedMessages = messages.map((msg) => {
          'key': msg.key,
          'value': _serialize(msg.value),
          'partition': msg.partition,
          'headers': msg.headers,
          'timestamp': msg.timestamp?.millisecondsSinceEpoch ?? DateTime.now().millisecondsSinceEpoch,
        }).toList();

    try {
      final results = await _kafka.getTransport().call('producerSend', [
        _topic,
        serializedMessages,
        {
          'acks': _config?.acks?.value ?? -1,
          'compression': _config?.compression?.value ?? 0,
          'allowAutoTopicCreation': _config?.allowAutoTopicCreation ?? true,
        }
      ]);

      final resultList = results as List<dynamic>;
      return resultList
          .map((r) => RecordMetadata.fromJson(r as Map<String, dynamic>))
          .toList();
    } catch (e) {
      if (e is KafkaException) rethrow;
      throw ProducerException('Failed to send batch to topic "$_topic": $e');
    }
  }

  /// Begin a transaction (internal use)
  Future<void> _beginTransaction() async {
    await _ensureConnected();

    if (_config?.transactionalId == null) {
      throw const ProducerException('Transactions require a transactionalId in producer config');
    }

    if (_inTransaction) {
      throw const ProducerException('A transaction is already in progress');
    }

    try {
      await _kafka.getTransport().call('producerBeginTransaction', [
        {'transactionalId': _config!.transactionalId}
      ]);
      _inTransaction = true;
    } catch (e) {
      throw TransactionException('Failed to begin transaction: $e');
    }
  }

  /// Commit a transaction (internal use)
  Future<void> _commitTransaction() async {
    if (!_inTransaction) {
      throw const ProducerException('No transaction in progress');
    }

    try {
      await _kafka.getTransport().call('producerCommitTransaction', [
        {'transactionalId': _config!.transactionalId}
      ]);
    } finally {
      _inTransaction = false;
    }
  }

  /// Abort a transaction (internal use)
  Future<void> _abortTransaction() async {
    if (!_inTransaction) return;

    try {
      await _kafka.getTransport().call('producerAbortTransaction', [
        {'transactionalId': _config!.transactionalId}
      ]);
    } finally {
      _inTransaction = false;
    }
  }

  /// Serialize a value to string
  String _serialize(T value) {
    if (_toJson != null) {
      return _toJson(value);
    }
    if (value is String) {
      return value;
    }
    if (value is Map || value is List) {
      return jsonEncode(value);
    }
    // Try to call toJson if available
    try {
      final dynamic val = value;
      if (val.toJson != null) {
        return jsonEncode(val.toJson());
      }
    } catch (_) {
      // Fall through
    }
    return value.toString();
  }

  /// Ensure the producer is connected
  Future<void> _ensureConnected() async {
    if (_connectionState != ConnectionState.connected) {
      await connect();
    }
  }
}
