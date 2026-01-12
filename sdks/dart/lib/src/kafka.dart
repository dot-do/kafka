/// Kafka client for kafka.do
library;

import 'dart:async';
import 'dart:convert';

import 'package:http/http.dart' as http;

import 'types.dart';
import 'producer.dart';
import 'consumer.dart';
import 'admin.dart';
import 'exceptions.dart';

/// HTTP-based RPC transport for Kafka operations
class HttpRpcTransport implements RpcTransport {
  final String _baseUrl;
  final http.Client _client;
  final Map<String, String> _headers;
  bool _closed = false;

  HttpRpcTransport(
    String baseUrl, {
    String? apiKey,
    http.Client? client,
  })  : _baseUrl = baseUrl.endsWith('/') ? baseUrl.substring(0, baseUrl.length - 1) : baseUrl,
        _client = client ?? http.Client(),
        _headers = {
          'Content-Type': 'application/json',
          if (apiKey != null) 'Authorization': 'Bearer $apiKey',
        };

  @override
  Future<dynamic> call(String method, List<dynamic> args) async {
    if (_closed) {
      throw const ConnectionException('Transport is closed');
    }

    try {
      final response = await _client.post(
        Uri.parse('$_baseUrl/rpc'),
        headers: _headers,
        body: jsonEncode({
          'method': method,
          'args': args,
        }),
      );

      if (response.statusCode == 401) {
        throw const UnauthorizedException('Invalid or expired API key');
      }

      if (response.statusCode >= 400) {
        throw ConnectionException('RPC call failed: ${response.body}');
      }

      final result = jsonDecode(response.body);
      if (result is Map && result['error'] != null) {
        final error = result['error'];
        throw ConnectionException(
          error['message'] ?? 'Unknown error',
        );
      }

      return result['result'];
    } on http.ClientException catch (e) {
      throw ConnectionException('Network error: $e');
    }
  }

  @override
  Future<void> close() async {
    _closed = true;
    _client.close();
  }
}

/// Mock RPC transport for testing
class MockRpcTransport implements RpcTransport {
  final Map<String, List<Map<String, dynamic>>> _topics = {};
  final Map<String, Map<String, int>> _consumerOffsets = {};
  bool _closed = false;
  final List<({String method, List<dynamic> args})> _callLog = [];

  /// Get the call log for testing
  List<({String method, List<dynamic> args})> get callLog => List.unmodifiable(_callLog);

  /// Clear the call log
  void clearCallLog() => _callLog.clear();

  /// Seed test data for a topic
  void seed<T>(String topic, List<T> messages, {String Function(T)? toJson}) {
    _topics.putIfAbsent(topic, () => []);
    for (var i = 0; i < messages.length; i++) {
      final msg = messages[i];
      _topics[topic]!.add({
        'offset': _topics[topic]!.length,
        'key': null,
        'value': toJson != null ? toJson(msg) : jsonEncode(msg),
        'timestamp': DateTime.now().millisecondsSinceEpoch,
        'headers': <String, String>{},
      });
    }
  }

  /// Get messages from a topic (for testing assertions)
  List<Map<String, dynamic>> getMessages(String topic) {
    return List.from(_topics[topic] ?? []);
  }

  @override
  Future<dynamic> call(String method, List<dynamic> args) async {
    if (_closed) {
      throw const ConnectionException('Transport is closed');
    }

    _callLog.add((method: method, args: args));

    switch (method) {
      case 'connect':
      case 'ping':
        return {'ok': 1};

      case 'producerConnect':
      case 'producerDisconnect':
      case 'consumerConnect':
      case 'consumerDisconnect':
      case 'adminConnect':
      case 'adminDisconnect':
        return {'ok': 1};

      case 'producerSend':
        final topic = args[0] as String;
        final messages = args[1] as List<dynamic>;
        _topics.putIfAbsent(topic, () => []);

        final results = <Map<String, dynamic>>[];
        for (final msg in messages) {
          final offset = _topics[topic]!.length;
          _topics[topic]!.add({
            'offset': offset,
            'key': msg['key'],
            'value': msg['value'],
            'timestamp': msg['timestamp'] ?? DateTime.now().millisecondsSinceEpoch,
            'headers': msg['headers'] ?? {},
          });
          results.add({
            'topic': topic,
            'partition': 0,
            'offset': offset,
            'errorCode': 0,
          });
        }
        return results;

      case 'consumerSubscribe':
        return {'ok': 1};

      case 'consumerUnsubscribe':
        return {'ok': 1};

      case 'consumerRun':
        return {'ok': 1};

      case 'consumerStop':
        return {'ok': 1};

      case 'consumerPoll':
        final groupId = args[0] as String;
        final topics = args[1] as List<dynamic>;
        final maxMessages = args[2] as int? ?? 100;

        final messages = <Map<String, dynamic>>[];
        for (final topic in topics) {
          final topicName = topic as String;
          final topicMessages = _topics[topicName] ?? [];
          final offsetKey = '$groupId:$topicName:0';
          final currentOffset = _consumerOffsets[groupId]?[offsetKey] ?? 0;

          for (var i = currentOffset; i < topicMessages.length && messages.length < maxMessages; i++) {
            final msg = topicMessages[i];
            messages.add({
              'topic': topicName,
              'partition': 0,
              'offset': msg['offset'],
              'key': msg['key'],
              'value': msg['value'],
              'timestamp': msg['timestamp'],
              'headers': msg['headers'],
            });
          }
        }

        return {'messages': messages};

      case 'consumerCommitOffsets':
        final groupId = args[0] as String;
        final offsets = args[1] as List<dynamic>;
        _consumerOffsets.putIfAbsent(groupId, () => {});

        for (final offset in offsets) {
          final topic = offset['topic'] as String;
          final partition = offset['partition'] as int;
          final offsetValue = offset['offset'] as int;
          _consumerOffsets[groupId]!['$groupId:$topic:$partition'] = offsetValue + 1;
        }
        return {'ok': 1};

      case 'consumerHeartbeat':
        return {'ok': 1};

      case 'adminListTopics':
        return _topics.keys.toList();

      case 'adminCreateTopics':
        final topics = args[0] as List<dynamic>;
        for (final topic in topics) {
          final name = topic['topic'] as String;
          _topics.putIfAbsent(name, () => []);
        }
        return true;

      case 'adminDeleteTopics':
        final topics = args[0] as List<dynamic>;
        for (final topic in topics) {
          _topics.remove(topic as String);
        }
        return {'ok': 1};

      case 'adminDescribeTopic':
        final topic = args[0] as String;
        if (!_topics.containsKey(topic)) {
          throw TopicNotFoundException(topic);
        }
        return {
          'name': topic,
          'partitions': 1,
          'replicationFactor': 1,
        };

      case 'adminListGroups':
        return _consumerOffsets.keys.map((id) => {'groupId': id}).toList();

      case 'adminDescribeGroup':
        final groupId = args[0] as String;
        return {
          'groupId': groupId,
          'state': 'Stable',
          'memberCount': 1,
          'members': [],
        };

      case 'adminResetOffsets':
        final groupId = args[0] as String;
        final topic = args[1] as String;
        final earliest = args[2] as bool? ?? false;
        _consumerOffsets.putIfAbsent(groupId, () => {});
        _consumerOffsets[groupId]!['$groupId:$topic:0'] = earliest ? 0 : (_topics[topic]?.length ?? 0);
        return {'ok': 1};

      default:
        throw ConnectionException('Unknown method: $method');
    }
  }

  @override
  Future<void> close() async {
    _closed = true;
  }
}

/// Main Kafka client - entry point for all Kafka operations
///
/// Provides a KafkaJS-compatible interface for Dart and Flutter applications.
///
/// ```dart
/// final kafka = Kafka(config: KafkaConfig(
///   brokers: ['kafka.do'],
///   apiKey: 'your-api-key',
/// ));
///
/// // Create a producer
/// final producer = kafka.producer<Order>('orders');
/// await producer.send(Order(orderId: '123', amount: 99.99));
///
/// // Create a consumer
/// await for (final record in kafka.consumer<Order>('orders', group: 'processor')) {
///   print('Received: ${record.value}');
///   await record.commit();
/// }
/// ```
class Kafka {
  final KafkaConfig _config;
  RpcTransport? _transport;
  bool _connected = false;

  /// Create a new Kafka client
  ///
  /// [config] - Kafka configuration with brokers and optional API key
  Kafka({KafkaConfig? config})
      : _config = config ??
            KafkaConfig(
              brokers: [const String.fromEnvironment('KAFKA_DO_URL', defaultValue: 'https://kafka.do')],
              apiKey: const String.fromEnvironment('KAFKA_DO_API_KEY'),
            );

  /// Get the Kafka configuration
  KafkaConfig get config => _config;

  /// Whether the client is connected
  bool get isConnected => _connected;

  /// Get or create the RPC transport
  RpcTransport getTransport() {
    if (_transport == null) {
      final broker = _config.brokers.first;
      final url = broker.startsWith('http://') || broker.startsWith('https://')
          ? broker
          : 'https://${broker.replaceAll(':9092', '')}';

      _transport = HttpRpcTransport(url, apiKey: _config.apiKey);
    }
    return _transport!;
  }

  /// Set a custom transport (for testing)
  void setTransport(RpcTransport transport) {
    _transport = transport;
    _connected = true;
  }

  /// Connect to Kafka
  Future<void> connect() async {
    if (_connected) return;

    final transport = getTransport();
    await transport.call('connect', [_config.brokers]);
    _connected = true;
  }

  /// Create a typed producer for a topic
  ///
  /// ```dart
  /// final producer = kafka.producer<Order>('orders');
  /// await producer.send(Order(orderId: '123', amount: 99.99));
  /// ```
  Producer<T> producer<T>(
    String topic, {
    ProducerConfig? config,
    String Function(T)? toJson,
  }) {
    return Producer<T>(
      this,
      topic,
      config: config,
      toJson: toJson,
    );
  }

  /// Create a typed consumer stream for a topic
  ///
  /// ```dart
  /// await for (final record in kafka.consumer<Order>('orders', group: 'processor')) {
  ///   print('Received: ${record.value}');
  ///   await record.commit();
  /// }
  /// ```
  Stream<KafkaRecord<T>> consumer<T>(
    String topic, {
    required String group,
    ConsumerConfig? config,
    T Function(String)? fromJson,
  }) {
    final consumer = Consumer<T>(
      this,
      topic,
      config: config ?? ConsumerConfig(groupId: group),
      fromJson: fromJson,
    );
    return consumer.stream();
  }

  /// Create a batch consumer stream for a topic
  ///
  /// ```dart
  /// await for (final batch in kafka.batchConsumer<Order>('orders', group: 'processor')) {
  ///   for (final record in batch) {
  ///     await processOrder(record.value);
  ///   }
  ///   await batch.commit();
  /// }
  /// ```
  Stream<Batch<T>> batchConsumer<T>(
    String topic, {
    required String group,
    BatchConsumerConfig? config,
    T Function(String)? fromJson,
  }) {
    final consumer = Consumer<T>(
      this,
      topic,
      config: config ?? BatchConsumerConfig(groupId: group),
      fromJson: fromJson,
    );
    return consumer.batchStream(
      batchSize: config?.batchSize ?? 100,
      batchTimeout: config?.batchTimeout ?? const Duration(seconds: 5),
    );
  }

  /// Execute operations within a transaction
  ///
  /// ```dart
  /// await kafka.transaction<Order>('orders', (tx) async {
  ///   await tx.send(Order(orderId: '123', status: Status.created));
  ///   await tx.send(Order(orderId: '123', status: Status.validated));
  ///   // Automatically commits on success, aborts on exception
  /// });
  /// ```
  Future<void> transaction<T>(
    String topic,
    Future<void> Function(TransactionProducer<T> tx) operation, {
    String Function(T)? toJson,
  }) async {
    final producer = Producer<T>(
      this,
      topic,
      config: ProducerConfig(
        transactionalId: 'tx-${DateTime.now().millisecondsSinceEpoch}',
        idempotent: true,
      ),
      toJson: toJson,
    );

    await producer.connect();

    try {
      final tx = TransactionProducer<T>(producer);
      await tx._begin();
      await operation(tx);
      await tx._commit();
    } catch (e) {
      await producer._abortTransaction();
      rethrow;
    } finally {
      await producer.disconnect();
    }
  }

  /// Get the admin client
  Admin get admin => Admin(this);

  /// Close the Kafka client
  Future<void> close() async {
    if (_transport != null) {
      await _transport!.close();
      _transport = null;
    }
    _connected = false;
  }
}

/// Transaction producer wrapper
class TransactionProducer<T> {
  final Producer<T> _producer;

  TransactionProducer(this._producer);

  Future<void> _begin() async {
    await _producer._beginTransaction();
  }

  Future<void> _commit() async {
    await _producer._commitTransaction();
  }

  /// Send a message within the transaction
  Future<RecordMetadata> send(
    T value, {
    String? key,
    Map<String, String>? headers,
  }) {
    return _producer.send(value, key: key, headers: headers);
  }
}
