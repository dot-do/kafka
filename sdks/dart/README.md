# kafka_do

> Event Streaming for Dart. Streams. Futures. Zero Ops.

```dart
final producer = kafka.producer<Order>('orders');
await producer.send(Order(id: '123', amount: 99.99));
```

Async/await. Strong typing. The Dart way.

## Installation

```yaml
# pubspec.yaml
dependencies:
  kafka_do: ^0.1.0
```

```bash
dart pub get
```

Requires Dart 3.0+.

## Quick Start

```dart
import 'package:kafka_do/kafka_do.dart';

class Order {
  final String orderId;
  final double amount;

  Order({required this.orderId, required this.amount});

  factory Order.fromJson(Map<String, dynamic> json) =>
      Order(orderId: json['order_id'], amount: json['amount']);

  Map<String, dynamic> toJson() => {'order_id': orderId, 'amount': amount};
}

void main() async {
  final kafka = Kafka();

  // Produce
  final producer = kafka.producer<Order>('orders');
  await producer.send(Order(orderId: '123', amount: 99.99));

  // Consume
  await for (final record in kafka.consumer<Order>('orders', group: 'my-processor')) {
    print('Received: ${record.value}');
    await record.commit();
  }
}
```

## Producing Messages

### Simple Producer

```dart
final kafka = Kafka();
final producer = kafka.producer<Order>('orders');

// Send single message
await producer.send(Order(orderId: '123', amount: 99.99));

// Send with key for partitioning
await producer.send(
  Order(orderId: '123', amount: 99.99),
  key: 'customer-456',
);

// Send with headers
await producer.send(
  Order(orderId: '123', amount: 99.99),
  key: 'customer-456',
  headers: {'correlation-id': 'abc-123'},
);
```

### Batch Producer

```dart
final producer = kafka.producer<Order>('orders');

// Send batch
await producer.sendBatch([
  Order(orderId: '124', amount: 49.99),
  Order(orderId: '125', amount: 149.99),
  Order(orderId: '126', amount: 29.99),
]);

// Send batch with keys
await producer.sendBatch([
  Message(key: 'cust-1', value: Order(orderId: '124', amount: 49.99)),
  Message(key: 'cust-2', value: Order(orderId: '125', amount: 149.99)),
]);
```

### Fire-and-Forget

```dart
final producer = kafka.producer<Order>('orders');

// Non-blocking send
unawaited(producer.send(order).catchError((e) {
  print('Failed: $e');
}));
```

### Transactional Producer

```dart
await kafka.transaction<Order>('orders', (tx) async {
  await tx.send(Order(orderId: '123', status: Status.created));
  await tx.send(Order(orderId: '123', status: Status.validated));
  // Automatically commits on success, aborts on exception
});
```

## Consuming Messages

### Basic Consumer with Stream

```dart
final consumer = kafka.consumer<Order>('orders', group: 'order-processor');

await for (final record in consumer) {
  print('Topic: ${record.topic}');
  print('Partition: ${record.partition}');
  print('Offset: ${record.offset}');
  print('Key: ${record.key}');
  print('Value: ${record.value}');
  print('Timestamp: ${record.timestamp}');
  print('Headers: ${record.headers}');

  await processOrder(record.value);
  await record.commit();
}
```

### Consumer with Stream Transformations

```dart
final consumer = kafka.consumer<Order>('orders', group: 'processor');

// Filter and map
await consumer
    .where((r) => r.value.amount > 100)
    .map((r) => r.value)
    .forEach(processHighValueOrder);

// Take specific number
await consumer
    .take(10)
    .forEach((record) async {
      await processOrder(record.value);
      await record.commit();
    });
```

### Consumer with Configuration

```dart
final config = ConsumerConfig(
  offset: Offset.earliest,
  autoCommit: true,
  maxPollRecords: 100,
  sessionTimeout: Duration(seconds: 30),
);

final consumer = kafka.consumer<Order>(
  'orders',
  group: 'processor',
  config: config,
);

await for (final record in consumer) {
  await processOrder(record.value);
  // Auto-committed
}
```

### Consumer from Timestamp

```dart
final yesterday = DateTime.now().subtract(Duration(days: 1));

final config = ConsumerConfig(
  offset: Offset.timestamp(yesterday),
);

final consumer = kafka.consumer<Order>(
  'orders',
  group: 'replay',
  config: config,
);

await for (final record in consumer) {
  print('Replaying: ${record.value}');
}
```

### Batch Consumer

```dart
final config = BatchConsumerConfig(
  batchSize: 100,
  batchTimeout: Duration(seconds: 5),
);

final consumer = kafka.batchConsumer<Order>(
  'orders',
  group: 'batch-processor',
  config: config,
);

await for (final batch in consumer) {
  for (final record in batch) {
    await processOrder(record.value);
  }
  await batch.commit();
}
```

### Parallel Consumer with Isolates

```dart
import 'dart:isolate';

final consumer = kafka.consumer<Order>('orders', group: 'parallel-processor');
final receivePort = ReceivePort();

// Start worker isolates
for (var i = 0; i < 10; i++) {
  await Isolate.spawn(orderWorker, receivePort.sendPort);
}

// Distribute work
await for (final record in consumer) {
  final workers = await receivePort.first as SendPort;
  workers.send(record);
  await record.commit();
}
```

## Stream Processing

### Filter and Transform

```dart
kafka.stream<Order>('orders')
    .filter((order) => order.amount > 100)
    .map((order) => PremiumOrder(order: order, tier: 'premium'))
    .to('high-value-orders');
```

### Windowed Aggregations

```dart
kafka.stream<Order>('orders')
    .window(Window.tumbling(Duration(minutes: 5)))
    .groupBy((order) => order.customerId)
    .count()
    .listen((entry) {
      final MapEntry(key: customerId, value: window) = entry;
      print('Customer $customerId: ${window.value} orders in ${window.start}-${window.end}');
    });
```

### Joins

```dart
final orders = kafka.stream<Order>('orders');
final customers = kafka.stream<Customer>('customers');

orders.join(
  customers,
  on: (order, customer) => order.customerId == customer.id,
  window: Duration(hours: 1),
).listen((entry) {
  final (order, customer) = entry;
  print('Order by ${customer.name}');
});
```

### Branching

```dart
kafka.stream<Order>('orders').branch({
  (order) => order.region == 'us': 'us-orders',
  (order) => order.region == 'eu': 'eu-orders',
  (_) => true: 'other-orders',
});
```

### Aggregations

```dart
kafka.stream<Order>('orders')
    .groupBy((order) => order.customerId)
    .reduce(0.0, (total, order) => total + order.amount)
    .listen((entry) {
      final MapEntry(key: customerId, value: total) = entry;
      print('Customer $customerId total: \$$total');
    });
```

## Topic Administration

```dart
final admin = kafka.admin;

// Create topic
await admin.createTopic('orders', TopicConfig(
  partitions: 3,
  retentionMs: Duration(days: 7).inMilliseconds,
));

// List topics
await for (final topic in admin.listTopics()) {
  print('${topic.name}: ${topic.partitions} partitions');
}

// Describe topic
final info = await admin.describeTopic('orders');
print('Partitions: ${info.partitions}');
print('Retention: ${info.retentionMs}ms');

// Alter topic
await admin.alterTopic('orders', TopicConfig(
  retentionMs: Duration(days: 30).inMilliseconds,
));

// Delete topic
await admin.deleteTopic('old-events');
```

## Consumer Groups

```dart
final admin = kafka.admin;

// List groups
await for (final group in admin.listGroups()) {
  print('Group: ${group.id}, Members: ${group.memberCount}');
}

// Describe group
final info = await admin.describeGroup('order-processor');
print('State: ${info.state}');
print('Members: ${info.members.length}');
print('Total Lag: ${info.totalLag}');

// Reset offsets
await admin.resetOffsets('order-processor', 'orders', Offset.earliest);

// Reset to timestamp
await admin.resetOffsets('order-processor', 'orders', Offset.timestamp(yesterday));
```

## Error Handling

```dart
final producer = kafka.producer<Order>('orders');

try {
  await producer.send(order);
} on TopicNotFoundException {
  print('Topic not found');
} on MessageTooLargeException {
  print('Message too large');
} on TimeoutException {
  print('Request timed out');
} on KafkaException catch (e) {
  print('Kafka error: ${e.code} - ${e.message}');

  if (e.isRetriable) {
    // Safe to retry
  }
}
```

### Exception Hierarchy

```dart
sealed class KafkaException implements Exception {
  bool get isRetriable;
}

class TopicNotFoundException extends KafkaException {}
class PartitionNotFoundException extends KafkaException {}
class MessageTooLargeException extends KafkaException {}
class NotLeaderException extends KafkaException {}
class OffsetOutOfRangeException extends KafkaException {}
class GroupCoordinatorException extends KafkaException {}
class RebalanceInProgressException extends KafkaException {}
class UnauthorizedException extends KafkaException {}
class QuotaExceededException extends KafkaException {}
class TimeoutException extends KafkaException {}
class DisconnectedException extends KafkaException {}
class SerializationException extends KafkaException {}
```

### Dead Letter Queue Pattern

```dart
final consumer = kafka.consumer<Order>('orders', group: 'processor');
final dlqProducer = kafka.producer<DlqRecord>('orders-dlq');

await for (final record in consumer) {
  try {
    await processOrder(record.value);
  } on ProcessingException catch (e) {
    await dlqProducer.send(
      DlqRecord(
        originalRecord: record.value,
        error: e.message,
        timestamp: DateTime.now(),
      ),
      headers: {
        'original-topic': record.topic,
        'original-partition': record.partition.toString(),
        'original-offset': record.offset.toString(),
      },
    );
  }
  await record.commit();
}
```

### Retry with Exponential Backoff

```dart
Future<T> withRetry<T>(
  Future<T> Function() operation, {
  int maxAttempts = 3,
  Duration baseDelay = const Duration(seconds: 1),
}) async {
  var attempt = 0;
  while (true) {
    try {
      attempt++;
      return await operation();
    } on KafkaException catch (e) {
      if (!e.isRetriable || attempt >= maxAttempts) rethrow;
      final delay = baseDelay * (1 << (attempt - 1));
      await Future.delayed(delay + Duration(milliseconds: Random().nextInt(100)));
    }
  }
}

final producer = kafka.producer<Order>('orders');
await withRetry(() => producer.send(order));
```

## Configuration

### Environment Variables

```bash
export KAFKA_DO_URL=https://kafka.do
export KAFKA_DO_API_KEY=your-api-key
```

### Client Configuration

```dart
final config = KafkaConfig(
  url: 'https://kafka.do',
  apiKey: 'your-api-key',
  timeout: Duration(seconds: 30),
  retries: 3,
);

final kafka = Kafka(config: config);
```

### Producer Configuration

```dart
final config = ProducerConfig(
  batchSize: 16384,
  lingerMs: 5,
  compression: Compression.gzip,
  acks: Acks.all,
  retries: 3,
  retryBackoffMs: 100,
);

final producer = kafka.producer<Order>('orders', config: config);
```

### Consumer Configuration

```dart
final config = ConsumerConfig(
  offset: Offset.latest,
  autoCommit: false,
  fetchMinBytes: 1,
  fetchMaxWaitMs: 500,
  maxPollRecords: 500,
  sessionTimeout: Duration(seconds: 30),
  heartbeatInterval: Duration(seconds: 3),
);

final consumer = kafka.consumer<Order>('orders', group: 'processor', config: config);
```

## Flutter Integration

```dart
import 'package:flutter/material.dart';
import 'package:kafka_do/kafka_do.dart';

class OrdersProvider extends ChangeNotifier {
  final Kafka _kafka;
  final List<Order> _orders = [];
  StreamSubscription? _subscription;

  List<Order> get orders => List.unmodifiable(_orders);

  OrdersProvider(this._kafka);

  void startConsuming() {
    _subscription = _kafka
        .consumer<Order>('orders', group: 'flutter-app')
        .listen((record) {
      _orders.add(record.value);
      notifyListeners();
      record.commit();
    });
  }

  void stopConsuming() {
    _subscription?.cancel();
  }

  Future<void> sendOrder(Order order) async {
    await _kafka.producer<Order>('orders').send(order);
  }

  @override
  void dispose() {
    stopConsuming();
    super.dispose();
  }
}

class OrdersPage extends StatelessWidget {
  @override
  Widget build(BuildContext context) {
    return Consumer<OrdersProvider>(
      builder: (context, provider, _) {
        return ListView.builder(
          itemCount: provider.orders.length,
          itemBuilder: (context, index) {
            final order = provider.orders[index];
            return ListTile(
              title: Text('Order ${order.orderId}'),
              subtitle: Text('\$${order.amount}'),
            );
          },
        );
      },
    );
  }
}
```

## Testing

### Mock Client

```dart
import 'package:kafka_do/testing.dart';
import 'package:test/test.dart';

void main() {
  group('OrderProcessor', () {
    late MockKafka kafka;

    setUp(() {
      kafka = MockKafka();
    });

    test('processes all orders', () async {
      // Seed test data
      kafka.seed('orders', [
        Order(orderId: '123', amount: 99.99),
        Order(orderId: '124', amount: 149.99),
      ]);

      // Process
      final processed = <Order>[];
      await kafka
          .consumer<Order>('orders', group: 'test')
          .take(2)
          .forEach((record) async {
        processed.add(record.value);
        await record.commit();
      });

      expect(processed, hasLength(2));
      expect(processed.first.orderId, equals('123'));
    });

    test('sends messages', () async {
      await kafka.producer<Order>('orders').send(
        Order(orderId: '125', amount: 99.99),
      );

      final messages = kafka.getMessages<Order>('orders');
      expect(messages, hasLength(1));
      expect(messages.first.orderId, equals('125'));
    });
  });
}
```

### Integration Testing

```dart
import 'package:test/test.dart';
import 'dart:io';

void main() {
  late Kafka kafka;
  late String topic;

  setUpAll(() {
    kafka = Kafka(config: KafkaConfig(
      url: Platform.environment['TEST_KAFKA_URL']!,
      apiKey: Platform.environment['TEST_KAFKA_API_KEY']!,
    ));
    topic = 'test-orders-${Uuid().v4()}';
  });

  tearDownAll(() async {
    try {
      await kafka.admin.deleteTopic(topic);
    } catch (_) {}
  });

  test('end to end produces and consumes', () async {
    // Produce
    await kafka.producer<Order>(topic).send(
      Order(orderId: '123', amount: 99.99),
    );

    // Consume
    final record = await kafka
        .consumer<Order>(topic, group: 'test')
        .first;

    expect(record.value.orderId, equals('123'));
  });
}
```

## API Reference

### Kafka

```dart
class Kafka {
  Kafka({KafkaConfig? config});

  Producer<T> producer<T>(String topic, {ProducerConfig? config});
  Stream<KafkaRecord<T>> consumer<T>(String topic, {required String group, ConsumerConfig? config});
  Stream<Batch<T>> batchConsumer<T>(String topic, {required String group, BatchConsumerConfig? config});
  Future<void> transaction<T>(String topic, Future<void> Function(Transaction<T>) operation);
  KafkaStream<T> stream<T>(String topic);
  Admin get admin;
}
```

### Producer

```dart
class Producer<T> {
  Future<RecordMetadata> send(T value, {String? key, Map<String, String>? headers, int? partition});
  Future<List<RecordMetadata>> sendBatch(List<T> values);
  Future<List<RecordMetadata>> sendBatch(List<Message<T>> messages);
}
```

### KafkaRecord

```dart
class KafkaRecord<T> {
  final String topic;
  final int partition;
  final int offset;
  final String? key;
  final T value;
  final DateTime timestamp;
  final Map<String, String> headers;

  Future<void> commit();
}
```

### KafkaStream

```dart
class KafkaStream<T> {
  KafkaStream<T> filter(bool Function(T) predicate);
  KafkaStream<R> map<R>(R Function(T) transform);
  KafkaStream<R> flatMap<R>(List<R> Function(T) transform);
  WindowedStream<T> window(Window window);
  GroupedStream<K, T> groupBy<K>(K Function(T) keySelector);
  void branch(Map<bool Function(T), String> branches);
  JoinedStream<T, R> join<R>(KafkaStream<R> other, {required bool Function(T, R) on, required Duration window});
  Future<void> to(String topic);
  StreamSubscription<T> listen(void Function(T) onData);
}
```

## License

MIT
