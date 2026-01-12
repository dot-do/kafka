/// Event streaming for Dart - Kafka client with Streams, Futures, and zero ops.
///
/// KafkaJS-compatible API for Dart and Flutter applications.
///
/// ## Features
///
/// - Type-safe producers and consumers with automatic serialization
/// - Modern Dart Streams API with async/await
/// - Transaction support with exactly-once semantics
/// - Batch processing for high-throughput scenarios
/// - Built-in dead letter queue support
/// - Zero infrastructure management
///
/// ## Quick Start
///
/// ```dart
/// import 'package:kafka_do/kafka_do.dart';
///
/// void main() async {
///   final kafka = Kafka(config: KafkaConfig(
///     brokers: ['kafka.do'],
///     apiKey: 'your-api-key',
///   ));
///
///   // Send messages
///   final producer = kafka.producer<Map<String, dynamic>>('orders');
///   await producer.send({'orderId': '123', 'amount': 99.99});
///
///   // Receive messages
///   await for (final record in kafka.consumer<Map<String, dynamic>>(
///     'orders',
///     group: 'processor',
///   )) {
///     print('Received: ${record.value}');
///     await record.commit();
///   }
/// }
/// ```
///
/// ## Producer Usage
///
/// ```dart
/// // Typed producer
/// final producer = kafka.producer<Order>('orders',
///   toJson: (order) => jsonEncode(order.toJson()),
/// );
///
/// // Send with key for partitioning
/// await producer.send(
///   Order(orderId: '123', amount: 99.99),
///   key: 'customer-456',
/// );
///
/// // Send batch
/// await producer.sendBatch([
///   Order(orderId: '124', amount: 49.99),
///   Order(orderId: '125', amount: 149.99),
/// ]);
/// ```
///
/// ## Consumer Usage
///
/// ```dart
/// // Stream consumer
/// await for (final record in kafka.consumer<Order>(
///   'orders',
///   group: 'processor',
///   fromJson: Order.fromJson,
/// )) {
///   await processOrder(record.value);
///   await record.commit();
/// }
///
/// // Batch consumer
/// await for (final batch in kafka.batchConsumer<Order>(
///   'orders',
///   group: 'processor',
///   config: BatchConsumerConfig(
///     groupId: 'processor',
///     batchSize: 100,
///     batchTimeout: Duration(seconds: 5),
///   ),
/// )) {
///   for (final record in batch.records) {
///     await processOrder(record.value);
///   }
///   await batch.commit();
/// }
/// ```
///
/// ## Transactions
///
/// ```dart
/// await kafka.transaction<Order>('orders', (tx) async {
///   await tx.send(Order(orderId: '123', status: Status.created));
///   await tx.send(Order(orderId: '123', status: Status.validated));
///   // Automatically commits on success, aborts on exception
/// });
/// ```
///
/// ## Admin Operations
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
/// // Monitor consumer groups
/// final info = await admin.describeGroup('order-processor');
/// print('Lag: ${info.totalLag}');
/// ```
library kafka_do;

export 'src/kafka.dart' show Kafka, HttpRpcTransport, MockRpcTransport, TransactionProducer;
export 'src/producer.dart' show Producer;
export 'src/consumer.dart' show Consumer;
export 'src/admin.dart' show Admin;
export 'src/types.dart';
export 'src/exceptions.dart';
