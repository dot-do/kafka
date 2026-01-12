import 'package:test/test.dart';
import 'package:kafka_do/kafka_do.dart';

void main() {
  group('Kafka', () {
    late Kafka kafka;
    late MockRpcTransport mockTransport;

    setUp(() {
      kafka = Kafka(config: KafkaConfig(brokers: ['test-broker']));
      mockTransport = MockRpcTransport();
      kafka.setTransport(mockTransport);
    });

    tearDown(() async {
      await kafka.close();
    });

    test('should create producer for topic', () {
      final producer = kafka.producer<String>('test-topic');
      expect(producer.topic, equals('test-topic'));
    });

    test('should send message via producer', () async {
      final producer = kafka.producer<String>('test-topic');

      final result = await producer.send('Hello, Kafka!');

      expect(result.topic, equals('test-topic'));
      expect(result.partition, equals(0));
      expect(result.offset, isNotNull);
      expect(result.errorCode, equals(0));

      final messages = mockTransport.getMessages('test-topic');
      expect(messages.length, equals(1));
      expect(messages.first['value'], equals('Hello, Kafka!'));
    });

    test('should send message with key', () async {
      final producer = kafka.producer<String>('test-topic');

      await producer.send('Hello!', key: 'my-key');

      final messages = mockTransport.getMessages('test-topic');
      expect(messages.first['key'], equals('my-key'));
    });

    test('should send batch of messages', () async {
      final producer = kafka.producer<String>('test-topic');

      final results = await producer.sendBatch([
        'Message 1',
        'Message 2',
        'Message 3',
      ]);

      expect(results.length, equals(3));

      final messages = mockTransport.getMessages('test-topic');
      expect(messages.length, equals(3));
    });

    test('should serialize JSON objects', () async {
      final producer = kafka.producer<Map<String, dynamic>>('json-topic');

      await producer.send({'name': 'test', 'value': 42});

      final messages = mockTransport.getMessages('json-topic');
      expect(messages.first['value'], contains('name'));
      expect(messages.first['value'], contains('test'));
    });
  });

  group('Consumer', () {
    late Kafka kafka;
    late MockRpcTransport mockTransport;

    setUp(() {
      kafka = Kafka(config: KafkaConfig(brokers: ['test-broker']));
      mockTransport = MockRpcTransport();
      kafka.setTransport(mockTransport);
    });

    tearDown(() async {
      await kafka.close();
    });

    test('should consume messages from topic', () async {
      // Seed test data
      mockTransport.seed<String>('test-topic', ['Message 1', 'Message 2', 'Message 3']);

      final consumer = Consumer<String>(
        kafka,
        'test-topic',
        config: ConsumerConfig(groupId: 'test-group'),
      );

      final records = <KafkaRecord<String>>[];

      // Get stream and collect first 3 records
      final stream = consumer.stream();
      var count = 0;
      await for (final record in stream) {
        records.add(record);
        count++;
        if (count >= 3) break;
      }

      await consumer.stop();

      expect(records.length, equals(3));
      expect(records[0].value, contains('Message 1'));
      expect(records[1].value, contains('Message 2'));
      expect(records[2].value, contains('Message 3'));
    });

    test('should track partition and offset', () async {
      mockTransport.seed<String>('test-topic', ['Test']);

      final consumer = Consumer<String>(
        kafka,
        'test-topic',
        config: ConsumerConfig(groupId: 'test-group'),
      );

      final stream = consumer.stream();
      await for (final record in stream) {
        expect(record.partition, equals(0));
        expect(record.offset, greaterThanOrEqualTo(0));
        break;
      }

      await consumer.stop();
    });
  });

  group('Admin', () {
    late Kafka kafka;
    late MockRpcTransport mockTransport;

    setUp(() {
      kafka = Kafka(config: KafkaConfig(brokers: ['test-broker']));
      mockTransport = MockRpcTransport();
      kafka.setTransport(mockTransport);
    });

    tearDown(() async {
      await kafka.close();
    });

    test('should create topic', () async {
      final admin = kafka.admin;

      await admin.createTopic('new-topic', TopicConfig(partitions: 3));

      final topics = await admin.listTopicNames();
      expect(topics, contains('new-topic'));
    });

    test('should delete topic', () async {
      final admin = kafka.admin;

      await admin.createTopic('temp-topic');
      await admin.deleteTopic('temp-topic');

      final topics = await admin.listTopicNames();
      expect(topics, isNot(contains('temp-topic')));
    });

    test('should list topics', () async {
      final admin = kafka.admin;

      await admin.createTopic('topic-1');
      await admin.createTopic('topic-2');

      final topics = await admin.listTopicNames();
      expect(topics, containsAll(['topic-1', 'topic-2']));
    });
  });

  group('Types', () {
    test('RecordMetadata fromJson', () {
      final metadata = RecordMetadata.fromJson({
        'topicName': 'test',
        'partition': 0,
        'errorCode': 0,
        'baseOffset': '100',
      });

      expect(metadata.topic, equals('test'));
      expect(metadata.partition, equals(0));
      expect(metadata.errorCode, equals(0));
      expect(metadata.offset, equals(100));
    });

    test('TopicInfo fromJson', () {
      final info = TopicInfo.fromJson({
        'name': 'test-topic',
        'partitions': 3,
        'replicationFactor': 2,
      });

      expect(info.name, equals('test-topic'));
      expect(info.partitions, equals(3));
      expect(info.replicationFactor, equals(2));
    });

    test('GroupInfo fromJson', () {
      final info = GroupInfo.fromJson({
        'groupId': 'test-group',
        'state': 'Stable',
        'memberCount': 3,
      });

      expect(info.id, equals('test-group'));
      expect(info.state, equals('Stable'));
      expect(info.memberCount, equals(3));
    });

    test('Offset types', () {
      expect(Offset.earliest, isA<Offset>());
      expect(Offset.latest, isA<Offset>());
      expect(Offset.timestamp(DateTime.now()), isA<Offset>());
      expect(Offset.offset(100), isA<Offset>());
    });
  });

  group('Exceptions', () {
    test('ConnectionException is retriable', () {
      const exception = ConnectionException('Test error');
      expect(exception.isRetriable, isTrue);
      expect(exception.code, equals('CONNECTION_ERROR'));
    });

    test('TopicNotFoundException is not retriable', () {
      const exception = TopicNotFoundException('test-topic');
      expect(exception.isRetriable, isFalse);
      expect(exception.topic, equals('test-topic'));
    });

    test('TimeoutException is retriable', () {
      const exception = TimeoutException(Duration(seconds: 30));
      expect(exception.isRetriable, isTrue);
      expect(exception.timeout, equals(Duration(seconds: 30)));
    });
  });

  group('Serialization', () {
    late Kafka kafka;
    late MockRpcTransport mockTransport;

    setUp(() {
      kafka = Kafka(config: KafkaConfig(brokers: ['test-broker']));
      mockTransport = MockRpcTransport();
      kafka.setTransport(mockTransport);
    });

    tearDown(() async {
      await kafka.close();
    });

    test('should use custom serializer', () async {
      final producer = kafka.producer<TestOrder>(
        'orders',
        toJson: (order) => '{"id":"${order.id}","amount":${order.amount}}',
      );

      await producer.send(TestOrder(id: '123', amount: 99.99));

      final messages = mockTransport.getMessages('orders');
      expect(messages.first['value'], equals('{"id":"123","amount":99.99}'));
    });

    test('should use custom deserializer', () async {
      mockTransport.seed<String>('orders', ['{"id":"456","amount":49.99}']);

      final consumer = Consumer<TestOrder>(
        kafka,
        'orders',
        config: ConsumerConfig(groupId: 'test'),
        fromJson: (json) {
          final map = Map<String, dynamic>.from(
            json.startsWith('"')
              ? {'raw': json}
              : (json as dynamic),
          );
          return TestOrder(
            id: map['id']?.toString() ?? '',
            amount: (map['amount'] as num?)?.toDouble() ?? 0.0,
          );
        },
      );

      final stream = consumer.stream();
      await for (final record in stream) {
        expect(record.value.id, equals('456'));
        expect(record.value.amount, equals(49.99));
        break;
      }

      await consumer.stop();
    });
  });
}

/// Test order class
class TestOrder {
  final String id;
  final double amount;

  TestOrder({required this.id, required this.amount});

  Map<String, dynamic> toJson() => {'id': id, 'amount': amount};
}
