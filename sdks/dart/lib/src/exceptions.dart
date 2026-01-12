/// Kafka exceptions for kafka.do
library;

/// Base exception for all Kafka errors
sealed class KafkaException implements Exception {
  final String message;
  final String? code;
  final bool retriable;

  const KafkaException(
    this.message, {
    this.code,
    this.retriable = false,
  });

  /// Whether this exception indicates a retriable error
  bool get isRetriable => retriable;

  @override
  String toString() => '$runtimeType: $message${code != null ? ' ($code)' : ''}';
}

/// Connection-related errors
final class ConnectionException extends KafkaException {
  const ConnectionException(super.message)
      : super(code: 'CONNECTION_ERROR', retriable: true);
}

/// Topic not found error
final class TopicNotFoundException extends KafkaException {
  final String topic;

  const TopicNotFoundException(this.topic)
      : super('Topic not found: $topic', code: 'TOPIC_NOT_FOUND', retriable: false);
}

/// Partition not found error
final class PartitionNotFoundException extends KafkaException {
  final String topic;
  final int partition;

  const PartitionNotFoundException(this.topic, this.partition)
      : super(
          'Partition not found: $topic-$partition',
          code: 'PARTITION_NOT_FOUND',
          retriable: false,
        );
}

/// Message too large error
final class MessageTooLargeException extends KafkaException {
  final int size;
  final int maxSize;

  const MessageTooLargeException({required this.size, required this.maxSize})
      : super(
          'Message too large: $size bytes (max: $maxSize)',
          code: 'MESSAGE_TOO_LARGE',
          retriable: false,
        );
}

/// Not leader for partition error
final class NotLeaderException extends KafkaException {
  final String topic;
  final int partition;

  const NotLeaderException(this.topic, this.partition)
      : super(
          'Not leader for partition: $topic-$partition',
          code: 'NOT_LEADER',
          retriable: true,
        );
}

/// Offset out of range error
final class OffsetOutOfRangeException extends KafkaException {
  final int offset;

  const OffsetOutOfRangeException(this.offset)
      : super(
          'Offset out of range: $offset',
          code: 'OFFSET_OUT_OF_RANGE',
          retriable: false,
        );
}

/// Group coordinator error
final class GroupCoordinatorException extends KafkaException {
  final String groupId;

  const GroupCoordinatorException(this.groupId)
      : super(
          'Group coordinator error for: $groupId',
          code: 'GROUP_COORDINATOR_ERROR',
          retriable: true,
        );
}

/// Rebalance in progress error
final class RebalanceInProgressException extends KafkaException {
  final String groupId;

  const RebalanceInProgressException(this.groupId)
      : super(
          'Rebalance in progress for group: $groupId',
          code: 'REBALANCE_IN_PROGRESS',
          retriable: true,
        );
}

/// Unauthorized access error
final class UnauthorizedException extends KafkaException {
  const UnauthorizedException(super.message)
      : super(code: 'UNAUTHORIZED', retriable: false);
}

/// Quota exceeded error
final class QuotaExceededException extends KafkaException {
  final Duration? retryAfter;

  const QuotaExceededException({super.message = 'Quota exceeded', this.retryAfter})
      : super(code: 'QUOTA_EXCEEDED', retriable: true);
}

/// Timeout error
final class TimeoutException extends KafkaException {
  final Duration timeout;

  const TimeoutException(this.timeout)
      : super(
          'Request timed out after ${timeout.inMilliseconds}ms',
          code: 'TIMEOUT',
          retriable: true,
        );
}

/// Disconnected error
final class DisconnectedException extends KafkaException {
  const DisconnectedException()
      : super('Client is disconnected', code: 'DISCONNECTED', retriable: true);
}

/// Serialization error
final class SerializationException extends KafkaException {
  final Type? type;

  const SerializationException(super.message, {this.type})
      : super(code: 'SERIALIZATION_ERROR', retriable: false);
}

/// Producer error
final class ProducerException extends KafkaException {
  const ProducerException(super.message)
      : super(code: 'PRODUCER_ERROR', retriable: false);
}

/// Consumer error
final class ConsumerException extends KafkaException {
  final String? groupId;

  const ConsumerException(super.message, {this.groupId})
      : super(code: 'CONSUMER_ERROR', retriable: false);
}

/// Admin error
final class AdminException extends KafkaException {
  const AdminException(super.message)
      : super(code: 'ADMIN_ERROR', retriable: false);
}

/// Transaction error
final class TransactionException extends KafkaException {
  final String? transactionalId;

  const TransactionException(super.message, {this.transactionalId})
      : super(code: 'TRANSACTION_ERROR', retriable: false);
}

/// Processing exception (used in consumer handlers)
final class ProcessingException extends KafkaException {
  final dynamic originalError;

  const ProcessingException(super.message, {this.originalError})
      : super(code: 'PROCESSING_ERROR', retriable: false);
}
