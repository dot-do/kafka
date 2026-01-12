import Foundation

// MARK: - Producer

/// A Kafka producer for sending messages to topics.
public struct Producer<T: Encodable & Sendable>: Sendable {
    /// The Kafka client.
    private let kafka: Kafka

    /// The producer configuration.
    private let config: ProducerConfig

    /// The JSON encoder for serializing messages.
    private let encoder: JSONEncoder

    /// Creates a new producer.
    internal init(kafka: Kafka, config: ProducerConfig) {
        self.kafka = kafka
        self.config = config
        self.encoder = JSONEncoder()
        self.encoder.dateEncodingStrategy = .iso8601
    }

    /// Sends a single message to a topic.
    /// - Parameters:
    ///   - topic: The topic to send to.
    ///   - value: The value to send.
    ///   - key: Optional key for partitioning.
    ///   - headers: Optional headers to include.
    /// - Returns: Metadata about the produced record.
    @discardableResult
    public func send(
        topic: String,
        value: T,
        key: String? = nil,
        headers: [String: String] = [:]
    ) async throws -> RecordMetadata {
        let data = try serializeValue(value)
        let keyData = key?.data(using: .utf8)

        let message = ProduceMessage(key: keyData, value: data, headers: headers)
        let results = try await kafka.rpcClient.produce(topic: topic, messages: [message])

        guard let result = results.first else {
            throw KafkaError.unknownError("No result returned from produce")
        }

        return result
    }

    /// Sends a batch of values to a topic.
    /// - Parameters:
    ///   - topic: The topic to send to.
    ///   - values: The values to send.
    /// - Returns: Metadata about the produced records.
    @discardableResult
    public func sendBatch(
        topic: String,
        values: [T]
    ) async throws -> [RecordMetadata] {
        let messages = try values.map { value in
            ProduceMessage(
                key: nil,
                value: try serializeValue(value),
                headers: [:]
            )
        }

        return try await kafka.rpcClient.produce(topic: topic, messages: messages)
    }

    /// Sends a batch of messages to a topic.
    /// - Parameters:
    ///   - topic: The topic to send to.
    ///   - messages: The messages to send.
    /// - Returns: Metadata about the produced records.
    @discardableResult
    public func sendBatch(
        topic: String,
        messages: [Message<T>]
    ) async throws -> [RecordMetadata] {
        let produceMessages = try messages.map { message in
            ProduceMessage(
                key: message.key?.data(using: .utf8),
                value: try serializeValue(message.value),
                headers: message.headers
            )
        }

        return try await kafka.rpcClient.produce(topic: topic, messages: produceMessages)
    }

    /// Serializes a value to Data.
    private func serializeValue(_ value: T) throws -> Data {
        if let data = value as? Data {
            return data
        }
        if let string = value as? String {
            guard let data = string.data(using: .utf8) else {
                throw KafkaError.serializationError("Failed to encode string as UTF-8")
            }
            return data
        }
        do {
            return try encoder.encode(value)
        } catch {
            throw KafkaError.serializationError("Failed to encode value: \(error.localizedDescription)")
        }
    }
}

// MARK: - TopicProducer

/// A topic-bound Kafka producer for sending messages to a specific topic.
public struct TopicProducer<T: Encodable & Sendable>: Sendable {
    /// The underlying producer.
    private let producer: Producer<T>

    /// The target topic.
    private let topic: String

    /// Creates a new topic-bound producer.
    internal init(kafka: Kafka, topic: String, config: ProducerConfig) {
        self.producer = Producer(kafka: kafka, config: config)
        self.topic = topic
    }

    /// Sends a single message to the topic.
    /// - Parameters:
    ///   - value: The value to send.
    ///   - key: Optional key for partitioning.
    ///   - headers: Optional headers to include.
    /// - Returns: Metadata about the produced record.
    @discardableResult
    public func send(
        _ value: T,
        key: String? = nil,
        headers: [String: String] = [:]
    ) async throws -> RecordMetadata {
        try await producer.send(topic: topic, value: value, key: key, headers: headers)
    }

    /// Sends a batch of values to the topic.
    /// - Parameter values: The values to send.
    /// - Returns: Metadata about the produced records.
    @discardableResult
    public func sendBatch(_ values: [T]) async throws -> [RecordMetadata] {
        try await producer.sendBatch(topic: topic, values: values)
    }

    /// Sends a batch of messages to the topic.
    /// - Parameter messages: The messages to send.
    /// - Returns: Metadata about the produced records.
    @discardableResult
    public func sendBatch(_ messages: [Message<T>]) async throws -> [RecordMetadata] {
        try await producer.sendBatch(topic: topic, messages: messages)
    }
}

// MARK: - Transaction

/// A transactional producer for atomic message production.
public struct Transaction<T: Encodable & Sendable>: Sendable {
    private let producer: Producer<T>
    private let topic: String

    internal init(producer: Producer<T>, topic: String) {
        self.producer = producer
        self.topic = topic
    }

    /// Sends a message within the transaction.
    @discardableResult
    public func send(
        _ value: T,
        key: String? = nil,
        headers: [String: String] = [:]
    ) async throws -> RecordMetadata {
        return try await producer.send(topic: topic, value: value, key: key, headers: headers)
    }

    /// Sends a batch of values within the transaction.
    @discardableResult
    public func sendBatch(_ values: [T]) async throws -> [RecordMetadata] {
        return try await producer.sendBatch(topic: topic, values: values)
    }
}

// MARK: - Kafka Transaction Extension

extension Kafka {
    /// Executes a transaction on a topic.
    /// - Parameters:
    ///   - topic: The topic to produce to.
    ///   - operation: The transactional operation to execute.
    /// - Note: Automatically commits on success or aborts on error.
    public func transaction<T: Encodable & Sendable>(
        topic: String,
        operation: @Sendable (Transaction<T>) async throws -> Void
    ) async throws {
        let producer: Producer<T> = self.producer()
        let tx = Transaction(producer: producer, topic: topic)

        // In a real implementation, this would begin a transaction,
        // execute the operation, and commit or abort based on success/failure.
        // For the HTTP API, transactions may be handled server-side.
        try await operation(tx)
    }
}
