import Foundation

// MARK: - Consumer

/// A Kafka consumer for receiving messages from topics.
public struct Consumer<T: Decodable & Sendable>: AsyncSequence, Sendable {
    public typealias Element = KafkaRecord<T>

    /// The Kafka client.
    private let kafka: Kafka

    /// The consumer group ID.
    private let groupId: String

    /// The consumer configuration.
    private let config: ConsumerConfig

    /// The subscribed topics.
    private let subscribedTopics: [String]

    /// Creates a new consumer.
    internal init(kafka: Kafka, groupId: String, config: ConsumerConfig, topics: [String] = []) {
        self.kafka = kafka
        self.groupId = groupId
        self.config = config
        self.subscribedTopics = topics
    }

    /// Subscribes to one or more topics.
    /// - Parameter topics: The topics to subscribe to.
    /// - Returns: A new consumer subscribed to the topics.
    public func subscribe(topics: [String]) async throws -> Consumer<T> {
        try await kafka.rpcClient.subscribe(topics: topics, groupId: groupId)
        return Consumer(kafka: kafka, groupId: groupId, config: config, topics: topics)
    }

    /// Returns an async stream of messages from the subscribed topics.
    public func messages() -> AsyncThrowingStream<KafkaRecord<T>, Error> {
        let kafka = self.kafka
        let groupId = self.groupId
        let config = self.config
        let topics = self.subscribedTopics

        return AsyncThrowingStream { continuation in
            let task = Task {
                do {
                    while !Task.isCancelled {
                        // Fetch messages from each subscribed topic
                        for topic in topics {
                            let rawMessages = try await kafka.rpcClient.fetch(
                                topic: topic,
                                groupId: groupId,
                                config: config
                            )

                            for raw in rawMessages {
                                let value = try deserializeValue(raw.value, as: T.self)
                                let key = raw.key.flatMap { String(data: $0, encoding: .utf8) }

                                let record = KafkaRecord<T>(
                                    topic: raw.topic,
                                    partition: raw.partition,
                                    offset: raw.offset,
                                    key: key,
                                    value: value,
                                    timestamp: raw.timestamp,
                                    headers: raw.headers,
                                    commitHandler: { [kafka, groupId] in
                                        try await kafka.rpcClient.commit(
                                            topic: raw.topic,
                                            groupId: groupId,
                                            partition: raw.partition,
                                            offset: raw.offset + 1
                                        )
                                    }
                                )

                                continuation.yield(record)

                                // Auto-commit if configured
                                if config.autoCommit {
                                    try await record.commit()
                                }
                            }
                        }

                        // Small delay between polls to avoid busy-waiting
                        try await Task.sleep(for: .milliseconds(100))
                    }
                    continuation.finish()
                } catch {
                    continuation.finish(throwing: error)
                }
            }

            continuation.onTermination = { @Sendable _ in
                task.cancel()
            }
        }
    }

    /// Creates an async iterator for the consumer.
    public func makeAsyncIterator() -> AsyncIterator {
        AsyncIterator(consumer: self)
    }

    /// The async iterator for consuming messages.
    public struct AsyncIterator: AsyncIteratorProtocol {
        private let stream: AsyncThrowingStream<KafkaRecord<T>, Error>
        private var iterator: AsyncThrowingStream<KafkaRecord<T>, Error>.AsyncIterator

        init(consumer: Consumer<T>) {
            self.stream = consumer.messages()
            self.iterator = stream.makeAsyncIterator()
        }

        public mutating func next() async throws -> KafkaRecord<T>? {
            try await iterator.next()
        }
    }
}

// MARK: - TopicConsumer

/// A topic-bound Kafka consumer for receiving messages from a specific topic.
public struct TopicConsumer<T: Decodable & Sendable>: AsyncSequence, Sendable {
    public typealias Element = KafkaRecord<T>

    /// The underlying consumer.
    private let consumer: Consumer<T>

    /// The target topic.
    private let topic: String

    /// Creates a new topic-bound consumer.
    internal init(kafka: Kafka, topic: String, groupId: String, config: ConsumerConfig) {
        self.consumer = Consumer(kafka: kafka, groupId: groupId, config: config, topics: [topic])
        self.topic = topic
    }

    /// Creates an async iterator for the consumer.
    public func makeAsyncIterator() -> AsyncIterator {
        AsyncIterator(consumer: consumer, topic: topic)
    }

    /// The async iterator for consuming messages.
    public struct AsyncIterator: AsyncIteratorProtocol {
        private var innerIterator: Consumer<T>.AsyncIterator?
        private let consumer: Consumer<T>
        private let topic: String
        private var subscribed = false

        init(consumer: Consumer<T>, topic: String) {
            self.consumer = consumer
            self.topic = topic
            self.innerIterator = nil
        }

        public mutating func next() async throws -> KafkaRecord<T>? {
            if !subscribed {
                let subscribedConsumer = try await consumer.subscribe(topics: [topic])
                innerIterator = subscribedConsumer.makeAsyncIterator()
                subscribed = true
            }
            return try await innerIterator?.next()
        }
    }
}

// MARK: - Batch Consumer

/// Configuration for batch consuming.
public struct BatchConsumerConfig: Sendable, Equatable {
    /// The number of records per batch.
    public let batchSize: Int

    /// The maximum time to wait for a full batch.
    public let batchTimeout: Duration

    /// The underlying consumer configuration.
    public let consumerConfig: ConsumerConfig

    /// Creates a new batch consumer configuration.
    public init(
        batchSize: Int = 100,
        batchTimeout: Duration = .seconds(5),
        consumerConfig: ConsumerConfig = .default
    ) {
        self.batchSize = batchSize
        self.batchTimeout = batchTimeout
        self.consumerConfig = consumerConfig
    }
}

/// A batch of Kafka records.
public struct RecordBatch<T: Decodable & Sendable>: Sendable {
    /// The records in the batch.
    public let records: [KafkaRecord<T>]

    /// Commits all records in the batch.
    public func commit() async throws {
        for record in records {
            try await record.commit()
        }
    }
}

extension RecordBatch: Sequence {
    public typealias Element = KafkaRecord<T>

    public func makeIterator() -> Array<KafkaRecord<T>>.Iterator {
        records.makeIterator()
    }
}

/// A batch consumer for processing multiple records at once.
public struct BatchConsumer<T: Decodable & Sendable>: AsyncSequence, Sendable {
    public typealias Element = RecordBatch<T>

    /// The underlying consumer.
    private let consumer: Consumer<T>

    /// The batch configuration.
    private let config: BatchConsumerConfig

    /// Creates a new batch consumer.
    internal init(consumer: Consumer<T>, config: BatchConsumerConfig) {
        self.consumer = consumer
        self.config = config
    }

    /// Creates an async iterator for the batch consumer.
    public func makeAsyncIterator() -> AsyncIterator {
        AsyncIterator(consumer: consumer, config: config)
    }

    /// The async iterator for batch consuming.
    public final class AsyncIterator: AsyncIteratorProtocol, @unchecked Sendable {
        private var stream: AsyncThrowingStream<KafkaRecord<T>, Error>.AsyncIterator
        private let config: BatchConsumerConfig

        init(consumer: Consumer<T>, config: BatchConsumerConfig) {
            self.stream = consumer.messages().makeAsyncIterator()
            self.config = config
        }

        public func next() async throws -> RecordBatch<T>? {
            var batch: [KafkaRecord<T>] = []
            let deadline = ContinuousClock.now + config.batchTimeout

            while batch.count < config.batchSize && ContinuousClock.now < deadline {
                do {
                    // Simple polling approach - try to get next record with a timeout
                    let record = try await stream.next()
                    if let record = record {
                        batch.append(record)
                    } else {
                        // Stream ended
                        break
                    }
                } catch {
                    if batch.isEmpty {
                        throw error
                    }
                    break
                }
            }

            return batch.isEmpty ? nil : RecordBatch(records: batch)
        }
    }
}

// MARK: - Kafka Batch Consumer Extension

extension Kafka {
    /// Creates a batch consumer for processing multiple records at once.
    public func batchConsumer<T: Decodable & Sendable>(
        groupId: String,
        config: BatchConsumerConfig
    ) -> BatchConsumer<T> {
        let consumer: Consumer<T> = Consumer(
            kafka: self,
            groupId: groupId,
            config: config.consumerConfig
        )
        return BatchConsumer(consumer: consumer, config: config)
    }
}

// MARK: - Deserialization Helper

/// Deserializes data to the specified type.
internal func deserializeValue<T: Decodable>(_ data: Data, as type: T.Type) throws -> T {
    // Handle Data type directly
    if type == Data.self {
        return data as! T
    }

    // Handle String type
    if type == String.self {
        guard let string = String(data: data, encoding: .utf8) else {
            throw KafkaError.serializationError("Failed to decode string as UTF-8")
        }
        return string as! T
    }

    // Handle JSON-decodable types
    do {
        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601
        return try decoder.decode(T.self, from: data)
    } catch {
        throw KafkaError.serializationError("Failed to decode value: \(error.localizedDescription)")
    }
}
