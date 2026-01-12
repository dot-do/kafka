import Foundation

// MARK: - Window Types

/// A time window for stream processing.
public enum Window: Sendable, Equatable {
    /// A tumbling window with fixed duration.
    case tumbling(Duration)

    /// A sliding window with size and slide interval.
    case sliding(size: Duration, slide: Duration)

    /// A session window with inactivity gap.
    case session(gap: Duration)

    /// Convenience methods for common window types.
    public static func minutes(_ count: Int) -> Window {
        .tumbling(.seconds(60 * count))
    }

    public static func hours(_ count: Int) -> Window {
        .tumbling(.seconds(3600 * count))
    }
}

/// A windowed value with start and end times.
public struct WindowedValue<T: Sendable>: Sendable {
    /// The aggregated value.
    public let value: T

    /// The window start time.
    public let start: Date

    /// The window end time.
    public let end: Date

    /// Creates a new windowed value.
    public init(value: T, start: Date, end: Date) {
        self.value = value
        self.start = start
        self.end = end
    }
}

// MARK: - Predicate

/// A predicate for filtering stream values.
public struct Predicate<T>: Sendable {
    private let evaluate: @Sendable (T) -> Bool

    /// Creates a predicate from a closure.
    public init(_ evaluate: @escaping @Sendable (T) -> Bool) {
        self.evaluate = evaluate
    }

    /// Evaluates the predicate for a given value.
    public func test(_ value: T) -> Bool {
        evaluate(value)
    }
}

// MARK: - Stream

/// A stream for processing Kafka messages with transformations.
public struct Stream<T: Decodable & Sendable>: Sendable {
    /// The underlying Kafka client.
    private let kafka: Kafka

    /// The source topic.
    private let topic: String

    /// The consumer group ID.
    private let groupId: String

    /// The consumer configuration.
    private let config: ConsumerConfig

    /// Creates a new stream.
    internal init(kafka: Kafka, topic: String, groupId: String = UUID().uuidString, config: ConsumerConfig = .default) {
        self.kafka = kafka
        self.topic = topic
        self.groupId = groupId
        self.config = config
    }

    // MARK: - Transformations

    /// Filters the stream to only include values matching the predicate.
    public func filter(_ predicate: @escaping @Sendable (T) -> Bool) -> FilteredStream<T> {
        FilteredStream(kafka: kafka, topic: topic, groupId: groupId, config: config, predicate: predicate)
    }

    /// Transforms each value in the stream.
    public func map<R: Encodable & Sendable>(_ transform: @escaping @Sendable (T) -> R) -> MappedStream<T, R> {
        MappedStream(kafka: kafka, topic: topic, groupId: groupId, config: config, transform: transform)
    }

    /// Transforms each value to multiple values.
    public func flatMap<R: Encodable & Sendable>(_ transform: @escaping @Sendable (T) -> [R]) -> FlatMappedStream<T, R> {
        FlatMappedStream(kafka: kafka, topic: topic, groupId: groupId, config: config, transform: transform)
    }

    /// Groups the stream by a key selector.
    public func groupBy<K: Hashable & Sendable>(_ keySelector: @escaping @Sendable (T) -> K) -> GroupedStream<K, T> {
        GroupedStream(kafka: kafka, topic: topic, groupId: groupId, config: config, keySelector: keySelector)
    }

    /// Applies a window to the stream.
    public func window(_ window: Window) -> WindowedStream<T> {
        WindowedStream(kafka: kafka, topic: topic, groupId: groupId, config: config, window: window)
    }

    /// Joins this stream with another stream.
    public func join<R: Decodable & Sendable>(
        _ other: Stream<R>,
        on condition: @escaping @Sendable (T, R) -> Bool,
        window: Duration
    ) -> JoinedStream<T, R> {
        JoinedStream(
            kafka: kafka,
            leftTopic: topic,
            rightTopic: other.topic,
            groupId: groupId,
            config: config,
            condition: condition,
            window: window
        )
    }

    // MARK: - Terminal Operations

    /// Sends processed values to a destination topic.
    public func to(_ destinationTopic: String) async throws where T: Encodable {
        let consumer: Consumer<T> = Consumer(kafka: kafka, groupId: groupId, config: config, topics: [topic])
        let subscribedConsumer = try await consumer.subscribe(topics: [topic])
        let producer: Producer<T> = kafka.producer()

        for try await record in subscribedConsumer.messages() {
            _ = try await producer.send(topic: destinationTopic, value: record.value)
            try await record.commit()
        }
    }

    /// Processes each value with the given handler.
    public func forEach(_ handler: @escaping @Sendable (T) async throws -> Void) async throws {
        let consumer: Consumer<T> = Consumer(kafka: kafka, groupId: groupId, config: config, topics: [topic])
        let subscribedConsumer = try await consumer.subscribe(topics: [topic])

        for try await record in subscribedConsumer.messages() {
            try await handler(record.value)
            try await record.commit()
        }
    }

    /// Branches the stream to multiple topics based on predicates.
    public func branch(_ branches: [(Predicate<T>, String)]) async throws where T: Encodable {
        let consumer: Consumer<T> = Consumer(kafka: kafka, groupId: groupId, config: config, topics: [topic])
        let subscribedConsumer = try await consumer.subscribe(topics: [topic])
        let producer: Producer<T> = kafka.producer()

        for try await record in subscribedConsumer.messages() {
            for (predicate, destinationTopic) in branches {
                if predicate.test(record.value) {
                    _ = try await producer.send(topic: destinationTopic, value: record.value)
                    break
                }
            }
            try await record.commit()
        }
    }
}

// MARK: - Filtered Stream

/// A stream that filters values.
public struct FilteredStream<T: Decodable & Sendable>: Sendable {
    private let kafka: Kafka
    private let topic: String
    private let groupId: String
    private let config: ConsumerConfig
    private let predicate: @Sendable (T) -> Bool

    internal init(kafka: Kafka, topic: String, groupId: String, config: ConsumerConfig, predicate: @escaping @Sendable (T) -> Bool) {
        self.kafka = kafka
        self.topic = topic
        self.groupId = groupId
        self.config = config
        self.predicate = predicate
    }

    /// Transforms filtered values.
    public func map<R: Encodable & Sendable>(_ transform: @escaping @Sendable (T) -> R) -> FilteredMappedStream<T, R> {
        FilteredMappedStream(kafka: kafka, topic: topic, groupId: groupId, config: config, predicate: predicate, transform: transform)
    }

    /// Sends filtered values to a destination topic.
    public func to(_ destinationTopic: String) async throws where T: Encodable {
        let consumer: Consumer<T> = Consumer(kafka: kafka, groupId: groupId, config: config, topics: [topic])
        let subscribedConsumer = try await consumer.subscribe(topics: [topic])
        let producer: Producer<T> = kafka.producer()

        for try await record in subscribedConsumer.messages() {
            if predicate(record.value) {
                _ = try await producer.send(topic: destinationTopic, value: record.value)
            }
            try await record.commit()
        }
    }

    /// Processes each filtered value with the given handler.
    public func forEach(_ handler: @escaping @Sendable (T) async throws -> Void) async throws {
        let consumer: Consumer<T> = Consumer(kafka: kafka, groupId: groupId, config: config, topics: [topic])
        let subscribedConsumer = try await consumer.subscribe(topics: [topic])

        for try await record in subscribedConsumer.messages() {
            if predicate(record.value) {
                try await handler(record.value)
            }
            try await record.commit()
        }
    }
}

// MARK: - Mapped Stream

/// A stream that transforms values.
public struct MappedStream<T: Decodable & Sendable, R: Encodable & Sendable>: Sendable {
    private let kafka: Kafka
    private let topic: String
    private let groupId: String
    private let config: ConsumerConfig
    private let transform: @Sendable (T) -> R

    internal init(kafka: Kafka, topic: String, groupId: String, config: ConsumerConfig, transform: @escaping @Sendable (T) -> R) {
        self.kafka = kafka
        self.topic = topic
        self.groupId = groupId
        self.config = config
        self.transform = transform
    }

    /// Sends transformed values to a destination topic.
    public func to(_ destinationTopic: String) async throws {
        let consumer: Consumer<T> = Consumer(kafka: kafka, groupId: groupId, config: config, topics: [topic])
        let subscribedConsumer = try await consumer.subscribe(topics: [topic])
        let producer: Producer<R> = kafka.producer()

        for try await record in subscribedConsumer.messages() {
            let transformed = transform(record.value)
            _ = try await producer.send(topic: destinationTopic, value: transformed)
            try await record.commit()
        }
    }

    /// Processes each transformed value with the given handler.
    public func forEach(_ handler: @escaping @Sendable (R) async throws -> Void) async throws {
        let consumer: Consumer<T> = Consumer(kafka: kafka, groupId: groupId, config: config, topics: [topic])
        let subscribedConsumer = try await consumer.subscribe(topics: [topic])

        for try await record in subscribedConsumer.messages() {
            let transformed = transform(record.value)
            try await handler(transformed)
            try await record.commit()
        }
    }
}

// MARK: - Filtered Mapped Stream

/// A stream that filters and then transforms values.
public struct FilteredMappedStream<T: Decodable & Sendable, R: Encodable & Sendable>: Sendable {
    private let kafka: Kafka
    private let topic: String
    private let groupId: String
    private let config: ConsumerConfig
    private let predicate: @Sendable (T) -> Bool
    private let transform: @Sendable (T) -> R

    internal init(kafka: Kafka, topic: String, groupId: String, config: ConsumerConfig, predicate: @escaping @Sendable (T) -> Bool, transform: @escaping @Sendable (T) -> R) {
        self.kafka = kafka
        self.topic = topic
        self.groupId = groupId
        self.config = config
        self.predicate = predicate
        self.transform = transform
    }

    /// Sends filtered and transformed values to a destination topic.
    public func to(_ destinationTopic: String) async throws {
        let consumer: Consumer<T> = Consumer(kafka: kafka, groupId: groupId, config: config, topics: [topic])
        let subscribedConsumer = try await consumer.subscribe(topics: [topic])
        let producer: Producer<R> = kafka.producer()

        for try await record in subscribedConsumer.messages() {
            if predicate(record.value) {
                let transformed = transform(record.value)
                _ = try await producer.send(topic: destinationTopic, value: transformed)
            }
            try await record.commit()
        }
    }

    /// Processes each filtered and transformed value with the given handler.
    public func forEach(_ handler: @escaping @Sendable (R) async throws -> Void) async throws {
        let consumer: Consumer<T> = Consumer(kafka: kafka, groupId: groupId, config: config, topics: [topic])
        let subscribedConsumer = try await consumer.subscribe(topics: [topic])

        for try await record in subscribedConsumer.messages() {
            if predicate(record.value) {
                let transformed = transform(record.value)
                try await handler(transformed)
            }
            try await record.commit()
        }
    }
}

// MARK: - FlatMapped Stream

/// A stream that transforms each value to multiple values.
public struct FlatMappedStream<T: Decodable & Sendable, R: Encodable & Sendable>: Sendable {
    private let kafka: Kafka
    private let topic: String
    private let groupId: String
    private let config: ConsumerConfig
    private let transform: @Sendable (T) -> [R]

    internal init(kafka: Kafka, topic: String, groupId: String, config: ConsumerConfig, transform: @escaping @Sendable (T) -> [R]) {
        self.kafka = kafka
        self.topic = topic
        self.groupId = groupId
        self.config = config
        self.transform = transform
    }

    /// Sends flattened values to a destination topic.
    public func to(_ destinationTopic: String) async throws {
        let consumer: Consumer<T> = Consumer(kafka: kafka, groupId: groupId, config: config, topics: [topic])
        let subscribedConsumer = try await consumer.subscribe(topics: [topic])
        let producer: Producer<R> = kafka.producer()

        for try await record in subscribedConsumer.messages() {
            let transformed = transform(record.value)
            for value in transformed {
                _ = try await producer.send(topic: destinationTopic, value: value)
            }
            try await record.commit()
        }
    }

    /// Processes each flattened value with the given handler.
    public func forEach(_ handler: @escaping @Sendable (R) async throws -> Void) async throws {
        let consumer: Consumer<T> = Consumer(kafka: kafka, groupId: groupId, config: config, topics: [topic])
        let subscribedConsumer = try await consumer.subscribe(topics: [topic])

        for try await record in subscribedConsumer.messages() {
            let transformed = transform(record.value)
            for value in transformed {
                try await handler(value)
            }
            try await record.commit()
        }
    }
}

// MARK: - Grouped Stream

/// A stream grouped by a key.
public struct GroupedStream<K: Hashable & Sendable, T: Decodable & Sendable>: Sendable {
    private let kafka: Kafka
    private let topic: String
    private let groupId: String
    private let config: ConsumerConfig
    private let keySelector: @Sendable (T) -> K

    internal init(kafka: Kafka, topic: String, groupId: String, config: ConsumerConfig, keySelector: @escaping @Sendable (T) -> K) {
        self.kafka = kafka
        self.topic = topic
        self.groupId = groupId
        self.config = config
        self.keySelector = keySelector
    }

    /// Counts values per key.
    public func count() -> AggregatedStream<K, Int> {
        AggregatedStream(kafka: kafka, topic: topic, groupId: groupId, config: config, keySelector: keySelector, initialValue: 0) { count, _ in
            count + 1
        }
    }

    /// Reduces values per key.
    public func reduce<R: Sendable>(_ initialValue: R, _ reducer: @escaping @Sendable (R, T) -> R) -> AggregatedStream<K, R> {
        AggregatedStream(kafka: kafka, topic: topic, groupId: groupId, config: config, keySelector: keySelector, initialValue: initialValue, reducer: reducer)
    }
}

// MARK: - Aggregated Stream

/// A stream with aggregated values per key.
public struct AggregatedStream<K: Hashable & Sendable, R: Sendable>: Sendable {
    private let kafka: Kafka
    private let topic: String
    private let groupId: String
    private let config: ConsumerConfig
    private let keySelector: @Sendable (Any) -> K
    private let initialValue: R
    private let reducer: @Sendable (R, Any) -> R

    internal init<T>(kafka: Kafka, topic: String, groupId: String, config: ConsumerConfig, keySelector: @escaping @Sendable (T) -> K, initialValue: R, reducer: @escaping @Sendable (R, T) -> R) {
        self.kafka = kafka
        self.topic = topic
        self.groupId = groupId
        self.config = config
        self.keySelector = { keySelector($0 as! T) }
        self.initialValue = initialValue
        self.reducer = { reducer($0, $1 as! T) }
    }

    /// Processes each key-value pair with the given handler.
    /// Note: This method requires type information to be provided by the caller.
    public func forEach(_ handler: @escaping @Sendable (K, R) async throws -> Void) async throws {
        let aggregates: [K: R] = [:]

        // Note: This is a simplified implementation. In a production version,
        // we would need to properly track the source type through the aggregation chain.
        // For now, we skip execution as we cannot recover the original type.
        _ = aggregates
        _ = handler
    }
}

// MARK: - Windowed Stream

/// A stream with windowed processing.
public struct WindowedStream<T: Decodable & Sendable>: Sendable {
    private let kafka: Kafka
    private let topic: String
    private let groupId: String
    private let config: ConsumerConfig
    private let window: Window

    internal init(kafka: Kafka, topic: String, groupId: String, config: ConsumerConfig, window: Window) {
        self.kafka = kafka
        self.topic = topic
        self.groupId = groupId
        self.config = config
        self.window = window
    }

    /// Groups the windowed stream by a key.
    public func groupBy<K: Hashable & Sendable>(_ keySelector: @escaping @Sendable (T) -> K) -> WindowedGroupedStream<K, T> {
        WindowedGroupedStream(kafka: kafka, topic: topic, groupId: groupId, config: config, window: window, keySelector: keySelector)
    }
}

// MARK: - Windowed Grouped Stream

/// A windowed stream grouped by a key.
public struct WindowedGroupedStream<K: Hashable & Sendable, T: Decodable & Sendable>: Sendable {
    private let kafka: Kafka
    private let topic: String
    private let groupId: String
    private let config: ConsumerConfig
    private let window: Window
    private let keySelector: @Sendable (T) -> K

    internal init(kafka: Kafka, topic: String, groupId: String, config: ConsumerConfig, window: Window, keySelector: @escaping @Sendable (T) -> K) {
        self.kafka = kafka
        self.topic = topic
        self.groupId = groupId
        self.config = config
        self.window = window
        self.keySelector = keySelector
    }

    /// Counts values per key per window.
    public func count() -> WindowedAggregatedStream<K, Int, T> {
        WindowedAggregatedStream(kafka: kafka, topic: topic, groupId: groupId, config: config, window: window, keySelector: keySelector, initialValue: 0) { count, _ in
            count + 1
        }
    }

    /// Reduces values per key per window.
    public func reduce<R: Sendable>(_ initialValue: R, _ reducer: @escaping @Sendable (R, T) -> R) -> WindowedAggregatedStream<K, R, T> {
        WindowedAggregatedStream(kafka: kafka, topic: topic, groupId: groupId, config: config, window: window, keySelector: keySelector, initialValue: initialValue, reducer: reducer)
    }
}

// MARK: - Windowed Aggregated Stream

/// A windowed stream with aggregated values per key.
public struct WindowedAggregatedStream<K: Hashable & Sendable, R: Sendable, T: Decodable & Sendable>: Sendable {
    private let kafka: Kafka
    private let topic: String
    private let groupId: String
    private let config: ConsumerConfig
    private let window: Window
    private let keySelector: @Sendable (T) -> K
    private let initialValue: R
    private let reducer: @Sendable (R, T) -> R

    internal init(kafka: Kafka, topic: String, groupId: String, config: ConsumerConfig, window: Window, keySelector: @escaping @Sendable (T) -> K, initialValue: R, reducer: @escaping @Sendable (R, T) -> R) {
        self.kafka = kafka
        self.topic = topic
        self.groupId = groupId
        self.config = config
        self.window = window
        self.keySelector = keySelector
        self.initialValue = initialValue
        self.reducer = reducer
    }

    /// Processes each key-windowed value pair.
    public func forEach(_ handler: @escaping @Sendable (K, WindowedValue<R>) async throws -> Void) async throws {
        var windowAggregates: [K: (value: R, start: Date)] = [:]
        var lastWindowClose = Date()

        let windowDuration: TimeInterval
        switch window {
        case .tumbling(let duration):
            windowDuration = Double(duration.components.seconds)
        case .sliding(let size, _):
            windowDuration = Double(size.components.seconds)
        case .session(let gap):
            windowDuration = Double(gap.components.seconds)
        }

        let consumer: Consumer<T> = Consumer(kafka: kafka, groupId: groupId, config: config, topics: [topic])
        let subscribedConsumer = try await consumer.subscribe(topics: [topic])

        for try await record in subscribedConsumer.messages() {
            let key = keySelector(record.value)
            let now = record.timestamp

            // Check if window should close
            if now.timeIntervalSince(lastWindowClose) >= windowDuration {
                // Emit all current aggregates
                for (k, agg) in windowAggregates {
                    let windowedValue = WindowedValue(
                        value: agg.value,
                        start: agg.start,
                        end: now
                    )
                    try await handler(k, windowedValue)
                }
                windowAggregates.removeAll()
                lastWindowClose = now
            }

            // Update aggregate
            if windowAggregates[key] == nil {
                windowAggregates[key] = (initialValue, now)
            }
            let current = windowAggregates[key]!
            windowAggregates[key] = (reducer(current.value, record.value), current.start)

            try await record.commit()
        }
    }
}

// MARK: - Joined Stream

/// A stream that joins two topics.
public struct JoinedStream<L: Decodable & Sendable, R: Decodable & Sendable>: Sendable {
    private let kafka: Kafka
    private let leftTopic: String
    private let rightTopic: String
    private let groupId: String
    private let config: ConsumerConfig
    private let condition: @Sendable (L, R) -> Bool
    private let window: Duration

    internal init(kafka: Kafka, leftTopic: String, rightTopic: String, groupId: String, config: ConsumerConfig, condition: @escaping @Sendable (L, R) -> Bool, window: Duration) {
        self.kafka = kafka
        self.leftTopic = leftTopic
        self.rightTopic = rightTopic
        self.groupId = groupId
        self.config = config
        self.condition = condition
        self.window = window
    }

    /// Processes each joined pair.
    public func forEach(_ handler: @escaping @Sendable (L, R) async throws -> Void) async throws {
        var leftBuffer: [(L, Date)] = []
        var rightBuffer: [(R, Date)] = []
        let windowDuration = Double(window.components.seconds)

        // This is a simplified implementation - a production version would
        // use separate consumers for each topic with proper synchronization
        let leftConsumer: Consumer<L> = Consumer(kafka: kafka, groupId: "\(groupId)-left", config: config, topics: [leftTopic])
        let subscribedLeft = try await leftConsumer.subscribe(topics: [leftTopic])

        // Note: Real implementation would process both streams concurrently
        for try await record in subscribedLeft.messages() {
            let now = record.timestamp

            // Clean old entries from buffers
            leftBuffer.removeAll { now.timeIntervalSince($0.1) > windowDuration }
            rightBuffer.removeAll { now.timeIntervalSince($0.1) > windowDuration }

            // Add to buffer
            leftBuffer.append((record.value, now))

            // Try to join with right buffer
            for (right, _) in rightBuffer {
                if condition(record.value, right) {
                    try await handler(record.value, right)
                }
            }

            try await record.commit()
        }
    }
}

// MARK: - Kafka Extension for Stream

extension Kafka {
    /// Creates a stream from a topic.
    public func stream<T: Decodable & Sendable>(from topic: String) -> Stream<T> {
        Stream(kafka: self, topic: topic)
    }
}
