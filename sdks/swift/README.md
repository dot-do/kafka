# KafkaDo

> Event Streaming for Swift. Async/Await. Actors. Zero Ops.

```swift
let producer = kafka.producer(for: "orders")
try await producer.send(Order(id: "123", amount: 99.99))
```

Structured concurrency. Type safety. The Swift way.

## Installation

### Swift Package Manager

```swift
// Package.swift
dependencies: [
    .package(url: "https://github.com/dot-do/kafka-swift.git", from: "0.1.0")
]
```

Requires Swift 5.9+ and iOS 17+ / macOS 14+.

## Quick Start

```swift
import KafkaDo

struct Order: Codable, Sendable {
    let orderId: String
    let amount: Double
}

@main
struct App {
    static func main() async throws {
        let kafka = KafkaClient()

        // Produce
        let producer = kafka.producer(for: "orders")
        try await producer.send(Order(orderId: "123", amount: 99.99))

        // Consume
        for try await record in kafka.consumer(for: "orders", group: "my-processor") {
            print("Received: \(record.value)")
            try await record.commit()
        }
    }
}
```

## Producing Messages

### Simple Producer

```swift
let kafka = KafkaClient()
let producer = kafka.producer(for: "orders")

// Send single message
try await producer.send(Order(orderId: "123", amount: 99.99))

// Send with key for partitioning
try await producer.send(
    Order(orderId: "123", amount: 99.99),
    key: "customer-456"
)

// Send with headers
try await producer.send(
    Order(orderId: "123", amount: 99.99),
    key: "customer-456",
    headers: ["correlation-id": "abc-123"]
)
```

### Batch Producer

```swift
let producer = kafka.producer(for: "orders")

// Send batch
try await producer.sendBatch([
    Order(orderId: "124", amount: 49.99),
    Order(orderId: "125", amount: 149.99),
    Order(orderId: "126", amount: 29.99)
])

// Send batch with keys
try await producer.sendBatch([
    Message(key: "cust-1", value: Order(orderId: "124", amount: 49.99)),
    Message(key: "cust-2", value: Order(orderId: "125", amount: 149.99))
])
```

### Fire-and-Forget Producer

```swift
let producer = kafka.producer(for: "orders")

// Non-blocking send (returns immediately)
Task {
    do {
        try await producer.send(order)
    } catch {
        print("Failed to send: \(error)")
    }
}
```

### Transactional Producer

```swift
try await kafka.transaction(topic: "orders") { tx in
    try await tx.send(Order(orderId: "123", status: .created))
    try await tx.send(Order(orderId: "123", status: .validated))
    // Automatically commits on success, aborts on throw
}
```

## Consuming Messages

### Basic Consumer with AsyncSequence

```swift
let consumer = kafka.consumer(for: "orders", group: "order-processor")

for try await record in consumer {
    print("Topic: \(record.topic)")
    print("Partition: \(record.partition)")
    print("Offset: \(record.offset)")
    print("Key: \(record.key ?? "nil")")
    print("Value: \(record.value)")
    print("Timestamp: \(record.timestamp)")
    print("Headers: \(record.headers)")

    try await processOrder(record.value)
    try await record.commit()
}
```

### Typed Consumer

```swift
let consumer: KafkaConsumer<Order> = kafka.consumer(for: "orders", group: "processor")

for try await record in consumer {
    let order: Order = record.value  // Already decoded
    print("Order \(order.orderId): $\(order.amount)")
    try await record.commit()
}
```

### Consumer with Filter and Map

```swift
let consumer = kafka.consumer(for: "orders", group: "processor")

// Using AsyncSequence operators
for try await order in consumer
    .filter { $0.value.amount > 100 }
    .map(\.value) {
    try await processHighValueOrder(order)
}
```

### Consumer with Configuration

```swift
let config = ConsumerConfig(
    offset: .earliest,
    autoCommit: true,
    maxPollRecords: 100,
    sessionTimeout: .seconds(30)
)

let consumer = kafka.consumer(for: "orders", group: "processor", config: config)

for try await record in consumer {
    try await processOrder(record.value)
    // Auto-committed
}
```

### Consumer from Timestamp

```swift
let yesterday = Date().addingTimeInterval(-86400)

let config = ConsumerConfig(offset: .timestamp(yesterday))
let consumer = kafka.consumer(for: "orders", group: "replay", config: config)

for try await record in consumer {
    print("Replaying: \(record.value)")
}
```

### Batch Consumer

```swift
let config = BatchConsumerConfig(
    batchSize: 100,
    batchTimeout: .seconds(5)
)

let consumer = kafka.batchConsumer(for: "orders", group: "batch-processor", config: config)

for try await batch in consumer {
    for record in batch {
        try await processOrder(record.value)
    }
    try await batch.commit()
}
```

### Parallel Consumer with TaskGroup

```swift
let consumer = kafka.consumer(for: "orders", group: "parallel-processor")

try await withThrowingTaskGroup(of: Void.self) { group in
    for try await record in consumer {
        group.addTask {
            try await processOrder(record.value)
            try await record.commit()
        }

        // Limit concurrency
        if group.count >= 10 {
            try await group.next()
        }
    }
}
```

### Consumer with Actor Isolation

```swift
actor OrderProcessor {
    private var processedCount = 0

    func process(_ order: Order) async throws {
        // Process order...
        processedCount += 1
    }

    var count: Int { processedCount }
}

let processor = OrderProcessor()
let consumer = kafka.consumer(for: "orders", group: "actor-processor")

for try await record in consumer {
    try await processor.process(record.value)
    try await record.commit()
}
```

## Stream Processing

### Filter and Transform

```swift
try await kafka.stream(from: "orders")
    .filter { $0.amount > 100 }
    .map { order -> PremiumOrder in
        PremiumOrder(order: order, tier: "premium")
    }
    .to("high-value-orders")
```

### Windowed Aggregations

```swift
try await kafka.stream(from: "orders")
    .window(.tumbling(.minutes(5)))
    .groupBy(\.customerId)
    .count()
    .forEach { key, window in
        print("Customer \(key): \(window.value) orders in \(window.start)-\(window.end)")
    }
```

### Joins

```swift
let orders = kafka.stream(from: "orders")
let customers = kafka.stream(from: "customers")

try await orders
    .join(customers,
          on: { $0.customerId == $1.id },
          window: .hours(1))
    .forEach { order, customer in
        print("Order by \(customer.name)")
    }
```

### Branching

```swift
try await kafka.stream(from: "orders")
    .branch([
        (.init { $0.region == "us" }, "us-orders"),
        (.init { $0.region == "eu" }, "eu-orders"),
        (.init { _ in true }, "other-orders")
    ])
```

### Aggregations

```swift
try await kafka.stream(from: "orders")
    .groupBy(\.customerId)
    .reduce(0.0) { total, order in total + order.amount }
    .forEach { customerId, total in
        print("Customer \(customerId) total: $\(total)")
    }
```

## Topic Administration

```swift
let admin = kafka.admin

// Create topic
try await admin.createTopic("orders", config: TopicConfig(
    partitions: 3,
    retentionMs: 7 * 24 * 60 * 60 * 1000 // 7 days
))

// List topics
for topic in try await admin.listTopics() {
    print("\(topic.name): \(topic.partitions) partitions")
}

// Describe topic
let info = try await admin.describeTopic("orders")
print("Partitions: \(info.partitions)")
print("Retention: \(info.retentionMs)ms")

// Alter topic
try await admin.alterTopic("orders", config: TopicConfig(
    retentionMs: 30 * 24 * 60 * 60 * 1000 // 30 days
))

// Delete topic
try await admin.deleteTopic("old-events")
```

## Consumer Groups

```swift
let admin = kafka.admin

// List groups
for group in try await admin.listGroups() {
    print("Group: \(group.id), Members: \(group.memberCount)")
}

// Describe group
let info = try await admin.describeGroup("order-processor")
print("State: \(info.state)")
print("Members: \(info.members.count)")
print("Total Lag: \(info.totalLag)")

// Reset offsets
try await admin.resetOffsets("order-processor", topic: "orders", to: .earliest)

// Reset to timestamp
try await admin.resetOffsets("order-processor", topic: "orders", to: .timestamp(yesterday))
```

## Error Handling

```swift
do {
    let producer = kafka.producer(for: "orders")
    try await producer.send(order)
} catch KafkaError.topicNotFound {
    print("Topic not found")
} catch KafkaError.messageTooLarge {
    print("Message too large")
} catch KafkaError.timeout {
    print("Request timed out")
} catch let error as KafkaError {
    print("Kafka error: \(error.code) - \(error.message)")

    if error.isRetriable {
        // Safe to retry
    }
} catch {
    print("Unexpected error: \(error)")
}
```

### Error Types

```swift
enum KafkaError: Error {
    case topicNotFound(String)
    case partitionNotFound(topic: String, partition: Int)
    case messageTooLarge(size: Int, maxSize: Int)
    case notLeader(topic: String, partition: Int)
    case offsetOutOfRange(offset: Int64, topic: String)
    case groupCoordinator(String)
    case rebalanceInProgress
    case unauthorized(String)
    case quotaExceeded
    case timeout
    case disconnected
    case serialization(Error)

    var isRetriable: Bool { ... }
}
```

### Dead Letter Queue Pattern

```swift
let consumer = kafka.consumer(for: "orders", group: "processor")
let dlqProducer = kafka.producer(for: "orders-dlq")

for try await record in consumer {
    do {
        try await processOrder(record.value)
    } catch {
        try await dlqProducer.send(
            DLQRecord(
                originalRecord: record.value,
                error: error.localizedDescription,
                timestamp: Date()
            ),
            headers: [
                "original-topic": record.topic,
                "original-partition": String(record.partition),
                "original-offset": String(record.offset)
            ]
        )
    }
    try await record.commit()
}
```

### Retry with Exponential Backoff

```swift
func withRetry<T>(
    maxAttempts: Int = 3,
    baseDelay: Duration = .seconds(1),
    operation: () async throws -> T
) async throws -> T {
    var attempt = 0
    while true {
        do {
            attempt += 1
            return try await operation()
        } catch let error as KafkaError where error.isRetriable && attempt < maxAttempts {
            let delay = baseDelay * Double(1 << (attempt - 1))
            try await Task.sleep(for: delay)
        }
    }
}

let producer = kafka.producer(for: "orders")
try await withRetry {
    try await producer.send(order)
}
```

## Configuration

### Environment Variables

```bash
export KAFKA_DO_URL=https://kafka.do
export KAFKA_DO_API_KEY=your-api-key
```

### Client Configuration

```swift
let config = KafkaConfig(
    url: "https://kafka.do",
    apiKey: "your-api-key",
    timeout: .seconds(30),
    retries: 3
)

let kafka = KafkaClient(config: config)
```

### Producer Configuration

```swift
let config = ProducerConfig(
    batchSize: 16384,
    lingerMs: 5,
    compression: .gzip,
    acks: .all,
    retries: 3,
    retryBackoffMs: 100
)

let producer = kafka.producer(for: "orders", config: config)
```

### Consumer Configuration

```swift
let config = ConsumerConfig(
    offset: .latest,
    autoCommit: false,
    fetchMinBytes: 1,
    fetchMaxWaitMs: 500,
    maxPollRecords: 500,
    sessionTimeout: .seconds(30),
    heartbeatInterval: .seconds(3)
)

let consumer = kafka.consumer(for: "orders", group: "processor", config: config)
```

## SwiftUI Integration

### ObservableObject

```swift
@MainActor
class OrderViewModel: ObservableObject {
    @Published var orders: [Order] = []
    @Published var error: Error?

    private let kafka = KafkaClient()
    private var consumeTask: Task<Void, Never>?

    func startConsuming() {
        consumeTask = Task {
            do {
                for try await record in kafka.consumer(for: "orders", group: "ui") {
                    orders.append(record.value)
                    try await record.commit()
                }
            } catch {
                self.error = error
            }
        }
    }

    func stopConsuming() {
        consumeTask?.cancel()
    }

    func sendOrder(_ order: Order) async {
        do {
            let producer = kafka.producer(for: "orders")
            try await producer.send(order)
        } catch {
            self.error = error
        }
    }
}

struct OrdersView: View {
    @StateObject private var viewModel = OrderViewModel()

    var body: some View {
        List(viewModel.orders, id: \.orderId) { order in
            Text("Order \(order.orderId): $\(order.amount)")
        }
        .onAppear { viewModel.startConsuming() }
        .onDisappear { viewModel.stopConsuming() }
    }
}
```

## Testing

### Mock Client

```swift
import XCTest
@testable import KafkaDo

final class OrderProcessorTests: XCTestCase {
    func testProcessesAllOrders() async throws {
        let kafka = MockKafkaClient()

        // Seed test data
        kafka.seed("orders", with: [
            Order(orderId: "123", amount: 99.99),
            Order(orderId: "124", amount: 149.99)
        ])

        // Process
        var processed: [Order] = []
        for try await record in kafka.consumer(for: "orders", group: "test").prefix(2) {
            processed.append(record.value)
            try await record.commit()
        }

        XCTAssertEqual(processed.count, 2)
        XCTAssertEqual(processed[0].orderId, "123")
    }

    func testSendsMessages() async throws {
        let kafka = MockKafkaClient()
        let producer = kafka.producer(for: "orders")

        try await producer.send(Order(orderId: "125", amount: 99.99))

        let messages = kafka.getMessages("orders", as: Order.self)
        XCTAssertEqual(messages.count, 1)
        XCTAssertEqual(messages[0].orderId, "125")
    }
}
```

### Integration Testing

```swift
final class IntegrationTests: XCTestCase {
    var kafka: KafkaClient!
    var topic: String!

    override func setUp() async throws {
        kafka = KafkaClient(config: KafkaConfig(
            url: ProcessInfo.processInfo.environment["TEST_KAFKA_URL"]!,
            apiKey: ProcessInfo.processInfo.environment["TEST_KAFKA_API_KEY"]!
        ))
        topic = "test-orders-\(UUID())"
    }

    override func tearDown() async throws {
        try? await kafka.admin.deleteTopic(topic)
    }

    func testEndToEnd() async throws {
        // Produce
        let producer = kafka.producer(for: topic)
        try await producer.send(Order(orderId: "123", amount: 99.99))

        // Consume
        for try await record in kafka.consumer(for: topic, group: "test").prefix(1) {
            XCTAssertEqual(record.value.orderId, "123")
        }
    }
}
```

## API Reference

### KafkaClient

```swift
public actor KafkaClient {
    public init(config: KafkaConfig = .default)

    public func producer<T: Encodable & Sendable>(for topic: String, config: ProducerConfig = .default) -> Producer<T>
    public func consumer<T: Decodable & Sendable>(for topic: String, group: String, config: ConsumerConfig = .default) -> Consumer<T>
    public func batchConsumer<T: Decodable & Sendable>(for topic: String, group: String, config: BatchConsumerConfig) -> BatchConsumer<T>
    public func transaction<T: Encodable & Sendable>(topic: String, operation: (Transaction<T>) async throws -> Void) async throws
    public func stream<T: Decodable & Sendable>(from topic: String) -> Stream<T>
    public var admin: Admin { get }
}
```

### Producer

```swift
public struct Producer<T: Encodable & Sendable>: Sendable {
    public func send(_ value: T, key: String? = nil, headers: [String: String] = [:]) async throws -> RecordMetadata
    public func sendBatch(_ values: [T]) async throws -> [RecordMetadata]
    public func sendBatch(_ messages: [Message<T>]) async throws -> [RecordMetadata]
}
```

### Consumer

```swift
public struct Consumer<T: Decodable & Sendable>: AsyncSequence, Sendable {
    public typealias Element = KafkaRecord<T>
    public func makeAsyncIterator() -> AsyncIterator
}
```

### KafkaRecord

```swift
public struct KafkaRecord<T: Decodable & Sendable>: Sendable {
    public let topic: String
    public let partition: Int
    public let offset: Int64
    public let key: String?
    public let value: T
    public let timestamp: Date
    public let headers: [String: String]

    public func commit() async throws
}
```

### Stream

```swift
public struct Stream<T: Decodable & Sendable>: Sendable {
    public func filter(_ predicate: @escaping @Sendable (T) -> Bool) -> Stream<T>
    public func map<R: Encodable & Sendable>(_ transform: @escaping @Sendable (T) -> R) -> Stream<R>
    public func flatMap<R: Encodable & Sendable>(_ transform: @escaping @Sendable (T) -> [R]) -> Stream<R>
    public func window(_ window: Window) -> WindowedStream<T>
    public func groupBy<K: Hashable & Sendable>(_ keySelector: @escaping @Sendable (T) -> K) -> GroupedStream<K, T>
    public func branch(_ branches: [(Predicate<T>, String)]) async throws
    public func join<R: Decodable & Sendable>(_ other: Stream<R>, on: @escaping @Sendable (T, R) -> Bool, window: Duration) -> JoinedStream<T, R>
    public func to(_ topic: String) async throws
    public func forEach(_ handler: @escaping @Sendable (T) async throws -> Void) async throws
}
```

## License

MIT
