import Foundation

// MARK: - KafkaConfig

/// Configuration for the Kafka client.
public struct KafkaConfig: Sendable, Equatable {
    /// The URL of the Kafka.do service.
    public let url: String

    /// The API key for authentication.
    public let apiKey: String

    /// The request timeout.
    public let timeout: Duration

    /// The number of retries for failed requests.
    public let retries: Int

    /// Default configuration using environment variables.
    public static let `default` = KafkaConfig(
        url: ProcessInfo.processInfo.environment["KAFKA_DO_URL"] ?? "https://kafka.do",
        apiKey: ProcessInfo.processInfo.environment["KAFKA_DO_API_KEY"] ?? "",
        timeout: .seconds(30),
        retries: 3
    )

    /// Creates a new Kafka configuration.
    public init(url: String = "https://kafka.do", apiKey: String = "", timeout: Duration = .seconds(30), retries: Int = 3) {
        self.url = url
        self.apiKey = apiKey
        self.timeout = timeout
        self.retries = retries
    }
}

// MARK: - ProducerConfig

/// Configuration for a Kafka producer.
public struct ProducerConfig: Sendable, Equatable {
    /// The batch size in bytes.
    public let batchSize: Int

    /// The time to wait before sending a batch.
    public let lingerMs: Int

    /// The compression type.
    public let compression: CompressionType

    /// The acknowledgment mode.
    public let acks: Acks

    /// The number of retries for failed sends.
    public let retries: Int

    /// The backoff time between retries in milliseconds.
    public let retryBackoffMs: Int

    /// Default configuration.
    public static let `default` = ProducerConfig()

    /// Creates a new producer configuration.
    public init(
        batchSize: Int = 16384,
        lingerMs: Int = 5,
        compression: CompressionType = .none,
        acks: Acks = .all,
        retries: Int = 3,
        retryBackoffMs: Int = 100
    ) {
        self.batchSize = batchSize
        self.lingerMs = lingerMs
        self.compression = compression
        self.acks = acks
        self.retries = retries
        self.retryBackoffMs = retryBackoffMs
    }
}

/// Compression types for produced messages.
public enum CompressionType: String, Sendable {
    case none = "none"
    case gzip = "gzip"
    case snappy = "snappy"
    case lz4 = "lz4"
    case zstd = "zstd"
}

/// Acknowledgment modes for produced messages.
public enum Acks: String, Sendable {
    case none = "0"
    case leader = "1"
    case all = "all"
}

// MARK: - ConsumerConfig

/// Configuration for a Kafka consumer.
public struct ConsumerConfig: Sendable, Equatable {
    /// The starting offset position.
    public let offset: OffsetPosition

    /// Whether to automatically commit offsets.
    public let autoCommit: Bool

    /// The minimum bytes to fetch.
    public let fetchMinBytes: Int

    /// The maximum wait time for fetching in milliseconds.
    public let fetchMaxWaitMs: Int

    /// The maximum number of records per poll.
    public let maxPollRecords: Int

    /// The session timeout.
    public let sessionTimeout: Duration

    /// The heartbeat interval.
    public let heartbeatInterval: Duration

    /// Default configuration.
    public static let `default` = ConsumerConfig()

    /// Creates a new consumer configuration.
    public init(
        offset: OffsetPosition = .latest,
        autoCommit: Bool = false,
        fetchMinBytes: Int = 1,
        fetchMaxWaitMs: Int = 500,
        maxPollRecords: Int = 500,
        sessionTimeout: Duration = .seconds(30),
        heartbeatInterval: Duration = .seconds(3)
    ) {
        self.offset = offset
        self.autoCommit = autoCommit
        self.fetchMinBytes = fetchMinBytes
        self.fetchMaxWaitMs = fetchMaxWaitMs
        self.maxPollRecords = maxPollRecords
        self.sessionTimeout = sessionTimeout
        self.heartbeatInterval = heartbeatInterval
    }
}

// MARK: - KafkaError

/// Errors that can occur when interacting with Kafka.
public enum KafkaError: Error, Sendable, Equatable {
    case topicNotFound(String)
    case partitionNotFound(topic: String, partition: Int)
    case messageTooLarge(size: Int, maxSize: Int)
    case notLeader(topic: String, partition: Int)
    case offsetOutOfRange(offset: Int64, topic: String)
    case groupCoordinatorError(String)
    case rebalanceInProgress
    case unauthorized(String)
    case quotaExceeded
    case timeout
    case disconnected
    case serializationError(String)
    case networkError(String)
    case invalidConfiguration(String)
    case unknownError(String)

    /// Whether this error is safe to retry.
    public var isRetriable: Bool {
        switch self {
        case .notLeader, .rebalanceInProgress, .timeout, .disconnected, .networkError, .quotaExceeded:
            return true
        case .topicNotFound, .partitionNotFound, .messageTooLarge, .offsetOutOfRange,
             .groupCoordinatorError, .unauthorized, .serializationError, .invalidConfiguration, .unknownError:
            return false
        }
    }

    /// A human-readable error message.
    public var message: String {
        switch self {
        case .topicNotFound(let topic):
            return "Topic '\(topic)' not found"
        case .partitionNotFound(let topic, let partition):
            return "Partition \(partition) not found in topic '\(topic)'"
        case .messageTooLarge(let size, let maxSize):
            return "Message size \(size) exceeds maximum allowed size \(maxSize)"
        case .notLeader(let topic, let partition):
            return "Not the leader for partition \(partition) in topic '\(topic)'"
        case .offsetOutOfRange(let offset, let topic):
            return "Offset \(offset) is out of range for topic '\(topic)'"
        case .groupCoordinatorError(let reason):
            return "Group coordinator error: \(reason)"
        case .rebalanceInProgress:
            return "Consumer group rebalance in progress"
        case .unauthorized(let reason):
            return "Unauthorized: \(reason)"
        case .quotaExceeded:
            return "Quota exceeded"
        case .timeout:
            return "Request timed out"
        case .disconnected:
            return "Connection disconnected"
        case .serializationError(let reason):
            return "Serialization error: \(reason)"
        case .networkError(let reason):
            return "Network error: \(reason)"
        case .invalidConfiguration(let reason):
            return "Invalid configuration: \(reason)"
        case .unknownError(let reason):
            return "Unknown error: \(reason)"
        }
    }

    /// An error code for programmatic handling.
    public var code: Int {
        switch self {
        case .topicNotFound: return 3
        case .partitionNotFound: return 4
        case .messageTooLarge: return 10
        case .notLeader: return 6
        case .offsetOutOfRange: return 1
        case .groupCoordinatorError: return 15
        case .rebalanceInProgress: return 27
        case .unauthorized: return 31
        case .quotaExceeded: return 76
        case .timeout: return 7
        case .disconnected: return 13
        case .serializationError: return -1
        case .networkError: return -2
        case .invalidConfiguration: return -3
        case .unknownError: return -4
        }
    }
}

// MARK: - RPC Client Protocol

/// Protocol for RPC communication with the Kafka service.
public protocol RPCClientProtocol: Sendable {
    func produce(topic: String, messages: [ProduceMessage]) async throws -> [RecordMetadata]
    func fetch(topic: String, groupId: String, config: ConsumerConfig) async throws -> [RawMessage]
    func commit(topic: String, groupId: String, partition: Int, offset: Int64) async throws
    func subscribe(topics: [String], groupId: String) async throws
    func createTopic(_ name: String, config: TopicConfig) async throws
    func deleteTopic(_ name: String) async throws
    func listTopics() async throws -> [TopicInfo]
    func describeTopic(_ name: String) async throws -> TopicInfo
    func alterTopic(_ name: String, config: TopicConfig) async throws
    func listGroups() async throws -> [ConsumerGroupInfo]
    func describeGroup(_ groupId: String) async throws -> ConsumerGroupInfo
    func resetOffsets(_ groupId: String, topic: String, to position: OffsetPosition) async throws
}

/// A message to be produced via RPC.
public struct ProduceMessage: Sendable {
    public let key: Data?
    public let value: Data
    public let headers: [String: String]

    public init(key: Data?, value: Data, headers: [String: String]) {
        self.key = key
        self.value = value
        self.headers = headers
    }
}

// MARK: - HTTP RPC Client

/// Default RPC client using HTTP/JSON.
public actor HTTPRPCClient: RPCClientProtocol {
    private let config: KafkaConfig
    private let session: URLSession

    public init(config: KafkaConfig) {
        self.config = config
        let sessionConfig = URLSessionConfiguration.default
        sessionConfig.timeoutIntervalForRequest = Double(config.timeout.components.seconds)
        self.session = URLSession(configuration: sessionConfig)
    }

    public func produce(topic: String, messages: [ProduceMessage]) async throws -> [RecordMetadata] {
        let url = URL(string: "\(config.url)/api/topics/\(topic)/produce")!
        var request = URLRequest(url: url)
        request.httpMethod = "POST"
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        request.setValue("Bearer \(config.apiKey)", forHTTPHeaderField: "Authorization")

        let body = messages.map { msg -> [String: Any] in
            var dict: [String: Any] = [
                "value": msg.value.base64EncodedString(),
                "headers": msg.headers
            ]
            if let key = msg.key {
                dict["key"] = key.base64EncodedString()
            }
            return dict
        }

        request.httpBody = try JSONSerialization.data(withJSONObject: ["messages": body])

        let (data, response) = try await session.data(for: request)
        try validateResponse(response, data: data)

        let json = try JSONSerialization.jsonObject(with: data) as? [String: Any]
        guard let records = json?["records"] as? [[String: Any]] else {
            throw KafkaError.serializationError("Invalid response format")
        }

        return records.map { record in
            RecordMetadata(
                topic: topic,
                partition: record["partition"] as? Int ?? 0,
                offset: Int64(record["offset"] as? Int ?? 0),
                timestamp: Date(timeIntervalSince1970: (record["timestamp"] as? Double ?? Date().timeIntervalSince1970))
            )
        }
    }

    public func fetch(topic: String, groupId: String, config: ConsumerConfig) async throws -> [RawMessage] {
        var urlComponents = URLComponents(string: "\(self.config.url)/api/topics/\(topic)/consume")!
        urlComponents.queryItems = [
            URLQueryItem(name: "groupId", value: groupId),
            URLQueryItem(name: "maxRecords", value: String(config.maxPollRecords)),
            URLQueryItem(name: "autoCommit", value: String(config.autoCommit))
        ]

        var request = URLRequest(url: urlComponents.url!)
        request.httpMethod = "GET"
        request.setValue("Bearer \(self.config.apiKey)", forHTTPHeaderField: "Authorization")

        let (data, response) = try await session.data(for: request)
        try validateResponse(response, data: data)

        let json = try JSONSerialization.jsonObject(with: data) as? [String: Any]
        guard let messages = json?["messages"] as? [[String: Any]] else {
            return []
        }

        return try messages.map { msg in
            guard let valueBase64 = msg["value"] as? String,
                  let value = Data(base64Encoded: valueBase64) else {
                throw KafkaError.serializationError("Invalid message value")
            }

            let keyData: Data?
            if let keyBase64 = msg["key"] as? String {
                keyData = Data(base64Encoded: keyBase64)
            } else {
                keyData = nil
            }

            return RawMessage(
                topic: topic,
                partition: msg["partition"] as? Int ?? 0,
                offset: Int64(msg["offset"] as? Int ?? 0),
                key: keyData,
                value: value,
                timestamp: Date(timeIntervalSince1970: (msg["timestamp"] as? Double ?? Date().timeIntervalSince1970)),
                headers: msg["headers"] as? [String: String] ?? [:]
            )
        }
    }

    public func commit(topic: String, groupId: String, partition: Int, offset: Int64) async throws {
        let url = URL(string: "\(config.url)/api/topics/\(topic)/commit")!
        var request = URLRequest(url: url)
        request.httpMethod = "POST"
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        request.setValue("Bearer \(config.apiKey)", forHTTPHeaderField: "Authorization")

        let body: [String: Any] = [
            "groupId": groupId,
            "partition": partition,
            "offset": offset
        ]
        request.httpBody = try JSONSerialization.data(withJSONObject: body)

        let (data, response) = try await session.data(for: request)
        try validateResponse(response, data: data)
    }

    public func subscribe(topics: [String], groupId: String) async throws {
        let url = URL(string: "\(config.url)/api/groups/\(groupId)/subscribe")!
        var request = URLRequest(url: url)
        request.httpMethod = "POST"
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        request.setValue("Bearer \(config.apiKey)", forHTTPHeaderField: "Authorization")

        let body: [String: Any] = ["topics": topics]
        request.httpBody = try JSONSerialization.data(withJSONObject: body)

        let (data, response) = try await session.data(for: request)
        try validateResponse(response, data: data)
    }

    public func createTopic(_ name: String, config: TopicConfig) async throws {
        let url = URL(string: "\(self.config.url)/api/topics")!
        var request = URLRequest(url: url)
        request.httpMethod = "POST"
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        request.setValue("Bearer \(self.config.apiKey)", forHTTPHeaderField: "Authorization")

        var body: [String: Any] = ["name": name]
        if let partitions = config.partitions {
            body["partitions"] = partitions
        }
        if let replicationFactor = config.replicationFactor {
            body["replicationFactor"] = replicationFactor
        }
        if let retentionMs = config.retentionMs {
            body["retentionMs"] = retentionMs
        }
        request.httpBody = try JSONSerialization.data(withJSONObject: body)

        let (data, response) = try await session.data(for: request)
        try validateResponse(response, data: data)
    }

    public func deleteTopic(_ name: String) async throws {
        let url = URL(string: "\(config.url)/api/topics/\(name)")!
        var request = URLRequest(url: url)
        request.httpMethod = "DELETE"
        request.setValue("Bearer \(config.apiKey)", forHTTPHeaderField: "Authorization")

        let (data, response) = try await session.data(for: request)
        try validateResponse(response, data: data)
    }

    public func listTopics() async throws -> [TopicInfo] {
        let url = URL(string: "\(config.url)/api/topics")!
        var request = URLRequest(url: url)
        request.httpMethod = "GET"
        request.setValue("Bearer \(config.apiKey)", forHTTPHeaderField: "Authorization")

        let (data, response) = try await session.data(for: request)
        try validateResponse(response, data: data)

        let json = try JSONSerialization.jsonObject(with: data) as? [String: Any]
        guard let topics = json?["topics"] as? [[String: Any]] else {
            return []
        }

        return topics.map { topic in
            TopicInfo(
                name: topic["name"] as? String ?? "",
                partitions: topic["partitions"] as? Int ?? 1,
                replicationFactor: topic["replicationFactor"] as? Int ?? 1,
                retentionMs: Int64(topic["retentionMs"] as? Int ?? 604800000)
            )
        }
    }

    public func describeTopic(_ name: String) async throws -> TopicInfo {
        let url = URL(string: "\(config.url)/api/topics/\(name)")!
        var request = URLRequest(url: url)
        request.httpMethod = "GET"
        request.setValue("Bearer \(config.apiKey)", forHTTPHeaderField: "Authorization")

        let (data, response) = try await session.data(for: request)
        try validateResponse(response, data: data)

        let topic = try JSONSerialization.jsonObject(with: data) as? [String: Any] ?? [:]
        return TopicInfo(
            name: topic["name"] as? String ?? name,
            partitions: topic["partitions"] as? Int ?? 1,
            replicationFactor: topic["replicationFactor"] as? Int ?? 1,
            retentionMs: Int64(topic["retentionMs"] as? Int ?? 604800000)
        )
    }

    public func alterTopic(_ name: String, config: TopicConfig) async throws {
        let url = URL(string: "\(self.config.url)/api/topics/\(name)")!
        var request = URLRequest(url: url)
        request.httpMethod = "PATCH"
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        request.setValue("Bearer \(self.config.apiKey)", forHTTPHeaderField: "Authorization")

        var body: [String: Any] = [:]
        if let partitions = config.partitions {
            body["partitions"] = partitions
        }
        if let retentionMs = config.retentionMs {
            body["retentionMs"] = retentionMs
        }
        request.httpBody = try JSONSerialization.data(withJSONObject: body)

        let (data, response) = try await session.data(for: request)
        try validateResponse(response, data: data)
    }

    public func listGroups() async throws -> [ConsumerGroupInfo] {
        let url = URL(string: "\(config.url)/api/groups")!
        var request = URLRequest(url: url)
        request.httpMethod = "GET"
        request.setValue("Bearer \(config.apiKey)", forHTTPHeaderField: "Authorization")

        let (data, response) = try await session.data(for: request)
        try validateResponse(response, data: data)

        let json = try JSONSerialization.jsonObject(with: data) as? [String: Any]
        guard let groups = json?["groups"] as? [[String: Any]] else {
            return []
        }

        return groups.map { group in
            ConsumerGroupInfo(
                id: group["id"] as? String ?? "",
                state: ConsumerGroupState(rawValue: group["state"] as? String ?? "Unknown") ?? .unknown,
                memberCount: group["memberCount"] as? Int ?? 0,
                members: [],
                totalLag: Int64(group["totalLag"] as? Int ?? 0)
            )
        }
    }

    public func describeGroup(_ groupId: String) async throws -> ConsumerGroupInfo {
        let url = URL(string: "\(config.url)/api/groups/\(groupId)")!
        var request = URLRequest(url: url)
        request.httpMethod = "GET"
        request.setValue("Bearer \(config.apiKey)", forHTTPHeaderField: "Authorization")

        let (data, response) = try await session.data(for: request)
        try validateResponse(response, data: data)

        let group = try JSONSerialization.jsonObject(with: data) as? [String: Any] ?? [:]

        let members = (group["members"] as? [[String: Any]] ?? []).map { member in
            let assignments = (member["assignments"] as? [[String: Any]] ?? []).map { assignment in
                PartitionAssignment(
                    topic: assignment["topic"] as? String ?? "",
                    partition: assignment["partition"] as? Int ?? 0
                )
            }
            return ConsumerGroupMember(
                id: member["id"] as? String ?? "",
                clientId: member["clientId"] as? String ?? "",
                host: member["host"] as? String ?? "",
                assignments: assignments
            )
        }

        return ConsumerGroupInfo(
            id: group["id"] as? String ?? groupId,
            state: ConsumerGroupState(rawValue: group["state"] as? String ?? "Unknown") ?? .unknown,
            memberCount: group["memberCount"] as? Int ?? members.count,
            members: members,
            totalLag: Int64(group["totalLag"] as? Int ?? 0)
        )
    }

    public func resetOffsets(_ groupId: String, topic: String, to position: OffsetPosition) async throws {
        let url = URL(string: "\(config.url)/api/groups/\(groupId)/offsets")!
        var request = URLRequest(url: url)
        request.httpMethod = "POST"
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        request.setValue("Bearer \(config.apiKey)", forHTTPHeaderField: "Authorization")

        var body: [String: Any] = ["topic": topic]
        switch position {
        case .earliest:
            body["position"] = "earliest"
        case .latest:
            body["position"] = "latest"
        case .offset(let offset):
            body["position"] = "offset"
            body["offset"] = offset
        case .timestamp(let date):
            body["position"] = "timestamp"
            body["timestamp"] = date.timeIntervalSince1970 * 1000
        }
        request.httpBody = try JSONSerialization.data(withJSONObject: body)

        let (data, response) = try await session.data(for: request)
        try validateResponse(response, data: data)
    }

    private func validateResponse(_ response: URLResponse, data: Data) throws {
        guard let httpResponse = response as? HTTPURLResponse else {
            throw KafkaError.networkError("Invalid response type")
        }

        switch httpResponse.statusCode {
        case 200...299:
            return
        case 401, 403:
            throw KafkaError.unauthorized("Invalid API key or insufficient permissions")
        case 404:
            // Try to parse error message
            if let json = try? JSONSerialization.jsonObject(with: data) as? [String: Any],
               let message = json["error"] as? String {
                if message.contains("topic") {
                    throw KafkaError.topicNotFound(message)
                }
            }
            throw KafkaError.topicNotFound("Resource not found")
        case 429:
            throw KafkaError.quotaExceeded
        case 408, 504:
            throw KafkaError.timeout
        default:
            if let json = try? JSONSerialization.jsonObject(with: data) as? [String: Any],
               let message = json["error"] as? String {
                throw KafkaError.unknownError(message)
            }
            throw KafkaError.unknownError("HTTP \(httpResponse.statusCode)")
        }
    }
}

// MARK: - Kafka Client

/// The main Kafka client for interacting with Kafka.do.
public final class Kafka: Sendable {
    /// The brokers to connect to.
    public let brokers: [String]

    /// The client configuration.
    public let config: KafkaConfig

    /// The RPC client for communication.
    internal let rpcClient: any RPCClientProtocol

    /// Creates a new Kafka client with broker addresses.
    public init(brokers: [String], config: KafkaConfig = .default) {
        self.brokers = brokers
        self.config = config
        self.rpcClient = HTTPRPCClient(config: config)
    }

    /// Creates a new Kafka client with a custom RPC client (for testing).
    public init(brokers: [String], config: KafkaConfig = .default, rpcClient: any RPCClientProtocol) {
        self.brokers = brokers
        self.config = config
        self.rpcClient = rpcClient
    }

    /// Creates a producer for the specified topic.
    public func producer<T: Encodable & Sendable>(config: ProducerConfig = .default) -> Producer<T> {
        return Producer(kafka: self, config: config)
    }

    /// Creates a typed producer for sending messages to topics.
    public func producer() -> Producer<Data> {
        return Producer(kafka: self, config: .default)
    }

    /// Creates a topic-bound producer for the specified topic.
    /// - Parameters:
    ///   - topic: The topic to produce to.
    ///   - config: Optional producer configuration.
    /// - Returns: A topic-bound producer.
    public func producer<T: Encodable & Sendable>(for topic: String, config: ProducerConfig = .default) -> TopicProducer<T> {
        return TopicProducer(kafka: self, topic: topic, config: config)
    }

    /// Creates a consumer with the specified group ID.
    public func consumer<T: Decodable & Sendable>(groupId: String, config: ConsumerConfig = .default) -> Consumer<T> {
        return Consumer(kafka: self, groupId: groupId, config: config)
    }

    /// Creates a data consumer with the specified group ID.
    public func consumer(groupId: String, config: ConsumerConfig = .default) -> Consumer<Data> {
        return Consumer(kafka: self, groupId: groupId, config: config)
    }

    /// Creates a topic-bound consumer for the specified topic and group.
    /// - Parameters:
    ///   - topic: The topic to consume from.
    ///   - group: The consumer group ID.
    ///   - config: Optional consumer configuration.
    /// - Returns: An async sequence of records from the topic.
    public func consumer<T: Decodable & Sendable>(for topic: String, group: String, config: ConsumerConfig = .default) -> TopicConsumer<T> {
        return TopicConsumer(kafka: self, topic: topic, groupId: group, config: config)
    }

    /// Returns the admin client for topic and group management.
    public func admin() -> Admin {
        return Admin(kafka: self)
    }
}

// MARK: - KafkaClient (Alternative name)

/// Type alias for backward compatibility.
public typealias KafkaClient = Kafka
