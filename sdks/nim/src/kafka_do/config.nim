## Kafka configuration for the .do platform

import std/[os, times, httpclient, json]

type
  KafkaConfig* = ref object
    url*: string
    apiKey*: string
    timeout*: Duration
    retries*: int

  ProducerConfig* = ref object
    batchSize*: int
    lingerMs*: int
    compression*: int  # Compression enum value
    acks*: int         # Acks enum value
    retries*: int
    retryBackoffMs*: int

  ConsumerConfig* = ref object
    offset*: int       # 0 = Earliest, 1 = Latest
    timestampMs*: int64  # For timestamp-based offset
    autoCommit*: bool
    fetchMinBytes*: int
    fetchMaxWaitMs*: int
    maxPollRecords*: int
    sessionTimeoutMs*: int
    heartbeatIntervalMs*: int

  BatchConfig* = ref object
    batchSize*: int
    batchTimeoutMs*: int
    autoCommit*: bool

  TopicConfig* = ref object
    partitions*: int
    replicationFactor*: int
    retentionMs*: int64
    cleanupPolicy*: string
    compressionType*: string

const
  DefaultTimeout* = initDuration(seconds = 30)
  DefaultRetries* = 3
  DefaultBatchSize* = 16384
  DefaultLingerMs* = 5
  DefaultMaxPollRecords* = 500
  DefaultSessionTimeoutMs* = 30000
  DefaultHeartbeatIntervalMs* = 3000
  DefaultRetentionMs* = 7 * 24 * 60 * 60 * 1000'i64  # 7 days

proc newKafkaConfig*(): KafkaConfig =
  ## Create a new configuration with defaults from environment
  KafkaConfig(
    url: getEnv("KAFKA_DO_URL", "https://kafka.do"),
    apiKey: getEnv("KAFKA_DO_API_KEY", ""),
    timeout: DefaultTimeout,
    retries: DefaultRetries
  )

proc newKafkaConfig*(url: string, apiKey: string = ""): KafkaConfig =
  ## Create a configuration with a specific URL
  KafkaConfig(
    url: url,
    apiKey: apiKey,
    timeout: DefaultTimeout,
    retries: DefaultRetries
  )

proc headers*(config: KafkaConfig): HttpHeaders =
  ## Get HTTP headers for API requests
  result = newHttpHeaders()
  result["Content-Type"] = "application/json"
  if config.apiKey.len > 0:
    result["Authorization"] = "Bearer " & config.apiKey

proc newProducerConfig*(): ProducerConfig =
  ## Create default producer configuration
  ProducerConfig(
    batchSize: DefaultBatchSize,
    lingerMs: DefaultLingerMs,
    compression: 0,  # None
    acks: -1,        # All
    retries: DefaultRetries,
    retryBackoffMs: 100
  )

proc newProducerConfig*(
  batchSize: int = DefaultBatchSize,
  lingerMs: int = DefaultLingerMs,
  compression: int = 0,
  acks: int = -1,
  retries: int = DefaultRetries,
  retryBackoffMs: int = 100
): ProducerConfig =
  ProducerConfig(
    batchSize: batchSize,
    lingerMs: lingerMs,
    compression: compression,
    acks: acks,
    retries: retries,
    retryBackoffMs: retryBackoffMs
  )

proc newConsumerConfig*(): ConsumerConfig =
  ## Create default consumer configuration
  ConsumerConfig(
    offset: 1,  # Latest
    timestampMs: 0,
    autoCommit: true,
    fetchMinBytes: 1,
    fetchMaxWaitMs: 500,
    maxPollRecords: DefaultMaxPollRecords,
    sessionTimeoutMs: DefaultSessionTimeoutMs,
    heartbeatIntervalMs: DefaultHeartbeatIntervalMs
  )

proc newConsumerConfig*(
  offset: int = 1,
  timestampMs: int64 = 0,
  autoCommit: bool = true,
  fetchMinBytes: int = 1,
  fetchMaxWaitMs: int = 500,
  maxPollRecords: int = DefaultMaxPollRecords,
  sessionTimeoutMs: int = DefaultSessionTimeoutMs,
  heartbeatIntervalMs: int = DefaultHeartbeatIntervalMs
): ConsumerConfig =
  ConsumerConfig(
    offset: offset,
    timestampMs: timestampMs,
    autoCommit: autoCommit,
    fetchMinBytes: fetchMinBytes,
    fetchMaxWaitMs: fetchMaxWaitMs,
    maxPollRecords: maxPollRecords,
    sessionTimeoutMs: sessionTimeoutMs,
    heartbeatIntervalMs: heartbeatIntervalMs
  )

proc newBatchConfig*(): BatchConfig =
  ## Create default batch configuration
  BatchConfig(
    batchSize: 100,
    batchTimeoutMs: 5000,
    autoCommit: true
  )

proc newBatchConfig*(
  batchSize: int = 100,
  batchTimeoutMs: int = 5000,
  autoCommit: bool = true
): BatchConfig =
  BatchConfig(
    batchSize: batchSize,
    batchTimeoutMs: batchTimeoutMs,
    autoCommit: autoCommit
  )

proc newTopicConfig*(): TopicConfig =
  ## Create default topic configuration
  TopicConfig(
    partitions: 1,
    replicationFactor: 1,
    retentionMs: DefaultRetentionMs,
    cleanupPolicy: "delete",
    compressionType: "producer"
  )

proc newTopicConfig*(
  partitions: int = 1,
  replicationFactor: int = 1,
  retentionMs: int64 = DefaultRetentionMs,
  cleanupPolicy: string = "delete",
  compressionType: string = "producer"
): TopicConfig =
  TopicConfig(
    partitions: partitions,
    replicationFactor: replicationFactor,
    retentionMs: retentionMs,
    cleanupPolicy: cleanupPolicy,
    compressionType: compressionType
  )
