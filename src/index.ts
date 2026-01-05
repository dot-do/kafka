import { Hono } from 'hono'
import { cors } from 'hono/cors'
import type { Context } from 'hono'
import { createProducer } from './api/producer'
import { createAdmin } from './api/admin'
import { version } from '../package.json'

// ============================================================================
// Input Validation Helpers
// ============================================================================

/** Validation result type */
type ValidationResult<T> =
  | { valid: true; value: T }
  | { valid: false; error: string }

/** Topic name validation pattern: alphanumeric, dots, underscores, hyphens */
const TOPIC_NAME_PATTERN = /^[a-zA-Z0-9._-]+$/

/** Maximum topic name length */
const MAX_TOPIC_NAME_LENGTH = 255

/** Maximum message size (1MB) */
const MAX_MESSAGE_SIZE = 1024 * 1024

/** Maximum batch size (number of records) */
const MAX_BATCH_SIZE = 1000

/** Default and maximum values for pagination */
const DEFAULT_OFFSET = 0
const DEFAULT_LIMIT = 100
const MAX_LIMIT = 10000

/**
 * Validates a topic name
 * - Must be non-empty string
 * - Must match pattern: /^[a-zA-Z0-9._-]+$/
 * - Max length: 255 characters
 */
function validateTopicName(topic: string | undefined): ValidationResult<string> {
  if (!topic || typeof topic !== 'string' || topic.trim() === '') {
    return { valid: false, error: 'Topic name is required and must be a non-empty string' }
  }
  if (topic.length > MAX_TOPIC_NAME_LENGTH) {
    return { valid: false, error: `Topic name must not exceed ${MAX_TOPIC_NAME_LENGTH} characters` }
  }
  if (!TOPIC_NAME_PATTERN.test(topic)) {
    return {
      valid: false,
      error: 'Topic name must contain only alphanumeric characters, dots, underscores, and hyphens',
    }
  }
  return { valid: true, value: topic }
}

/**
 * Validates a group ID
 * - Must be non-empty string
 * - Must match pattern: /^[a-zA-Z0-9._-]+$/
 * - Max length: 255 characters
 */
function validateGroupId(groupId: string | undefined): ValidationResult<string> {
  if (!groupId || typeof groupId !== 'string' || groupId.trim() === '') {
    return { valid: false, error: 'Group ID is required and must be a non-empty string' }
  }
  if (groupId.length > MAX_TOPIC_NAME_LENGTH) {
    return { valid: false, error: `Group ID must not exceed ${MAX_TOPIC_NAME_LENGTH} characters` }
  }
  if (!TOPIC_NAME_PATTERN.test(groupId)) {
    return {
      valid: false,
      error: 'Group ID must contain only alphanumeric characters, dots, underscores, and hyphens',
    }
  }
  return { valid: true, value: groupId }
}

/**
 * Validates a partition number
 * - Must be a non-negative integer
 */
function validatePartition(value: string | undefined): ValidationResult<number> {
  if (value === undefined || value === '') {
    return { valid: false, error: 'Partition number is required' }
  }
  const partition = parseInt(value, 10)
  if (isNaN(partition) || partition < 0) {
    return { valid: false, error: 'Partition must be a non-negative integer' }
  }
  return { valid: true, value: partition }
}

/**
 * Validates offset query parameter
 * - Must be a non-negative integer
 * - Defaults to 0
 */
function validateOffset(value: string | undefined): ValidationResult<number> {
  if (value === undefined || value === '') {
    return { valid: true, value: DEFAULT_OFFSET }
  }
  const offset = parseInt(value, 10)
  if (isNaN(offset) || offset < 0) {
    return { valid: false, error: 'Offset must be a non-negative integer' }
  }
  return { valid: true, value: offset }
}

/**
 * Validates limit query parameter
 * - Must be a positive integer
 * - Defaults to 100, max 10000
 */
function validateLimit(value: string | undefined): ValidationResult<number> {
  if (value === undefined || value === '') {
    return { valid: true, value: DEFAULT_LIMIT }
  }
  const limit = parseInt(value, 10)
  if (isNaN(limit) || limit < 1) {
    return { valid: false, error: 'Limit must be a positive integer' }
  }
  if (limit > MAX_LIMIT) {
    return { valid: false, error: `Limit must not exceed ${MAX_LIMIT}` }
  }
  return { valid: true, value: limit }
}

/**
 * Validates partition count for topic creation/modification
 * - Must be a positive integer
 */
function validatePartitionCount(value: number | undefined): ValidationResult<number> {
  if (value === undefined) {
    return { valid: false, error: 'Partition count is required' }
  }
  if (!Number.isInteger(value) || value < 1) {
    return { valid: false, error: 'Partition count must be a positive integer' }
  }
  return { valid: true, value }
}

/**
 * Safely parses JSON request body with error handling
 */
async function parseJsonBody<T>(c: Context): Promise<{ data: T } | { error: string }> {
  try {
    const data = await c.req.json<T>()
    return { data }
  } catch {
    return { error: 'Invalid JSON in request body' }
  }
}

/**
 * Returns a 400 Bad Request response with error details
 */
function badRequest(c: Context, message: string) {
  return c.json({ error: message, code: 'BAD_REQUEST' }, 400)
}

/**
 * Returns a 413 Payload Too Large response with error details
 */
function payloadTooLarge(c: Context, message: string) {
  return c.json({ error: message, code: 'PAYLOAD_TOO_LARGE' }, 413)
}

// Re-export API classes
export { KafkaProducer, createProducer } from './api/producer'
export { KafkaConsumer, createConsumer } from './api/consumer'
export { KafkaAdmin, createAdmin } from './api/admin'

// Re-export error classes
export {
  KafkaError,
  TopicNotFoundError,
  PartitionNotFoundError,
  ConsumerGroupError,
  ProduceError,
  ConsumeError,
  ConnectionError,
  TimeoutError,
} from './errors'

// Environment interface for Cloudflare Workers
export interface Env {
  TOPIC_PARTITION: DurableObjectNamespace
  CONSUMER_GROUP: DurableObjectNamespace
  CLUSTER_METADATA: DurableObjectNamespace
  /**
   * CORS configuration - comma-separated list of allowed origins.
   * Examples:
   *   - "*" (allow all origins - default, suitable for development)
   *   - "https://example.com" (single origin)
   *   - "https://example.com,https://api.example.com" (multiple origins)
   *
   * For production deployments, set this to your specific domain(s).
   * If not set, defaults to '*' (all origins allowed).
   */
  CORS_ORIGINS?: string
}

// Main Hono application for HTTP routing
const app = new Hono<{ Bindings: Env }>()

/**
 * CORS Middleware Configuration
 *
 * This middleware enables Cross-Origin Resource Sharing (CORS) for browser-based clients.
 * CORS is configurable via the CORS_ORIGINS environment variable.
 *
 * Configuration options:
 *   - Set CORS_ORIGINS="*" to allow all origins (default - suitable for development)
 *   - Set CORS_ORIGINS="https://yourdomain.com" for a single allowed origin
 *   - Set CORS_ORIGINS="https://app.example.com,https://admin.example.com" for multiple origins
 *
 * For production deployments:
 *   1. Set CORS_ORIGINS in your wrangler.toml or via `wrangler secret put CORS_ORIGINS`
 *   2. Use specific origins instead of "*" to enhance security
 *
 * Example wrangler.toml configuration:
 *   [vars]
 *   CORS_ORIGINS = "https://yourdomain.com"
 */
app.use('*', async (c, next) => {
  const corsOrigins = c.env.CORS_ORIGINS ?? '*'

  // Parse comma-separated origins, supporting single origin, multiple origins, or wildcard
  const origin =
    corsOrigins === '*'
      ? '*'
      : corsOrigins.includes(',')
        ? corsOrigins.split(',').map((o) => o.trim())
        : corsOrigins

  const corsMiddleware = cors({
    origin,
    allowMethods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
    allowHeaders: ['Content-Type', 'Authorization'],
    exposeHeaders: ['Content-Length'],
    maxAge: 86400, // 24 hours
    credentials: corsOrigins !== '*', // Only allow credentials for specific origins
  })

  return corsMiddleware(c, next)
})

// Health and info endpoints
app.get('/', (c) => {
  return c.json({
    name: 'kafka.do',
    description: 'Kafka-compatible streaming platform on Cloudflare Workers',
    version,
    status: 'ok',
  })
})

app.get('/health', (c) => {
  return c.json({ status: 'healthy', timestamp: Date.now() })
})

// ============================================================================
// Producer API - Send messages to topics
// ============================================================================

/**
 * POST /topics/:topic/produce
 * Produce a single message to a topic
 */
app.post('/topics/:topic/produce', async (c) => {
  // Validate topic name
  const topicValidation = validateTopicName(c.req.param('topic'))
  if (!topicValidation.valid) {
    return badRequest(c, topicValidation.error)
  }
  const topic = topicValidation.value

  // Parse and validate request body
  const bodyResult = await parseJsonBody<{
    key?: string
    value: unknown
    partition?: number
    headers?: Record<string, string>
  }>(c)
  if ('error' in bodyResult) {
    return badRequest(c, bodyResult.error)
  }
  const body = bodyResult.data

  // Validate required field: value
  if (body.value === undefined) {
    return badRequest(c, 'Message value is required')
  }

  // Validate optional partition
  if (body.partition !== undefined) {
    if (!Number.isInteger(body.partition) || body.partition < 0) {
      return badRequest(c, 'Partition must be a non-negative integer')
    }
  }

  // Validate message size
  const messageSize = JSON.stringify(body.value).length
  if (messageSize > MAX_MESSAGE_SIZE) {
    return payloadTooLarge(c, `Message size ${messageSize} bytes exceeds maximum allowed size of ${MAX_MESSAGE_SIZE} bytes (1MB)`)
  }

  const producer = createProducer(c.env)
  try {
    const result = await producer.send({
      topic,
      key: body.key,
      value: body.value,
      partition: body.partition,
      headers: body.headers,
    })
    return c.json(result, 201)
  } finally {
    await producer.close()
  }
})

/**
 * POST /topics/:topic/produce-batch
 * Produce multiple messages to a topic
 */
app.post('/topics/:topic/produce-batch', async (c) => {
  // Validate topic name
  const topicValidation = validateTopicName(c.req.param('topic'))
  if (!topicValidation.valid) {
    return badRequest(c, topicValidation.error)
  }
  const topic = topicValidation.value

  // Parse and validate request body
  const bodyResult = await parseJsonBody<{
    records: Array<{
      key?: string
      value: unknown
      partition?: number
      headers?: Record<string, string>
    }>
  }>(c)
  if ('error' in bodyResult) {
    return badRequest(c, bodyResult.error)
  }
  const body = bodyResult.data

  // Validate records array
  if (!body.records || !Array.isArray(body.records)) {
    return badRequest(c, 'Records array is required')
  }
  if (body.records.length === 0) {
    return badRequest(c, 'Records array must not be empty')
  }
  if (body.records.length > MAX_BATCH_SIZE) {
    return badRequest(c, `Batch size ${body.records.length} exceeds maximum allowed size of ${MAX_BATCH_SIZE} records`)
  }

  // Validate each record
  for (let i = 0; i < body.records.length; i++) {
    const record = body.records[i]
    if (record.value === undefined) {
      return badRequest(c, `Record at index ${i} is missing required field: value`)
    }
    if (record.partition !== undefined) {
      if (!Number.isInteger(record.partition) || record.partition < 0) {
        return badRequest(c, `Record at index ${i} has invalid partition: must be a non-negative integer`)
      }
    }
    // Validate message size for each record
    const recordSize = JSON.stringify(record.value).length
    if (recordSize > MAX_MESSAGE_SIZE) {
      return payloadTooLarge(
        c,
        `Record at index ${i}: message size ${recordSize} bytes exceeds maximum allowed size of ${MAX_MESSAGE_SIZE} bytes (1MB)`
      )
    }
  }

  const producer = createProducer(c.env)
  try {
    const results = await producer.sendBatch(
      body.records.map((r) => ({ ...r, topic }))
    )
    return c.json({ results }, 201)
  } finally {
    await producer.close()
  }
})

// ============================================================================
// Consumer API - Read messages from topics (stateless fetch)
// ============================================================================

/**
 * GET /topics/:topic/partitions/:partition/messages
 * Read messages from a specific partition
 */
app.get('/topics/:topic/partitions/:partition/messages', async (c) => {
  // Validate topic name
  const topicValidation = validateTopicName(c.req.param('topic'))
  if (!topicValidation.valid) {
    return badRequest(c, topicValidation.error)
  }
  const topic = topicValidation.value

  // Validate partition number
  const partitionValidation = validatePartition(c.req.param('partition'))
  if (!partitionValidation.valid) {
    return badRequest(c, partitionValidation.error)
  }
  const partition = partitionValidation.value

  // Validate offset query parameter
  const offsetValidation = validateOffset(c.req.query('offset'))
  if (!offsetValidation.valid) {
    return badRequest(c, offsetValidation.error)
  }
  const offset = offsetValidation.value

  // Validate limit query parameter
  const limitValidation = validateLimit(c.req.query('limit'))
  if (!limitValidation.valid) {
    return badRequest(c, limitValidation.error)
  }
  const limit = limitValidation.value

  const partitionKey = `${topic}-${partition}`
  const partitionId = c.env.TOPIC_PARTITION.idFromName(partitionKey)
  const partitionStub = c.env.TOPIC_PARTITION.get(partitionId)

  const response = await partitionStub.fetch(
    `http://localhost/read?offset=${offset}&limit=${limit}`
  )

  if (!response.ok) {
    return c.json({ error: 'Failed to fetch messages' }, 500)
  }

  const messages = await response.json()
  return c.json({
    topic,
    partition,
    messages,
  })
})

/**
 * GET /topics/:topic/partitions/:partition/offsets
 * Get offset information for a partition
 */
app.get('/topics/:topic/partitions/:partition/offsets', async (c) => {
  // Validate topic name
  const topicValidation = validateTopicName(c.req.param('topic'))
  if (!topicValidation.valid) {
    return badRequest(c, topicValidation.error)
  }
  const topic = topicValidation.value

  // Validate partition number
  const partitionValidation = validatePartition(c.req.param('partition'))
  if (!partitionValidation.valid) {
    return badRequest(c, partitionValidation.error)
  }
  const partition = partitionValidation.value

  const partitionKey = `${topic}-${partition}`
  const partitionId = c.env.TOPIC_PARTITION.idFromName(partitionKey)
  const partitionStub = c.env.TOPIC_PARTITION.get(partitionId)

  const response = await partitionStub.fetch('http://localhost/watermarks')
  if (!response.ok) {
    return c.json({ earliest: 0, latest: 0 })
  }

  const { low, high } = (await response.json()) as { low: number; high: number }
  return c.json({
    topic,
    partition,
    earliest: low,
    latest: high,
  })
})

// ============================================================================
// Consumer Group API - Stateful consumption
// ============================================================================

/**
 * POST /consumer-groups/:groupId/join
 * Join a consumer group
 */
app.post('/consumer-groups/:groupId/join', async (c) => {
  // Validate group ID
  const groupIdValidation = validateGroupId(c.req.param('groupId'))
  if (!groupIdValidation.valid) {
    return badRequest(c, groupIdValidation.error)
  }
  const groupId = groupIdValidation.value

  // Parse and validate request body
  const bodyResult = await parseJsonBody<{
    clientId: string
    topics: string[]
    sessionTimeoutMs?: number
  }>(c)
  if ('error' in bodyResult) {
    return badRequest(c, bodyResult.error)
  }
  const body = bodyResult.data

  // Validate required field: clientId
  if (!body.clientId || typeof body.clientId !== 'string' || body.clientId.trim() === '') {
    return badRequest(c, 'clientId is required and must be a non-empty string')
  }

  // Validate required field: topics
  if (!body.topics || !Array.isArray(body.topics)) {
    return badRequest(c, 'topics is required and must be an array')
  }
  if (body.topics.length === 0) {
    return badRequest(c, 'topics array must not be empty')
  }

  // Validate each topic name in the array
  for (let i = 0; i < body.topics.length; i++) {
    const topicValidation = validateTopicName(body.topics[i])
    if (!topicValidation.valid) {
      return badRequest(c, `topics[${i}]: ${topicValidation.error}`)
    }
  }

  // Validate optional sessionTimeoutMs
  if (body.sessionTimeoutMs !== undefined) {
    if (!Number.isInteger(body.sessionTimeoutMs) || body.sessionTimeoutMs < 1) {
      return badRequest(c, 'sessionTimeoutMs must be a positive integer')
    }
  }

  const groupDOId = c.env.CONSUMER_GROUP.idFromName(groupId)
  const groupStub = c.env.CONSUMER_GROUP.get(groupDOId)

  const response = await groupStub.fetch('http://localhost/join', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      clientId: body.clientId,
      clientHost: c.req.header('CF-Connecting-IP') ?? 'unknown',
      sessionTimeout: body.sessionTimeoutMs ?? 30000,
      rebalanceTimeout: 60000,
      topics: body.topics,
    }),
  })

  if (!response.ok) {
    return c.json({ error: 'Failed to join group' }, 500)
  }

  const result = await response.json()
  return c.json(result)
})

/**
 * POST /consumer-groups/:groupId/heartbeat
 * Send heartbeat to consumer group
 */
app.post('/consumer-groups/:groupId/heartbeat', async (c) => {
  // Validate group ID
  const groupIdValidation = validateGroupId(c.req.param('groupId'))
  if (!groupIdValidation.valid) {
    return badRequest(c, groupIdValidation.error)
  }
  const groupId = groupIdValidation.value

  // Parse and validate request body
  const bodyResult = await parseJsonBody<{
    memberId: string
    generationId: number
  }>(c)
  if ('error' in bodyResult) {
    return badRequest(c, bodyResult.error)
  }
  const body = bodyResult.data

  // Validate required field: memberId
  if (!body.memberId || typeof body.memberId !== 'string' || body.memberId.trim() === '') {
    return badRequest(c, 'memberId is required and must be a non-empty string')
  }

  // Validate required field: generationId
  if (body.generationId === undefined || !Number.isInteger(body.generationId) || body.generationId < 0) {
    return badRequest(c, 'generationId is required and must be a non-negative integer')
  }

  const groupDOId = c.env.CONSUMER_GROUP.idFromName(groupId)
  const groupStub = c.env.CONSUMER_GROUP.get(groupDOId)

  const response = await groupStub.fetch('http://localhost/heartbeat', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  })

  if (!response.ok) {
    return c.json({ error: 'Heartbeat failed' }, 500)
  }

  const result = await response.json()
  return c.json(result)
})

/**
 * POST /consumer-groups/:groupId/commit
 * Commit offsets for a consumer group
 */
app.post('/consumer-groups/:groupId/commit', async (c) => {
  // Validate group ID
  const groupIdValidation = validateGroupId(c.req.param('groupId'))
  if (!groupIdValidation.valid) {
    return badRequest(c, groupIdValidation.error)
  }
  const groupId = groupIdValidation.value

  // Parse and validate request body
  const bodyResult = await parseJsonBody<{
    memberId: string
    offsets: Array<{ topic: string; partition: number; offset: number }>
  }>(c)
  if ('error' in bodyResult) {
    return badRequest(c, bodyResult.error)
  }
  const body = bodyResult.data

  // Validate required field: memberId
  if (!body.memberId || typeof body.memberId !== 'string' || body.memberId.trim() === '') {
    return badRequest(c, 'memberId is required and must be a non-empty string')
  }

  // Validate required field: offsets
  if (!body.offsets || !Array.isArray(body.offsets)) {
    return badRequest(c, 'offsets is required and must be an array')
  }

  // Validate each offset entry
  for (let i = 0; i < body.offsets.length; i++) {
    const offsetEntry = body.offsets[i]
    const topicValidation = validateTopicName(offsetEntry.topic)
    if (!topicValidation.valid) {
      return badRequest(c, `offsets[${i}].topic: ${topicValidation.error}`)
    }
    if (!Number.isInteger(offsetEntry.partition) || offsetEntry.partition < 0) {
      return badRequest(c, `offsets[${i}].partition must be a non-negative integer`)
    }
    if (!Number.isInteger(offsetEntry.offset) || offsetEntry.offset < 0) {
      return badRequest(c, `offsets[${i}].offset must be a non-negative integer`)
    }
  }

  const groupDOId = c.env.CONSUMER_GROUP.idFromName(groupId)
  const groupStub = c.env.CONSUMER_GROUP.get(groupDOId)

  const response = await groupStub.fetch('http://localhost/commit', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  })

  if (!response.ok) {
    return c.json({ error: 'Commit failed' }, 500)
  }

  return c.json({ success: true })
})

/**
 * POST /consumer-groups/:groupId/leave
 * Leave a consumer group
 */
app.post('/consumer-groups/:groupId/leave', async (c) => {
  // Validate group ID
  const groupIdValidation = validateGroupId(c.req.param('groupId'))
  if (!groupIdValidation.valid) {
    return badRequest(c, groupIdValidation.error)
  }
  const groupId = groupIdValidation.value

  // Parse and validate request body
  const bodyResult = await parseJsonBody<{ memberId: string }>(c)
  if ('error' in bodyResult) {
    return badRequest(c, bodyResult.error)
  }
  const body = bodyResult.data

  // Validate required field: memberId
  if (!body.memberId || typeof body.memberId !== 'string' || body.memberId.trim() === '') {
    return badRequest(c, 'memberId is required and must be a non-empty string')
  }

  const groupDOId = c.env.CONSUMER_GROUP.idFromName(groupId)
  const groupStub = c.env.CONSUMER_GROUP.get(groupDOId)

  const response = await groupStub.fetch('http://localhost/leave', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  })

  if (!response.ok) {
    return c.json({ error: 'Leave failed' }, 500)
  }

  return c.json({ success: true })
})

/**
 * GET /consumer-groups/:groupId
 * Describe a consumer group
 */
app.get('/consumer-groups/:groupId', async (c) => {
  // Validate group ID
  const groupIdValidation = validateGroupId(c.req.param('groupId'))
  if (!groupIdValidation.valid) {
    return badRequest(c, groupIdValidation.error)
  }
  const groupId = groupIdValidation.value

  const groupDOId = c.env.CONSUMER_GROUP.idFromName(groupId)
  const groupStub = c.env.CONSUMER_GROUP.get(groupDOId)

  const response = await groupStub.fetch('http://localhost/describe')
  if (!response.ok) {
    return c.json({ error: 'Failed to describe group' }, 500)
  }

  const result = await response.json()
  return c.json(result)
})

// ============================================================================
// Admin API - Topic and cluster management
// ============================================================================

/**
 * GET /admin/topics
 * List all topics
 */
app.get('/admin/topics', async (c) => {
  const admin = createAdmin(c.env)
  try {
    const topics = await admin.listTopics()
    return c.json({ topics })
  } finally {
    await admin.close()
  }
})

/**
 * POST /admin/topics
 * Create a new topic
 */
app.post('/admin/topics', async (c) => {
  // Parse and validate request body
  const bodyResult = await parseJsonBody<{
    topic: string
    partitions: number
    config?: Record<string, string>
  }>(c)
  if ('error' in bodyResult) {
    return badRequest(c, bodyResult.error)
  }
  const body = bodyResult.data

  // Validate topic name
  const topicValidation = validateTopicName(body.topic)
  if (!topicValidation.valid) {
    return badRequest(c, topicValidation.error)
  }

  // Validate partition count
  const partitionCountValidation = validatePartitionCount(body.partitions)
  if (!partitionCountValidation.valid) {
    return badRequest(c, partitionCountValidation.error)
  }

  const admin = createAdmin(c.env)
  try {
    await admin.createTopic({
      topic: body.topic,
      partitions: body.partitions,
      config: body.config,
    })
    return c.json({ success: true, topic: body.topic }, 201)
  } finally {
    await admin.close()
  }
})

/**
 * GET /admin/topics/:topic
 * Describe a topic
 */
app.get('/admin/topics/:topic', async (c) => {
  // Validate topic name
  const topicValidation = validateTopicName(c.req.param('topic'))
  if (!topicValidation.valid) {
    return badRequest(c, topicValidation.error)
  }
  const topic = topicValidation.value

  const admin = createAdmin(c.env)
  try {
    const metadata = await admin.describeTopic(topic)
    return c.json(metadata)
  } catch (error) {
    return c.json({ error: `Topic ${topic} not found` }, 404)
  } finally {
    await admin.close()
  }
})

/**
 * DELETE /admin/topics/:topic
 * Delete a topic
 */
app.delete('/admin/topics/:topic', async (c) => {
  // Validate topic name
  const topicValidation = validateTopicName(c.req.param('topic'))
  if (!topicValidation.valid) {
    return badRequest(c, topicValidation.error)
  }
  const topic = topicValidation.value

  const admin = createAdmin(c.env)
  try {
    await admin.deleteTopic(topic)
    return c.json({ success: true })
  } finally {
    await admin.close()
  }
})

/**
 * POST /admin/topics/:topic/partitions
 * Add partitions to a topic
 */
app.post('/admin/topics/:topic/partitions', async (c) => {
  // Validate topic name
  const topicValidation = validateTopicName(c.req.param('topic'))
  if (!topicValidation.valid) {
    return badRequest(c, topicValidation.error)
  }
  const topic = topicValidation.value

  // Parse and validate request body
  const bodyResult = await parseJsonBody<{ count: number }>(c)
  if ('error' in bodyResult) {
    return badRequest(c, bodyResult.error)
  }
  const body = bodyResult.data

  // Validate partition count
  const partitionCountValidation = validatePartitionCount(body.count)
  if (!partitionCountValidation.valid) {
    return badRequest(c, partitionCountValidation.error)
  }

  const admin = createAdmin(c.env)
  try {
    await admin.createPartitions(topic, body.count)
    return c.json({ success: true })
  } finally {
    await admin.close()
  }
})

/**
 * GET /admin/topics/:topic/offsets
 * Get partition offsets for a topic
 */
app.get('/admin/topics/:topic/offsets', async (c) => {
  const topic = c.req.param('topic')
  const admin = createAdmin(c.env)
  try {
    const offsets = await admin.listOffsets(topic)
    return c.json({
      topic,
      partitions: Array.from(offsets.entries()).map(([partition, info]) => ({
        partition,
        earliest: info.earliest,
        latest: info.latest,
      })),
    })
  } finally {
    await admin.close()
  }
})

/**
 * GET /admin/groups
 * List all consumer groups
 */
app.get('/admin/groups', async (c) => {
  const admin = createAdmin(c.env)
  try {
    const groups = await admin.listGroups()
    return c.json({ groups })
  } finally {
    await admin.close()
  }
})

/**
 * GET /admin/groups/:groupId
 * Describe a consumer group
 */
app.get('/admin/groups/:groupId', async (c) => {
  // Validate group ID
  const groupIdValidation = validateGroupId(c.req.param('groupId'))
  if (!groupIdValidation.valid) {
    return badRequest(c, groupIdValidation.error)
  }
  const groupId = groupIdValidation.value

  const admin = createAdmin(c.env)
  try {
    const description = await admin.describeGroup(groupId)
    return c.json(description)
  } finally {
    await admin.close()
  }
})

/**
 * DELETE /admin/groups/:groupId
 * Delete a consumer group
 */
app.delete('/admin/groups/:groupId', async (c) => {
  // Validate group ID
  const groupIdValidation = validateGroupId(c.req.param('groupId'))
  if (!groupIdValidation.valid) {
    return badRequest(c, groupIdValidation.error)
  }
  const groupId = groupIdValidation.value

  const admin = createAdmin(c.env)
  try {
    await admin.deleteGroup(groupId)
    return c.json({ success: true })
  } finally {
    await admin.close()
  }
})

// ============================================================================
// Error handling
// ============================================================================

app.onError((err, c) => {
  console.error('Request error:', err)
  return c.json(
    {
      error: err.message || 'Internal server error',
      code: 'INTERNAL_ERROR',
    },
    500
  )
})

app.notFound((c) => {
  return c.json(
    {
      error: 'Not found',
      code: 'NOT_FOUND',
    },
    404
  )
})

// Export the worker entrypoint
export default app
