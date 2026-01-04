import { Hono } from 'hono'
import { cors } from 'hono/cors'
import { createProducer } from './api/producer'
import { createAdmin } from './api/admin'

// Re-export Durable Objects
export { TopicPartitionDO } from './durable-objects/topic-partition'
export { ConsumerGroupDO } from './durable-objects/consumer-group'
export { ClusterMetadataDO } from './durable-objects/cluster-metadata'

// Re-export API classes
export { KafdoProducer, createProducer } from './api/producer'
export { KafdoConsumer, createConsumer } from './api/consumer'
export { KafdoAdmin, createAdmin } from './api/admin'

// Environment interface for Cloudflare Workers
export interface Env {
  TOPIC_PARTITION: DurableObjectNamespace
  CONSUMER_GROUP: DurableObjectNamespace
  CLUSTER_METADATA: DurableObjectNamespace
}

// Main Hono application for HTTP routing
const app = new Hono<{ Bindings: Env }>()

// Enable CORS for client access
app.use('*', cors())

// Health and info endpoints
app.get('/', (c) => {
  return c.json({
    name: 'kafdo',
    description: 'Kafka-compatible streaming platform on Cloudflare Workers',
    version: '0.1.0',
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
  const topic = c.req.param('topic')
  const body = await c.req.json<{
    key?: string
    value: unknown
    partition?: number
    headers?: Record<string, string>
  }>()

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
  const topic = c.req.param('topic')
  const body = await c.req.json<{
    records: Array<{
      key?: string
      value: unknown
      partition?: number
      headers?: Record<string, string>
    }>
  }>()

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
  const topic = c.req.param('topic')
  const partition = parseInt(c.req.param('partition'), 10)
  const offset = parseInt(c.req.query('offset') ?? '0', 10)
  const limit = parseInt(c.req.query('limit') ?? '100', 10)

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
  const topic = c.req.param('topic')
  const partition = parseInt(c.req.param('partition'), 10)

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
  const groupId = c.req.param('groupId')
  const body = await c.req.json<{
    clientId: string
    topics: string[]
    sessionTimeoutMs?: number
  }>()

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
  const groupId = c.req.param('groupId')
  const body = await c.req.json<{
    memberId: string
    generationId: number
  }>()

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
  const groupId = c.req.param('groupId')
  const body = await c.req.json<{
    memberId: string
    offsets: Array<{ topic: string; partition: number; offset: number }>
  }>()

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
  const groupId = c.req.param('groupId')
  const body = await c.req.json<{ memberId: string }>()

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
  const groupId = c.req.param('groupId')

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
  const body = await c.req.json<{
    topic: string
    partitions: number
    config?: Record<string, string>
  }>()

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
  const topic = c.req.param('topic')
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
  const topic = c.req.param('topic')
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
  const topic = c.req.param('topic')
  const body = await c.req.json<{ count: number }>()

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
  const groupId = c.req.param('groupId')
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
  const groupId = c.req.param('groupId')
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
