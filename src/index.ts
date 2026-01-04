import { Hono } from 'hono'

// Re-export Durable Objects
export { TopicPartitionDO } from './durable-objects/topic-partition'
export { ConsumerGroupDO } from './durable-objects/consumer-group'
export { ClusterMetadataDO } from './durable-objects/cluster-metadata'

// Environment interface for Cloudflare Workers
export interface Env {
  TOPIC_PARTITION: DurableObjectNamespace
  CONSUMER_GROUP: DurableObjectNamespace
  CLUSTER_METADATA: DurableObjectNamespace
}

// Main Hono application for HTTP routing
const app = new Hono<{ Bindings: Env }>()

app.get('/', (c) => {
  return c.json({
    name: 'kafdo',
    description: 'Kafka-compatible streaming platform on Cloudflare Workers',
    status: 'ok'
  })
})

app.get('/health', (c) => {
  return c.json({ status: 'healthy' })
})

// Export the worker entrypoint
export default app
