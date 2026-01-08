# kafka.do

> Event Streaming. Edge-Native. Natural Language. AI-First.

Confluent charges $0.10/GB for data in, $0.30/GB for data out, plus cluster fees, connector fees, and support tiers. Amazon MSK requires provisioning brokers, managing partitions, and paying for storage you don't use. Self-hosted Kafka means ZooKeeper, JVM tuning, and 3am pager alerts.

**kafka.do** is the open-source alternative. Kafka semantics without Kafka operations. Deploys in seconds, not weeks. Streams that just work.

## AI-Native API

```typescript
import { kafka } from 'kafka.do'           // Full SDK
import { kafka } from 'kafka.do/tiny'      // Minimal client
import { kafka } from 'kafka.do/streams'   // Streams DSL only
```

Natural language for event streaming:

```typescript
import { kafka } from 'kafka.do'

// Talk to it like a colleague
await kafka`stream orders`.produce({ orderId: '123', amount: 99.99 })
await kafka`stream user events to analytics`
await kafka`replay orders from yesterday`

// Consume naturally
for await (const record of kafka`consume from orders group order-processor`) {
  console.log(`Processing ${record.key}: ${record.value.amount}`)
}

// Chain like sentences
await kafka`orders`
  .filter(o => o.amount > 100)
  .map(o => ({ ...o, tier: 'premium' }))
  .to(`high-value-orders`)
```

## The Problem

Kafka infrastructure is complex and expensive:

| What They Charge | The Reality |
|------------------|-------------|
| **Confluent Cloud** | $0.10/GB in, $0.30/GB out, $1.50/hr base |
| **Amazon MSK** | $0.10/hr per broker + storage + networking |
| **Self-Hosted** | 3+ brokers, ZooKeeper, DevOps team |
| **Connectors** | $0.25/hr per connector (Confluent) |
| **Schema Registry** | Extra service, extra cost, extra complexity |
| **Operations** | Partition rebalancing, consumer lag, retention tuning |

### The Managed Kafka Tax

Modern "managed" Kafka still requires:

- Provisioning cluster capacity upfront
- Managing partition counts per topic
- Monitoring consumer lag and rebalancing
- Configuring retention policies everywhere
- Debugging serialization issues
- Running separate schema registries

### The Developer Experience Gap

```typescript
// This is what Kafka looks like today
const producer = new Kafka({
  clientId: 'my-app',
  brokers: ['localhost:9092'],
  ssl: true,
  sasl: { mechanism: 'plain', username: 'user', password: 'pass' }
}).producer()

await producer.connect()
await producer.send({
  topic: 'orders',
  messages: [{
    key: 'order-123',
    value: JSON.stringify({ orderId: '123', amount: 99.99 }),
    headers: { 'correlation-id': 'abc' }
  }]
})
await producer.disconnect()
```

Six lines to send one message. Connection management. Manual serialization. This is 2024.

## The Solution

**kafka.do** reimagines event streaming for developers:

```
Confluent/MSK/Self-Hosted         kafka.do
-----------------------------------------------------------------
Provision clusters                Just use it
$0.10-0.30/GB transfer            Pay for what you stream
Manage partitions                 Auto-scaling partitions
ZooKeeper/KRaft complexity        No coordination layer
Connection pooling                Stateless HTTP or DO RPC
Manual serialization              Native JSON/TypeScript
Separate schema registry          Schemas in code
Broker affinity                   Global edge distribution
Consumer group rebalancing        Automatic coordination
Retention configuration           Tiered storage automatic
```

## One-Click Deploy

```bash
npx create-dotdo kafka
```

A production-ready event streaming platform. Running on infrastructure you control.

```typescript
import { Kafka } from 'kafka.do'

export default Kafka({
  name: 'my-streams',
  domain: 'streams.my-startup.com',
})
```

## Features

### Producing Messages

```typescript
// Just say it
await kafka`stream order 123 to orders topic`
await kafka`produce user signup to events`
await kafka`send payment confirmed to transactions`

// Natural with data
await kafka`orders`.produce({ orderId: '123', amount: 99.99 })
await kafka`events`.produce({ type: 'signup', userId: 'u-456' })

// Batch naturally
await kafka`orders`.produceBatch([
  { orderId: '124', amount: 49.99 },
  { orderId: '125', amount: 149.99 }
])
```

### Consuming Messages

```typescript
// Consume with natural language
for await (const record of kafka`consume orders as order-processor`) {
  console.log(`Order: ${record.value.orderId}`)
}

// Consumer groups are just words
for await (const record of kafka`orders group analytics-team`) {
  await analytics.track(record.value)
}

// Start from beginning
for await (const record of kafka`orders from beginning`) {
  await reprocess(record)
}

// Replay from timestamp
for await (const record of kafka`orders since yesterday`) {
  console.log(record)
}
```

### Stream Processing

```typescript
// Filter, map, route - all natural
await kafka`orders`
  .filter(o => o.amount > 100)
  .to(`high-value-orders`)

// Enrich on the fly
await kafka`orders`
  .map(async o => ({
    ...o,
    customer: await customers.get(o.customerId)
  }))
  .to(`enriched-orders`)

// Fan out to multiple topics
await kafka`orders`
  .branch(
    [o => o.region === 'us', `us-orders`],
    [o => o.region === 'eu', `eu-orders`],
    [() => true, `other-orders`]
  )

// Aggregate windows
await kafka`events`
  .window(`5 minutes`)
  .groupBy(e => e.userId)
  .count()
  .to(`user-activity`)
```

### Topic Administration

```typescript
// Create topics naturally
await kafka`create topic orders with 3 partitions`
await kafka`create topic logs with 7-day retention`
await kafka`create topic events partitioned by user_id`

// Describe topics
const info = await kafka`describe orders`
console.log(`Partitions: ${info.partitions}`)

// Modify topics
await kafka`add 3 partitions to orders`
await kafka`set orders retention to 30 days`

// List and manage
const topics = await kafka`list topics`
await kafka`delete topic old-events`
```

### Consumer Groups

```typescript
// Consumer groups are automatic
for await (const msg of kafka`orders group my-processor`) {
  // Automatic partition assignment
  // Automatic offset commits
  // Automatic rebalancing
}

// Check group status
const status = await kafka`describe group my-processor`
console.log(`Members: ${status.members}, Lag: ${status.lag}`)

// Reset offsets naturally
await kafka`reset group my-processor to earliest`
await kafka`reset group my-processor to yesterday 9am`
```

## Promise Pipelining

Chain operations without waiting:

```typescript
// All in one network round trip
const processed = await kafka`orders`
  .filter(o => o.status === 'pending')
  .map(o => process(o))
  .tap(o => kafka`processed-orders`.produce(o))
  .collect()

// Fan out and collect
const [analytics, warehouse, notifications] = await Promise.all([
  kafka`orders`.map(o => o.metrics).to(`analytics`),
  kafka`orders`.map(o => o.inventory).to(`warehouse`),
  kafka`orders`.map(o => o.alert).to(`notifications`)
])
```

## Architecture

### Edge-Native Design

```
Message Flow:

Producer --> Cloudflare Edge --> Partition DO --> SQLite
                  |                    |
             Global PoP          Strong consistency
                              (per-partition ordering)

Consumer <-- Cloudflare Edge <-- Partition DO <-- SQLite
                  |                    |
            Nearest edge          Durable storage
```

### Durable Object per Partition

```
TopicDO (metadata, partitioning)
  |
  +-- PartitionDO-0 (messages, offsets)
  |     |-- SQLite: Message log (hot)
  |     +-- R2: Archived segments (cold)
  |
  +-- PartitionDO-1
  |     |-- SQLite: Message log
  |     +-- R2: Archived segments
  |
  +-- PartitionDO-N
        |-- ...

ConsumerGroupDO (membership, assignments)
  |-- SQLite: Member state, offsets
```

### Storage Tiers

| Tier | Storage | Latency | Use Case |
|------|---------|---------|----------|
| **Hot** | SQLite | <10ms | Recent messages, active consumption |
| **Warm** | R2 | <100ms | Historical replay, batch processing |
| **Cold** | R2 Archive | <1s | Compliance, long-term retention |

Automatic tiering. No configuration needed.

## vs Confluent/MSK/Self-Hosted

| Feature | Confluent Cloud | Amazon MSK | kafka.do |
|---------|----------------|------------|----------|
| **Setup** | Minutes + config | Hours + VPC | Seconds |
| **Pricing** | $0.10-0.30/GB | Per broker/hour | Per message |
| **Scaling** | Manual partitions | Provision brokers | Automatic |
| **Global** | Multi-region extra | Single region | Every edge |
| **Operations** | Managed (mostly) | Semi-managed | Zero-ops |
| **API** | Kafka protocol | Kafka protocol | Natural language |
| **DX** | Complex SDKs | Complex SDKs | Tagged templates |
| **Lock-in** | Confluent ecosystem | AWS ecosystem | MIT licensed |

## Use Cases

### Event Sourcing

```typescript
// Append events naturally
await kafka`user-events`.produce({
  type: 'account.created',
  userId: 'u-123',
  email: 'alice@example.com'
})

// Replay to rebuild state
const user = await kafka`user-events`
  .filter(e => e.userId === 'u-123')
  .reduce((state, event) => applyEvent(state, event), {})
```

### Real-time Analytics

```typescript
// Stream to analytics
await kafka`page-views`
  .window(`1 minute`)
  .groupBy(v => v.page)
  .count()
  .to(`page-view-counts`)

// Query the stream
const topPages = await kafka`page-view-counts`
  .filter(c => c.count > 1000)
  .collect()
```

### Microservice Communication

```typescript
// Order service produces
await kafka`orders`.produce(order)

// Inventory service consumes
for await (const order of kafka`orders group inventory`) {
  await inventory.reserve(order.items)
}

// Shipping service consumes (same topic, different group)
for await (const order of kafka`orders group shipping`) {
  await shipping.schedule(order)
}
```

### Change Data Capture

```typescript
// Stream database changes
await kafka`cdc.users`
  .filter(change => change.op === 'insert')
  .map(change => change.after)
  .to(`new-users`)

// Sync to search index
for await (const user of kafka`new-users group search-indexer`) {
  await elasticsearch.index('users', user)
}
```

## Integrations

### MongoDB CDC

```typescript
// Stream MongoDB changes to kafka.do
await kafka`stream mongodb changes to cdc.users`

// Or configure explicitly
await kafka`create cdc pipeline from mongodb.users`
```

### R2 Event Bridge

```typescript
// Stream R2 events
for await (const event of kafka`r2.my-bucket`) {
  if (event.type === 'object-created') {
    await processUpload(event.object)
  }
}
```

### Webhooks

```typescript
// Ingest webhooks to streams
await kafka`stripe-events`.fromWebhook('/webhooks/stripe')
await kafka`github-events`.fromWebhook('/webhooks/github')
```

## Configuration

### Wrangler Configuration

```toml
name = "my-kafka-app"
main = "src/index.ts"
compatibility_date = "2024-01-01"
compatibility_flags = ["nodejs_compat"]

[durable_objects]
bindings = [
  { name = "TOPIC_PARTITION", class_name = "TopicPartitionDO" },
  { name = "CONSUMER_GROUP", class_name = "ConsumerGroupDO" },
  { name = "CLUSTER_METADATA", class_name = "ClusterMetadataDO" }
]

[[migrations]]
tag = "v1"
new_sqlite_classes = ["TopicPartitionDO", "ConsumerGroupDO", "ClusterMetadataDO"]
```

### Environment Type

```typescript
interface Env {
  TOPIC_PARTITION: DurableObjectNamespace
  CONSUMER_GROUP: DurableObjectNamespace
  CLUSTER_METADATA: DurableObjectNamespace
}
```

## Why Open Source for Event Streaming?

### 1. No Vendor Lock-in

Confluent and AWS want you dependent on their ecosystems. Open source means:
- Standard semantics, edge-native implementation
- No broker affinity or cluster dependencies
- Community-driven development
- MIT licensed, forever

### 2. Developer Experience First

Kafka was built for infrastructure teams. kafka.do is built for developers:
- Natural language over configuration objects
- Tagged templates over SDK boilerplate
- Streams DSL for processing
- TypeScript-native with full types

### 3. Edge-Native Performance

Traditional Kafka requires round trips to regional clusters. kafka.do:
- Runs at the edge, closest to your users
- Per-partition Durable Objects for isolation
- SQLite for hot data, R2 for archives
- Global by default

### 4. Cost Liberation

Stop paying for idle brokers and data transfer:
- Pay per message, not per broker
- No ingress/egress fees within Cloudflare
- No reserved capacity
- No connector licensing

## Roadmap

### Core Streaming
- [x] Produce messages
- [x] Consume with groups
- [x] Partition assignment
- [x] Offset management
- [x] Batch operations
- [ ] Exactly-once semantics
- [ ] Transactions
- [ ] Compacted topics

### Stream Processing
- [x] Filter/Map/Branch
- [x] Windowing
- [x] Aggregations
- [ ] Joins
- [ ] State stores
- [ ] Interactive queries

### Integrations
- [x] MongoDB CDC
- [x] R2 Event Bridge
- [x] Webhook ingestion
- [ ] PostgreSQL CDC
- [ ] MySQL CDC
- [ ] Supabase Realtime

### Operations
- [x] Topic management
- [x] Consumer group management
- [x] Offset management
- [ ] Metrics export
- [ ] Dead letter queues
- [ ] Schema registry

## Contributing

kafka.do is open source under the MIT license.

```bash
git clone https://github.com/dotdo/kafka.do
cd kafka.do
pnpm install
pnpm test
```

## License

MIT License - Stream freely.

---

<p align="center">
  <strong>Kafka without the ops.</strong>
  <br />
  Edge-native. Natural language. Zero infrastructure.
  <br /><br />
  <a href="https://kafka.do">Website</a> |
  <a href="https://docs.kafka.do">Docs</a> |
  <a href="https://discord.gg/dotdo">Discord</a> |
  <a href="https://github.com/dotdo/kafka.do">GitHub</a>
</p>
