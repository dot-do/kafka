# Kafdo Implementation Plan

> Kafka-compatible streaming platform on Cloudflare Workers + Durable Object SQLite

## Vision

Kafdo brings Kafka semantics to the edge with:
- **Topics** → Named Durable Object namespaces
- **Partitions** → Individual DOs with SQLite storage
- **Consumer Groups** → Coordinator DOs for stateful consumption
- **Offsets** → SQLite `INTEGER PRIMARY KEY` for ACID guarantees
- **Log Compaction** → Native key-value semantics via `ON CONFLICT REPLACE`

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         KAFDO ARCHITECTURE                           │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐          │
│  │   Producer   │    │   Consumer   │    │    Admin     │          │
│  │     API      │    │     API      │    │     API      │          │
│  └──────┬───────┘    └──────┬───────┘    └──────┬───────┘          │
│         │                   │                   │                   │
│         └───────────────────┼───────────────────┘                   │
│                             │                                        │
│                    ┌────────▼────────┐                              │
│                    │   RPC Router    │                              │
│                    │  (HTTP/WS/RPC)  │                              │
│                    └────────┬────────┘                              │
│                             │                                        │
│    ┌────────────────────────┼────────────────────────┐              │
│    │                        │                        │              │
│    ▼                        ▼                        ▼              │
│ ┌──────────────┐    ┌──────────────┐    ┌──────────────┐           │
│ │TopicPartition│    │ConsumerGroup │    │ClusterMeta   │           │
│ │     DO       │    │     DO       │    │     DO       │           │
│ │  ┌────────┐  │    │  ┌────────┐  │    │  ┌────────┐  │           │
│ │  │ SQLite │  │    │  │ SQLite │  │    │  │ SQLite │  │           │
│ │  │Messages│  │    │  │Offsets │  │    │  │ Topics │  │           │
│ │  └────────┘  │    │  │Members │  │    │  │Partns  │  │           │
│ └──────────────┘    └────────────┘  │    └──────────────┘           │
│                                                                      │
│  INTEGRATIONS                                                        │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐           │
│  │  MondoDB     │    │     R2       │    │   Webhook    │           │
│  │    CDC       │    │   Events     │    │  Consumers   │           │
│  └──────────────┘    └──────────────┘    └──────────────┘           │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

## Module Breakdown

### Phase 1: Core Foundation
1. **Schema & Types** - TypeScript interfaces, SQLite schema definitions
2. **TopicPartitionDO** - Message storage, append, read, watermarks
3. **Producer API** - send(), sendBatch(), transactional producer
4. **Consumer API** - poll(), commit(), async iterator

### Phase 2: Coordination
5. **ConsumerGroupDO** - Member registry, partition assignment, rebalancing
6. **ClusterMetadataDO** - Topic registry, partition mapping
7. **Admin API** - createTopic(), deleteTopic(), describeGroup()

### Phase 3: Transport
8. **RPC Router** - HTTP endpoints, request routing
9. **WebSocket Transport** - Long-lived connections, streaming
10. **Service Bindings** - Zero-latency worker-to-worker

### Phase 4: Integrations
11. **MondoDB CDC Connector** - Change event emission
12. **R2 Event Bridge** - Object notification forwarding
13. **Webhook Consumer** - External system integration

### Phase 5: Advanced Features
14. **Log Compaction** - Key-based deduplication
15. **Retention Policies** - Time/size-based cleanup
16. **Exactly-Once Semantics** - Idempotent producers, transactional consumers

### Phase 6: Client SDK
17. **Kafdo Client** - JavaScript/TypeScript client library
18. **Connection Management** - Pooling, reconnection
19. **Batch Operations** - Client-side batching

## TDD Structure

Each module follows Red-Green-Refactor:

```
Module X
├── Issue X.1: RED - Write failing tests for Module X
├── Issue X.2: GREEN - Implement Module X to pass tests
└── Issue X.3: REFACTOR - Optimize and clean up Module X
```

## Scale Targets

- **Messages/second**: 100,000+ (across partitions)
- **Partitions/topic**: 256
- **Consumer groups**: Unlimited
- **Message retention**: Configurable (default 7 days)
- **Message size**: Up to 1MB

## Key Files

```
src/
├── index.ts                    # Worker entrypoint
├── types/
│   ├── producer.ts             # Producer types
│   ├── consumer.ts             # Consumer types
│   ├── admin.ts                # Admin types
│   └── records.ts              # Message record types
├── schema/
│   ├── messages.sql            # Message table schema
│   ├── offsets.sql             # Offset table schema
│   └── metadata.sql            # Cluster metadata schema
├── durable-objects/
│   ├── topic-partition.ts      # TopicPartitionDO
│   ├── consumer-group.ts       # ConsumerGroupDO
│   └── cluster-metadata.ts     # ClusterMetadataDO
├── api/
│   ├── producer.ts             # Producer API
│   ├── consumer.ts             # Consumer API
│   └── admin.ts                # Admin API
├── rpc/
│   ├── router.ts               # RPC router
│   ├── http.ts                 # HTTP transport
│   └── websocket.ts            # WebSocket transport
├── integrations/
│   ├── mondodb-cdc.ts          # MondoDB CDC connector
│   ├── r2-bridge.ts            # R2 event bridge
│   └── webhook.ts              # Webhook consumer
├── client/
│   ├── kafdo.ts                # Client SDK
│   ├── producer.ts             # Producer client
│   └── consumer.ts             # Consumer client
└── utils/
    ├── partitioner.ts          # Partition assignment
    ├── serializer.ts           # Message serialization
    └── compaction.ts           # Log compaction
```

## Dependencies

```json
{
  "dependencies": {
    "@cloudflare/workers-types": "^4.x",
    "hono": "^4.x"
  },
  "devDependencies": {
    "vitest": "^1.x",
    "wrangler": "^3.x",
    "@cloudflare/vitest-pool-workers": "^0.x"
  }
}
```
