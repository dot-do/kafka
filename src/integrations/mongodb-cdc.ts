/**
 * MongoDB CDC Integration
 *
 * Provides a Pipeline adapter that sends MongoDB CDC events to Kafdo topics.
 * This allows real-time streaming of document changes from MongoDB to Kafdo.
 */

import type { Env } from '../index'
import { createProducer } from '../api/producer'

// ============================================================================
// CDC Event Types (mirrors MongoDB's CDC schema)
// ============================================================================

/**
 * Namespace containing database and collection information
 */
export interface CDCNamespace {
  db: string
  coll: string
}

/**
 * Document key containing the _id field
 */
export interface DocumentKey {
  _id: string | { $oid: string }
}

/**
 * Update description with changed and removed fields
 */
export interface UpdateDescription {
  updatedFields: Record<string, unknown>
  removedFields: string[]
}

/**
 * Base interface for all CDC events
 */
export interface CDCEventBase {
  eventId: string
  operationType: 'insert' | 'update' | 'delete'
  ns: CDCNamespace
  documentKey: DocumentKey
  timestamp: string | Date
}

/**
 * Insert event - emitted when a document is inserted
 */
export interface InsertEvent extends CDCEventBase {
  operationType: 'insert'
  fullDocument: Record<string, unknown>
}

/**
 * Update event - emitted when a document is updated
 */
export interface UpdateEvent extends CDCEventBase {
  operationType: 'update'
  fullDocumentBeforeChange: Record<string, unknown> | null
  fullDocument: Record<string, unknown>
  updateDescription: UpdateDescription
}

/**
 * Delete event - emitted when a document is deleted
 */
export interface DeleteEvent extends CDCEventBase {
  operationType: 'delete'
  fullDocumentBeforeChange: Record<string, unknown> | null
}

/**
 * Union type for all CDC events
 */
export type CDCEvent = InsertEvent | UpdateEvent | DeleteEvent

// ============================================================================
// Pipeline Interface (compatible with MongoDB's CDCEmitter)
// ============================================================================

/**
 * Pipeline interface that CDCEmitter expects
 */
export interface Pipeline {
  send(data: unknown): Promise<void>
  sendBatch(data: unknown[]): Promise<void>
}

// ============================================================================
// Kafdo Pipeline Adapter
// ============================================================================

/**
 * Configuration for the Kafdo Pipeline adapter
 */
export interface KafdoPipelineConfig {
  /** Kafdo environment bindings */
  env: Env
  /** Topic name pattern for CDC events (default: 'cdc.{db}.{coll}') */
  topicPattern?: string
  /** Custom topic resolver function */
  topicResolver?: (event: CDCEvent) => string
  /** Include full document in message key (default: false) */
  includeFullDocumentInKey?: boolean
}

/**
 * Extract document ID as string from various formats
 */
function extractDocumentId(documentKey: DocumentKey): string {
  const id = documentKey._id
  if (typeof id === 'string') {
    return id
  }
  if (id && typeof id === 'object' && '$oid' in id) {
    return id.$oid
  }
  return JSON.stringify(id)
}

/**
 * KafdoPipeline - Implements Pipeline interface for MongoDB CDC integration
 *
 * This adapter receives CDC events from MongoDB and publishes them to Kafdo topics.
 */
export class KafdoPipeline implements Pipeline {
  private env: Env
  private topicPattern: string
  private topicResolver: (event: CDCEvent) => string
  private includeFullDocumentInKey: boolean

  constructor(config: KafdoPipelineConfig) {
    this.env = config.env
    this.topicPattern = config.topicPattern ?? 'cdc.{db}.{coll}'
    this.includeFullDocumentInKey = config.includeFullDocumentInKey ?? false

    // Default topic resolver uses the pattern
    this.topicResolver =
      config.topicResolver ??
      ((event: CDCEvent) => {
        return this.topicPattern
          .replace('{db}', event.ns.db)
          .replace('{coll}', event.ns.coll)
      })
  }

  /**
   * Resolve the topic name for a CDC event
   */
  private resolveTopic(event: CDCEvent): string {
    return this.topicResolver(event)
  }

  /**
   * Create message key from CDC event
   */
  private createMessageKey(event: CDCEvent): string {
    const docId = extractDocumentId(event.documentKey)
    if (this.includeFullDocumentInKey) {
      return `${event.ns.db}.${event.ns.coll}.${docId}`
    }
    return docId
  }

  /**
   * Send a single CDC event to Kafdo
   */
  async send(data: unknown): Promise<void> {
    const event = data as CDCEvent
    const topic = this.resolveTopic(event)
    const key = this.createMessageKey(event)

    const producer = createProducer(this.env)
    try {
      await producer.send({
        topic,
        key,
        value: event,
        headers: {
          'cdc-operation': event.operationType,
          'cdc-database': event.ns.db,
          'cdc-collection': event.ns.coll,
          'cdc-event-id': event.eventId,
        },
      })
    } finally {
      await producer.close()
    }
  }

  /**
   * Send a batch of CDC events to Kafdo
   */
  async sendBatch(data: unknown[]): Promise<void> {
    const events = data as CDCEvent[]
    if (events.length === 0) return

    const producer = createProducer(this.env)
    try {
      const records = events.map((event) => ({
        topic: this.resolveTopic(event),
        key: this.createMessageKey(event),
        value: event,
        headers: {
          'cdc-operation': event.operationType,
          'cdc-database': event.ns.db,
          'cdc-collection': event.ns.coll,
          'cdc-event-id': event.eventId,
        },
      }))

      await producer.sendBatch(records)
    } finally {
      await producer.close()
    }
  }
}

// ============================================================================
// Factory Functions
// ============================================================================

/**
 * Create a new Kafdo Pipeline for MongoDB CDC
 */
export function createKafdoPipeline(config: KafdoPipelineConfig): KafdoPipeline {
  return new KafdoPipeline(config)
}

/**
 * Create a Pipeline with a fixed topic
 */
export function createFixedTopicPipeline(env: Env, topic: string): KafdoPipeline {
  return new KafdoPipeline({
    env,
    topicResolver: () => topic,
  })
}

/**
 * Create a Pipeline with database-level topics
 */
export function createDatabaseTopicPipeline(env: Env): KafdoPipeline {
  return new KafdoPipeline({
    env,
    topicPattern: 'cdc.{db}',
  })
}

/**
 * Create a Pipeline with collection-level topics
 */
export function createCollectionTopicPipeline(env: Env): KafdoPipeline {
  return new KafdoPipeline({
    env,
    topicPattern: 'cdc.{db}.{coll}',
  })
}

// ============================================================================
// CDC Consumer Helpers
// ============================================================================

/**
 * Configuration for CDC consumer
 */
export interface CDCConsumerConfig {
  /** Database name to listen to */
  database: string
  /** Collection name to listen to (optional - all collections if not specified) */
  collection?: string
  /** Consumer group ID */
  groupId: string
  /** Handler for insert events */
  onInsert?: (event: InsertEvent) => Promise<void>
  /** Handler for update events */
  onUpdate?: (event: UpdateEvent) => Promise<void>
  /** Handler for delete events */
  onDelete?: (event: DeleteEvent) => Promise<void>
  /** Handler for all events */
  onEvent?: (event: CDCEvent) => Promise<void>
}

/**
 * Process CDC events from Kafdo messages
 */
export async function processCDCMessage(
  message: { value: unknown; headers?: Record<string, string> },
  config: CDCConsumerConfig
): Promise<void> {
  const event = message.value as CDCEvent

  // Check if this event is for the configured database/collection
  if (event.ns.db !== config.database) {
    return
  }
  if (config.collection && event.ns.coll !== config.collection) {
    return
  }

  // Call the appropriate handler
  if (config.onEvent) {
    await config.onEvent(event)
  }

  switch (event.operationType) {
    case 'insert':
      if (config.onInsert) {
        await config.onInsert(event as InsertEvent)
      }
      break
    case 'update':
      if (config.onUpdate) {
        await config.onUpdate(event as UpdateEvent)
      }
      break
    case 'delete':
      if (config.onDelete) {
        await config.onDelete(event as DeleteEvent)
      }
      break
  }
}

/**
 * Type guard for insert events
 */
export function isInsertEvent(event: CDCEvent): event is InsertEvent {
  return event.operationType === 'insert'
}

/**
 * Type guard for update events
 */
export function isUpdateEvent(event: CDCEvent): event is UpdateEvent {
  return event.operationType === 'update'
}

/**
 * Type guard for delete events
 */
export function isDeleteEvent(event: CDCEvent): event is DeleteEvent {
  return event.operationType === 'delete'
}
