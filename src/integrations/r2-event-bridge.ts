/**
 * R2 Event Bridge Integration
 *
 * Streams R2 object events (creates, deletes) to Kafdo topics.
 * Can be used as a Queue consumer for R2 event notifications.
 */

import type { Env } from '../index'
import { createProducer } from '../api/producer'

// ============================================================================
// R2 Event Types
// ============================================================================

/**
 * R2 object metadata
 */
export interface R2ObjectMetadata {
  /** Object key (path) */
  key: string
  /** Object size in bytes */
  size: number
  /** ETag of the object */
  etag: string
  /** HTTP metadata */
  httpMetadata?: {
    contentType?: string
    contentLanguage?: string
    contentDisposition?: string
    contentEncoding?: string
    cacheControl?: string
    cacheExpiry?: string
  }
  /** Custom metadata */
  customMetadata?: Record<string, string>
  /** Upload timestamp */
  uploaded: string
}

/**
 * Base R2 event
 */
export interface R2EventBase {
  /** Event type */
  type: 'object-created' | 'object-deleted'
  /** Event timestamp */
  timestamp: string
  /** R2 bucket name */
  bucket: string
  /** Account ID */
  account: string
}

/**
 * R2 object created event
 */
export interface R2ObjectCreatedEvent extends R2EventBase {
  type: 'object-created'
  /** Object metadata */
  object: R2ObjectMetadata
  /** Copy source if this was a copy operation */
  copySource?: {
    bucket: string
    key: string
  }
}

/**
 * R2 object deleted event
 */
export interface R2ObjectDeletedEvent extends R2EventBase {
  type: 'object-deleted'
  /** Deleted object key */
  key: string
  /** Version ID if versioning enabled */
  versionId?: string
}

/**
 * Union type for all R2 events
 */
export type R2Event = R2ObjectCreatedEvent | R2ObjectDeletedEvent

// ============================================================================
// R2 Event Bridge
// ============================================================================

/**
 * Configuration for R2 Event Bridge
 */
export interface R2EventBridgeConfig {
  /** Kafdo environment bindings */
  env: Env
  /** Topic name pattern (default: 'r2.{bucket}') */
  topicPattern?: string
  /** Custom topic resolver */
  topicResolver?: (event: R2Event) => string
  /** Filter by bucket name */
  bucketFilter?: string | string[]
  /** Filter by key prefix */
  keyPrefixFilter?: string
  /** Include object metadata in messages */
  includeMetadata?: boolean
}

/**
 * R2EventBridge - Streams R2 events to Kafdo topics
 */
export class R2EventBridge {
  private env: Env
  private topicPattern: string
  private topicResolver: (event: R2Event) => string
  private bucketFilter: Set<string> | null
  private keyPrefixFilter: string | null

  constructor(config: R2EventBridgeConfig) {
    this.env = config.env
    this.topicPattern = config.topicPattern ?? 'r2.{bucket}'

    // Set up bucket filter
    if (config.bucketFilter) {
      this.bucketFilter = new Set(
        Array.isArray(config.bucketFilter) ? config.bucketFilter : [config.bucketFilter]
      )
    } else {
      this.bucketFilter = null
    }

    this.keyPrefixFilter = config.keyPrefixFilter ?? null

    // Default topic resolver
    this.topicResolver =
      config.topicResolver ??
      ((event: R2Event) => {
        return this.topicPattern.replace('{bucket}', event.bucket)
      })
  }

  /**
   * Check if an event should be processed based on filters
   */
  private shouldProcess(event: R2Event): boolean {
    // Check bucket filter
    if (this.bucketFilter && !this.bucketFilter.has(event.bucket)) {
      return false
    }

    // Check key prefix filter
    if (this.keyPrefixFilter) {
      const key = event.type === 'object-created' ? event.object.key : event.key
      if (!key.startsWith(this.keyPrefixFilter)) {
        return false
      }
    }

    return true
  }

  /**
   * Create message key from R2 event
   */
  private createMessageKey(event: R2Event): string {
    const key = event.type === 'object-created' ? event.object.key : event.key
    return `${event.bucket}/${key}`
  }

  /**
   * Process a single R2 event
   */
  async processEvent(event: R2Event): Promise<void> {
    if (!this.shouldProcess(event)) {
      return
    }

    const topic = this.topicResolver(event)
    const key = this.createMessageKey(event)

    const producer = createProducer(this.env)
    try {
      await producer.send({
        topic,
        key,
        value: event,
        headers: {
          'r2-event-type': event.type,
          'r2-bucket': event.bucket,
          'r2-account': event.account,
        },
      })
    } finally {
      await producer.close()
    }
  }

  /**
   * Process a batch of R2 events
   */
  async processBatch(events: R2Event[]): Promise<void> {
    const filteredEvents = events.filter((e) => this.shouldProcess(e))
    if (filteredEvents.length === 0) {
      return
    }

    const producer = createProducer(this.env)
    try {
      const records = filteredEvents.map((event) => ({
        topic: this.topicResolver(event),
        key: this.createMessageKey(event),
        value: event,
        headers: {
          'r2-event-type': event.type,
          'r2-bucket': event.bucket,
          'r2-account': event.account,
        },
      }))

      await producer.sendBatch(records)
    } finally {
      await producer.close()
    }
  }

  /**
   * Create an event from R2 Queue message
   * Use this in your Queue consumer handler
   */
  static parseQueueMessage(message: {
    body: unknown
    timestamp: Date
    id: string
  }): R2Event | null {
    const body = message.body as Record<string, unknown>

    if (!body || typeof body !== 'object') {
      return null
    }

    const eventType = body.type as string
    const bucket = body.bucket as string
    const account = body.account as string

    if (!eventType || !bucket) {
      return null
    }

    if (eventType === 'object-created') {
      return {
        type: 'object-created',
        timestamp: message.timestamp.toISOString(),
        bucket,
        account,
        object: body.object as R2ObjectMetadata,
        copySource: body.copySource as R2ObjectCreatedEvent['copySource'],
      }
    }

    if (eventType === 'object-deleted') {
      return {
        type: 'object-deleted',
        timestamp: message.timestamp.toISOString(),
        bucket,
        account,
        key: body.key as string,
        versionId: body.versionId as string | undefined,
      }
    }

    return null
  }
}

// ============================================================================
// Factory Functions
// ============================================================================

/**
 * Create a new R2 Event Bridge
 */
export function createR2EventBridge(config: R2EventBridgeConfig): R2EventBridge {
  return new R2EventBridge(config)
}

// ============================================================================
// Consumer Helpers
// ============================================================================

/**
 * Configuration for R2 event consumer
 */
export interface R2EventConsumerConfig {
  /** Handler for object created events */
  onObjectCreated?: (event: R2ObjectCreatedEvent) => Promise<void>
  /** Handler for object deleted events */
  onObjectDeleted?: (event: R2ObjectDeletedEvent) => Promise<void>
  /** Handler for all events */
  onEvent?: (event: R2Event) => Promise<void>
  /** Filter by bucket name */
  bucketFilter?: string | string[]
  /** Filter by key prefix */
  keyPrefixFilter?: string
}

/**
 * Process R2 events from Kafdo messages
 */
export async function processR2Event(
  message: { value: unknown; headers?: Record<string, string> },
  config: R2EventConsumerConfig
): Promise<void> {
  const event = message.value as R2Event

  // Check bucket filter
  if (config.bucketFilter) {
    const buckets = Array.isArray(config.bucketFilter)
      ? config.bucketFilter
      : [config.bucketFilter]
    if (!buckets.includes(event.bucket)) {
      return
    }
  }

  // Check key prefix filter
  if (config.keyPrefixFilter) {
    const key = event.type === 'object-created' ? event.object.key : event.key
    if (!key.startsWith(config.keyPrefixFilter)) {
      return
    }
  }

  // Call handlers
  if (config.onEvent) {
    await config.onEvent(event)
  }

  switch (event.type) {
    case 'object-created':
      if (config.onObjectCreated) {
        await config.onObjectCreated(event)
      }
      break
    case 'object-deleted':
      if (config.onObjectDeleted) {
        await config.onObjectDeleted(event)
      }
      break
  }
}

/**
 * Type guard for object created events
 */
export function isR2ObjectCreated(event: R2Event): event is R2ObjectCreatedEvent {
  return event.type === 'object-created'
}

/**
 * Type guard for object deleted events
 */
export function isR2ObjectDeleted(event: R2Event): event is R2ObjectDeletedEvent {
  return event.type === 'object-deleted'
}
