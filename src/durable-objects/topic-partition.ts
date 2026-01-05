/**
 * TopicPartitionDO - Durable Object for managing topic partition storage
 * 
 * This DO handles:
 * - Message storage with sequential offsets
 * - Read operations from specific offsets
 * - Watermark tracking (high watermark, log start offset)
 * - Batch append operations
 */

import { DurableObject } from 'cloudflare:workers'
import { Hono } from 'hono'
import { initializeSchema } from '../schema/schema'
import { safeJsonParse } from '../utils'

/**
 * Message to append to the partition
 */
export interface AppendMessage {
  key?: string
  value: string | ArrayBuffer
  headers?: Record<string, string>
  producerId?: string
  sequence?: number
}

/**
 * Result of appending a message
 */
export interface AppendResult {
  offset: number
  timestamp: number
}

/**
 * Options for reading messages
 */
export interface ReadOptions {
  fromOffset: number
  maxRecords: number
}

/**
 * A message read from the partition
 */
export interface PartitionMessage {
  offset: number
  key: string | null
  value: string
  headers: Record<string, string>
  timestamp: number
}

/**
 * Offset information for the partition
 */
export interface OffsetInfo {
  earliest: number
  latest: number
}

export class TopicPartitionDO extends DurableObject<Record<string, unknown>> {
  private sql: SqlStorage
  private initialized = false
  private app: Hono

  constructor(ctx: DurableObjectState, env: Record<string, unknown>) {
    super(ctx, env)
    this.sql = ctx.storage.sql

    // Initialize Hono app for HTTP interface
    this.app = new Hono()
    this.setupRoutes()
  }

  private setupRoutes() {
    this.app.post('/append', async (c) => {
      const body = await c.req.json<AppendMessage>()
      const result = await this.append(body)
      return c.json(result)
    })

    this.app.post('/append-batch', async (c) => {
      const body = await c.req.json<AppendMessage[]>()
      const results = await this.appendBatch(body)
      return c.json(results)
    })

    this.app.get('/read', async (c) => {
      const fromOffset = parseInt(c.req.query('offset') ?? '0', 10)
      const maxRecords = parseInt(c.req.query('limit') ?? '100', 10)
      const messages = await this.read({ fromOffset, maxRecords })
      return c.json(messages)
    })

    this.app.get('/offsets', async (c) => {
      const info = await this.getOffsets()
      return c.json(info)
    })

    this.app.get('/high-watermark', async (c) => {
      const hw = await this.getHighWatermark()
      return c.json({ highWatermark: hw })
    })

    this.app.get('/watermarks', async (c) => {
      const low = await this.getLogStartOffset()
      const high = await this.getHighWatermark()
      return c.json({ low, high })
    })
  }

  private async ensureInitialized() {
    await this.ctx.blockConcurrencyWhile(async () => {
      if (this.initialized) return

      // Check if already initialized
      const tables = this.sql.exec<{ name: string }>(
        `SELECT name FROM sqlite_master WHERE type='table' AND name='messages'`
      ).toArray()

      if (tables.length === 0) {
        // Initialize schema
        const schema = initializeSchema('topic-partition')
        // Execute each statement separately
        const statements = schema.split(';').filter(s => s.trim())
        for (const stmt of statements) {
          if (stmt.trim()) {
            this.sql.exec(stmt)
          }
        }
      }
      this.initialized = true
    })
  }

  /**
   * Append a single message to the partition
   */
  async append(message: AppendMessage): Promise<AppendResult> {
    await this.ensureInitialized()

    const timestamp = Date.now()
    const value = typeof message.value === 'string'
      ? message.value
      : new TextDecoder().decode(message.value as ArrayBuffer)
    const headers = JSON.stringify(message.headers || {})

    this.sql.exec('BEGIN TRANSACTION')
    try {
      const result = this.sql.exec<{ offset: number }>(
        `INSERT INTO messages (key, value, headers, timestamp, producer_id, sequence)
         VALUES (?, ?, ?, ?, ?, ?)
         RETURNING offset`,
        message.key ?? null,
        value,
        headers,
        timestamp,
        message.producerId ?? null,
        message.sequence ?? null
      ).one()

      // Update high watermark
      this.sql.exec(
        `UPDATE watermarks SET high_watermark = ?, updated_at = ? WHERE partition = 0`,
        result.offset,
        timestamp
      )

      this.sql.exec('COMMIT')
      return { offset: result.offset, timestamp }
    } catch (e) {
      this.sql.exec('ROLLBACK')
      throw e
    }
  }

  /**
   * Append multiple messages atomically
   */
  async appendBatch(messages: AppendMessage[]): Promise<AppendResult[]> {
    await this.ensureInitialized()

    const timestamp = Date.now()

    // Use transactionSync for atomic batch writes
    const results = this.ctx.storage.transactionSync(() => {
      const batchResults: AppendResult[] = []

      for (const message of messages) {
        const value = typeof message.value === 'string'
          ? message.value
          : new TextDecoder().decode(message.value as ArrayBuffer)
        const headers = JSON.stringify(message.headers || {})

        const result = this.sql.exec<{ offset: number }>(
          `INSERT INTO messages (key, value, headers, timestamp, producer_id, sequence)
           VALUES (?, ?, ?, ?, ?, ?)
           RETURNING offset`,
          message.key ?? null,
          value,
          headers,
          timestamp,
          message.producerId ?? null,
          message.sequence ?? null
        ).one()

        batchResults.push({ offset: result.offset, timestamp })
      }

      // Update high watermark to last offset
      if (batchResults.length > 0) {
        this.sql.exec(
          `UPDATE watermarks SET high_watermark = ?, updated_at = ? WHERE partition = 0`,
          batchResults[batchResults.length - 1].offset,
          timestamp
        )
      }

      return batchResults
    })

    return results
  }

  /**
   * Read messages from a specific offset
   */
  async read(options: ReadOptions): Promise<PartitionMessage[]> {
    await this.ensureInitialized()

    const rows = this.sql.exec<{
      offset: number
      key: string | null
      value: string
      headers: string
      timestamp: number
    }>(
      `SELECT offset, key, value, headers, timestamp
       FROM messages
       WHERE offset >= ?
       ORDER BY offset ASC
       LIMIT ?`,
      options.fromOffset,
      options.maxRecords
    ).toArray()

    return rows.map(row => ({
      offset: row.offset,
      key: row.key,
      value: row.value,
      headers: safeJsonParse<Record<string, string>>(row.headers, {}),
      timestamp: row.timestamp,
    }))
  }

  /**
   * Get the high watermark (latest committed offset)
   */
  async getHighWatermark(): Promise<number> {
    await this.ensureInitialized()

    const result = this.sql.exec<{ high_watermark: number }>(
      `SELECT high_watermark FROM watermarks WHERE partition = 0`
    ).one()

    return result?.high_watermark ?? 0
  }

  /**
   * Get the log start offset (earliest available offset)
   */
  async getLogStartOffset(): Promise<number> {
    await this.ensureInitialized()

    const result = this.sql.exec<{ log_start_offset: number }>(
      `SELECT log_start_offset FROM watermarks WHERE partition = 0`
    ).one()

    return result?.log_start_offset ?? 0
  }

  /**
   * Get offset information
   */
  async getOffsets(): Promise<OffsetInfo> {
    await this.ensureInitialized()

    const watermarks = this.sql.exec<{ high_watermark: number; log_start_offset: number }>(
      `SELECT high_watermark, log_start_offset FROM watermarks WHERE partition = 0`
    ).one()

    return {
      earliest: watermarks?.log_start_offset ?? 0,
      latest: watermarks?.high_watermark ?? 0,
    }
  }

  /**
   * HTTP fetch handler
   */
  async fetch(request: Request): Promise<Response> {
    await this.ensureInitialized()
    return this.app.fetch(request)
  }
}
