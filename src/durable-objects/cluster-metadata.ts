/**
 * ClusterMetadataDO - Durable Object for cluster-wide metadata
 *
 * This DO handles:
 * - Topic registry
 * - Partition assignments
 * - Cluster configuration
 */

import { DurableObject } from 'cloudflare:workers'
import { Hono } from 'hono'
import { initializeSchema } from '../schema/schema'
import type { TopicConfig, TopicMetadata, PartitionMetadata } from '../types/admin'

export class ClusterMetadataDO extends DurableObject<Record<string, unknown>> {
  private sql: SqlStorage
  private initialized = false
  private app: Hono

  constructor(ctx: DurableObjectState, env: Record<string, unknown>) {
    super(ctx, env)
    this.sql = ctx.storage.sql
    this.app = new Hono()
    this.setupRoutes()
  }

  private setupRoutes() {
    this.app.post('/topics', async (c) => {
      const body = await c.req.json<TopicConfig>()
      await this.createTopic(body)
      return c.json({ success: true })
    })

    this.app.delete('/topics/:name', async (c) => {
      const name = c.req.param('name')
      await this.deleteTopic(name)
      return c.json({ success: true })
    })

    this.app.get('/topics', async (c) => {
      const topics = await this.listTopics()
      return c.json(topics)
    })

    this.app.get('/topics/:name', async (c) => {
      const name = c.req.param('name')
      const metadata = await this.describeTopic(name)
      if (!metadata) {
        return c.json({ error: 'Topic not found' }, 404)
      }
      return c.json(metadata)
    })

    this.app.post('/topics/:name/partitions', async (c) => {
      const name = c.req.param('name')
      const body = await c.req.json<{ count: number }>()
      await this.addPartitions(name, body.count)
      return c.json({ success: true })
    })

    this.app.get('/partitions/:topic/:partition', async (c) => {
      const topic = c.req.param('topic')
      const partition = parseInt(c.req.param('partition'))
      const info = await this.getPartition(topic, partition)
      return c.json(info)
    })
  }

  private async ensureInitialized() {
    if (this.initialized) return

    await this.ctx.blockConcurrencyWhile(async () => {
      const tables = this.sql.exec<{ name: string }>(
        `SELECT name FROM sqlite_master WHERE type='table' AND name='topics'`
      ).toArray()

      if (tables.length === 0) {
        const schema = initializeSchema('cluster-metadata')
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

  async createTopic(config: TopicConfig): Promise<void> {
    await this.ensureInitialized()

    const now = Date.now()
    const configJson = JSON.stringify(config.config ?? {})

    this.sql.exec(`
      INSERT INTO topics (name, partition_count, replication_factor, config, created_at)
      VALUES (?, ?, ?, ?, ?)
    `, config.topic, config.partitions, config.replicationFactor ?? 1, configJson, now)

    // Create partition entries
    for (let i = 0; i < config.partitions; i++) {
      const doId = `${config.topic}-${i}`
      this.sql.exec(`
        INSERT INTO partitions (topic, partition, leader_do_id, replicas, isr)
        VALUES (?, ?, ?, '[]', '[]')
      `, config.topic, i, doId)
    }
  }

  async deleteTopic(name: string): Promise<void> {
    await this.ensureInitialized()

    this.sql.exec(`DELETE FROM partitions WHERE topic = ?`, name)
    this.sql.exec(`DELETE FROM topics WHERE name = ?`, name)
  }

  async listTopics(): Promise<string[]> {
    await this.ensureInitialized()

    const rows = this.sql.exec<{ name: string }>(`SELECT name FROM topics`).toArray()
    return rows.map(r => r.name)
  }

  async describeTopic(name: string): Promise<TopicMetadata | null> {
    await this.ensureInitialized()

    const topicRow = this.sql.exec<{ name: string; partition_count: number; config: string }>(
      `SELECT name, partition_count, config FROM topics WHERE name = ?`,
      name
    ).toArray()

    if (topicRow.length === 0) {
      return null
    }

    const partitionRows = this.sql.exec<{
      partition: number
      leader_do_id: string
      replicas: string
      isr: string
    }>(`SELECT partition, leader_do_id, replicas, isr FROM partitions WHERE topic = ? ORDER BY partition`, name).toArray()

    return {
      topic: name,
      partitions: partitionRows.map(p => ({
        partition: p.partition,
        leader: p.leader_do_id,
        replicas: JSON.parse(p.replicas),
        isr: JSON.parse(p.isr),
      })),
      config: JSON.parse(topicRow[0].config),
    }
  }

  async addPartitions(topic: string, newTotal: number): Promise<void> {
    await this.ensureInitialized()

    const topicRow = this.sql.exec<{ partition_count: number }>(
      `SELECT partition_count FROM topics WHERE name = ?`,
      topic
    ).one()

    const currentCount = topicRow.partition_count

    if (newTotal <= currentCount) {
      throw new Error(`New partition count (${newTotal}) must be greater than current (${currentCount})`)
    }

    // Add new partitions
    for (let i = currentCount; i < newTotal; i++) {
      const doId = `${topic}-${i}`
      this.sql.exec(`
        INSERT INTO partitions (topic, partition, leader_do_id, replicas, isr)
        VALUES (?, ?, ?, '[]', '[]')
      `, topic, i, doId)
    }

    // Update topic partition count
    this.sql.exec(`UPDATE topics SET partition_count = ? WHERE name = ?`, newTotal, topic)
  }

  async getPartition(topic: string, partition: number): Promise<PartitionMetadata | null> {
    await this.ensureInitialized()

    const rows = this.sql.exec<{
      partition: number
      leader_do_id: string
      replicas: string
      isr: string
    }>(
      `SELECT partition, leader_do_id, replicas, isr FROM partitions WHERE topic = ? AND partition = ?`,
      topic,
      partition
    ).toArray()

    if (rows.length === 0) {
      return null
    }

    const row = rows[0]
    return {
      partition: row.partition,
      leader: row.leader_do_id,
      replicas: JSON.parse(row.replicas),
      isr: JSON.parse(row.isr),
    }
  }

  async fetch(request: Request): Promise<Response> {
    await this.ensureInitialized()
    return this.app.fetch(request)
  }
}
