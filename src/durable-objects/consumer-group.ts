/**
 * ConsumerGroupDO - Durable Object for managing consumer groups
 *
 * This DO handles:
 * - Consumer group membership
 * - Partition assignment
 * - Offset tracking
 * - Rebalancing
 */

import { DurableObject } from 'cloudflare:workers'
import { Hono } from 'hono'
import { initializeSchema } from '../schema/schema'
import type { ConsumerGroupState } from '../types/consumer'
import type { TopicPartition } from '../types/records'

export interface GroupMember {
  memberId: string
  clientId: string
  clientHost: string
  sessionTimeout: number
  rebalanceTimeout: number
  assignedPartitions: TopicPartition[]
  lastHeartbeat: number
}

export class ConsumerGroupDO extends DurableObject<Record<string, unknown>> {
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
    this.app.post('/join', async (c) => {
      const body = await c.req.json()
      const result = await this.join(body)
      return c.json(result)
    })

    this.app.post('/leave', async (c) => {
      const body = await c.req.json<{ memberId: string }>()
      await this.leave(body.memberId)
      return c.json({ success: true })
    })

    this.app.post('/heartbeat', async (c) => {
      const body = await c.req.json<{ memberId: string; generationId: number }>()
      const result = await this.heartbeat(body.memberId, body.generationId)
      return c.json(result)
    })

    this.app.post('/commit', async (c) => {
      const body = await c.req.json()
      await this.commitOffsets(body.memberId, body.offsets)
      return c.json({ success: true })
    })

    this.app.post('/fetch-offsets', async (c) => {
      const body = await c.req.json<{ partitions: TopicPartition[] }>()
      const offsets = await this.fetchOffsets(body.partitions)
      return c.json(Object.fromEntries(offsets))
    })

    this.app.get('/describe', async (c) => {
      const description = await this.describe()
      return c.json(description)
    })
  }

  private async ensureInitialized() {
    if (this.initialized) return

    await this.ctx.blockConcurrencyWhile(async () => {
      const tables = this.sql.exec<{ name: string }>(
        `SELECT name FROM sqlite_master WHERE type='table' AND name='group_metadata'`
      ).toArray()

      if (tables.length === 0) {
        const schema = initializeSchema('consumer-group')
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

  private getGroupId(): string {
    return this.ctx.id.name ?? this.ctx.id.toString()
  }

  async join(params: {
    memberId?: string
    clientId: string
    clientHost: string
    sessionTimeout: number
    rebalanceTimeout: number
    topics: string[]
  }): Promise<{ memberId: string; generationId: number; leaderId: string; members: GroupMember[] }> {
    await this.ensureInitialized()

    const groupId = this.getGroupId()
    const memberId = params.memberId || `${params.clientId}-${Date.now()}-${Math.random().toString(36).slice(2)}`
    const now = Date.now()

    this.sql.exec(`
      INSERT INTO group_metadata (group_id, state, generation_id, leader_id, protocol, updated_at)
      VALUES (?, 'PreparingRebalance', 1, ?, 'range', ?)
      ON CONFLICT(group_id) DO UPDATE SET
        state = 'PreparingRebalance',
        generation_id = generation_id + 1,
        updated_at = ?
    `, groupId, memberId, now, now)

    this.sql.exec(`
      INSERT INTO members (member_id, client_id, client_host, session_timeout, rebalance_timeout, last_heartbeat, assigned_partitions)
      VALUES (?, ?, ?, ?, ?, ?, '[]')
      ON CONFLICT(member_id) DO UPDATE SET
        client_id = ?,
        client_host = ?,
        session_timeout = ?,
        rebalance_timeout = ?,
        last_heartbeat = ?
    `, memberId, params.clientId, params.clientHost, params.sessionTimeout, params.rebalanceTimeout, now,
       params.clientId, params.clientHost, params.sessionTimeout, params.rebalanceTimeout, now)

    const metadata = this.sql.exec<{ generation_id: number; leader_id: string }>(
      `SELECT generation_id, leader_id FROM group_metadata WHERE group_id = ?`,
      groupId
    ).one()

    const members = this.sql.exec<{
      member_id: string
      client_id: string
      client_host: string
      session_timeout: number
      rebalance_timeout: number
      assigned_partitions: string
      last_heartbeat: number
    }>(`SELECT * FROM members`).toArray()

    return {
      memberId,
      generationId: metadata.generation_id,
      leaderId: metadata.leader_id,
      members: members.map(m => ({
        memberId: m.member_id,
        clientId: m.client_id,
        clientHost: m.client_host,
        sessionTimeout: m.session_timeout,
        rebalanceTimeout: m.rebalance_timeout,
        assignedPartitions: JSON.parse(m.assigned_partitions),
        lastHeartbeat: m.last_heartbeat,
      })),
    }
  }

  async leave(memberId: string): Promise<void> {
    await this.ensureInitialized()
    this.sql.exec(`DELETE FROM members WHERE member_id = ?`, memberId)

    const remaining = this.sql.exec<{ count: number }>(
      `SELECT COUNT(*) as count FROM members`
    ).one()

    const groupId = this.getGroupId()
    if (remaining.count === 0) {
      this.sql.exec(`UPDATE group_metadata SET state = 'Empty' WHERE group_id = ?`, groupId)
    } else {
      this.sql.exec(`UPDATE group_metadata SET state = 'PreparingRebalance', generation_id = generation_id + 1 WHERE group_id = ?`, groupId)
    }
  }

  async heartbeat(memberId: string, generationId: number): Promise<{ needsRebalance: boolean }> {
    await this.ensureInitialized()

    const now = Date.now()
    this.sql.exec(`UPDATE members SET last_heartbeat = ? WHERE member_id = ?`, now, memberId)

    const groupId = this.getGroupId()
    const metadata = this.sql.exec<{ generation_id: number; state: string }>(
      `SELECT generation_id, state FROM group_metadata WHERE group_id = ?`,
      groupId
    ).one()

    return {
      needsRebalance: metadata.generation_id !== generationId || metadata.state === 'PreparingRebalance',
    }
  }

  async commitOffsets(
    _memberId: string,
    offsets: Array<{ topic: string; partition: number; offset: number; metadata?: string }>
  ): Promise<void> {
    await this.ensureInitialized()

    const now = Date.now()
    for (const offset of offsets) {
      this.sql.exec(`
        INSERT INTO offsets (topic, partition, committed_offset, metadata, commit_timestamp)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(topic, partition) DO UPDATE SET
          committed_offset = ?,
          metadata = ?,
          commit_timestamp = ?
      `, offset.topic, offset.partition, offset.offset, offset.metadata ?? '', now,
         offset.offset, offset.metadata ?? '', now)
    }
  }

  async fetchOffsets(partitions: TopicPartition[]): Promise<Map<string, number>> {
    await this.ensureInitialized()

    const result = new Map<string, number>()
    for (const tp of partitions) {
      const row = this.sql.exec<{ committed_offset: number }>(
        `SELECT committed_offset FROM offsets WHERE topic = ? AND partition = ?`,
        tp.topic,
        tp.partition
      ).toArray()

      const key = `${tp.topic}-${tp.partition}`
      result.set(key, row.length > 0 ? row[0].committed_offset : -1)
    }

    return result
  }

  async describe(): Promise<{
    groupId: string
    state: ConsumerGroupState
    members: GroupMember[]
    generationId: number
  }> {
    await this.ensureInitialized()

    const groupId = this.getGroupId()
    const metadata = this.sql.exec<{ state: string; generation_id: number }>(
      `SELECT state, generation_id FROM group_metadata WHERE group_id = ?`,
      groupId
    ).toArray()

    const members = this.sql.exec<{
      member_id: string
      client_id: string
      client_host: string
      session_timeout: number
      rebalance_timeout: number
      assigned_partitions: string
      last_heartbeat: number
    }>(`SELECT * FROM members`).toArray()

    return {
      groupId,
      state: (metadata[0]?.state as ConsumerGroupState) ?? 'Empty',
      generationId: metadata[0]?.generation_id ?? 0,
      members: members.map(m => ({
        memberId: m.member_id,
        clientId: m.client_id,
        clientHost: m.client_host,
        sessionTimeout: m.session_timeout,
        rebalanceTimeout: m.rebalance_timeout,
        assignedPartitions: JSON.parse(m.assigned_partitions),
        lastHeartbeat: m.last_heartbeat,
      })),
    }
  }

  async fetch(request: Request): Promise<Response> {
    await this.ensureInitialized()
    return this.app.fetch(request)
  }
}
