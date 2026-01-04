import { describe, it, expect } from 'vitest'
import {
  MESSAGES_SCHEMA,
  WATERMARKS_SCHEMA,
  GROUP_METADATA_SCHEMA,
  MEMBERS_SCHEMA,
  OFFSETS_SCHEMA,
  TOPICS_SCHEMA,
  PARTITIONS_SCHEMA,
  initializeSchema,
} from './schema'

describe('SQLite Schema Definitions', () => {
  describe('Message Storage Schema', () => {
    it('MESSAGES_SCHEMA creates messages table', () => {
      expect(MESSAGES_SCHEMA).toContain('CREATE TABLE')
      expect(MESSAGES_SCHEMA).toContain('messages')
      expect(MESSAGES_SCHEMA).toContain('offset INTEGER PRIMARY KEY')
      expect(MESSAGES_SCHEMA).toContain('key TEXT')
      expect(MESSAGES_SCHEMA).toContain('value BLOB NOT NULL')
      expect(MESSAGES_SCHEMA).toContain('headers TEXT')
      expect(MESSAGES_SCHEMA).toContain('timestamp INTEGER')
    })

    it('WATERMARKS_SCHEMA tracks partition progress', () => {
      expect(WATERMARKS_SCHEMA).toContain('watermarks')
      expect(WATERMARKS_SCHEMA).toContain('high_watermark')
      expect(WATERMARKS_SCHEMA).toContain('log_start_offset')
    })
  })

  describe('Consumer Group Schema', () => {
    it('GROUP_METADATA_SCHEMA tracks group state', () => {
      expect(GROUP_METADATA_SCHEMA).toContain('group_metadata')
      expect(GROUP_METADATA_SCHEMA).toContain('group_id')
      expect(GROUP_METADATA_SCHEMA).toContain('state')
      expect(GROUP_METADATA_SCHEMA).toContain('generation_id')
      expect(GROUP_METADATA_SCHEMA).toContain('leader_id')
    })

    it('MEMBERS_SCHEMA tracks group members', () => {
      expect(MEMBERS_SCHEMA).toContain('members')
      expect(MEMBERS_SCHEMA).toContain('member_id')
      expect(MEMBERS_SCHEMA).toContain('client_id')
      expect(MEMBERS_SCHEMA).toContain('session_timeout')
      expect(MEMBERS_SCHEMA).toContain('assigned_partitions')
      expect(MEMBERS_SCHEMA).toContain('last_heartbeat')
    })

    it('OFFSETS_SCHEMA stores committed offsets', () => {
      expect(OFFSETS_SCHEMA).toContain('offsets')
      expect(OFFSETS_SCHEMA).toContain('topic')
      expect(OFFSETS_SCHEMA).toContain('partition')
      expect(OFFSETS_SCHEMA).toContain('committed_offset')
      expect(OFFSETS_SCHEMA).toContain('PRIMARY KEY')
    })
  })

  describe('Cluster Metadata Schema', () => {
    it('TOPICS_SCHEMA registers topics', () => {
      expect(TOPICS_SCHEMA).toContain('topics')
      expect(TOPICS_SCHEMA).toContain('name')
      expect(TOPICS_SCHEMA).toContain('partition_count')
      expect(TOPICS_SCHEMA).toContain('config')
    })

    it('PARTITIONS_SCHEMA maps partitions to DOs', () => {
      expect(PARTITIONS_SCHEMA).toContain('partitions')
      expect(PARTITIONS_SCHEMA).toContain('topic')
      expect(PARTITIONS_SCHEMA).toContain('partition')
      expect(PARTITIONS_SCHEMA).toContain('leader_do_id')
    })
  })

  describe('Schema Initialization', () => {
    it('initializeSchema returns all schemas', () => {
      const schemas = initializeSchema('topic-partition')
      expect(schemas).toContain('messages')
      expect(schemas).toContain('watermarks')
    })

    it('initializeSchema returns consumer group schemas', () => {
      const schemas = initializeSchema('consumer-group')
      expect(schemas).toContain('group_metadata')
      expect(schemas).toContain('members')
      expect(schemas).toContain('offsets')
    })

    it('initializeSchema returns cluster metadata schemas', () => {
      const schemas = initializeSchema('cluster-metadata')
      expect(schemas).toContain('topics')
      expect(schemas).toContain('partitions')
    })
  })
})
