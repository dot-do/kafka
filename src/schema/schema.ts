/**
 * SQLite Schema Definitions for Kafdo
 * 
 * These schemas are used by Durable Objects with SQLite storage
 */

/**
 * Schema for topic partition message storage
 * Used by TopicPartitionDO
 */
export const MESSAGES_SCHEMA = `
CREATE TABLE IF NOT EXISTS messages (
  offset INTEGER PRIMARY KEY AUTOINCREMENT,
  key TEXT,
  value BLOB NOT NULL,
  headers TEXT DEFAULT '{}',
  timestamp INTEGER DEFAULT (unixepoch() * 1000),
  producer_id TEXT,
  sequence INTEGER
);

CREATE INDEX IF NOT EXISTS idx_messages_key ON messages(key);
CREATE INDEX IF NOT EXISTS idx_messages_timestamp ON messages(timestamp);
`

/**
 * Schema for tracking partition watermarks
 */
export const WATERMARKS_SCHEMA = `
CREATE TABLE IF NOT EXISTS watermarks (
  partition INTEGER PRIMARY KEY DEFAULT 0,
  high_watermark INTEGER DEFAULT 0,
  log_start_offset INTEGER DEFAULT 0,
  updated_at INTEGER DEFAULT (unixepoch() * 1000)
);

INSERT OR IGNORE INTO watermarks (partition) VALUES (0);
`

/**
 * Schema for consumer group metadata
 * Used by ConsumerGroupDO
 */
export const GROUP_METADATA_SCHEMA = `
CREATE TABLE IF NOT EXISTS group_metadata (
  group_id TEXT PRIMARY KEY,
  state TEXT DEFAULT 'Empty',
  protocol_type TEXT DEFAULT 'consumer',
  generation_id INTEGER DEFAULT 0,
  leader_id TEXT,
  protocol TEXT,
  updated_at INTEGER DEFAULT (unixepoch() * 1000)
);
`

/**
 * Schema for consumer group members
 */
export const MEMBERS_SCHEMA = `
CREATE TABLE IF NOT EXISTS members (
  member_id TEXT PRIMARY KEY,
  client_id TEXT,
  client_host TEXT,
  session_timeout INTEGER DEFAULT 30000,
  rebalance_timeout INTEGER DEFAULT 60000,
  assigned_partitions TEXT DEFAULT '[]',
  last_heartbeat INTEGER DEFAULT (unixepoch() * 1000),
  metadata BLOB
);

CREATE INDEX IF NOT EXISTS idx_members_heartbeat ON members(last_heartbeat);
`

/**
 * Schema for committed offsets
 */
export const OFFSETS_SCHEMA = `
CREATE TABLE IF NOT EXISTS offsets (
  group_id TEXT NOT NULL,
  topic TEXT NOT NULL,
  partition INTEGER NOT NULL,
  committed_offset INTEGER NOT NULL,
  metadata TEXT DEFAULT '',
  commit_timestamp INTEGER DEFAULT (unixepoch() * 1000),
  PRIMARY KEY (group_id, topic, partition)
);
`

/**
 * Schema for cluster topic registry
 * Used by ClusterMetadataDO
 */
export const TOPICS_SCHEMA = `
CREATE TABLE IF NOT EXISTS topics (
  name TEXT PRIMARY KEY,
  partition_count INTEGER DEFAULT 1,
  replication_factor INTEGER DEFAULT 1,
  config TEXT DEFAULT '{}',
  created_at INTEGER DEFAULT (unixepoch() * 1000)
);
`

/**
 * Schema for partition assignments
 */
export const PARTITIONS_SCHEMA = `
CREATE TABLE IF NOT EXISTS partitions (
  topic TEXT NOT NULL,
  partition INTEGER NOT NULL,
  leader_do_id TEXT,
  replicas TEXT DEFAULT '[]',
  isr TEXT DEFAULT '[]',
  PRIMARY KEY (topic, partition)
);

CREATE INDEX IF NOT EXISTS idx_partitions_topic ON partitions(topic);
`

/**
 * Schema type for different Durable Object types
 */
export type SchemaType = 'topic-partition' | 'consumer-group' | 'cluster-metadata'

/**
 * Initialize schema for a specific Durable Object type
 * Returns the combined SQL statements to execute
 */
export function initializeSchema(type: SchemaType): string {
  switch (type) {
    case 'topic-partition':
      return `${MESSAGES_SCHEMA}\n${WATERMARKS_SCHEMA}`
    case 'consumer-group':
      return `${GROUP_METADATA_SCHEMA}\n${MEMBERS_SCHEMA}\n${OFFSETS_SCHEMA}`
    case 'cluster-metadata':
      return `${TOPICS_SCHEMA}\n${PARTITIONS_SCHEMA}`
    default:
      throw new Error(`Unknown schema type: ${type}`)
  }
}

/**
 * Get all schemas combined (useful for testing)
 */
export function getAllSchemas(): string {
  return [
    MESSAGES_SCHEMA,
    WATERMARKS_SCHEMA,
    GROUP_METADATA_SCHEMA,
    MEMBERS_SCHEMA,
    OFFSETS_SCHEMA,
    TOPICS_SCHEMA,
    PARTITIONS_SCHEMA,
  ].join('\n')
}
