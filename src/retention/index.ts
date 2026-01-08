/**
 * Retention Policy Module
 *
 * Implements Kafka-compatible message retention policies:
 * - Time-based retention (retention.ms)
 * - Size-based retention (retention.bytes)
 * - Log compaction (cleanup.policy=compact)
 *
 * @module retention
 */

export * from './types'

// Implementation exports will be added in GREEN phase
// export { createRetentionPolicy } from './policy'
// export { parseRetentionConfig } from './config'
// export { createRetentionScheduler } from './scheduler'
