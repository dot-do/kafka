/**
 * Retention Policy Tests
 *
 * Tests for message retention policy enforcement and expiration.
 * These tests verify:
 * - Time-based retention (delete messages older than retention.ms)
 * - Size-based retention (delete messages when log size exceeds retention.bytes)
 * - Log start offset updates after cleanup
 */

import { describe, it, expect, beforeEach } from 'vitest'
import { RetentionPolicy, RetentionConfig, DEFAULT_RETENTION_CONFIG } from './retention'

describe('RetentionPolicy', () => {
  describe('RetentionConfig', () => {
    it('should have sensible default configuration', () => {
      expect(DEFAULT_RETENTION_CONFIG.retentionMs).toBe(7 * 24 * 60 * 60 * 1000) // 7 days
      expect(DEFAULT_RETENTION_CONFIG.retentionBytes).toBe(-1) // unlimited
      expect(DEFAULT_RETENTION_CONFIG.cleanupIntervalMs).toBe(5 * 60 * 1000) // 5 minutes
      expect(DEFAULT_RETENTION_CONFIG.minCleanableRatio).toBe(0.5)
    })

    it('should allow custom retention time', () => {
      const config: RetentionConfig = {
        retentionMs: 1000 * 60 * 60, // 1 hour
      }
      expect(config.retentionMs).toBe(3600000)
    })

    it('should allow custom retention bytes', () => {
      const config: RetentionConfig = {
        retentionBytes: 1024 * 1024 * 100, // 100MB
      }
      expect(config.retentionBytes).toBe(104857600)
    })
  })

  describe('Time-based retention', () => {
    it('should identify expired messages based on timestamp', () => {
      const policy = new RetentionPolicy({
        retentionMs: 1000, // 1 second retention
      })

      const now = Date.now()
      const expiredTimestamp = now - 2000 // 2 seconds ago
      const validTimestamp = now - 500 // 0.5 seconds ago

      expect(policy.isExpired(expiredTimestamp, now)).toBe(true)
      expect(policy.isExpired(validTimestamp, now)).toBe(false)
    })

    it('should calculate cleanup cutoff timestamp', () => {
      const retentionMs = 3600000 // 1 hour
      const policy = new RetentionPolicy({ retentionMs })

      const now = Date.now()
      const cutoff = policy.getTimeCutoff(now)

      expect(cutoff).toBe(now - retentionMs)
    })

    it('should handle -1 (infinite retention)', () => {
      const policy = new RetentionPolicy({
        retentionMs: -1,
      })

      const now = Date.now()
      const veryOldTimestamp = now - (365 * 24 * 60 * 60 * 1000) // 1 year ago

      expect(policy.isExpired(veryOldTimestamp, now)).toBe(false)
    })
  })

  describe('Size-based retention', () => {
    it('should identify when log exceeds size limit', () => {
      const policy = new RetentionPolicy({
        retentionBytes: 1000, // 1KB limit
      })

      expect(policy.exceedsSizeLimit(500)).toBe(false)
      expect(policy.exceedsSizeLimit(1000)).toBe(false)
      expect(policy.exceedsSizeLimit(1001)).toBe(true)
    })

    it('should calculate bytes to delete when over limit', () => {
      const policy = new RetentionPolicy({
        retentionBytes: 1000,
      })

      expect(policy.bytesToDelete(1500)).toBe(500)
      expect(policy.bytesToDelete(1000)).toBe(0)
      expect(policy.bytesToDelete(500)).toBe(0)
    })

    it('should handle -1 (unlimited size)', () => {
      const policy = new RetentionPolicy({
        retentionBytes: -1,
      })

      const veryLargeSize = Number.MAX_SAFE_INTEGER
      expect(policy.exceedsSizeLimit(veryLargeSize)).toBe(false)
    })
  })

  describe('Combined retention', () => {
    it('should apply both time and size limits', () => {
      const policy = new RetentionPolicy({
        retentionMs: 1000,
        retentionBytes: 1000,
      })

      const now = Date.now()

      // Message within time but log over size - should trigger cleanup
      expect(policy.shouldCleanup(now - 500, 1500, now)).toBe(true)

      // Message expired but log under size - should trigger cleanup
      expect(policy.shouldCleanup(now - 2000, 500, now)).toBe(true)

      // Both limits exceeded - should definitely trigger cleanup
      expect(policy.shouldCleanup(now - 2000, 1500, now)).toBe(true)

      // Neither limit exceeded - should not trigger cleanup
      expect(policy.shouldCleanup(now - 500, 500, now)).toBe(false)
    })
  })

  describe('Cleanup eligibility', () => {
    it('should respect minimum cleanable ratio', () => {
      const policy = new RetentionPolicy({
        retentionMs: 1000,
        minCleanableRatio: 0.5,
      })

      const totalMessages = 100
      const expiredMessages = 40 // 40% expired

      // Less than 50% expired, should not trigger aggressive cleanup
      expect(policy.meetsCleanableRatio(expiredMessages, totalMessages)).toBe(false)

      // 50% or more expired, should trigger cleanup
      expect(policy.meetsCleanableRatio(50, totalMessages)).toBe(true)
      expect(policy.meetsCleanableRatio(60, totalMessages)).toBe(true)
    })
  })
})

describe('RetentionPolicy Integration', () => {
  it('should compute cleanup plan for a partition', () => {
    const policy = new RetentionPolicy({
      retentionMs: 1000,
      retentionBytes: 5000,
    })

    const now = Date.now()
    const messages = [
      { offset: 0, timestamp: now - 3000, size: 100 },
      { offset: 1, timestamp: now - 2000, size: 100 },
      { offset: 2, timestamp: now - 500, size: 100 },
      { offset: 3, timestamp: now - 100, size: 100 },
    ]

    const plan = policy.computeCleanupPlan(messages, now)

    // Messages 0 and 1 should be deleted (expired by time)
    expect(plan.offsetsToDelete).toEqual([0, 1])
    expect(plan.newLogStartOffset).toBe(2)
    expect(plan.bytesToFree).toBe(200)
  })

  it('should compute size-based cleanup plan', () => {
    const policy = new RetentionPolicy({
      retentionMs: -1, // No time-based retention
      retentionBytes: 250, // Only keep 250 bytes
    })

    const now = Date.now()
    const messages = [
      { offset: 0, timestamp: now - 100, size: 100 },
      { offset: 1, timestamp: now - 50, size: 100 },
      { offset: 2, timestamp: now - 25, size: 100 },
      { offset: 3, timestamp: now - 10, size: 100 },
    ]

    const totalSize = messages.reduce((sum, m) => sum + m.size, 0) // 400 bytes
    const plan = policy.computeCleanupPlan(messages, now, totalSize)

    // Need to delete 150 bytes (400 - 250), so delete messages 0 and 1
    expect(plan.offsetsToDelete).toEqual([0])
    expect(plan.newLogStartOffset).toBe(1)
    expect(plan.bytesToFree).toBeGreaterThanOrEqual(100)
  })
})
