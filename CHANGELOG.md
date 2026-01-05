# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.0.2] - 2026-01-04

### Fixed

- Added SQLite transactions to multi-statement Durable Object operations for atomicity
- Changed partition key delimiter from `-` to `::` to support hyphenated topic names
- Added `group_id` column to offsets table schema for proper consumer group tracking
- Added `blockConcurrencyWhile()` to join/commit/createTopic/addPartitions for consistency
- Changed `Promise.all` to `Promise.allSettled` in consumer poll for better error handling
- Replaced generic `throw new Error()` with custom error classes for better error handling
- Renamed `mondodb-cdc.ts` to `mongodb-cdc.ts` (typo fix)
- Added input validation to HTTP API endpoints
- Fixed vitest interactive mode with `--run` flag in prepublishOnly script

## [0.0.1] - 2026-01-04

### Added

- Initial release of Kafdo - Kafka on Cloudflare Durable Objects
- Producer API for publishing messages to topics with partition support
- Consumer API with consumer groups, offset management, and rebalancing
- Admin API for topic and partition management
- HTTP Client SDK for interacting with Kafdo clusters
- Durable Objects for distributed storage:
  - TopicPartition for message storage and retrieval
  - ConsumerGroup for consumer coordination and offset tracking
  - ClusterMetadata for topic and partition metadata
- Integrations:
  - R2 Event Bridge for event-driven workflows with R2 storage
  - MongoDB CDC (Change Data Capture) for streaming database changes
