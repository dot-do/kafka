/**
 * Kafdo Integrations
 *
 * Pre-built integrations for common data sources and sinks.
 */

// MondoDB CDC Integration
export {
  KafdoPipeline,
  createKafdoPipeline,
  createFixedTopicPipeline,
  createDatabaseTopicPipeline,
  createCollectionTopicPipeline,
  processCDCMessage,
  isInsertEvent,
  isUpdateEvent,
  isDeleteEvent,
  type Pipeline,
  type KafdoPipelineConfig,
  type CDCEvent,
  type InsertEvent,
  type UpdateEvent,
  type DeleteEvent,
  type CDCNamespace,
  type CDCConsumerConfig,
} from './mondodb-cdc'

// R2 Event Bridge Integration
export {
  R2EventBridge,
  createR2EventBridge,
  processR2Event,
  isR2ObjectCreated,
  isR2ObjectDeleted,
  type R2EventBridgeConfig,
  type R2Event,
  type R2ObjectCreatedEvent,
  type R2ObjectDeletedEvent,
} from './r2-event-bridge'
