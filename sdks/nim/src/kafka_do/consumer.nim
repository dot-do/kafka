## Kafka consumer for the .do platform

import std/[json, options, tables, asyncdispatch, times]
import ./errors
import ./config
import ./record

type
  Consumer* = ref object
    client*: pointer  # Forward reference to KafkaClient
    topic*: string
    group*: string
    config*: ConsumerConfig
    subscribed: bool
    subscriptionId: string

  BatchConsumer* = ref object
    client*: pointer
    topic*: string
    group*: string
    config*: BatchConfig
    consumer: Consumer

  AsyncConsumer* = ref object
    client*: pointer
    topic*: string
    group*: string
    config*: ConsumerConfig
    consumer: Consumer
    running: bool

# Forward declare RPC call
proc rpcCall*(client: pointer, methodName: string, params: JsonNode): Future[JsonNode] {.async, importc.}

# Consumer

proc newConsumer*(client: pointer, topic: string, group: string, config: ConsumerConfig = nil): Consumer =
  Consumer(
    client: client,
    topic: topic,
    group: group,
    config: if config.isNil: newConsumerConfig() else: config,
    subscribed: false,
    subscriptionId: ""
  )

proc subscribe*(consumer: Consumer): Future[void] {.async.} =
  ## Subscribe to the topic
  if consumer.subscribed:
    return

  var offsetConfig = %*{"type": "latest"}
  if consumer.config.offset == 0:
    offsetConfig = %*{"type": "earliest"}
  elif consumer.config.timestampMs > 0:
    offsetConfig = %*{"type": "timestamp", "timestamp": consumer.config.timestampMs}

  let params = %*{
    "topic": consumer.topic,
    "group": consumer.group,
    "offset": offsetConfig,
    "autoCommit": consumer.config.autoCommit,
    "sessionTimeoutMs": consumer.config.sessionTimeoutMs,
    "heartbeatIntervalMs": consumer.config.heartbeatIntervalMs
  }

  let response = await rpcCall(consumer.client, "consumer.subscribe", params)
  consumer.subscriptionId = response.getOrDefault("subscriptionId").getStr("")
  consumer.subscribed = true

proc unsubscribe*(consumer: Consumer): Future[void] {.async.} =
  ## Unsubscribe from the topic
  if not consumer.subscribed:
    return

  if consumer.subscriptionId.len > 0:
    let params = %*{"subscriptionId": consumer.subscriptionId}
    discard await rpcCall(consumer.client, "consumer.unsubscribe", params)

  consumer.subscribed = false
  consumer.subscriptionId = ""

proc poll*(consumer: Consumer, timeoutMs: int = 1000): Future[seq[KafkaRecord]] {.async.} =
  ## Poll for records
  if not consumer.subscribed:
    await consumer.subscribe()

  let params = %*{
    "topic": consumer.topic,
    "group": consumer.group,
    "subscriptionId": consumer.subscriptionId,
    "maxRecords": consumer.config.maxPollRecords,
    "timeoutMs": timeoutMs
  }

  let response = await rpcCall(consumer.client, "consumer.poll", params)

  result = @[]
  if response.hasKey("records"):
    for r in response["records"]:
      result.add(KafkaRecord.fromJson(r, consumer.client))

proc take*(consumer: Consumer, n: int): Future[seq[KafkaRecord]] {.async.} =
  ## Take a specific number of records
  if not consumer.subscribed:
    await consumer.subscribe()

  result = @[]
  while result.len < n:
    let records = await consumer.poll()
    for record in records:
      result.add(record)
      if result.len >= n:
        break

proc commit*(consumer: Consumer, partition: int32, offset: int64): Future[void] {.async.} =
  ## Commit offset for a partition
  let params = %*{
    "topic": consumer.topic,
    "partition": partition,
    "offset": offset
  }
  discard await rpcCall(consumer.client, "consumer.commit", params)

proc seek*(consumer: Consumer, partition: int32, offset: int64): Future[void] {.async.} =
  ## Seek to a specific offset
  let params = %*{
    "topic": consumer.topic,
    "group": consumer.group,
    "partition": partition,
    "offset": offset,
    "subscriptionId": consumer.subscriptionId
  }
  discard await rpcCall(consumer.client, "consumer.seek", params)

proc seekToBeginning*(consumer: Consumer, partition: int32): Future[void] {.async.} =
  ## Seek to the beginning of a partition
  let params = %*{
    "topic": consumer.topic,
    "group": consumer.group,
    "partition": partition,
    "subscriptionId": consumer.subscriptionId
  }
  discard await rpcCall(consumer.client, "consumer.seekToBeginning", params)

proc seekToEnd*(consumer: Consumer, partition: int32): Future[void] {.async.} =
  ## Seek to the end of a partition
  let params = %*{
    "topic": consumer.topic,
    "group": consumer.group,
    "partition": partition,
    "subscriptionId": consumer.subscriptionId
  }
  discard await rpcCall(consumer.client, "consumer.seekToEnd", params)

proc position*(consumer: Consumer, partition: int32): Future[int64] {.async.} =
  ## Get current position for a partition
  let params = %*{
    "topic": consumer.topic,
    "group": consumer.group,
    "partition": partition,
    "subscriptionId": consumer.subscriptionId
  }
  let response = await rpcCall(consumer.client, "consumer.position", params)
  return response.getOrDefault("offset").getBiggestInt(0)

proc pause*(consumer: Consumer): Future[void] {.async.} =
  ## Pause consumption
  let params = %*{"subscriptionId": consumer.subscriptionId}
  discard await rpcCall(consumer.client, "consumer.pause", params)

proc resume*(consumer: Consumer): Future[void] {.async.} =
  ## Resume consumption
  let params = %*{"subscriptionId": consumer.subscriptionId}
  discard await rpcCall(consumer.client, "consumer.resume", params)

proc partitions*(consumer: Consumer): Future[seq[int32]] {.async.} =
  ## Get assigned partitions
  let params = %*{
    "topic": consumer.topic,
    "group": consumer.group,
    "subscriptionId": consumer.subscriptionId
  }
  let response = await rpcCall(consumer.client, "consumer.partitions", params)

  result = @[]
  if response.hasKey("partitions"):
    for p in response["partitions"]:
      result.add(p.getInt.int32)

proc close*(consumer: Consumer): Future[void] {.async.} =
  ## Close the consumer
  await consumer.unsubscribe()

# BatchConsumer

proc newBatchConsumer*(client: pointer, topic: string, group: string, config: BatchConfig = nil): BatchConsumer =
  let batchCfg = if config.isNil: newBatchConfig() else: config
  let consumerCfg = newConsumerConfig(
    autoCommit = false,
    maxPollRecords = batchCfg.batchSize
  )

  BatchConsumer(
    client: client,
    topic: topic,
    group: group,
    config: batchCfg,
    consumer: newConsumer(client, topic, group, consumerCfg)
  )

proc poll*(bc: BatchConsumer): Future[Batch] {.async.} =
  ## Poll for a batch of records
  let records = await bc.consumer.poll(bc.config.batchTimeoutMs)
  return newBatch(records, bc.client)

proc close*(bc: BatchConsumer): Future[void] {.async.} =
  ## Close the batch consumer
  await bc.consumer.close()

# AsyncConsumer

proc newAsyncConsumer*(client: pointer, topic: string, group: string, config: ConsumerConfig = nil): AsyncConsumer =
  AsyncConsumer(
    client: client,
    topic: topic,
    group: group,
    config: if config.isNil: newConsumerConfig() else: config,
    consumer: newConsumer(client, topic, group, config),
    running: false
  )

proc start*(ac: AsyncConsumer): Future[void] {.async.} =
  ## Start consuming in background
  if ac.running:
    return
  ac.running = true
  await ac.consumer.subscribe()

proc stop*(ac: AsyncConsumer): Future[void] {.async.} =
  ## Stop consuming
  ac.running = false
  await ac.consumer.close()

proc next*(ac: AsyncConsumer): Future[KafkaRecord] {.async.} =
  ## Get next record
  while ac.running:
    let records = await ac.consumer.poll(1000)
    if records.len > 0:
      return records[0]

proc isRunning*(ac: AsyncConsumer): bool =
  ac.running
