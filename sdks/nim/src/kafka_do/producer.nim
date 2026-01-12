## Kafka producer for the .do platform

import std/[json, options, tables, asyncdispatch, times]
import ./errors
import ./config
import ./record

type
  Producer*[T] = ref object
    client*: pointer  # Forward reference to KafkaClient
    topic*: string
    config*: ProducerConfig

  Transaction*[T] = ref object
    client*: pointer
    topic*: string
    messages: seq[JsonNode]
    committed: bool

# Forward declare RPC call
proc rpcCall*(client: pointer, methodName: string, params: JsonNode): Future[JsonNode] {.async, importc.}

proc serialize*[T](value: T): JsonNode =
  ## Serialize value to JSON
  when T is JsonNode:
    value
  else:
    %value

# Producer

proc newProducer*[T](client: pointer, topic: string, config: ProducerConfig = nil): Producer[T] =
  Producer[T](
    client: client,
    topic: topic,
    config: if config.isNil: newProducerConfig() else: config
  )

proc send*[T](producer: Producer[T], value: T, key: Option[string] = none(string), headers: Table[string, string] = initTable[string, string]()): Future[RecordMetadata] {.async.} =
  ## Send a single message
  let params = %*{
    "topic": producer.topic,
    "key": if key.isSome: %key.get else: newJNull(),
    "value": serialize(value),
    "headers": headers,
    "acks": producer.config.acks
  }

  let response = await rpcCall(producer.client, "producer.send", params)
  return RecordMetadata.fromJson(response)

proc send*[T](producer: Producer[T], value: T, key: string, headers: Table[string, string] = initTable[string, string]()): Future[RecordMetadata] =
  ## Send with string key
  producer.send(value, some(key), headers)

proc sendBatch*[T](producer: Producer[T], values: seq[T]): Future[seq[RecordMetadata]] {.async.} =
  ## Send a batch of messages
  var messages: seq[JsonNode] = @[]
  for v in values:
    messages.add(%*{"value": serialize(v)})

  let params = %*{
    "topic": producer.topic,
    "messages": messages,
    "acks": producer.config.acks
  }

  let response = await rpcCall(producer.client, "producer.sendBatch", params)

  result = @[]
  if response.hasKey("results"):
    for r in response["results"]:
      result.add(RecordMetadata.fromJson(r))

proc sendBatch*[T](producer: Producer[T], messages: seq[Message[T]]): Future[seq[RecordMetadata]] {.async.} =
  ## Send a batch of messages with keys and headers
  var messageData: seq[JsonNode] = @[]
  for m in messages:
    messageData.add(%*{
      "key": if m.key.isSome: %m.key.get else: newJNull(),
      "value": serialize(m.value),
      "headers": m.headers
    })

  let params = %*{
    "topic": producer.topic,
    "messages": messageData,
    "acks": producer.config.acks
  }

  let response = await rpcCall(producer.client, "producer.sendBatch", params)

  result = @[]
  if response.hasKey("results"):
    for r in response["results"]:
      result.add(RecordMetadata.fromJson(r))

# Transaction

proc newTransaction*[T](client: pointer, topic: string): Transaction[T] =
  Transaction[T](
    client: client,
    topic: topic,
    messages: @[],
    committed: false
  )

proc send*[T](tx: Transaction[T], value: T, key: Option[string] = none(string), headers: Table[string, string] = initTable[string, string]()) =
  ## Add a message to the transaction
  if tx.committed:
    raise newTransactionError("Transaction already committed")

  tx.messages.add(%*{
    "key": if key.isSome: %key.get else: newJNull(),
    "value": serialize(value),
    "headers": headers
  })

proc commit*[T](tx: Transaction[T]): Future[seq[RecordMetadata]] {.async.} =
  ## Commit the transaction
  if tx.committed:
    raise newTransactionError("Transaction already committed")

  let params = %*{
    "topic": tx.topic,
    "messages": tx.messages
  }

  let response = await rpcCall(tx.client, "producer.transaction", params)
  tx.committed = true

  result = @[]
  if response.hasKey("results"):
    for r in response["results"]:
      result.add(RecordMetadata.fromJson(r))

proc abort*[T](tx: Transaction[T]) =
  ## Abort the transaction
  tx.messages = @[]
  tx.committed = true
