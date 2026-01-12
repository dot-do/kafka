## Kafka client for the .do platform

import std/[json, options, tables, asyncdispatch, httpclient, random, times]
import ./errors
import ./config
import ./record
import ./producer
import ./consumer
import ./admin
import ./stream

type
  KafkaClient* = ref object
    config*: KafkaConfig
    httpClient: HttpClient
    connected: bool

proc newKafkaClient*(): KafkaClient =
  ## Create a new Kafka client with defaults from environment
  KafkaClient(
    config: newKafkaConfig(),
    httpClient: newHttpClient(),
    connected: true
  )

proc newKafkaClient*(config: KafkaConfig): KafkaClient =
  ## Create a new Kafka client with custom configuration
  KafkaClient(
    config: config,
    httpClient: newHttpClient(),
    connected: true
  )

proc newKafkaClient*(url: string, apiKey: string = ""): KafkaClient =
  ## Create a new Kafka client with URL and optional API key
  newKafkaClient(newKafkaConfig(url, apiKey))

# ---------------------------------------------------------
# RPC Call Implementation
# ---------------------------------------------------------

proc rpcCall*(client: KafkaClient, methodName: string, params: JsonNode): Future[JsonNode] {.async.} =
  ## Make an RPC call to the server
  if not client.connected:
    raise newConnectionError("Client is not connected")

  let body = %*{
    "jsonrpc": "2.0",
    "method": methodName,
    "params": params,
    "id": $rand(high(int))
  }

  let response = client.httpClient.request(
    client.config.url & "/rpc",
    httpMethod = HttpPost,
    headers = client.config.headers,
    body = $body
  )

  if response.code != Http200:
    case response.code
    of Http401, Http403:
      raise newUnauthorizedError("Authentication failed: " & $response.code)
    of Http404:
      raise newConnectionError("Endpoint not found", client.config.url)
    of Http408, Http504:
      raise newTimeoutError("Request timed out")
    of Http429:
      raise newQuotaExceededError("Rate limit exceeded")
    else:
      raise newConnectionError("Server error: " & $response.code, client.config.url)

  let json = parseJson(response.body)

  if json.hasKey("error"):
    let error = json["error"]
    let code = error.getOrDefault("code").getStr("UNKNOWN")
    let message = error.getOrDefault("message").getStr("Unknown error")

    case code
    of "TOPIC_NOT_FOUND":
      raise newTopicNotFoundError(message)
    of "MESSAGE_TOO_LARGE":
      raise newMessageTooLargeError(0, 0)
    of "UNAUTHORIZED":
      raise newUnauthorizedError(message)
    of "TIMEOUT":
      raise newTimeoutError(message)
    of "QUOTA_EXCEEDED":
      raise newQuotaExceededError(message)
    of "REBALANCE_IN_PROGRESS":
      raise newRebalanceInProgressError()
    else:
      raise newKafkaError(code, message)

  return json.getOrDefault("result")

# Wrapper for pointer-based RPC call
proc rpcCall*(clientPtr: pointer, methodName: string, params: JsonNode): Future[JsonNode] {.async.} =
  let client = cast[KafkaClient](clientPtr)
  return await client.rpcCall(methodName, params)

# ---------------------------------------------------------
# Producer Operations
# ---------------------------------------------------------

proc producer*[T](client: KafkaClient, topic: string, config: ProducerConfig = nil): Producer[T] =
  ## Create a producer for a specific topic
  newProducer[T](cast[pointer](client), topic, config)

proc producer*(client: KafkaClient, topic: string, config: ProducerConfig = nil): Producer[JsonNode] =
  ## Create a JSON producer for a specific topic
  newProducer[JsonNode](cast[pointer](client), topic, config)

proc produce*[T](client: KafkaClient, topic: string, value: T, key: Option[string] = none(string), headers: Table[string, string] = initTable[string, string]()): Future[RecordMetadata] {.async.} =
  ## Produce a message directly (convenience method)
  let prod = client.producer[T](topic)
  return await prod.send(value, key, headers)

proc produce*[T](client: KafkaClient, topic: string, value: T, key: string, headers: Table[string, string] = initTable[string, string]()): Future[RecordMetadata] =
  ## Produce with string key
  client.produce(topic, value, some(key), headers)

# ---------------------------------------------------------
# Consumer Operations
# ---------------------------------------------------------

proc consume*(client: KafkaClient, topic: string, group: string, config: ConsumerConfig = nil): Consumer =
  ## Create a consumer for a topic
  newConsumer(cast[pointer](client), topic, group, config)

proc consumer*(client: KafkaClient, topic: string, group: string, config: ConsumerConfig = nil): Consumer =
  ## Alias for consume
  client.consume(topic, group, config)

proc consumeBatch*(client: KafkaClient, topic: string, group: string, config: BatchConfig = nil): BatchConsumer =
  ## Create a batch consumer
  newBatchConsumer(cast[pointer](client), topic, group, config)

proc asyncConsumer*(client: KafkaClient, topic: string, group: string, config: ConsumerConfig = nil): AsyncConsumer =
  ## Create an async consumer
  newAsyncConsumer(cast[pointer](client), topic, group, config)

# ---------------------------------------------------------
# Transaction Operations
# ---------------------------------------------------------

proc transaction*[T](client: KafkaClient, topic: string): Transaction[T] =
  ## Start a transaction
  newTransaction[T](cast[pointer](client), topic)

template transaction*(client: KafkaClient, topic: string, body: untyped): seq[RecordMetadata] =
  ## Execute operations in a transaction
  ##
  ## Example:
  ## ```nim
  ## let results = client.transaction("orders"):
  ##   tx.send(Order(orderId: "123", status: Created))
  ##   tx.send(Order(orderId: "123", status: Validated))
  ## ```
  block:
    let tx {.inject.} = client.transaction[JsonNode](topic)
    try:
      body
      waitFor tx.commit()
    except CatchableError as e:
      tx.abort()
      raise e

# ---------------------------------------------------------
# Stream Operations
# ---------------------------------------------------------

proc stream*[T](client: KafkaClient, topic: string): Stream[T] =
  ## Create a stream for the given topic
  newStream[T](cast[pointer](client), topic)

proc stream*(client: KafkaClient, topic: string): Stream[JsonNode] =
  ## Create a JSON stream for the given topic
  newStream[JsonNode](cast[pointer](client), topic)

# ---------------------------------------------------------
# Admin Operations
# ---------------------------------------------------------

proc admin*(client: KafkaClient): Admin =
  ## Get admin client
  newAdmin(cast[pointer](client))

# ---------------------------------------------------------
# Commit Operations (for Record)
# ---------------------------------------------------------

proc commitOffset*(client: KafkaClient, topic: string, partition: int32, offset: int64): Future[void] {.async.} =
  ## Commit an offset
  let params = %*{
    "topic": topic,
    "partition": partition,
    "offset": offset
  }
  discard await client.rpcCall("consumer.commit", params)

# Wrapper for pointer-based commit
proc commitOffset*(clientPtr: pointer, topic: string, partition: int32, offset: int64) =
  let client = cast[KafkaClient](clientPtr)
  waitFor client.commitOffset(topic, partition, offset)

# ---------------------------------------------------------
# Connection Management
# ---------------------------------------------------------

proc connected*(client: KafkaClient): bool =
  ## Check if connected
  client.connected

proc close*(client: KafkaClient) =
  ## Close the client
  client.httpClient.close()
  client.connected = false
