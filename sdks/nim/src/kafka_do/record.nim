## Kafka record types for the .do platform

import std/[json, options, tables, times]

type
  KafkaRecord* = ref object
    topic*: string
    partition*: int32
    offset*: int64
    key*: Option[string]
    value*: JsonNode
    timestamp*: DateTime
    headers*: Table[string, string]
    client*: pointer  # Forward reference to KafkaClient
    committed: bool

  RecordMetadata* = object
    topic*: string
    partition*: int32
    offset*: int64
    timestamp*: DateTime

  Message*[T] = object
    key*: Option[string]
    value*: T
    headers*: Table[string, string]

  Batch* = ref object
    records*: seq[KafkaRecord]
    client*: pointer
    committed: bool

# Forward declare commit_offset
proc commitOffset*(client: pointer, topic: string, partition: int32, offset: int64) {.importc.}

# KafkaRecord

proc newKafkaRecord*(
  topic: string,
  partition: int32,
  offset: int64,
  value: JsonNode,
  key: Option[string] = none(string),
  timestamp: DateTime = now(),
  headers: Table[string, string] = initTable[string, string](),
  client: pointer = nil
): KafkaRecord =
  KafkaRecord(
    topic: topic,
    partition: partition,
    offset: offset,
    key: key,
    value: value,
    timestamp: timestamp,
    headers: headers,
    client: client,
    committed: false
  )

proc fromJson*(T: typedesc[KafkaRecord], json: JsonNode, client: pointer = nil): KafkaRecord =
  ## Create record from JSON response
  var headers = initTable[string, string]()
  if json.hasKey("headers"):
    for key, val in json["headers"].pairs:
      headers[key] = val.getStr

  let tsStr = json.getOrDefault("timestamp").getStr("")
  let timestamp = if tsStr.len > 0:
    try:
      parse(tsStr, "yyyy-MM-dd'T'HH:mm:ss'Z'")
    except:
      now()
  else:
    now()

  newKafkaRecord(
    topic = json["topic"].getStr,
    partition = json["partition"].getInt.int32,
    offset = json["offset"].getBiggestInt,
    key = if json.hasKey("key") and json["key"].kind != JNull: some(json["key"].getStr) else: none(string),
    value = json.getOrDefault("value"),
    timestamp = timestamp,
    headers = headers,
    client = client
  )

proc commit*(record: KafkaRecord) =
  ## Commit this record's offset
  if record.committed:
    return
  if record.client != nil:
    commitOffset(record.client, record.topic, record.partition, record.offset)
  record.committed = true

proc isCommitted*(record: KafkaRecord): bool =
  record.committed

proc valueTo*[T](record: KafkaRecord): T =
  ## Parse the value as a specific type
  to(record.value, T)

# RecordMetadata

proc newRecordMetadata*(topic: string, partition: int32, offset: int64, timestamp: DateTime = now()): RecordMetadata =
  RecordMetadata(topic: topic, partition: partition, offset: offset, timestamp: timestamp)

proc fromJson*(T: typedesc[RecordMetadata], json: JsonNode): RecordMetadata =
  let tsStr = json.getOrDefault("timestamp").getStr("")
  let timestamp = if tsStr.len > 0:
    try:
      parse(tsStr, "yyyy-MM-dd'T'HH:mm:ss'Z'")
    except:
      now()
  else:
    now()

  RecordMetadata(
    topic: json["topic"].getStr,
    partition: json["partition"].getInt.int32,
    offset: json["offset"].getBiggestInt,
    timestamp: timestamp
  )

# Message

proc newMessage*[T](value: T, key: Option[string] = none(string), headers: Table[string, string] = initTable[string, string]()): Message[T] =
  Message[T](key: key, value: value, headers: headers)

# Batch

proc newBatch*(records: seq[KafkaRecord], client: pointer = nil): Batch =
  Batch(records: records, client: client, committed: false)

proc commit*(batch: Batch) =
  ## Commit all records in the batch
  if batch.committed:
    return
  for record in batch.records:
    record.commit()
  batch.committed = true

iterator items*(batch: Batch): KafkaRecord =
  for record in batch.records:
    yield record

proc len*(batch: Batch): int =
  batch.records.len

proc isEmpty*(batch: Batch): bool =
  batch.records.len == 0

proc first*(batch: Batch): Option[KafkaRecord] =
  if batch.records.len > 0:
    some(batch.records[0])
  else:
    none(KafkaRecord)

proc last*(batch: Batch): Option[KafkaRecord] =
  if batch.records.len > 0:
    some(batch.records[^1])
  else:
    none(KafkaRecord)
