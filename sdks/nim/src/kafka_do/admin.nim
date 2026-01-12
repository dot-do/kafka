## Kafka admin for the .do platform

import std/[json, options, tables, asyncdispatch, times]
import ./errors
import ./config

type
  Admin* = ref object
    client*: pointer  # Forward reference to KafkaClient

  TopicInfo* = object
    name*: string
    partitions*: int
    replicationFactor*: int
    retentionMs*: int64
    cleanupPolicy*: string
    internal*: bool

  GroupInfo* = object
    id*: string
    state*: string
    memberCount*: int
    members*: seq[GroupMember]
    totalLag*: int64

  GroupMember* = object
    id*: string
    clientId*: string
    host*: string
    partitions*: seq[int32]

  PartitionInfo* = object
    id*: int32
    leader*: int32
    replicas*: seq[int32]
    isr*: seq[int32]
    beginningOffset*: int64
    endOffset*: int64

# Forward declare RPC call
proc rpcCall*(client: pointer, methodName: string, params: JsonNode): Future[JsonNode] {.async, importc.}

# Admin

proc newAdmin*(client: pointer): Admin =
  Admin(client: client)

# Topic Info from JSON

proc fromJson*(T: typedesc[TopicInfo], json: JsonNode): TopicInfo =
  TopicInfo(
    name: json["name"].getStr,
    partitions: json.getOrDefault("partitions").getInt(1),
    replicationFactor: json.getOrDefault("replicationFactor").getInt(1),
    retentionMs: json.getOrDefault("retentionMs").getBiggestInt(0),
    cleanupPolicy: json.getOrDefault("cleanupPolicy").getStr("delete"),
    internal: json.getOrDefault("internal").getBool(false)
  )

proc fromJson*(T: typedesc[GroupMember], json: JsonNode): GroupMember =
  var partitions: seq[int32] = @[]
  if json.hasKey("partitions"):
    for p in json["partitions"]:
      partitions.add(p.getInt.int32)

  GroupMember(
    id: json.getOrDefault("memberId").getStr(""),
    clientId: json.getOrDefault("clientId").getStr(""),
    host: json.getOrDefault("host").getStr(""),
    partitions: partitions
  )

proc fromJson*(T: typedesc[GroupInfo], json: JsonNode): GroupInfo =
  var members: seq[GroupMember] = @[]
  if json.hasKey("members"):
    for m in json["members"]:
      members.add(GroupMember.fromJson(m))

  GroupInfo(
    id: json.getOrDefault("id").getStr(json.getOrDefault("groupId").getStr("")),
    state: json.getOrDefault("state").getStr("unknown"),
    memberCount: json.getOrDefault("memberCount").getInt(members.len),
    members: members,
    totalLag: json.getOrDefault("totalLag").getBiggestInt(0)
  )

proc fromJson*(T: typedesc[PartitionInfo], json: JsonNode): PartitionInfo =
  var replicas: seq[int32] = @[]
  if json.hasKey("replicas"):
    for r in json["replicas"]:
      replicas.add(r.getInt.int32)

  var isr: seq[int32] = @[]
  if json.hasKey("isr"):
    for i in json["isr"]:
      isr.add(i.getInt.int32)

  PartitionInfo(
    id: json.getOrDefault("partition").getInt(json.getOrDefault("id").getInt(0)).int32,
    leader: json.getOrDefault("leader").getInt(0).int32,
    replicas: replicas,
    isr: isr,
    beginningOffset: json.getOrDefault("beginningOffset").getBiggestInt(0),
    endOffset: json.getOrDefault("endOffset").getBiggestInt(0)
  )

# Topic Operations

proc createTopic*(admin: Admin, name: string, config: TopicConfig = nil): Future[void] {.async.} =
  ## Create a new topic
  let cfg = if config.isNil: newTopicConfig() else: config

  let params = %*{
    "name": name,
    "partitions": cfg.partitions,
    "replicationFactor": cfg.replicationFactor,
    "retentionMs": cfg.retentionMs,
    "cleanupPolicy": cfg.cleanupPolicy,
    "compressionType": cfg.compressionType
  }

  discard await rpcCall(admin.client, "admin.createTopic", params)

proc deleteTopic*(admin: Admin, name: string): Future[void] {.async.} =
  ## Delete a topic
  let params = %*{"name": name}
  discard await rpcCall(admin.client, "admin.deleteTopic", params)

proc listTopics*(admin: Admin): Future[seq[TopicInfo]] {.async.} =
  ## List all topics
  let response = await rpcCall(admin.client, "admin.listTopics", %*{})

  result = @[]
  if response.hasKey("topics"):
    for t in response["topics"]:
      result.add(TopicInfo.fromJson(t))

proc describeTopic*(admin: Admin, name: string): Future[TopicInfo] {.async.} =
  ## Describe a specific topic
  let params = %*{"name": name}
  let response = await rpcCall(admin.client, "admin.describeTopic", params)
  return TopicInfo.fromJson(response)

proc alterTopic*(admin: Admin, name: string, config: TopicConfig): Future[void] {.async.} =
  ## Alter topic configuration
  let params = %*{
    "name": name,
    "retentionMs": config.retentionMs,
    "cleanupPolicy": config.cleanupPolicy,
    "compressionType": config.compressionType
  }
  discard await rpcCall(admin.client, "admin.alterTopic", params)

# Consumer Group Operations

proc listGroups*(admin: Admin): Future[seq[GroupInfo]] {.async.} =
  ## List consumer groups
  let response = await rpcCall(admin.client, "admin.listGroups", %*{})

  result = @[]
  if response.hasKey("groups"):
    for g in response["groups"]:
      result.add(GroupInfo.fromJson(g))

proc describeGroup*(admin: Admin, groupId: string): Future[GroupInfo] {.async.} =
  ## Describe a consumer group
  let params = %*{"groupId": groupId}
  let response = await rpcCall(admin.client, "admin.describeGroup", params)
  return GroupInfo.fromJson(response)

proc deleteGroup*(admin: Admin, groupId: string): Future[void] {.async.} =
  ## Delete a consumer group
  let params = %*{"groupId": groupId}
  discard await rpcCall(admin.client, "admin.deleteGroup", params)

proc resetOffsets*(admin: Admin, groupId: string, topic: string, offsetType: int, timestampMs: int64 = 0): Future[void] {.async.} =
  ## Reset offsets for a consumer group
  var offsetConfig: JsonNode
  if timestampMs > 0:
    offsetConfig = %*{"type": "timestamp", "timestamp": timestampMs}
  elif offsetType == 0:
    offsetConfig = %*{"type": "earliest"}
  else:
    offsetConfig = %*{"type": "latest"}

  let params = %*{
    "groupId": groupId,
    "topic": topic,
    "offset": offsetConfig
  }
  discard await rpcCall(admin.client, "admin.resetOffsets", params)

proc getOffsets*(admin: Admin, groupId: string, topic: string): Future[Table[int32, int64]] {.async.} =
  ## Get consumer group offsets
  let params = %*{
    "groupId": groupId,
    "topic": topic
  }
  let response = await rpcCall(admin.client, "admin.getOffsets", params)

  result = initTable[int32, int64]()
  if response.hasKey("offsets"):
    for key, val in response["offsets"].pairs:
      result[parseInt(key).int32] = val.getBiggestInt

# Partition Operations

proc getPartitions*(admin: Admin, topic: string): Future[seq[PartitionInfo]] {.async.} =
  ## Get topic partition info
  let params = %*{"topic": topic}
  let response = await rpcCall(admin.client, "admin.getPartitions", params)

  result = @[]
  if response.hasKey("partitions"):
    for p in response["partitions"]:
      result.add(PartitionInfo.fromJson(p))
