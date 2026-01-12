## Kafka SDK for the .do platform
## Event Streaming for Nim. Async. Templates. Zero Ops.
##
## Example usage:
## ```nim
## import kafka_do
##
## let kafka = newKafkaClient()
##
## # Produce
## kafka.produce("orders", Order(orderId: "123", amount: 99.99))
##
## # Consume
## for record in kafka.consume("orders", group = "my-processor"):
##   echo "Received: ", record.value
##   record.commit()
##
## # Stream processing
## kafka.stream("orders")
##   .filter(proc(o: Order): bool = o.amount > 100)
##   .map(proc(o: Order): PremiumOrder = PremiumOrder(order: o))
##   .to("premium-orders")
## ```

import std/[asyncdispatch, json, tables, options, strutils, sequtils, locks, times, random]

import ./kafka_do/errors
import ./kafka_do/config
import ./kafka_do/record
import ./kafka_do/producer
import ./kafka_do/consumer
import ./kafka_do/admin
import ./kafka_do/stream
import ./kafka_do/client

export errors, config, record, producer, consumer, admin, stream, client
export asyncdispatch, json, options, tables, times

const Version* = "0.1.0"

# ---------------------------------------------------------
# Enums and Types
# ---------------------------------------------------------

type
  Offset* = enum
    Earliest
    Latest

  TimestampOffset* = object
    timestamp*: Time

  Acks* = enum
    None = 0
    Leader = 1
    All = -1

  Compression* = enum
    CompressionNone = 0
    CompressionGzip = 1
    CompressionSnappy = 2
    CompressionLz4 = 3
    CompressionZstd = 4

proc newTimestampOffset*(ts: Time): TimestampOffset =
  TimestampOffset(timestamp: ts)

# ---------------------------------------------------------
# Module-level Helpers
# ---------------------------------------------------------

proc connect*(url: string = "", apiKey: string = ""): KafkaClient =
  ## Connect to Kafka
  var config = newKafkaConfig()
  if url.len > 0:
    config.url = url
  if apiKey.len > 0:
    config.apiKey = apiKey
  newKafkaClient(config)

proc connect*(): KafkaClient =
  ## Connect using environment configuration
  newKafkaClient()

# ---------------------------------------------------------
# Context Manager Pattern
# ---------------------------------------------------------

template withKafka*(url: string, body: untyped): untyped =
  ## Context manager for Kafka client
  ##
  ## Example:
  ## ```nim
  ## withKafka("https://kafka.do"):
  ##   kafka.produce("orders", order)
  ## ```
  block:
    let kafka {.inject.} = connect(url)
    try:
      body
    finally:
      kafka.close()
