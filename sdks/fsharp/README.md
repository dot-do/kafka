# Kafka.Do

> Event Streaming for F#. Functional. Async. Zero Ops.

```fsharp
let producer = kafka.Producer<Order>("orders")
do! producer.Send { OrderId = "123"; Amount = 99.99 }
```

Computation expressions. Discriminated unions. The F# way.

## Installation

```bash
dotnet add package DotDo.Kafka.FSharp
```

Requires .NET 8.0+ and F# 8+.

## Quick Start

```fsharp
open DotDo.Kafka

type Order = { OrderId: string; Amount: decimal }

[<EntryPoint>]
let main _ =
    async {
        let kafka = Kafka()

        // Produce
        let producer = kafka.Producer<Order>("orders")
        do! producer.Send { OrderId = "123"; Amount = 99.99m }

        // Consume
        for record in kafka.Consumer<Order>("orders", "my-processor") do
            printfn "Received: %A" record.Value
            do! record.Commit()
    } |> Async.RunSynchronously
    0
```

## Producing Messages

### Simple Producer

```fsharp
let kafka = Kafka()
let producer = kafka.Producer<Order>("orders")

// Send single message
do! producer.Send { OrderId = "123"; Amount = 99.99m }

// Send with key for partitioning
do! producer.Send(
    { OrderId = "123"; Amount = 99.99m },
    key = "customer-456"
)

// Send with headers
do! producer.Send(
    { OrderId = "123"; Amount = 99.99m },
    key = "customer-456",
    headers = Map.ofList [ "correlation-id", "abc-123" ]
)
```

### Batch Producer

```fsharp
let producer = kafka.Producer<Order>("orders")

// Send batch
do! producer.SendBatch [
    { OrderId = "124"; Amount = 49.99m }
    { OrderId = "125"; Amount = 149.99m }
    { OrderId = "126"; Amount = 29.99m }
]

// Send batch with keys
do! producer.SendBatch [
    { Key = Some "cust-1"; Value = { OrderId = "124"; Amount = 49.99m } }
    { Key = Some "cust-2"; Value = { OrderId = "125"; Amount = 149.99m } }
]
```

### Fire-and-Forget

```fsharp
let producer = kafka.Producer<Order>("orders")

// Non-blocking send
producer.Send order |> Async.Start
```

### Transactional Producer

```fsharp
do! kafka.Transaction<Order>("orders", fun tx -> async {
    do! tx.Send { OrderId = "123"; Status = Created }
    do! tx.Send { OrderId = "123"; Status = Validated }
    // Automatically commits on success, aborts on exception
})
```

## Consuming Messages

### Basic Consumer with AsyncSeq

```fsharp
open FSharp.Control

let consumer = kafka.Consumer<Order>("orders", "order-processor")

consumer
|> AsyncSeq.iter (fun record ->
    printfn "Topic: %s" record.Topic
    printfn "Partition: %d" record.Partition
    printfn "Offset: %d" record.Offset
    printfn "Key: %A" record.Key
    printfn "Value: %A" record.Value
    printfn "Timestamp: %A" record.Timestamp
    printfn "Headers: %A" record.Headers

    processOrder record.Value
    record.Commit() |> Async.RunSynchronously
)
|> Async.RunSynchronously
```

### Consumer with Functional Operations

```fsharp
let consumer = kafka.Consumer<Order>("orders", "processor")

// Filter and map
consumer
|> AsyncSeq.filter (fun r -> r.Value.Amount > 100m)
|> AsyncSeq.map (fun r -> r.Value)
|> AsyncSeq.iter processHighValueOrder
|> Async.RunSynchronously

// Take specific number
consumer
|> AsyncSeq.take 10
|> AsyncSeq.iter (fun record ->
    processOrder record.Value
    record.Commit() |> Async.RunSynchronously
)
|> Async.RunSynchronously
```

### Consumer with Configuration

```fsharp
let config = {
    Offset = Earliest
    AutoCommit = true
    MaxPollRecords = 100
    SessionTimeout = TimeSpan.FromSeconds 30.0
}

let consumer = kafka.Consumer<Order>("orders", "processor", config)

consumer
|> AsyncSeq.iter (fun record ->
    processOrder record.Value
    // Auto-committed
)
|> Async.RunSynchronously
```

### Consumer from Timestamp

```fsharp
let yesterday = DateTimeOffset.UtcNow.AddDays(-1.0)

let config = { ConsumerConfig.Default with Offset = Timestamp yesterday }
let consumer = kafka.Consumer<Order>("orders", "replay", config)

consumer
|> AsyncSeq.iter (fun record ->
    printfn "Replaying: %A" record.Value
)
|> Async.RunSynchronously
```

### Batch Consumer

```fsharp
let config = {
    BatchSize = 100
    BatchTimeout = TimeSpan.FromSeconds 5.0
}

let consumer = kafka.BatchConsumer<Order>("orders", "batch-processor", config)

consumer
|> AsyncSeq.iter (fun batch ->
    batch.Records |> Seq.iter (fun r -> processOrder r.Value)
    batch.Commit() |> Async.RunSynchronously
)
|> Async.RunSynchronously
```

### Parallel Consumer with MailboxProcessor

```fsharp
let agent = MailboxProcessor.Start(fun inbox ->
    let rec loop () = async {
        let! record = inbox.Receive()
        do! processOrderAsync record.Value
        do! record.Commit()
        return! loop()
    }
    loop()
)

kafka.Consumer<Order>("orders", "parallel-processor")
|> AsyncSeq.iter agent.Post
|> Async.RunSynchronously
```

## Stream Processing

### Filter and Transform

```fsharp
kafka.Stream<Order>("orders")
|> KafkaStream.filter (fun order -> order.Amount > 100m)
|> KafkaStream.map (fun order -> { order with Tier = Some "premium" })
|> KafkaStream.toTopic "high-value-orders"
|> Async.RunSynchronously
```

### Windowed Aggregations

```fsharp
kafka.Stream<Order>("orders")
|> KafkaStream.window (Tumbling (TimeSpan.FromMinutes 5.0))
|> KafkaStream.groupBy (fun order -> order.CustomerId)
|> KafkaStream.count
|> AsyncSeq.iter (fun (key, window) ->
    printfn "Customer %s: %d orders in %A-%A" key window.Value window.Start window.End
)
|> Async.RunSynchronously
```

### Joins

```fsharp
let orders = kafka.Stream<Order>("orders")
let customers = kafka.Stream<Customer>("customers")

KafkaStream.join orders customers
    (fun order customer -> order.CustomerId = customer.Id)
    (TimeSpan.FromHours 1.0)
|> AsyncSeq.iter (fun (order, customer) ->
    printfn "Order by %s" customer.Name
)
|> Async.RunSynchronously
```

### Branching

```fsharp
kafka.Stream<Order>("orders")
|> KafkaStream.branch [
    (fun o -> o.Region = "us"), "us-orders"
    (fun o -> o.Region = "eu"), "eu-orders"
    (fun _ -> true), "other-orders"
]
|> Async.RunSynchronously
```

### Aggregations with Fold

```fsharp
kafka.Stream<Order>("orders")
|> KafkaStream.groupBy (fun order -> order.CustomerId)
|> KafkaStream.fold 0m (fun total order -> total + order.Amount)
|> AsyncSeq.iter (fun (customerId, total) ->
    printfn "Customer %s total: $%M" customerId total
)
|> Async.RunSynchronously
```

## Topic Administration

```fsharp
let admin = kafka.Admin

// Create topic
do! admin.CreateTopic("orders", {
    Partitions = 3
    RetentionMs = int64 (TimeSpan.FromDays(7.0).TotalMilliseconds)
})

// List topics
let! topics = admin.ListTopics()
topics |> Seq.iter (fun topic ->
    printfn "%s: %d partitions" topic.Name topic.Partitions
)

// Describe topic
let! info = admin.DescribeTopic("orders")
printfn "Partitions: %d" info.Partitions
printfn "Retention: %dms" info.RetentionMs

// Alter topic
do! admin.AlterTopic("orders", {
    RetentionMs = int64 (TimeSpan.FromDays(30.0).TotalMilliseconds)
})

// Delete topic
do! admin.DeleteTopic("old-events")
```

## Consumer Groups

```fsharp
let admin = kafka.Admin

// List groups
let! groups = admin.ListGroups()
groups |> Seq.iter (fun group ->
    printfn "Group: %s, Members: %d" group.Id group.MemberCount
)

// Describe group
let! info = admin.DescribeGroup("order-processor")
printfn "State: %A" info.State
printfn "Members: %d" info.Members.Length
printfn "Total Lag: %d" info.TotalLag

// Reset offsets
do! admin.ResetOffsets("order-processor", "orders", Earliest)

// Reset to timestamp
do! admin.ResetOffsets("order-processor", "orders", Timestamp yesterday)
```

## Error Handling

### Result-Based Error Handling

```fsharp
let producer = kafka.Producer<Order>("orders")

match! producer.TrySend order with
| Ok metadata ->
    printfn "Sent to partition %d" metadata.Partition
| Error (TopicNotFound _) ->
    printfn "Topic not found"
| Error (MessageTooLarge _) ->
    printfn "Message too large"
| Error Timeout ->
    printfn "Request timed out"
| Error e when e.IsRetriable ->
    // Safe to retry
    do! retryWithBackoff (fun () -> producer.Send order)
| Error e ->
    printfn "Kafka error: %s - %s" e.Code e.Message
```

### Discriminated Union Errors

```fsharp
type KafkaError =
    | TopicNotFound of topic: string
    | PartitionNotFound of topic: string * partition: int
    | MessageTooLarge of size: int64 * maxSize: int64
    | NotLeader of topic: string * partition: int
    | OffsetOutOfRange of offset: int64 * topic: string
    | GroupCoordinator of message: string
    | RebalanceInProgress
    | Unauthorized of message: string
    | QuotaExceeded
    | Timeout
    | Disconnected
    | Serialization of exn

    member this.IsRetriable =
        match this with
        | NotLeader _ | GroupCoordinator _ | RebalanceInProgress | Timeout -> true
        | _ -> false
```

### Dead Letter Queue Pattern

```fsharp
let consumer = kafka.Consumer<Order>("orders", "processor")
let dlqProducer = kafka.Producer<DlqRecord>("orders-dlq")

consumer
|> AsyncSeq.iter (fun record ->
    match processOrder record.Value with
    | Ok () -> ()
    | Error e ->
        dlqProducer.Send(
            { OriginalRecord = record.Value
              Error = e.ToString()
              Timestamp = DateTimeOffset.UtcNow },
            headers = Map.ofList [
                "original-topic", record.Topic
                "original-partition", string record.Partition
                "original-offset", string record.Offset
            ]
        ) |> Async.RunSynchronously

    record.Commit() |> Async.RunSynchronously
)
|> Async.RunSynchronously
```

### Retry with Exponential Backoff

```fsharp
let rec withRetry maxAttempts baseDelay attempt operation = async {
    match! operation () with
    | Ok result -> return Ok result
    | Error e when e.IsRetriable && attempt < maxAttempts ->
        let delay = baseDelay * (pown 2.0 (attempt - 1))
        let jitter = Random().NextDouble() * delay * 0.1
        do! Async.Sleep (int (delay + jitter) * 1000)
        return! withRetry maxAttempts baseDelay (attempt + 1) operation
    | Error e -> return Error e
}

let producer = kafka.Producer<Order>("orders")
let! result = withRetry 3 1.0 1 (fun () -> producer.TrySend order)
```

## Configuration

### Environment Variables

```bash
export KAFKA_DO_URL=https://kafka.do
export KAFKA_DO_API_KEY=your-api-key
```

### Client Configuration

```fsharp
let config = {
    Url = "https://kafka.do"
    ApiKey = "your-api-key"
    Timeout = TimeSpan.FromSeconds 30.0
    Retries = 3
}

let kafka = Kafka(config)
```

### Producer Configuration

```fsharp
let config = {
    BatchSize = 16384
    LingerMs = 5
    Compression = Gzip
    Acks = All
    Retries = 3
    RetryBackoffMs = 100
}

let producer = kafka.Producer<Order>("orders", config)
```

### Consumer Configuration

```fsharp
let config = {
    Offset = Latest
    AutoCommit = false
    FetchMinBytes = 1
    FetchMaxWaitMs = 500
    MaxPollRecords = 500
    SessionTimeout = TimeSpan.FromSeconds 30.0
    HeartbeatInterval = TimeSpan.FromSeconds 3.0
}

let consumer = kafka.Consumer<Order>("orders", "processor", config)
```

## Testing

### Mock Client

```fsharp
open DotDo.Kafka.Testing
open Expecto

[<Tests>]
let tests =
    testList "OrderProcessor" [
        testAsync "processes all orders" {
            let kafka = MockKafka()

            // Seed test data
            kafka.Seed("orders", [
                { OrderId = "123"; Amount = 99.99m }
                { OrderId = "124"; Amount = 149.99m }
            ])

            // Process
            let processed =
                kafka.Consumer<Order>("orders", "test")
                |> AsyncSeq.take 2
                |> AsyncSeq.map (fun r -> r.Value)
                |> AsyncSeq.toList
                |> Async.RunSynchronously

            Expect.equal (List.length processed) 2 "Should have 2 orders"
            Expect.equal processed.[0].OrderId "123" "First order should be 123"
        }

        testAsync "sends messages" {
            let kafka = MockKafka()
            let producer = kafka.Producer<Order>("orders")

            do! producer.Send { OrderId = "125"; Amount = 99.99m }

            let messages = kafka.GetMessages<Order>("orders")
            Expect.equal (Seq.length messages) 1 "Should have 1 message"
            Expect.equal (Seq.head messages).OrderId "125" "Order ID should be 125"
        }
    ]
```

### Integration Testing

```fsharp
open Expecto

[<Tests>]
let integrationTests =
    testList "Integration" [
        testAsync "end to end produces and consumes" {
            let kafka = Kafka({
                Url = Environment.GetEnvironmentVariable("TEST_KAFKA_URL")
                ApiKey = Environment.GetEnvironmentVariable("TEST_KAFKA_API_KEY")
            })

            let topic = sprintf "test-orders-%s" (Guid.NewGuid().ToString())

            try
                // Produce
                let producer = kafka.Producer<Order>(topic)
                do! producer.Send { OrderId = "123"; Amount = 99.99m }

                // Consume
                let! record =
                    kafka.Consumer<Order>(topic, "test")
                    |> AsyncSeq.take 1
                    |> AsyncSeq.tryFirst

                match record with
                | Some r -> Expect.equal r.Value.OrderId "123" "Order ID should match"
                | None -> failtest "No record received"
            finally
                kafka.Admin.DeleteTopic(topic) |> Async.RunSynchronously |> ignore
        }
    ] |> testLabel "integration"
```

## API Reference

### Kafka

```fsharp
type Kafka =
    new: ?config: KafkaConfig -> Kafka

    member Producer<'T> : topic: string * ?config: ProducerConfig -> Producer<'T>
    member Consumer<'T> : topic: string * group: string * ?config: ConsumerConfig -> AsyncSeq<KafkaRecord<'T>>
    member BatchConsumer<'T> : topic: string * group: string * config: BatchConsumerConfig -> AsyncSeq<Batch<'T>>
    member Transaction<'T> : topic: string * operation: (Transaction<'T> -> Async<unit>) -> Async<unit>
    member Stream<'T> : topic: string -> KafkaStream<'T>
    member Admin : Admin
```

### Producer

```fsharp
type Producer<'T> =
    member Send : value: 'T * ?key: string * ?headers: Map<string, string> -> Async<RecordMetadata>
    member TrySend : value: 'T * ?key: string * ?headers: Map<string, string> -> Async<Result<RecordMetadata, KafkaError>>
    member SendBatch : values: 'T list -> Async<RecordMetadata list>
    member SendBatch : messages: Message<'T> list -> Async<RecordMetadata list>
```

### KafkaRecord

```fsharp
type KafkaRecord<'T> = {
    Topic: string
    Partition: int
    Offset: int64
    Key: string option
    Value: 'T
    Timestamp: DateTimeOffset
    Headers: Map<string, string>
}

member Commit : unit -> Async<unit>
```

### KafkaStream

```fsharp
module KafkaStream =
    val filter : predicate: ('T -> bool) -> stream: KafkaStream<'T> -> KafkaStream<'T>
    val map : mapper: ('T -> 'R) -> stream: KafkaStream<'T> -> KafkaStream<'R>
    val flatMap : mapper: ('T -> 'R list) -> stream: KafkaStream<'T> -> KafkaStream<'R>
    val window : window: Window -> stream: KafkaStream<'T> -> WindowedStream<'T>
    val groupBy : keySelector: ('T -> 'K) -> stream: KafkaStream<'T> -> GroupedStream<'K, 'T>
    val fold : initial: 'S -> folder: ('S -> 'T -> 'S) -> stream: GroupedStream<'K, 'T> -> AsyncSeq<'K * 'S>
    val count : stream: GroupedStream<'K, 'T> -> AsyncSeq<'K * WindowResult<int64>>
    val branch : branches: (('T -> bool) * string) list -> stream: KafkaStream<'T> -> Async<unit>
    val join : other: KafkaStream<'R> -> condition: ('T -> 'R -> bool) -> window: TimeSpan -> stream: KafkaStream<'T> -> AsyncSeq<'T * 'R>
    val toTopic : topic: string -> stream: KafkaStream<'T> -> Async<unit>
```

## License

MIT
