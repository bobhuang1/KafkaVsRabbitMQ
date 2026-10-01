# KafkaVsRabbitMQ

**The same order pipeline, implemented twice** — once on **Apache Kafka** and once
on **RabbitMQ**, in C# / .NET 10 — so the differences can be read rather than
believed.

The events, the business rule and the demo workload are shared and identical
([`OrderSimulation`](src/KafkaVsRabbitMQ.Contracts/OrderSimulation.cs)). Only the
transport differs. Any difference you observe in behaviour is therefore a difference
in the broker or in the client code, never in the business logic.

> Part three of a series. [KafkaDotNet](https://github.com/bobhuang1/KafkaDotNet) and
> [RedisDotNet](https://github.com/bobhuang1/RedisDotNet) go deeper on each broker
> individually; this repo is the head-to-head.

---

## TL;DR

| | Kafka | RabbitMQ |
| --- | --- | --- |
| **Mental model** | A distributed, partitioned **log** | A **message router** built on exchanges and queues |
| **You publish to** | A topic | An **exchange** + routing key |
| **The broker decides** | which partition (hash of key) | which queues (bindings) |
| **Consumers pull** | yes — `Consume`, you control the pace | no — the broker **pushes**, capped by `prefetch` |
| **Acknowledgement** | commit an **offset** (a cursor) | `basic.ack` per **message** |
| **Unacked work lives** | in the group's committed offset | in the queue's *unacked* count |
| **Dead consumer** | automatic rebalance, replay | message is re-delivered when the channel dies |
| **Redelivery of old messages** | reset the offset — the log is still there | not possible; acked messages are gone |
| **Delayed retry** | build it: retry topic + `not-before` header, or pause a partition | build it declaratively: TTL queue + dead-letter exchange |
| **Dead-lettering** | a topic you publish to yourself | first-class: the queue's `x-dead-letter-exchange` |
| **Per-attempt backoff** | one header and a delay you compute | one retry queue per delay |
| **Ordering** | per partition, guaranteed and cheap | per queue, best effort with concurrency > 1 |
| **Fan-out to N systems** | N consumer groups, each with its own offsets | N queues bound to the exchange (a copy each) |
| **Replay after a bug fix** | yes — reset offsets, re-process | no — the messages are gone |
| **Retention** | a policy on the log (time/size) | until acked (or a queue-level TTL) |
| **Scaling consumers** | up to the partition count | unbounded, bounded only by prefetch |
| **Routing sophistication** | none (topics and keys) | topic/fanout/headers exchanges, wildcards, priorities |
| **Best at** | event streams, replay, high throughput, ordered per key | task queues, work distribution, flexible routing, per-message ack |

The shortest honest summary: **Kafka is a log you read; RabbitMQ is a queue you are
pushed from.** Most "which one?" arguments are really arguments about whether you
need to read the same message twice.

---

## See it run

```bash
docker compose up -d
```

Both brokers come up: Kafka on `localhost:9092`, RabbitMQ on `localhost:5672` with a
management UI at <http://localhost:15672> (guest / guest).

**Terminal 1 — a Kafka consumer**

```bash
dotnet run --project src/KafkaVsRabbitMQ.App -- kafka consume
```

**Terminal 2 — a RabbitMQ consumer**

```bash
dotnet run --project src/KafkaVsRabbitMQ.App -- rabbit consume
```

**Terminal 3 — publish the identical workload to each**

```bash
dotnet run --project src/KafkaVsRabbitMQ.App -- kafka produce --count 20
dotnet run --project src/KafkaVsRabbitMQ.App -- rabbit produce --count 20
```

Watch both logs. Order `0` is out of stock on both (→ rejected), order `1` is over the
threshold (→ one retry, then confirmed), the rest are confirmed. The outcomes match;
the machinery under them does not.

```bash
dotnet run --project src/KafkaVsRabbitMQ.App -- benchmark --count 5000
```

---

## Difference 1 — producing

**Kafka.** Name a topic and append. Nothing is declared; the partitioner decides
placement; durability is `Acks.All` plus topic replication.

```csharp
// src/KafkaVsRabbitMQ.Kafka/KafkaOrderProducer.cs
var record = new Message<string, byte[]>
{
    Key = KafkaTopology.PartitionKeyFor(order.CustomerId),   // decides the partition
    Value = JsonDefaults.Serialize(order),
    Headers = KafkaHeaders.Build(attempt, notBefore),
};
await _producer.ProduceAsync(KafkaTopology.Placed, record, cancellationToken);
```

**RabbitMQ.** Publish to an **exchange** with a **routing key**; the exchange copies
the message into whichever queues are bound with that key. The client also declares
the whole topology first.

```csharp
// src/KafkaVsRabbitMQ.RabbitMq/RabbitOrderPublisher.cs
var properties = new BasicProperties
{
    DeliveryMode = DeliveryModes.Persistent,                        // survive a restart
    MessageId = order.OrderId.ToString(),
    Headers = new Dictionary<string, object?> { [RabbitHeaders.Attempt] = 0 },
};

await _channel.BasicPublishAsync(
    RabbitTopology.Exchange, RabbitTopology.RoutingKeys.Placed,
    mandatory: false, basicProperties: properties,
    body: JsonDefaults.Serialize(order), cancellationToken: cancellationToken);
```

The consequence is the fan-out story. In RabbitMQ, to add a third consumer system you
**declare another queue and bind it**; the broker starts copying messages into it. In
Kafka you tell the new system to **join its own consumer group**; the broker does not
know or care that it exists. RabbitMQ routing is more expressive; Kafka's decoupling
is stronger.

---

## Difference 2 — consuming and acknowledging

**Kafka: the commit is the acknowledgement, and it is a cursor.**

```csharp
// src/KafkaVsRabbitMQ.Kafka/KafkaOrderConsumer.cs
var record = consumer.Consume(TimeSpan.FromMilliseconds(500));
await HandleAsync(record, candidateToken);
consumer.Commit(record);          // advances the cursor for the whole partition
```

One commit covers every record up to that offset. A crash between doing the work and
committing replays that record — at-least-once. Because the log is *retained*, you can
also deliberately rewind and process yesterday's data again.

**RabbitMQ: the acknowledgement is per message, and the broker pushes.**

```csharp
// src/KafkaVsRabbitMQ.RabbitMq/RabbitOrderConsumer.cs
var consumer = new AsyncEventingBasicConsumer(channel);
consumer.ReceivedAsync += (_, args) => HandleAsync(channel, args);
await channel.BasicConsumeAsync(RabbitTopology.PlacedQueue, autoAck: false, consumer: consumer);
// ... and inside the handler:
await channel.BasicAckAsync(args.DeliveryTag, multiple: false);
```

There is no cursor and no replay. Once acked, the message is deleted. An unacked
message is re-delivered when its channel closes — which is why `prefetch` matters: it
bounds how much a consumer can hold at once, and how much it re-does after a crash.

**This is the crux of the whole comparison.** Kafka keeps the history and lets you
choose your position in it; RabbitMQ keeps only what is not yet done.

---

## Difference 3 — delayed retries (the interesting one)

Both brokers can retry a transient failure. The amount of code is genuinely different.

**Kafka has no delay, so the consumer implements it.** A transient failure is written
to a retry topic with a `not-before` header; the consumer then honours that timestamp
itself by seeking back onto the record and pausing the partition:

```csharp
// src/KafkaVsRabbitMQ.Kafka/KafkaOrderConsumer.cs
var notBefore = record.Message.Headers.ReadNotBefore();
if (notBefore is not null && DateTimeOffset.UtcNow < notBefore)
{
    consumer.Seek(record.TopicPartitionOffset);   // stay on this record
    consumer.Pause([record.TopicPartition]);      // stop consuming this partition
    parkedUntil[record.TopicPartition] = notBefore.Value;
    continue;
}
```

That is real scheduling logic in your application, and it is per partition: everything
behind the parked record waits too.

**RabbitMQ does it declaratively, and no consumer is involved while it waits:**

```csharp
// src/KafkaVsRabbitMQ.RabbitMq/RabbitTopology.cs
public static IReadOnlyDictionary<string, object?> RetryQueueArguments(int retryDelayMs) => new Dictionary<string, object?>
{
    ["x-message-ttl"] = retryDelayMs,                     // hold for this long…
    ["x-dead-letter-exchange"] = Exchange,                // …then dead-letter…
    ["x-dead-letter-routing-key"] = RoutingKeys.Placed,   // …back to the pipeline
};
```

and the consumer's whole retry path is one call:

```csharp
await channel.BasicNackAsync(args.DeliveryTag, multiple: false, requeue: false);
```

The nack routes the message to the retry queue (via the main queue's
`x-dead-letter-routing-key`), it sits there for the TTL, and RabbitMQ dead-letters it
back. The message is *in a queue* rather than held in a consumer.

The catch: a TTL is a property of the **queue**, so it is the same for every message in
it — that is where the well-known **staircase pattern** comes from. For exponential
backoff you declare `orders.placed.retry.5s`, `…retry.30s`, `…retry.5m`, each with its
own TTL, and route between them based on the `x-death` count. Kafka's per-message
header gives you arbitrary per-message delays with one topic.

---

## Difference 4 — dead-lettering and knowing how many times a message failed

**RabbitMQ counts failures for you.** Every dead-lettering appends to the message's
`x-death` header, so the consumer can just read the tally:

```csharp
// src/KafkaVsRabbitMQ.RabbitMq/RabbitHeaders.cs
public static int ReadAttempt(IDictionary<string, object?>? headers)
{
    if (headers.TryGetValue(DeadLetterHistory, out var history))
    {
        var deadLetters = CountDeadLetters(history);   // the broker kept count
        if (deadLetters > 0) return deadLetters;
    }
    return headers.TryGetValue(Attempt, out var raw) && TryToInt(raw, out var attempt) ? attempt : 0;
}
```

Dead-lettering itself is a queue setting (`x-dead-letter-exchange`), not application
code.

**Kafka has no such history.** The record is immutable and the broker tracks nothing
about it, so the attempt count is a header *you* write before publishing the retry:

```csharp
// src/KafkaVsRabbitMQ.Kafka/KafkaHeaders.cs
public static Headers Build(int attempt, DateTimeOffset? notBefore = null) { /* ... */ }
```

And the dead-letter topic is exactly that — another topic you publish to yourself.

---

## Difference 5 — the topology is code, or it is not

Kafka: create the topics, and that is the entire server-side configuration.

```csharp
// src/KafkaVsRabbitMQ.Kafka/KafkaTopicBootstrapper.cs
await admin.CreateTopicsAsync([new TopicSpecification { Name = "orders.placed", NumPartitions = 3, ... }]);
```

RabbitMQ: exchanges, queues, bindings, and queue arguments — all declared by the
client, and searchable in the UI at <http://localhost:15672>:

```csharp
// src/KafkaVsRabbitMQ.RabbitMq/RabbitTopology.cs
new(PlacedQueue,  Exchange,           RoutingKeys.Placed,        MainQueueArguments),
new(RetryQueue,   Exchange,           RoutingKeys.Retried,       RetryQueueArguments(delay)),
new(ConfirmedQueue, Exchange,         RoutingKeys.Confirmed,     null),
new(RejectedQueue, Exchange,          RoutingKeys.Rejected,      null),
new(DeadLetterQueue, DeadLetterExchange, RoutingKeys.DeadLettered, null),
```

You can read the retry policy of a RabbitMQ system off the broker. In Kafka the
equivalent policy lives in your consumers' code.

---

## Choosing

**Kafka when…**

- you need to **replay** — reprocess after a bug fix, rebuild a projection, onboard a new consumer over existing history;
- ordering **per key** at high throughput matters (everything for one customer, one device, one account in order);
- it is an event **stream** many systems subscribe to, with the broker agnostic about who is listening;
- you want one durable log as the system of record between services.

**RabbitMQ when…**

- it is **work**: jobs to be distributed, retried, and dropped once done;
- you need **routing**: fan-out, wildcards, header-based routing, priorities, per-queue TTLs;
- per-message acknowledgement, prefetch, and built-in dead-lettering fit the problem;
- message volumes are modest and the messages are transient by nature.

**Both** give at-least-once delivery with manual acknowledgement, and in both the
consumer therefore has to be idempotent. Neither gives exactly-once end to end.

---

## Projects

| Project | Role |
| --- | --- |
| `src/KafkaVsRabbitMQ.Contracts` | Events, JSON, the shared `OrderSimulation` rule, demo workload. |
| `src/KafkaVsRabbitMQ.Kafka` | Topics, headers, producer, consumer, topic bootstrapper. |
| `src/KafkaVsRabbitMQ.RabbitMq` | Topology, header/`x-death` parsing, publisher, consumer. |
| `src/KafkaVsRabbitMQ.App` | Console: `kafka|rabbit produce|consume`, `benchmark`. |
| `tests/KafkaVsRabbitMQ.Tests` | 32 tests over the payload, the business rule, the Kafka headers, and the RabbitMQ topology and `x-death` parsing. |

## Tests

```bash
dotnet test
```

```
Passed! - Failed: 0, Passed: 32, Skipped: 0, Total: 32
```

## Inspecting the brokers

```bash
# Kafka: read a topic from the beginning
docker exec kvr-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic orders.confirmed --from-beginning

# Kafka: consumer group lag
docker exec kvr-kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group orders-compare-kafka

# RabbitMQ: queue depths, including the retry queue, in the UI
open http://localhost:15672
```

---

## License

This project is free software, released under the **GNU General Public License
v3.0**. You may redistribute and/or modify it under those terms; see
[LICENSE.md](LICENSE.md) for the full text.
