# 4. Idempotent consumers by default

## Status

Accepted

## Context

Kafka's delivery guarantee is at-least-once, not exactly-once: a rebalance, a crash between
processing and committing, or a retry can all cause the same message to be delivered again. A
consumer that isn't safe to run twice on the same event will eventually double-count something.
See the "Consumers should expect to receive a message more than once" note in
[Designing Topics and Messages](https://github.com/toddschilling/architects-toolkit/blob/main/articles/event-streaming/designing-topics-and-messages.md).

## Decision

[`KafkaEventConsumer<T>`](../../src/EventBus.Kafka/Consuming/KafkaEventConsumer.cs) checks an
[`IProcessedEventStore`](../../src/EventBus.Kafka/Idempotency/IProcessedEventStore.cs) keyed on
`EventEnvelope<T>.EventId` before invoking the handler, and marks the event processed only after
the handler completes — then, and only then, commits the Kafka offset. Concretely, for each
message:

1. Skip (and commit past) messages whose `eventType` header doesn't match this consumer's
   registered event type — a topic can carry several event types (ADR 2), and this consumer only
   cares about one.
2. If `EventId` is already marked processed, commit and move on without calling the handler again.
3. Otherwise: deserialize, resolve any claim-check reference (ADR 5), call
   `IEventHandler<T>.HandleAsync`, mark `EventId` processed, then commit.

The shipped [`InMemoryProcessedEventStore`](../../src/EventBus.Kafka/Idempotency/InMemoryProcessedEventStore.cs)
is explicitly **not** production-durable — it forgets everything on restart and isn't shared
across instances. It exists for tests and single-instance samples; a real deployment needs
`IProcessedEventStore` backed by something durable and shared (a database table, a Redis set)
that every replica of the consumer can see.

## Consequences

- `IEventHandler<T>.HandleAsync` should still be written defensively where practical (e.g.
  upserts instead of inserts) — the built-in dedupe narrows the redelivery window a lot, but a
  store that isn't durable/shared, or a crash between "mark processed" and "commit", can still
  let a duplicate through in edge cases. Idempotent-by-design handler logic and the
  `IProcessedEventStore` check are complementary, not either/or.
- Manual offset commits (`EnableAutoCommit = false`) are required for this ordering to hold;
  the samples set this explicitly and `KafkaEventConsumer<T>` assumes it.
- Choosing a durable `IProcessedEventStore` (and its retention — how long to remember an
  `EventId`) is left to the application, since it depends on how long redelivery can realistically
  lag behind first delivery in a given deployment.
