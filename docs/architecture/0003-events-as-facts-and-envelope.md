# 3. Events as facts, carried in a standard envelope

## Status

Accepted

## Context

Pub/sub only delivers on its promise — producers not needing to know who's listening — if what's
published is a fact ("this happened") rather than an instruction ("do this"). See
[Coupled in the Wrong Direction](https://github.com/toddschilling/architects-toolkit/blob/main/articles/event-streaming/coupled-in-the-wrong-direction.md)
and the "Naming topics" section of
[Designing Topics and Messages](https://github.com/toddschilling/architects-toolkit/blob/main/articles/event-streaming/designing-topics-and-messages.md).
Separately, every consumer ends up needing the same handful of metadata fields (an idempotency
key, a partition/ordering key, tracing IDs), and it's better for the SDK to define that shape
once than for every team to invent it independently.

## Decision

Every event is published as an [`EventEnvelope<T>`](../../src/EventBus.Kafka/Events/EventEnvelope.cs):

| Field | Purpose |
|---|---|
| `EventId` | Idempotency key; see [ADR 4](0004-idempotent-consumers.md). |
| `EventType` | The fact, named in the past tense (`CourseCompleted`, not `CompleteCourse`). |
| `OccurredAt` | When the fact became true, per the producer. |
| `PartitionKey` | Required — the ordering key. Kafka orders within a partition, not across a topic, so `IEventProducer.PublishAsync` takes this as a mandatory argument rather than defaulting to round-robin. |
| `Source` | The service that owns this event's contract. |
| `SchemaVersion` | For compatible, in-place evolution of `Data`'s shape. |
| `CorrelationId` / `CausationId` | Optional tracing back to the request or event that led here. |
| `Data` / `ClaimCheck` | The payload, or a reference to it; see [ADR 5](0005-claim-check-for-large-payloads.md). |

[`EventNamingConvention.Validate`](../../src/EventBus.Kafka/Events/EventNamingConvention.cs) checks
`EventType` against a list of common imperative verbs (`Create`, `Send`, `Process`, ...) and warns
(or, with `EventProducerOptions.EnforceEventNamingConvention`, throws) when a name reads as a
command instead of a fact. It's a heuristic, not a grammar checker — it won't catch every
command-shaped name, and it isn't meant to.

`IEventSerializer` serializes the whole envelope, not just `Data`, so the metadata above travels
on the wire in whatever format is chosen. The default, [`JsonEventSerializer`](../../src/EventBus.Kafka/Serialization/JsonEventSerializer.cs),
uses plain `System.Text.Json` and needs no external infrastructure.

## Consequences

- Every consumer gets `EventId`, ordering key, and tracing IDs for free, instead of each team
  re-inventing (or forgetting to add) them.
- The naming-convention check is advisory by default on purpose: turning it on as a hard failure
  (`EnforceEventNamingConvention = true`) is a per-producer opt-in, not a library-wide default,
  since a team migrating existing event names shouldn't be blocked from publishing on day one.
- **Schema Registry is not wired in by default.** `IEventSerializer` is the extension point: a
  topic that needs registry-enforced compatibility checking can supply an implementation backed
  by `Confluent.SchemaRegistry.Serdes.Json.JsonSerializer<EventEnvelope<T>>` /
  `JsonDeserializer<EventEnvelope<T>>` (or Avro/Protobuf equivalents), registered per event type.
  This was deliberately left as a documented pattern rather than a shipped class, since it can
  only be verified against a running Schema Registry instance, which this repo doesn't assume
  every consumer of the library has on hand.
