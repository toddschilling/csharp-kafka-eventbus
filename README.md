# csharp-kafka-eventbus

An opinionated C# event-streaming client SDK: a thin, well-tested layer over
[Confluent.Kafka](https://github.com/confluentinc/confluent-kafka-dotnet) that bakes in the
event-streaming best practices from
[architects-toolkit](https://github.com/toddschilling/architects-toolkit), rather than leaving
every team to rediscover them independently.

Named `csharp-` deliberately — this is the C# SDK in what may eventually be a family of
language-specific event-bus clients sharing the same conventions.

## Why this exists

[architects-toolkit](https://github.com/toddschilling/architects-toolkit/tree/main/articles/event-streaming)
lays out a set of hard-won pub/sub conventions: producers should announce facts without knowing
who's listening, topics should be named and scoped by resource rather than by action, large
payloads shouldn't ride inline on the broker, and consumers have to assume at-least-once
delivery. Every team that adopts Kafka directly has to relearn (or skip) all of that on its own.
This library encodes those decisions as defaults and types, so getting it wrong takes more effort
than getting it right:

| Best practice | Where it lives |
|---|---|
| One topic per resource, `{visibility}.{group}.{service}.{resource}.{version}` naming | [`TopicName`](src/EventBus.Kafka/Topics/TopicName.cs) · [ADR 2](docs/architecture/0002-topic-naming-convention.md) |
| Events are facts (past tense), carried in a standard envelope with a required ordering key | [`EventEnvelope<T>`](src/EventBus.Kafka/Events/EventEnvelope.cs) · [ADR 3](docs/architecture/0003-events-as-facts-and-envelope.md) |
| Consumers must tolerate at-least-once delivery | [`IProcessedEventStore`](src/EventBus.Kafka/Idempotency/IProcessedEventStore.cs) · [ADR 4](docs/architecture/0004-idempotent-consumers.md) |
| Large payloads are stored and referenced, not published inline | [`IClaimCheckStore`](src/EventBus.Kafka/ClaimCheck/IClaimCheckStore.cs) · [ADR 5](docs/architecture/0005-claim-check-for-large-payloads.md) |
| Retention matched to a topic's visibility | [`TopicProvisioner`](src/EventBus.Kafka/Administration/TopicProvisioner.cs) · [ADR 2](docs/architecture/0002-topic-naming-convention.md) |

## Quick example

```csharp
services.AddKafkaEventProducer(
    new ProducerConfig { BootstrapServers = "localhost:9092" },
    new EventProducerOptions { ServiceName = "enrollment" });

var topic = TopicName.Parse("public.learning.enrollment.courses.v1");

await producer.PublishAsync(
    topic,
    eventType: "CourseCompleted",
    partitionKey: learnerId,
    data: new CourseCompletedData(learnerId, courseId, DateTimeOffset.UtcNow));
```

See [docs/getting-started.md](docs/getting-started.md) for the full walkthrough, and
`samples/Sample.Producer` / `samples/Sample.Consumer` for runnable code.

## Repository layout

```
src/EventBus.Kafka/       the library
test/EventBus.Kafka.Tests/  unit tests (no broker required)
samples/                  runnable producer/consumer console apps
docs/architecture/        ADRs explaining each design decision and its trade-offs
docs/getting-started.md   setup, build/test, and a five-minute API tour
```

## Status

Core producer/consumer, topic naming, event envelope, claim-check, and idempotency support are
implemented and unit-tested. Not yet included, and left as documented extension points rather
than shipped, untested code:

- **Schema Registry-backed serialization** — `IEventSerializer` is the seam; see
  [ADR 3](docs/architecture/0003-events-as-facts-and-envelope.md) for the intended integration
  with `Confluent.SchemaRegistry.Serdes`.
- **Durable `IProcessedEventStore` / `IClaimCheckStore` implementations** — the shipped ones
  (in-memory, filesystem) are for tests and local development only; see
  [ADR 4](docs/architecture/0004-idempotent-consumers.md) and
  [ADR 5](docs/architecture/0005-claim-check-for-large-payloads.md).
- **OpenTelemetry tracing/metrics** around publish and consume.
- **Transactional outbox** support for publishing atomically with a database write.

## License

[MIT](LICENSE)
