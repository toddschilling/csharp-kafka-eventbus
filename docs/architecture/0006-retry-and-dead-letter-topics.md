# 6. Retry with backoff and dead-letter topics for consumers

## Status

Accepted

## Context

`KafkaEventConsumer<T>.ProcessAsync` currently calls `IEventSerializer.DeserializeAsync` and
`IEventHandler<T>.HandleAsync` with nothing catching a throw. An exception from either — a
transient downstream dependency blip, or a genuinely unprocessable "poison" message — propagates
out of `ExecuteAsync`'s consume loop and out of the `BackgroundService`. Depending on the host's
`BackgroundServiceExceptionBehavior`, that either takes the whole process down or leaves this
consumer silently and permanently stopped, with no further messages consumed and no operator
signal that it happened.

At-least-once delivery (ADR 4) already means every handler must tolerate redelivery; it should
also mean the SDK gives a failing message somewhere to go besides "block this partition forever"
or "crash the host." A transient failure deserves a few retries; a message that still fails after
that deserves to be quarantined, not to wedge the consumer indefinitely.

## Decision

`EventConsumerOptions` gains a `Retry` property (`ConsumerRetryOptions`): `MaxAttempts` (default
3), `InitialDelay` (default 1s), `MaxDelay` (default 30s), and `BackoffMultiplier` (default 2.0).
`KafkaEventConsumer<T>.ProcessAsync` wraps the deserialize-then-handle step in a retry loop using
these settings, delaying between attempts with capped exponential backoff. This loop only covers
exceptions from deserialization and handler logic — exceptions from `Consume()` or `Commit()`
(transport-level Kafka client failures) are unchanged and still propagate, same as today.

Once `MaxAttempts` is exhausted, the consumer republishes the message **as received** — the exact
`Message<string, byte[]>` bytes and headers `KafkaEventConsumer<T>` originally read, before any
claim-check resolution — to a dead-letter topic, via a new narrow interface:

```csharp
public interface IDeadLetterPublisher
{
    Task PublishAsync(
        TopicName deadLetterTopic,
        Message<string, byte[]> originalMessage,
        DeadLetterContext context,
        CancellationToken cancellationToken);
}
```

`DeadLetterContext` carries `OriginalTopic`, `Partition`, `Offset`, `Attempts`, `ExceptionType`,
`ExceptionMessage`, and `FirstFailedAt`/`LastFailedAt`; the Kafka implementation adds these as new
headers (`originalTopic`, `attempts`, `exceptionType`, `exceptionMessage`, `firstFailedAt`,
`lastFailedAt`) alongside the message's existing `eventType`/`eventId`/`correlationId` headers, and
publishes the untouched original bytes as the value. Bypassing `IEventSerializer` here is
deliberate: the failure might *be* a deserialization failure, so the one thing we know we can
still do is move the bytes we already have without asking anything of them.

**Dead-letter topic name.** Rather than a literal `.dlq` suffix (a sixth segment, which would
break the fixed five-segment shape ADR 2 depends on for tooling), the dead-letter topic is the
same `TopicName` with `-dlq` appended to the `resource` segment — `TopicName.DeadLetterTopic()`
returns `public.learning.enrollment.courses-dlq.v1` for `public.learning.enrollment.courses.v1`.
`-dlq` is a valid kebab-case resource suffix under the existing segment regex, so no naming-rule
changes are needed, and tooling that filters on segment position keeps working unmodified.

After a successful dead-letter publish, `KafkaEventConsumer<T>` commits the original offset — the
poison message doesn't get redelivered and retried forever, and the partition moves on.

`EventConsumerOptions.DeadLetter.Enabled` defaults to `true`. Set to `false` for a consumer where
an operator wants a loud crash instead of silent quarantine; in that case, exhausting retries
rethrows and today's crash-and-stop behavior is preserved unchanged.

## Consequences

- Retries run in-process on `KafkaEventConsumer<T>`'s single consume loop, so all partitions
  assigned to this consumer instance stall for the retry window while one message is retried —
  consistent with the SDK's existing single-consumer-per-registration model, not solved by this
  ADR. Parallelizing per-partition processing is future work if this becomes a real bottleneck.
- Backoff here is a fixed exponential curve with no jitter. If a shared downstream dependency
  fails and recovers, many consumer instances retrying on the same cadence could hit it at the
  same moment. Deliberately left simple for now; add jitter to `ConsumerRetryOptions` if a
  thundering-herd pattern shows up in practice.
- A dead-lettered message that was claim-checked still points at the same `IClaimCheckStore`
  location. If the claim-check store's retention is shorter than the dead-letter topic's, a
  message reprocessed off the DLQ later can fail to resolve its payload — claim-check and
  dead-letter retention need to be chosen together, not independently.
- Dead-letter topics need the same provisioning (partitions, replication, retention) as their
  source topic. `TopicProvisioner.EnsureTopicExistsAsync` isn't wired into `AddKafkaEventConsumer`
  today for the primary topic either, so this doesn't newly break anything, but whatever
  provisions the primary topic needs to also provision `TopicName.DeadLetterTopic()`.
- `IEventHandler<T>` implementations that are *not* idempotent-safe under a partial-failure retry
  (e.g. a handler with a side effect before the exception point) will re-run that side effect on
  every retry attempt, not just on redelivery after a crash. ADR 4's guidance — write handlers
  idempotently — now also covers in-process retries, not only Kafka-level redelivery.

## Action Items

1. [ ] Add `ConsumerRetryOptions` and wire `EventConsumerOptions.Retry` / `EventConsumerOptions.DeadLetter`.
2. [ ] Add `TopicName.DeadLetterTopic()`.
3. [ ] Add `IDeadLetterPublisher` and a Kafka-backed implementation; register it in `AddKafkaEventConsumer<T>`.
4. [ ] Replace the unguarded deserialize/handle call in `KafkaEventConsumer<T>.ProcessAsync` with the retry-then-dead-letter flow.
5. [ ] Tests: retry exhaustion publishes to the dead-letter topic with original bytes/headers intact plus failure headers; offset commits only after a successful dead-letter publish; a handler that succeeds within the retry budget never reaches the dead-letter topic; `DeadLetter.Enabled = false` rethrows instead of publishing.
6. [ ] Update `docs/architecture/0002-topic-naming-convention.md`'s consequences to mention the `-dlq` resource-suffix convention, since it's now a second thing beyond `TopicProvisioner` that depends on the fixed segment shape.
