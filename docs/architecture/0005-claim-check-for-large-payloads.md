# 5. Claim check for large payloads

## Status

Accepted

## Context

Message brokers move large volumes of small facts quickly; they aren't built or priced to
shuttle megabytes of binary content, and a large message hits every subscriber whether it needs
the payload or not. See "Large payloads: store it, reference it" in
[Designing Topics and Messages](https://github.com/toddschilling/architects-toolkit/blob/main/articles/event-streaming/designing-topics-and-messages.md).

## Decision

[`KafkaEventProducer`](../../src/EventBus.Kafka/Producing/KafkaEventProducer.cs) serializes the
full envelope first; if the result is at or above `EventProducerOptions.ClaimCheck.ThresholdBytes`
(default 1,000,000 bytes — comfortably under Kafka brokers' common 1&nbsp;MB `message.max.bytes`
default), it stores those bytes via [`IClaimCheckStore`](../../src/EventBus.Kafka/ClaimCheck/IClaimCheckStore.cs)
and publishes a much smaller "pointer" envelope instead — same metadata, `Data` cleared, and a
`ClaimCheckReference` (`Location`, `SizeBytes`, `ContentType`) pointing at the stored payload.

On the consumer side, `KafkaEventConsumer<T>` checks `envelope.ClaimCheck` after deserializing;
if it's set, it retrieves the stored bytes from `IClaimCheckStore` and deserializes *those* as the
full envelope, so `IEventHandler<T>` always sees a fully populated `EventEnvelope<T>` regardless
of whether the payload traveled inline or via claim check.

The shipped [`FileSystemClaimCheckStore`](../../src/EventBus.Kafka/ClaimCheck/FileSystemClaimCheckStore.cs)
writes to a local directory — useful for the samples and for local development, but not suitable
for production, since a consumer is not guaranteed to run on the same machine as the producer
that wrote the file.

## Consequences

- Production use requires implementing `IClaimCheckStore` against durable, shared storage (blob
  storage, a document store) that every consumer instance can reach.
- Publishing without a configured `IClaimCheckStore` throws the moment a payload crosses the
  threshold, rather than silently attempting (and likely failing) to publish an oversized
  message — the failure is loud and immediate instead of a broker-side rejection deep in a retry
  loop.
- The threshold applies to the whole serialized envelope, not just `Data`, so envelope metadata
  overhead does count against it — this only matters near the threshold boundary and was chosen
  for implementation simplicity: one serialize call produces both "is this too big" and "what do
  we store" in one step.
