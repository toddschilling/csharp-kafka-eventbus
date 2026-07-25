# 1. Wrap Confluent.Kafka

## Status

Accepted

## Context

We need a Kafka client for .NET. The realistic choices are Confluent.Kafka (the official,
[librdkafka](https://github.com/confluentinc/librdkafka)-based client maintained by Confluent)
or a community client such as kafka-sharp or a raw librdkafka P/Invoke layer.

## Decision

Wrap `Confluent.Kafka`. It's the most widely used and best-supported .NET Kafka client, ships
official Schema Registry integration, and its `IProducer<TKey,TValue>` / `IConsumer<TKey,TValue>`
abstractions are already close to what an event-streaming SDK needs — a thin, opinionated layer
on top is enough, and there's no reason to maintain a from-scratch protocol implementation.

This library publishes and consumes raw `byte[]` values (see [`IEventSerializer`](../../src/EventBus.Kafka/Serialization/IEventSerializer.cs))
rather than exposing Confluent's own key/value serializer pipeline directly, so the envelope,
claim-check, and idempotency behavior described in the other ADRs sits above Confluent.Kafka
instead of being wired through it.

## Consequences

- Anything Confluent.Kafka can't do, this library can't do either, without dropping to the
  underlying `IProducer`/`IConsumer` directly (both are still exposed via DI, not hidden).
- `librdkafka.redist` ships native binaries per platform/architecture; this is a Confluent.Kafka
  characteristic, not something this library adds on top.
- If Confluent ever stops maintaining this client, replacing it means rewriting `KafkaEventProducer`
  and `KafkaEventConsumer<T>` — but the public surface (`IEventProducer`, `IEventHandler<T>`,
  `TopicName`, `EventEnvelope<T>`) wouldn't need to change, since nothing above those two classes
  depends on Confluent.Kafka types.
