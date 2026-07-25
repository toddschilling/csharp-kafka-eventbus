# 7. Long-running work stays out of this SDK

## Status

Accepted

## Context

`IEventHandler<T>.HandleAsync` is expected to be fast — ADR 6's retry-with-backoff loop exists to
absorb transient failures on the order of seconds, not to babysit a handler that's genuinely slow.
In practice, some legitimate reactions to an event aren't fast: a call to a rate-limited external
service, a batch job, anything that runs for minutes rather than milliseconds. A handler that does
that work inline blocks `KafkaEventConsumer<T>`'s single consume loop for its duration, which
starves every other partition assigned to that consumer instance and risks a rebalance if the
broker decides the consumer has gone quiet. Retrying that kind of failure through ADR 6's backoff
loop doesn't help either — it just blocks the loop again, for a different reason, on a schedule
tuned for transient blips rather than genuinely slow work.

This is explored at length in
[Fast Events, Slow Work: Bridging Pub/Sub to Long-Running Tasks](https://github.com/toddschilling/architects-toolkit/blob/main/articles/event-streaming/bridging-events-to-long-running-work.md),
which also covers the two approaches considered and rejected below.

## Decision

This SDK does not add task-queue or job-dispatch functionality, and `IEventHandler<T>` stays
scoped to fast reactions. Two alternatives were considered and rejected:

- **Building queue/task-dispatch support directly into this SDK.** Rejected — it's a different
  transport with different semantics (a lease/visibility-timeout model, not an offset-commit
  model), and per [ADR 1](0001-wrap-confluent-kafka.md) this SDK's scope is wrapping
  Confluent.Kafka. Every consumer of this SDK would inherit a second broker dependency whether it
  needed long-running task offload or not.
- **A shared wrapper library that imports both this SDK and a task-queue SDK, doing the
  translation on everyone's behalf.** Rejected — it doesn't remove the coupling, it relocates it
  into a library every affected consumer now depends on, whose release cadence they inherit for
  both transports at once.

Instead, a consumer whose reaction to an event is long-running should be split into two pieces:

1. A **bridge**: a standalone consumer, built with this SDK exactly as any other
   `IEventHandler<T>` is, whose only job is to translate the envelope into a task and enqueue it
   on a separate task-queue SDK (not part of this repository). It deploys and scales
   independently of whatever executes the task.
2. The actual long-running work, implemented as a plain task-queue worker that has no dependency
   on this SDK, Kafka, or the event's shape at all.

The bridge's own correctness leans entirely on guarantees this SDK already provides:
`KafkaEventConsumer<T>` only commits an offset after `IEventHandler<T>.HandleAsync` completes
(ADR 4), so a bridge's handler must not return successfully until the task is durably enqueued —
otherwise an offset can commit for an event whose task was never created.

The mapping of which events become which tasks, for which components, should live somewhere
centrally readable — for example, a platform monorepo holding every bridge's source — even though
the bridges themselves deploy independently and don't share a runtime dependency on each other.

## Consequences

- No new surface area in this SDK. `IEventHandler<T>`, `KafkaEventConsumer<T>`, and the ADR 4/6
  guarantees are sufficient to build a bridge on top of, unchanged.
- Teams needing to hand off to long-running work must build or adopt a separate task-queue SDK.
  Until one exists as a shared org-wide dependency, different bridges may sit on different queue
  technologies — an acceptable near-term inconsistency against scope-creeping this SDK with
  transport code most consumers don't need.
- ADR 6's retry budget is not a substitute for a bridge and won't reliably tell a team when they
  should have delegated instead of retried — a handler that's slow for legitimate reasons will
  still exhaust `MaxAttempts`, hit the dead-letter topic, and look identical to a genuinely broken
  handler unless someone notices the pattern.
- The enqueue-then-commit ordering described above is the one place a message can be durably
  consumed but never turned into a task (if the bridge dies after enqueueing but before commit, a
  redelivery enqueues the task again — survivable if the task queue or downstream work is
  idempotent, per the same principle as ADR 4). If a companion task-queue SDK is built, that
  boundary deserves its own ADR once real transport semantics are on the table, not a promise made
  here in the abstract.
