# 2. Topic naming convention

## Status

Accepted

## Context

Left unconstrained, topic names drift: some teams name by action, some by resource, some by
team, retention and access-control policy end up full of special cases, and nobody can guess a
topic's name without asking. This is explored at length in
[Designing Topics and Messages That Scale With the Org](https://github.com/toddschilling/architects-toolkit/blob/main/articles/event-streaming/designing-topics-and-messages.md).

## Decision

Every topic name is exactly five dot-separated segments, enforced by
[`TopicName`](../../src/EventBus.Kafka/Topics/TopicName.cs):

```
{visibility}.{group}.{service}.{resource}.{version}
```

for example `public.learning.enrollment.courses.v1`.

- **`visibility`** — `public` (a cross-team contract) or `private` (internal plumbing for one
  team; can move fast, graduates to `public` when a second team needs it).
- **`group`** / **`service`** — the owning service group and specific service, kebab-case.
- **`resource`** — the resource the topic represents, kebab-case. **The action lives in the
  message's `eventType`, not here** — one topic per resource, not per resource-action pair, so
  topic count stays proportional to resources instead of resources × actions.
- **`version`** — `v` followed by a positive integer, bumped only for an incompatible schema
  change. A schema registry (or just additive JSON fields) should handle the overwhelming
  majority of a topic's evolution without ever touching this segment.

`TopicName.Parse`/`TryParse` reject anything that doesn't fit this shape, with an error message
naming exactly which segment is wrong.

## Consequences

- A fixed segment count means tooling (access control, dashboards, discovery) can depend on
  segment position instead of a pile of special cases — `private.*` is a valid, reliable filter.
- A topic can't be named without knowing its visibility and version up front; this is
  intentional friction, meant to force the "is this a real cross-team contract yet" question at
  creation time rather than after a second team has already started depending on it.
- [`TopicProvisioner`](../../src/EventBus.Kafka/Administration/TopicProvisioner.cs) uses
  `visibility` to pick a sane retention default (longer for `public`, shorter for `private`),
  since a public topic's subscribers are more likely to include someone who needs to catch up on
  history.
- The fixed segment shape is also why a dead-letter topic (ADR 6) isn't a literal `.dlq` sixth
  segment: [`TopicName.DeadLetterTopic()`](../../src/EventBus.Kafka/Topics/TopicName.cs) instead
  suffixes the `resource` segment (`courses.v1` becomes `courses-dlq.v1`), so segment-position
  tooling keeps working unmodified.
