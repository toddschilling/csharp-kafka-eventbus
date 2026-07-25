using EventBus.Kafka.ClaimCheck;

namespace EventBus.Kafka.Events;

/// <summary>
/// The standard envelope every event is published in. Carries enough metadata for a
/// subscriber to dedupe, trace, and interpret the payload without a schema registry
/// being the only source of truth. See docs/architecture/0003-events-as-facts-and-envelope.md.
/// </summary>
public sealed record EventEnvelope<T>
{
    /// <summary>Unique identifier for this specific occurrence of the event; the idempotency key.</summary>
    public required Guid EventId { get; init; }

    /// <summary>
    /// The fact being announced, named in the past tense (e.g. "CourseCompleted", not
    /// "CompleteCourse"). Lives in the envelope, not the topic name, so one topic can
    /// carry every action that applies to a resource.
    /// </summary>
    public required string EventType { get; init; }

    /// <summary>When the fact became true, as observed by the producer.</summary>
    public required DateTimeOffset OccurredAt { get; init; }

    /// <summary>
    /// The key used for partition assignment. Kafka only guarantees ordering within a
    /// partition, so this should be the identifier of the entity that needs ordered
    /// delivery (e.g. a learner ID), not left to a random/round-robin assignment.
    /// </summary>
    public required string PartitionKey { get; init; }

    /// <summary>The service that owns this event's contract and produced this instance.</summary>
    public required string Source { get; init; }

    /// <summary>Schema version of <see cref="Data"/>'s shape, for compatible in-place evolution.</summary>
    public int SchemaVersion { get; init; } = 1;

    /// <summary>Groups events that belong to the same end-to-end request or workflow.</summary>
    public string? CorrelationId { get; init; }

    /// <summary>The ID of the event or command that directly caused this one, if any.</summary>
    public string? CausationId { get; init; }

    /// <summary>
    /// The event payload, when it is small enough to publish inline. Null when the
    /// payload was large enough to be offloaded via <see cref="ClaimCheck"/> instead
    /// (see docs/architecture/0005-claim-check-for-large-payloads.md).
    /// </summary>
    public T? Data { get; init; }

    /// <summary>Reference to an out-of-band store holding the payload, when <see cref="Data"/> is not inline.</summary>
    public ClaimCheckReference? ClaimCheck { get; init; }

    public static EventEnvelope<T> Create(
        string eventType,
        string partitionKey,
        string source,
        T data,
        string? correlationId = null,
        string? causationId = null,
        int schemaVersion = 1)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(eventType);
        ArgumentException.ThrowIfNullOrWhiteSpace(partitionKey);
        ArgumentException.ThrowIfNullOrWhiteSpace(source);

        return new EventEnvelope<T>
        {
            EventId = Guid.NewGuid(),
            EventType = eventType,
            OccurredAt = DateTimeOffset.UtcNow,
            PartitionKey = partitionKey,
            Source = source,
            SchemaVersion = schemaVersion,
            CorrelationId = correlationId,
            CausationId = causationId,
            Data = data,
        };
    }
}
