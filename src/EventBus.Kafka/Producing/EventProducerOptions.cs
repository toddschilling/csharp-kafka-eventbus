using EventBus.Kafka.ClaimCheck;

namespace EventBus.Kafka.Producing;

public sealed class EventProducerOptions
{
    /// <summary>Recorded as <see cref="Events.EventEnvelope{T}.Source"/> on every event this producer publishes.</summary>
    public required string ServiceName { get; init; }

    public ClaimCheckOptions ClaimCheck { get; init; } = new();

    /// <summary>Content type recorded against a claim-checked payload; should match what <see cref="Serialization.IEventSerializer"/> produces.</summary>
    public string ContentType { get; init; } = "application/json";

    /// <summary>
    /// When true, an <see cref="Events.EventNamingConvention"/> violation throws instead of
    /// just logging a warning. Off by default so adopting this library doesn't require
    /// renaming every existing event type on day one.
    /// </summary>
    public bool EnforceEventNamingConvention { get; init; }
}
