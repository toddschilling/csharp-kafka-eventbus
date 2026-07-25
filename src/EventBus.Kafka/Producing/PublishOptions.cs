namespace EventBus.Kafka.Producing;

public sealed class PublishOptions
{
    /// <summary>Groups events that belong to the same end-to-end request or workflow.</summary>
    public string? CorrelationId { get; init; }

    /// <summary>The ID of the event or command that directly caused this one, if any.</summary>
    public string? CausationId { get; init; }

    /// <summary>Schema version of the payload's shape, for compatible in-place evolution.</summary>
    public int SchemaVersion { get; init; } = 1;
}
