namespace EventBus.Kafka.Idempotency;

/// <summary>
/// Tracks which events a consumer has already handled, so Kafka's at-least-once delivery
/// guarantee doesn't turn into double-processing. See docs/architecture/0004-idempotent-consumers.md.
/// </summary>
public interface IProcessedEventStore
{
    Task<bool> IsProcessedAsync(Guid eventId, CancellationToken cancellationToken = default);

    Task MarkProcessedAsync(Guid eventId, CancellationToken cancellationToken = default);
}
