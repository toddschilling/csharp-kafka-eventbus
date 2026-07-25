using System.Collections.Concurrent;

namespace EventBus.Kafka.Idempotency;

/// <summary>
/// An in-process, non-durable <see cref="IProcessedEventStore"/>. Useful for tests and
/// single-instance samples only — it forgets everything on restart and isn't shared across
/// consumer instances, so a real deployment needs a store backed by a database, Redis, or
/// similar durable, shared storage instead.
/// </summary>
public sealed class InMemoryProcessedEventStore : IProcessedEventStore
{
    private readonly ConcurrentDictionary<Guid, byte> _processedEventIds = new();

    public Task<bool> IsProcessedAsync(Guid eventId, CancellationToken cancellationToken = default) =>
        Task.FromResult(_processedEventIds.ContainsKey(eventId));

    public Task MarkProcessedAsync(Guid eventId, CancellationToken cancellationToken = default)
    {
        _processedEventIds[eventId] = 0;
        return Task.CompletedTask;
    }
}
