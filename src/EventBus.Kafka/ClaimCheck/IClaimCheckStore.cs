namespace EventBus.Kafka.ClaimCheck;

/// <summary>
/// Stores payloads that are too large to publish inline on the event stream. Implementations
/// should use durable, shared storage (e.g. blob storage) since the producer and consumer
/// processes are not guaranteed to run on the same machine.
/// </summary>
public interface IClaimCheckStore
{
    Task<ClaimCheckReference> StoreAsync(
        string topic, Guid eventId, ReadOnlyMemory<byte> payload, string contentType,
        CancellationToken cancellationToken = default);

    Task<byte[]> RetrieveAsync(ClaimCheckReference reference, CancellationToken cancellationToken = default);
}
