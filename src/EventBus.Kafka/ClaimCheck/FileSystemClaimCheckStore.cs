namespace EventBus.Kafka.ClaimCheck;

/// <summary>
/// Stores claim-checked payloads as files under a root directory. Meant for local
/// development and the samples in this repo — a real deployment needs
/// <see cref="IClaimCheckStore"/> backed by durable, shared storage (e.g. blob storage),
/// since a consumer will often run on a different machine than the producer that wrote the file.
/// </summary>
public sealed class FileSystemClaimCheckStore : IClaimCheckStore
{
    private readonly string _rootDirectory;

    public FileSystemClaimCheckStore(string rootDirectory)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(rootDirectory);
        _rootDirectory = rootDirectory;
        Directory.CreateDirectory(_rootDirectory);
    }

    public async Task<ClaimCheckReference> StoreAsync(
        string topic, Guid eventId, ReadOnlyMemory<byte> payload, string contentType,
        CancellationToken cancellationToken = default)
    {
        var path = Path.Combine(_rootDirectory, $"{topic}_{eventId:N}.bin");
        await File.WriteAllBytesAsync(path, payload.ToArray(), cancellationToken).ConfigureAwait(false);
        return new ClaimCheckReference(path, payload.Length, contentType);
    }

    public Task<byte[]> RetrieveAsync(ClaimCheckReference reference, CancellationToken cancellationToken = default) =>
        File.ReadAllBytesAsync(reference.Location, cancellationToken);
}
