using EventBus.Kafka.Events;

namespace EventBus.Kafka.Serialization;

/// <summary>
/// Converts an <see cref="EventEnvelope{T}"/> to and from the bytes published on the wire.
/// The default <see cref="JsonEventSerializer"/> needs no external infrastructure; swap in an
/// implementation backed by Confluent Schema Registry when a topic needs registry-enforced
/// compatibility checking (see docs/architecture/0003-events-as-facts-and-envelope.md).
/// </summary>
public interface IEventSerializer
{
    Task<byte[]> SerializeAsync<T>(
        EventEnvelope<T> envelope, EventSerializationContext context, CancellationToken cancellationToken = default);

    Task<EventEnvelope<T>> DeserializeAsync<T>(
        byte[] data, EventSerializationContext context, CancellationToken cancellationToken = default);
}
