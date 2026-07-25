using System.Text.Json;
using EventBus.Kafka.Events;

namespace EventBus.Kafka.Serialization;

/// <summary>Default <see cref="IEventSerializer"/>: plain System.Text.Json, no external infrastructure required.</summary>
public sealed class JsonEventSerializer : IEventSerializer
{
    private readonly JsonSerializerOptions _options;

    public JsonEventSerializer(JsonSerializerOptions? options = null)
    {
        _options = options ?? new JsonSerializerOptions(JsonSerializerDefaults.Web);
    }

    public Task<byte[]> SerializeAsync<T>(
        EventEnvelope<T> envelope, EventSerializationContext context, CancellationToken cancellationToken = default) =>
        Task.FromResult(JsonSerializer.SerializeToUtf8Bytes(envelope, _options));

    public Task<EventEnvelope<T>> DeserializeAsync<T>(
        byte[] data, EventSerializationContext context, CancellationToken cancellationToken = default)
    {
        var envelope = JsonSerializer.Deserialize<EventEnvelope<T>>(data, _options);
        if (envelope is null)
        {
            throw new InvalidOperationException(
                $"Deserializing event type '{context.EventType}' on topic '{context.Topic}' produced null.");
        }

        return Task.FromResult(envelope);
    }
}
