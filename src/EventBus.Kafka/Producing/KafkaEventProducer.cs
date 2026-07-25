using System.Text;
using Confluent.Kafka;
using EventBus.Kafka.ClaimCheck;
using EventBus.Kafka.Events;
using EventBus.Kafka.Serialization;
using EventBus.Kafka.Topics;
using Microsoft.Extensions.Logging;

namespace EventBus.Kafka.Producing;

/// <summary>Publishes events to Kafka via Confluent.Kafka, applying claim-check offload and naming-convention checks.</summary>
public sealed class KafkaEventProducer : IEventProducer
{
    private readonly IProducer<string, byte[]> _producer;
    private readonly IEventSerializer _serializer;
    private readonly IClaimCheckStore? _claimCheckStore;
    private readonly EventProducerOptions _options;
    private readonly ILogger<KafkaEventProducer> _logger;

    public KafkaEventProducer(
        IProducer<string, byte[]> producer,
        IEventSerializer serializer,
        EventProducerOptions options,
        ILogger<KafkaEventProducer> logger,
        IClaimCheckStore? claimCheckStore = null)
    {
        _producer = producer;
        _serializer = serializer;
        _options = options;
        _logger = logger;
        _claimCheckStore = claimCheckStore;
    }

    public async Task PublishAsync<T>(
        TopicName topic,
        string eventType,
        string partitionKey,
        T data,
        PublishOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(partitionKey);
        ValidateEventType(eventType);

        options ??= new PublishOptions();

        var envelope = EventEnvelope<T>.Create(
            eventType,
            partitionKey,
            _options.ServiceName,
            data,
            options.CorrelationId,
            options.CausationId,
            options.SchemaVersion);

        var context = new EventSerializationContext(topic.ToString(), eventType);
        var fullBytes = await _serializer.SerializeAsync(envelope, context, cancellationToken).ConfigureAwait(false);

        var wireBytes = fullBytes;
        if (fullBytes.Length >= _options.ClaimCheck.ThresholdBytes)
        {
            if (_claimCheckStore is null)
            {
                throw new InvalidOperationException(
                    $"Event '{eventType}' on topic '{topic}' is {fullBytes.Length} bytes, at or above the " +
                    $"{_options.ClaimCheck.ThresholdBytes}-byte claim-check threshold, but no IClaimCheckStore " +
                    "was configured for this producer.");
            }

            var reference = await _claimCheckStore
                .StoreAsync(topic.ToString(), envelope.EventId, fullBytes, _options.ContentType, cancellationToken)
                .ConfigureAwait(false);

            var pointerEnvelope = envelope with { Data = default, ClaimCheck = reference };
            wireBytes = await _serializer.SerializeAsync(pointerEnvelope, context, cancellationToken).ConfigureAwait(false);

            _logger.LogInformation(
                "Claim-checked event {EventType} ({EventId}) on topic {Topic}: {SizeBytes} bytes stored at {Location}.",
                eventType, envelope.EventId, topic, reference.SizeBytes, reference.Location);
        }

        var message = new Message<string, byte[]>
        {
            Key = partitionKey,
            Value = wireBytes,
            Headers = BuildHeaders(envelope, eventType),
        };

        await _producer.ProduceAsync(topic.ToString(), message, cancellationToken).ConfigureAwait(false);
    }

    private void ValidateEventType(string eventType)
    {
        var warnings = EventNamingConvention.Validate(eventType);
        if (warnings.Count == 0)
        {
            return;
        }

        if (_options.EnforceEventNamingConvention)
        {
            throw new ArgumentException(string.Join(" ", warnings), nameof(eventType));
        }

        foreach (var warning in warnings)
        {
            _logger.LogWarning("{Warning}", warning);
        }
    }

    private static Headers BuildHeaders<T>(EventEnvelope<T> envelope, string eventType)
    {
        var headers = new Headers
        {
            { "eventType", Encoding.UTF8.GetBytes(eventType) },
            { "eventId", Encoding.UTF8.GetBytes(envelope.EventId.ToString("N")) },
        };

        if (envelope.CorrelationId is not null)
        {
            headers.Add("correlationId", Encoding.UTF8.GetBytes(envelope.CorrelationId));
        }

        return headers;
    }

    public ValueTask DisposeAsync()
    {
        _producer.Dispose();
        return ValueTask.CompletedTask;
    }
}
