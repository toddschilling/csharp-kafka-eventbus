using System.Text;
using Confluent.Kafka;
using EventBus.Kafka.ClaimCheck;
using EventBus.Kafka.Idempotency;
using EventBus.Kafka.Serialization;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace EventBus.Kafka.Consuming;

/// <summary>
/// Consumes one event type off one or more topics, dedupes via <see cref="IProcessedEventStore"/>,
/// resolves claim-checked payloads, and hands the result to a single <see cref="IEventHandler{T}"/>.
/// Commits offsets manually, after the handler and the processed-event store both succeed, so a
/// crash mid-handling redelivers the message rather than silently dropping it.
/// </summary>
public sealed class KafkaEventConsumer<T> : BackgroundService
{
    private readonly IConsumer<string, byte[]> _consumer;
    private readonly IEventSerializer _serializer;
    private readonly IProcessedEventStore _processedEventStore;
    private readonly IEventHandler<T> _handler;
    private readonly EventConsumerOptions _options;
    private readonly ILogger<KafkaEventConsumer<T>> _logger;
    private readonly IClaimCheckStore? _claimCheckStore;

    public KafkaEventConsumer(
        IConsumer<string, byte[]> consumer,
        IEventSerializer serializer,
        IProcessedEventStore processedEventStore,
        IEventHandler<T> handler,
        EventConsumerOptions options,
        ILogger<KafkaEventConsumer<T>> logger,
        IClaimCheckStore? claimCheckStore = null)
    {
        _consumer = consumer;
        _serializer = serializer;
        _processedEventStore = processedEventStore;
        _handler = handler;
        _options = options;
        _logger = logger;
        _claimCheckStore = claimCheckStore;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _consumer.Subscribe(_options.Topics.Select(t => t.ToString()).ToList());
        _logger.LogInformation(
            "Consumer group {GroupId} subscribed to {Topics} for event type {EventType}.",
            _options.GroupId, string.Join(", ", _options.Topics), _options.EventType);

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                ConsumeResult<string, byte[]>? consumeResult;
                try
                {
                    consumeResult = _consumer.Consume(stoppingToken);
                }
                catch (OperationCanceledException)
                {
                    break;
                }

                if (consumeResult?.Message is null)
                {
                    continue;
                }

                await ProcessAsync(consumeResult, stoppingToken).ConfigureAwait(false);
            }
        }
        finally
        {
            _consumer.Close();
        }
    }

    private async Task ProcessAsync(ConsumeResult<string, byte[]> consumeResult, CancellationToken cancellationToken)
    {
        var eventTypeHeader = GetHeader(consumeResult.Message.Headers, "eventType");
        if (!string.Equals(eventTypeHeader, _options.EventType, StringComparison.Ordinal))
        {
            _consumer.Commit(consumeResult);
            return;
        }

        var eventIdHeader = GetHeader(consumeResult.Message.Headers, "eventId");
        if (eventIdHeader is null || !Guid.TryParse(eventIdHeader, out var eventId))
        {
            _logger.LogWarning(
                "Message on {Topic} at offset {Offset} is missing a valid eventId header; skipping.",
                consumeResult.Topic, consumeResult.Offset);
            _consumer.Commit(consumeResult);
            return;
        }

        if (await _processedEventStore.IsProcessedAsync(eventId, cancellationToken).ConfigureAwait(false))
        {
            _consumer.Commit(consumeResult);
            return;
        }

        var context = new EventSerializationContext(consumeResult.Topic, _options.EventType);
        var envelope = await _serializer
            .DeserializeAsync<T>(consumeResult.Message.Value, context, cancellationToken)
            .ConfigureAwait(false);

        if (envelope.ClaimCheck is { } reference)
        {
            if (_claimCheckStore is null)
            {
                throw new InvalidOperationException(
                    $"Event '{envelope.EventType}' ({envelope.EventId}) on topic '{consumeResult.Topic}' was " +
                    "claim-checked, but no IClaimCheckStore was configured for this consumer.");
            }

            var storedBytes = await _claimCheckStore.RetrieveAsync(reference, cancellationToken).ConfigureAwait(false);
            envelope = await _serializer.DeserializeAsync<T>(storedBytes, context, cancellationToken).ConfigureAwait(false);
        }

        await _handler.HandleAsync(envelope, cancellationToken).ConfigureAwait(false);
        await _processedEventStore.MarkProcessedAsync(eventId, cancellationToken).ConfigureAwait(false);
        _consumer.Commit(consumeResult);
    }

    private static string? GetHeader(Headers? headers, string key)
    {
        if (headers is null || !headers.TryGetLastBytes(key, out var bytes))
        {
            return null;
        }

        return Encoding.UTF8.GetString(bytes);
    }
}
