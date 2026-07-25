using System.Text;
using Confluent.Kafka;
using EventBus.Kafka.ClaimCheck;
using EventBus.Kafka.DeadLetter;
using EventBus.Kafka.Idempotency;
using EventBus.Kafka.Serialization;
using EventBus.Kafka.Topics;
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
    private readonly IDeadLetterPublisher? _deadLetterPublisher;

    public KafkaEventConsumer(
        IConsumer<string, byte[]> consumer,
        IEventSerializer serializer,
        IProcessedEventStore processedEventStore,
        IEventHandler<T> handler,
        EventConsumerOptions options,
        ILogger<KafkaEventConsumer<T>> logger,
        IClaimCheckStore? claimCheckStore = null,
        IDeadLetterPublisher? deadLetterPublisher = null)
    {
        _consumer = consumer;
        _serializer = serializer;
        _processedEventStore = processedEventStore;
        _handler = handler;
        _options = options;
        _logger = logger;
        _claimCheckStore = claimCheckStore;
        _deadLetterPublisher = deadLetterPublisher;
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

        if (await TryHandleWithRetryAsync(consumeResult, cancellationToken).ConfigureAwait(false))
        {
            await _processedEventStore.MarkProcessedAsync(eventId, cancellationToken).ConfigureAwait(false);
        }

        _consumer.Commit(consumeResult);
    }

    /// <summary>
    /// Deserializes and hands the message to <see cref="IEventHandler{T}"/>, retrying on failure
    /// per <see cref="EventConsumerOptions.Retry"/>. Once attempts are exhausted, dead-letters the
    /// message (or rethrows, if <see cref="EventConsumerOptions.DeadLetter"/> is disabled) rather
    /// than retrying forever. Returns true if the message was handled, false if it was
    /// dead-lettered instead. See docs/architecture/0006-retry-and-dead-letter-topics.md.
    /// </summary>
    private async Task<bool> TryHandleWithRetryAsync(
        ConsumeResult<string, byte[]> consumeResult, CancellationToken cancellationToken)
    {
        var context = new EventSerializationContext(consumeResult.Topic, _options.EventType);
        DateTimeOffset? firstFailedAt = null;
        Exception lastException;

        var attempt = 1;
        while (true)
        {
            try
            {
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
                return true;
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                lastException = ex;
                firstFailedAt ??= DateTimeOffset.UtcNow;

                _logger.LogWarning(
                    ex,
                    "Attempt {Attempt}/{MaxAttempts} failed for event type {EventType} on topic {Topic} at offset {Offset}.",
                    attempt, _options.Retry.MaxAttempts, _options.EventType, consumeResult.Topic, consumeResult.Offset);

                if (attempt >= _options.Retry.MaxAttempts)
                {
                    break;
                }

                await Task.Delay(_options.Retry.DelayForAttempt(attempt), cancellationToken).ConfigureAwait(false);
                attempt++;
            }
        }

        if (!_options.DeadLetter.Enabled)
        {
            throw lastException;
        }

        if (_deadLetterPublisher is null)
        {
            throw new InvalidOperationException(
                $"Event type '{_options.EventType}' on topic '{consumeResult.Topic}' exhausted its retry " +
                "attempts, but no IDeadLetterPublisher was configured for this consumer.");
        }

        var deadLetterTopic = TopicName.Parse(consumeResult.Topic).DeadLetterTopic();
        var deadLetterContext = new DeadLetterContext(
            consumeResult.Topic,
            consumeResult.Partition.Value,
            consumeResult.Offset.Value,
            _options.Retry.MaxAttempts,
            lastException.GetType().FullName ?? lastException.GetType().Name,
            lastException.Message,
            firstFailedAt!.Value,
            DateTimeOffset.UtcNow);

        await _deadLetterPublisher
            .PublishAsync(deadLetterTopic, consumeResult.Message, deadLetterContext, cancellationToken)
            .ConfigureAwait(false);

        return false;
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
