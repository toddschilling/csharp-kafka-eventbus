using System.Text;
using Confluent.Kafka;
using EventBus.Kafka.Topics;
using Microsoft.Extensions.Logging;

namespace EventBus.Kafka.DeadLetter;

/// <summary>Republishes exhausted messages to their dead-letter topic via Confluent.Kafka.</summary>
public sealed class KafkaDeadLetterPublisher : IDeadLetterPublisher, IAsyncDisposable
{
    private readonly IProducer<string, byte[]> _producer;
    private readonly ILogger<KafkaDeadLetterPublisher> _logger;

    public KafkaDeadLetterPublisher(IProducer<string, byte[]> producer, ILogger<KafkaDeadLetterPublisher> logger)
    {
        _producer = producer;
        _logger = logger;
    }

    public async Task PublishAsync(
        TopicName deadLetterTopic,
        Message<string, byte[]> originalMessage,
        DeadLetterContext context,
        CancellationToken cancellationToken = default)
    {
        var headers = new Headers();
        if (originalMessage.Headers is not null)
        {
            foreach (var header in originalMessage.Headers)
            {
                headers.Add(header.Key, header.GetValueBytes());
            }
        }

        headers.Add("originalTopic", Encoding.UTF8.GetBytes(context.OriginalTopic));
        headers.Add("originalPartition", Encoding.UTF8.GetBytes(context.Partition.ToString()));
        headers.Add("originalOffset", Encoding.UTF8.GetBytes(context.Offset.ToString()));
        headers.Add("attempts", Encoding.UTF8.GetBytes(context.Attempts.ToString()));
        headers.Add("exceptionType", Encoding.UTF8.GetBytes(context.ExceptionType));
        headers.Add("exceptionMessage", Encoding.UTF8.GetBytes(context.ExceptionMessage));
        headers.Add("firstFailedAt", Encoding.UTF8.GetBytes(context.FirstFailedAt.ToString("O")));
        headers.Add("lastFailedAt", Encoding.UTF8.GetBytes(context.LastFailedAt.ToString("O")));

        var message = new Message<string, byte[]>
        {
            Key = originalMessage.Key,
            Value = originalMessage.Value,
            Headers = headers,
        };

        await _producer.ProduceAsync(deadLetterTopic.ToString(), message, cancellationToken).ConfigureAwait(false);

        _logger.LogWarning(
            "Dead-lettered message from {OriginalTopic} (partition {Partition}, offset {Offset}) to {DeadLetterTopic} " +
            "after {Attempts} attempt(s): {ExceptionType}: {ExceptionMessage}",
            context.OriginalTopic, context.Partition, context.Offset, deadLetterTopic, context.Attempts,
            context.ExceptionType, context.ExceptionMessage);
    }

    public ValueTask DisposeAsync()
    {
        _producer.Dispose();
        return ValueTask.CompletedTask;
    }
}
