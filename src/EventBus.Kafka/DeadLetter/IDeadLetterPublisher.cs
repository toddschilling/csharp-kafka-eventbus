using Confluent.Kafka;
using EventBus.Kafka.Topics;

namespace EventBus.Kafka.DeadLetter;

/// <summary>
/// Republishes a message that exhausted its retry attempts to a dead-letter topic. Deliberately
/// works with the raw <see cref="Message{TKey,TValue}"/> bytes and headers as originally consumed
/// rather than going through <see cref="Serialization.IEventSerializer"/> again — the failure
/// might be a deserialization failure, so the one thing guaranteed to still work is republishing
/// the bytes untouched. See docs/architecture/0006-retry-and-dead-letter-topics.md.
/// </summary>
public interface IDeadLetterPublisher
{
    Task PublishAsync(
        TopicName deadLetterTopic,
        Message<string, byte[]> originalMessage,
        DeadLetterContext context,
        CancellationToken cancellationToken = default);
}
