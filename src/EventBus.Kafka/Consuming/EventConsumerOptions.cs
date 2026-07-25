using EventBus.Kafka.Topics;

namespace EventBus.Kafka.Consuming;

public sealed class EventConsumerOptions
{
    /// <summary>The Kafka consumer group this consumer participates in.</summary>
    public required string GroupId { get; init; }

    /// <summary>Topics this consumer subscribes to.</summary>
    public required IReadOnlyList<TopicName> Topics { get; init; }

    /// <summary>
    /// The single event type this consumer instance handles. A topic can carry several event
    /// types (one topic per resource, per docs/architecture/0002-topic-naming-convention.md);
    /// messages whose "eventType" header doesn't match this value are skipped and committed
    /// past without being deserialized or handed to the handler.
    /// </summary>
    public required string EventType { get; init; }
}
