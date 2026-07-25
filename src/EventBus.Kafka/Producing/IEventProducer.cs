using EventBus.Kafka.Topics;

namespace EventBus.Kafka.Producing;

/// <summary>
/// Publishes events to a topic. Deliberately has no notion of "who's listening" — see
/// docs/architecture/0002-topic-naming-convention.md and the coupled-in-the-wrong-direction
/// article this SDK is built around: a producer announces a fact, it doesn't address anyone.
/// </summary>
public interface IEventProducer : IAsyncDisposable
{
    /// <param name="topic">The resource topic to publish to; one topic per resource, not per action.</param>
    /// <param name="eventType">The fact being announced, named in the past tense (e.g. "CourseCompleted").</param>
    /// <param name="partitionKey">
    /// The ordering key. Kafka only guarantees order within a partition, so this must be the
    /// identifier of whatever needs its events processed in order (e.g. a learner ID) — it is
    /// required, not optional, so ordering is a decision made at every call site, not an afterthought.
    /// </param>
    Task PublishAsync<T>(
        TopicName topic,
        string eventType,
        string partitionKey,
        T data,
        PublishOptions? options = null,
        CancellationToken cancellationToken = default);
}
