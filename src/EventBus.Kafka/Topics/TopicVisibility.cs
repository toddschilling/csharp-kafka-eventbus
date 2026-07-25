namespace EventBus.Kafka.Topics;

/// <summary>
/// Whether a topic is a cross-team contract or a single team's internal plumbing.
/// See docs/architecture/0002-topic-naming-convention.md.
/// </summary>
public enum TopicVisibility
{
    /// <summary>
    /// Internal to a single service or a small, tightly related group of services.
    /// No cross-team compatibility guarantees are implied.
    /// </summary>
    Private,

    /// <summary>
    /// A stable, cross-team contract. Changes must stay backward compatible;
    /// breaking changes require a new <see cref="TopicName.Version"/>, not an in-place change.
    /// </summary>
    Public,
}
