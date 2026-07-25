namespace EventBus.Kafka.Consuming;

/// <summary>
/// Controls what happens once <see cref="ConsumerRetryOptions.MaxAttempts"/> is exhausted. See
/// docs/architecture/0006-retry-and-dead-letter-topics.md.
/// </summary>
public sealed class DeadLetterOptions
{
    /// <summary>
    /// When true (the default), a message that exhausts its retry attempts is published to
    /// <see cref="Topics.TopicName.DeadLetterTopic"/> and its offset is committed. When false, the
    /// exception is rethrown instead — today's crash-and-stop behavior — for consumers where an
    /// operator wants a loud failure rather than silent quarantine.
    /// </summary>
    public bool Enabled { get; init; } = true;
}
