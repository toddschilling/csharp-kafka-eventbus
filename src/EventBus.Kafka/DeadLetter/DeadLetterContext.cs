namespace EventBus.Kafka.DeadLetter;

/// <summary>
/// Diagnostic metadata attached to a message when it's republished to a dead-letter topic after
/// exhausting its retry attempts. See docs/architecture/0006-retry-and-dead-letter-topics.md.
/// </summary>
public sealed record DeadLetterContext(
    string OriginalTopic,
    int Partition,
    long Offset,
    int Attempts,
    string ExceptionType,
    string ExceptionMessage,
    DateTimeOffset FirstFailedAt,
    DateTimeOffset LastFailedAt);
