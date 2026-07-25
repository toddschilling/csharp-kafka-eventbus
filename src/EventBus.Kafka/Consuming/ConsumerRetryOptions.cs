namespace EventBus.Kafka.Consuming;

/// <summary>
/// Controls how many times, and with what backoff, <see cref="KafkaEventConsumer{T}"/> retries a
/// message that fails deserialization or handling before dead-lettering it. See
/// docs/architecture/0006-retry-and-dead-letter-topics.md.
/// </summary>
public sealed class ConsumerRetryOptions
{
    /// <summary>Total attempts (including the first) before the message is dead-lettered. Defaults to 3.</summary>
    public int MaxAttempts { get; init; } = 3;

    /// <summary>Delay before the second attempt. Defaults to 1 second.</summary>
    public TimeSpan InitialDelay { get; init; } = TimeSpan.FromSeconds(1);

    /// <summary>Upper bound the backoff delay is capped at, regardless of attempt count. Defaults to 30 seconds.</summary>
    public TimeSpan MaxDelay { get; init; } = TimeSpan.FromSeconds(30);

    /// <summary>Multiplier applied to the delay after each failed attempt. Defaults to 2.0.</summary>
    public double BackoffMultiplier { get; init; } = 2.0;

    /// <summary>The delay to wait before retry attempt number <paramref name="attempt"/> (1-based, i.e. the delay before the 2nd try is for attempt 1).</summary>
    public TimeSpan DelayForAttempt(int attempt)
    {
        var uncappedMs = InitialDelay.TotalMilliseconds * Math.Pow(BackoffMultiplier, attempt - 1);
        var cappedMs = Math.Min(uncappedMs, MaxDelay.TotalMilliseconds);
        return TimeSpan.FromMilliseconds(cappedMs);
    }
}
