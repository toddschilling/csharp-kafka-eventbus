using EventBus.Kafka.Events;

namespace EventBus.Kafka.Consuming;

/// <summary>
/// Handles one event type. Implementations should be idempotent: Kafka guarantees at-least-once
/// delivery, and <see cref="EventEnvelope{T}.EventId"/> may be redelivered after a crash or
/// rebalance even though <see cref="KafkaEventConsumer{T}"/> already dedupes via
/// <see cref="Idempotency.IProcessedEventStore"/> on its own.
/// </summary>
public interface IEventHandler<T>
{
    Task HandleAsync(EventEnvelope<T> envelope, CancellationToken cancellationToken);
}
