namespace EventBus.Kafka.Serialization;

/// <summary>Context passed to an <see cref="IEventSerializer"/> so it can make topic- or type-aware decisions.</summary>
public readonly record struct EventSerializationContext(string Topic, string EventType);
