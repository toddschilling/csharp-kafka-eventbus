using Confluent.Kafka;
using EventBus.Kafka.ClaimCheck;
using EventBus.Kafka.Consuming;
using EventBus.Kafka.DeadLetter;
using EventBus.Kafka.Idempotency;
using EventBus.Kafka.Producing;
using EventBus.Kafka.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace EventBus.Kafka.DependencyInjection;

public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Registers the shared defaults (JSON serialization, an in-memory processed-event store)
    /// used by both producers and consumers unless already registered. Safe to call more than
    /// once; each call also invoked implicitly by <see cref="AddKafkaEventProducer"/> and
    /// <see cref="AddKafkaEventConsumer{T}"/>.
    /// </summary>
    public static IServiceCollection AddEventBusKafkaDefaults(this IServiceCollection services)
    {
        services.TryAddSingleton<IEventSerializer, JsonEventSerializer>();
        services.TryAddSingleton<IProcessedEventStore, InMemoryProcessedEventStore>();
        return services;
    }

    /// <summary>Registers an <see cref="IEventProducer"/> backed by Confluent.Kafka.</summary>
    public static IServiceCollection AddKafkaEventProducer(
        this IServiceCollection services,
        ProducerConfig producerConfig,
        EventProducerOptions producerOptions,
        IClaimCheckStore? claimCheckStore = null)
    {
        services.AddEventBusKafkaDefaults();
        services.AddSingleton(producerOptions);

        if (claimCheckStore is not null)
        {
            services.TryAddSingleton(claimCheckStore);
        }

        services.AddSingleton<IProducer<string, byte[]>>(
            _ => new ProducerBuilder<string, byte[]>(producerConfig).Build());

        services.AddSingleton<IEventProducer, KafkaEventProducer>();

        return services;
    }

    /// <summary>
    /// Registers a hosted <see cref="KafkaEventConsumer{T}"/> for a single event type. Call once
    /// per (topic set, event type, handler) combination; each call creates its own Kafka consumer
    /// and consumer group, so registering the same topic for several event types is expected and
    /// each instance simply skips messages whose "eventType" header doesn't match its own.
    /// Requires an <see cref="IEventHandler{T}"/> already registered in <paramref name="services"/>.
    /// </summary>
    public static IServiceCollection AddKafkaEventConsumer<T>(
        this IServiceCollection services,
        ConsumerConfig consumerConfig,
        EventConsumerOptions consumerOptions,
        IClaimCheckStore? claimCheckStore = null)
    {
        services.AddEventBusKafkaDefaults();

        if (claimCheckStore is not null)
        {
            services.TryAddSingleton(claimCheckStore);
        }

        services.AddSingleton<IHostedService>(provider =>
        {
            var consumer = new ConsumerBuilder<string, byte[]>(consumerConfig).Build();
            var serializer = provider.GetRequiredService<IEventSerializer>();
            var processedEventStore = provider.GetRequiredService<IProcessedEventStore>();
            var handler = provider.GetRequiredService<IEventHandler<T>>();
            var resolvedClaimCheckStore = provider.GetService<IClaimCheckStore>();
            var logger = provider.GetRequiredService<ILogger<KafkaEventConsumer<T>>>();

            IDeadLetterPublisher? deadLetterPublisher = null;
            if (consumerOptions.DeadLetter.Enabled)
            {
                var deadLetterProducer = new ProducerBuilder<string, byte[]>(
                    new ProducerConfig { BootstrapServers = consumerConfig.BootstrapServers }).Build();
                var deadLetterLogger = provider.GetRequiredService<ILogger<KafkaDeadLetterPublisher>>();
                deadLetterPublisher = new KafkaDeadLetterPublisher(deadLetterProducer, deadLetterLogger);
            }

            return new KafkaEventConsumer<T>(
                consumer, serializer, processedEventStore, handler, consumerOptions, logger,
                resolvedClaimCheckStore, deadLetterPublisher);
        });

        return services;
    }
}
