using Confluent.Kafka;
using EventBus.Kafka.Consuming;
using EventBus.Kafka.DependencyInjection;
using EventBus.Kafka.Events;
using EventBus.Kafka.Topics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Hosting;

// Consumes CourseCompleted events published by Sample.Producer and logs them. Requires a Kafka
// broker reachable at KAFKA_BOOTSTRAP_SERVERS (defaults to localhost:9092); see docs/getting-started.md.
var builder = Host.CreateApplicationBuilder(args);

var bootstrapServers = Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP_SERVERS") ?? "localhost:9092";
const string groupId = "skills-graph.course-completed";

builder.Services.AddSingleton<IEventHandler<CourseCompletedData>, CourseCompletedHandler>();

builder.Services.AddKafkaEventConsumer<CourseCompletedData>(
    new ConsumerConfig
    {
        BootstrapServers = bootstrapServers,
        GroupId = groupId,
        AutoOffsetReset = AutoOffsetReset.Earliest,
        // KafkaEventConsumer<T> commits manually, only after the handler and the processed-event
        // store both succeed, so auto-commit must stay off; see docs/architecture/0004-idempotent-consumers.md.
        EnableAutoCommit = false,
    },
    new EventConsumerOptions
    {
        GroupId = groupId,
        Topics = [TopicName.Parse("public.learning.enrollment.courses.v1")],
        EventType = "CourseCompleted",
    });

using var host = builder.Build();
await host.RunAsync();

internal sealed record CourseCompletedData(string LearnerId, string CourseId, DateTimeOffset CompletedAt);

internal sealed class CourseCompletedHandler(ILogger<CourseCompletedHandler> logger)
    : IEventHandler<CourseCompletedData>
{
    public Task HandleAsync(EventEnvelope<CourseCompletedData> envelope, CancellationToken cancellationToken)
    {
        logger.LogInformation(
            "Learner {LearnerId} completed course {CourseId} at {CompletedAt}.",
            envelope.Data!.LearnerId, envelope.Data.CourseId, envelope.Data.CompletedAt);
        return Task.CompletedTask;
    }
}
