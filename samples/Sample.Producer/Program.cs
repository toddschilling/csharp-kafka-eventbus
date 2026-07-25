using Confluent.Kafka;
using EventBus.Kafka.DependencyInjection;
using EventBus.Kafka.Producing;
using EventBus.Kafka.Topics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

// Publishes a single CourseCompleted fact and exits. Requires a Kafka broker reachable at
// KAFKA_BOOTSTRAP_SERVERS (defaults to localhost:9092); see docs/getting-started.md.
var builder = Host.CreateApplicationBuilder(args);

var bootstrapServers = Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP_SERVERS") ?? "localhost:9092";

builder.Services.AddKafkaEventProducer(
    new ProducerConfig { BootstrapServers = bootstrapServers },
    new EventProducerOptions { ServiceName = "enrollment" });

using var host = builder.Build();
await host.StartAsync();

var producer = host.Services.GetRequiredService<IEventProducer>();

// One topic per resource, per docs/architecture/0002-topic-naming-convention.md — every lifecycle
// action on a course (completed, updated, deleted) is published here as a distinct eventType.
var topic = TopicName.Parse("public.learning.enrollment.courses.v1");
const string learnerId = "learner-42";

await producer.PublishAsync(
    topic,
    eventType: "CourseCompleted", // a fact that already happened, not an instruction
    partitionKey: learnerId, // keeps this learner's course events in order; see IEventProducer.PublishAsync docs
    data: new CourseCompletedData(learnerId, CourseId: "course-7", CompletedAt: DateTimeOffset.UtcNow));

Console.WriteLine($"Published CourseCompleted for {learnerId} to {topic}.");

await host.StopAsync();

internal sealed record CourseCompletedData(string LearnerId, string CourseId, DateTimeOffset CompletedAt);
