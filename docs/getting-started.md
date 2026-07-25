# Getting started

## Prerequisites

- .NET 8 SDK
- A reachable Kafka broker. For local development, any single-broker Kafka image works, e.g.:

  ```bash
  docker run -d --name kafka -p 9092:9092 \
    -e KAFKA_NODE_ID=1 \
    -e KAFKA_PROCESS_ROLES=broker,controller \
    -e KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
    -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
    -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
    -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
    -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT \
    -e CLUSTER_ID=csharp-kafka-eventbus-dev \
    apache/kafka:latest
  ```

## Build and test

```bash
dotnet build
dotnet test
```

Neither requires a running broker — the unit tests exercise `TopicName`, `EventEnvelope<T>`,
`JsonEventSerializer`, `InMemoryProcessedEventStore`, and `FileSystemClaimCheckStore` in isolation.

## Run the samples

With a broker reachable at `localhost:9092` (or set `KAFKA_BOOTSTRAP_SERVERS`):

```bash
dotnet run --project samples/Sample.Consumer &
dotnet run --project samples/Sample.Producer
```

`Sample.Producer` publishes one `CourseCompleted` event to `public.learning.enrollment.courses.v1`
and exits; `Sample.Consumer` subscribes and logs each one it sees. `Sample.Consumer` keeps running
(it's a hosted `BackgroundService`) — stop it with Ctrl+C.

## A five-minute tour of the API

**Publish an event:**

```csharp
var topic = TopicName.Parse("public.learning.enrollment.courses.v1");

await producer.PublishAsync(
    topic,
    eventType: "CourseCompleted",
    partitionKey: learnerId, // ordering key — required, not optional
    data: new CourseCompletedData(learnerId, courseId, DateTimeOffset.UtcNow));
```

**Handle it:**

```csharp
internal sealed class CourseCompletedHandler : IEventHandler<CourseCompletedData>
{
    public Task HandleAsync(EventEnvelope<CourseCompletedData> envelope, CancellationToken cancellationToken)
    {
        // envelope.Data, envelope.EventId, envelope.OccurredAt, ...
        return Task.CompletedTask;
    }
}
```

**Wire it up:**

```csharp
services.AddKafkaEventProducer(producerConfig, new EventProducerOptions { ServiceName = "enrollment" });

services.AddSingleton<IEventHandler<CourseCompletedData>, CourseCompletedHandler>();
services.AddKafkaEventConsumer<CourseCompletedData>(
    consumerConfig,
    new EventConsumerOptions
    {
        GroupId = "skills-graph.course-completed",
        Topics = [topic],
        EventType = "CourseCompleted",
    });
```

See `samples/Sample.Producer` and `samples/Sample.Consumer` for complete, runnable versions, and
`docs/architecture/` for the reasoning behind each piece.
