using System.Text;
using Confluent.Kafka;
using EventBus.Kafka.Consuming;
using EventBus.Kafka.DeadLetter;
using EventBus.Kafka.Events;
using EventBus.Kafka.Idempotency;
using EventBus.Kafka.Serialization;
using EventBus.Kafka.Topics;
using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Xunit;

namespace EventBus.Kafka.Tests.Consuming;

/// <summary>
/// Drives <see cref="KafkaEventConsumer{T}"/> through its BackgroundService lifecycle with
/// substituted <see cref="IConsumer{TKey,TValue}"/>/<see cref="IEventHandler{T}"/>/
/// <see cref="IDeadLetterPublisher"/> dependencies, to cover the retry-then-dead-letter path
/// described in docs/architecture/0006-retry-and-dead-letter-topics.md end-to-end — including
/// that the offset is only committed after the dead-letter publish (or the processed-store mark)
/// succeeds, not before.
/// </summary>
public class KafkaEventConsumerRetryAndDeadLetterTests
{
    private static readonly TopicName Topic = TopicName.Parse("public.learning.enrollment.courses.v1");

    private readonly IConsumer<string, byte[]> _consumer = Substitute.For<IConsumer<string, byte[]>>();
    private readonly IEventSerializer _serializer = Substitute.For<IEventSerializer>();
    private readonly IProcessedEventStore _processedEventStore = Substitute.For<IProcessedEventStore>();
    private readonly IEventHandler<string> _handler = Substitute.For<IEventHandler<string>>();
    private readonly IDeadLetterPublisher _deadLetterPublisher = Substitute.For<IDeadLetterPublisher>();

    private readonly EventEnvelope<string> _envelope =
        EventEnvelope<string>.Create("CourseCompleted", "learner-1", "enrollment-service", "payload");

    public KafkaEventConsumerRetryAndDeadLetterTests()
    {
        _processedEventStore.IsProcessedAsync(Arg.Any<Guid>(), Arg.Any<CancellationToken>()).Returns(false);
    }

    [Fact]
    public async Task ExhaustingRetries_PublishesToDeadLetterTopic_ThenCommitsOffset()
    {
        var consumeResult = BuildConsumeResult();
        StubSingleConsume(consumeResult);
        StubSuccessfulDeserialize();
        _handler.HandleAsync(Arg.Any<EventEnvelope<string>>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromException(new InvalidOperationException("handler exploded")));

        var sut = BuildConsumer(maxAttempts: 2, deadLetterEnabled: true);

        await RunUntilLoopExitsAsync(sut);

        await _handler.Received(2).HandleAsync(Arg.Any<EventEnvelope<string>>(), Arg.Any<CancellationToken>());
        await _processedEventStore.DidNotReceive().MarkProcessedAsync(Arg.Any<Guid>(), Arg.Any<CancellationToken>());

        var expectedDlqTopic = Topic.DeadLetterTopic();
        await _deadLetterPublisher.Received(1).PublishAsync(
            expectedDlqTopic,
            Arg.Any<Message<string, byte[]>>(),
            Arg.Is<DeadLetterContext>(ctx =>
                ctx.OriginalTopic == Topic.ToString() &&
                ctx.Partition == 0 &&
                ctx.Offset == 42 &&
                ctx.Attempts == 2 &&
                ctx.ExceptionType == typeof(InvalidOperationException).FullName &&
                ctx.ExceptionMessage == "handler exploded"),
            Arg.Any<CancellationToken>());

        _consumer.Received(1).Commit(consumeResult);

        // The whole point of the DLQ flow: the offset must not be committed until the
        // dead-letter publish has actually gone through, so a crash before that point
        // redelivers the message instead of silently losing it.
        Received.InOrder(() =>
        {
            _deadLetterPublisher.PublishAsync(
                expectedDlqTopic, Arg.Any<Message<string, byte[]>>(), Arg.Any<DeadLetterContext>(), Arg.Any<CancellationToken>());
            _consumer.Commit(consumeResult);
        });
    }

    [Fact]
    public async Task SucceedingOnRetry_MarksProcessed_CommitsOffset_NeverDeadLetters()
    {
        var consumeResult = BuildConsumeResult();
        StubSingleConsume(consumeResult);
        StubSuccessfulDeserialize();
        _handler.HandleAsync(Arg.Any<EventEnvelope<string>>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromException(new InvalidOperationException("transient")), Task.CompletedTask);

        var sut = BuildConsumer(maxAttempts: 3, deadLetterEnabled: true);

        await RunUntilLoopExitsAsync(sut);

        await _handler.Received(2).HandleAsync(Arg.Any<EventEnvelope<string>>(), Arg.Any<CancellationToken>());
        await _processedEventStore.Received(1).MarkProcessedAsync(_envelope.EventId, Arg.Any<CancellationToken>());
        _deadLetterPublisher.ReceivedCalls().Should().BeEmpty();
        _consumer.Received(1).Commit(consumeResult);

        Received.InOrder(() =>
        {
            _processedEventStore.MarkProcessedAsync(_envelope.EventId, Arg.Any<CancellationToken>());
            _consumer.Commit(consumeResult);
        });
    }

    [Fact]
    public async Task DeadLetterDisabled_RethrowsAfterExhaustingRetries_NeverCommitsOffset()
    {
        var consumeResult = BuildConsumeResult();
        StubSingleConsume(consumeResult);
        StubSuccessfulDeserialize();
        _handler.HandleAsync(Arg.Any<EventEnvelope<string>>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromException(new InvalidOperationException("handler exploded")));

        var sut = BuildConsumer(maxAttempts: 2, deadLetterEnabled: false);

        await sut.StartAsync(CancellationToken.None);
        var act = async () => await sut.ExecuteTask!.WaitAsync(TimeSpan.FromSeconds(5));

        await act.Should().ThrowAsync<InvalidOperationException>().WithMessage("handler exploded");

        _deadLetterPublisher.ReceivedCalls().Should().BeEmpty();
        _consumer.DidNotReceive().Commit(Arg.Any<ConsumeResult<string, byte[]>>());
    }

    private ConsumeResult<string, byte[]> BuildConsumeResult()
    {
        var headers = new Headers();
        headers.Add("eventType", Encoding.UTF8.GetBytes("CourseCompleted"));
        headers.Add("eventId", Encoding.UTF8.GetBytes(_envelope.EventId.ToString()));

        return new ConsumeResult<string, byte[]>
        {
            TopicPartitionOffset = new TopicPartitionOffset(Topic.ToString(), new Partition(0), new Offset(42)),
            Message = new Message<string, byte[]>
            {
                Key = "learner-1",
                Value = Encoding.UTF8.GetBytes("irrelevant-wire-bytes"),
                Headers = headers,
            },
        };
    }

    /// <summary>
    /// Returns <paramref name="consumeResult"/> once, then throws <see cref="OperationCanceledException"/>
    /// as the real Confluent consumer does once cancelled — which is also how the test stops
    /// <see cref="KafkaEventConsumer{T}.ExecuteAsync"/>'s loop after processing exactly one message.
    /// </summary>
    private void StubSingleConsume(ConsumeResult<string, byte[]> consumeResult)
    {
        var callCount = 0;
        _consumer.Consume(Arg.Any<CancellationToken>()).Returns(_ =>
        {
            callCount++;
            return callCount == 1 ? consumeResult : throw new OperationCanceledException();
        });
    }

    private void StubSuccessfulDeserialize() =>
        _serializer
            .DeserializeAsync<string>(Arg.Any<byte[]>(), Arg.Any<EventSerializationContext>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(_envelope));

    private KafkaEventConsumer<string> BuildConsumer(int maxAttempts, bool deadLetterEnabled)
    {
        var options = new EventConsumerOptions
        {
            GroupId = "test-group",
            Topics = new[] { Topic },
            EventType = "CourseCompleted",
            Retry = new ConsumerRetryOptions
            {
                MaxAttempts = maxAttempts,
                InitialDelay = TimeSpan.FromMilliseconds(1),
                MaxDelay = TimeSpan.FromMilliseconds(1),
            },
            DeadLetter = new DeadLetterOptions { Enabled = deadLetterEnabled },
        };

        return new KafkaEventConsumer<string>(
            _consumer,
            _serializer,
            _processedEventStore,
            _handler,
            options,
            NullLogger<KafkaEventConsumer<string>>.Instance,
            claimCheckStore: null,
            deadLetterPublisher: _deadLetterPublisher);
    }

    private static async Task RunUntilLoopExitsAsync(KafkaEventConsumer<string> sut)
    {
        await sut.StartAsync(CancellationToken.None);
        await sut.ExecuteTask!.WaitAsync(TimeSpan.FromSeconds(5));
    }
}
