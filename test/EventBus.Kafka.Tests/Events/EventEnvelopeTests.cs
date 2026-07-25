using EventBus.Kafka.Events;
using EventBus.Kafka.Serialization;
using FluentAssertions;
using Xunit;

namespace EventBus.Kafka.Tests.Events;

public class EventEnvelopeTests
{
    private sealed record CourseCompletedData(string LearnerId, string CourseId);

    [Fact]
    public void Create_PopulatesRequiredMetadata()
    {
        var before = DateTimeOffset.UtcNow;

        var envelope = EventEnvelope<CourseCompletedData>.Create(
            "CourseCompleted",
            partitionKey: "learner-42",
            source: "enrollment",
            data: new CourseCompletedData("learner-42", "course-7"));

        var after = DateTimeOffset.UtcNow;

        envelope.EventId.Should().NotBeEmpty();
        envelope.EventType.Should().Be("CourseCompleted");
        envelope.PartitionKey.Should().Be("learner-42");
        envelope.Source.Should().Be("enrollment");
        envelope.SchemaVersion.Should().Be(1);
        envelope.OccurredAt.Should().BeOnOrAfter(before).And.BeOnOrBefore(after);
        envelope.Data!.LearnerId.Should().Be("learner-42");
        envelope.ClaimCheck.Should().BeNull();
    }

    [Theory]
    [InlineData("")]
    [InlineData(" ")]
    public void Create_Throws_WhenEventTypeMissing(string eventType)
    {
        var act = () => EventEnvelope<CourseCompletedData>.Create(
            eventType, "learner-42", "enrollment", new CourseCompletedData("learner-42", "course-7"));

        act.Should().Throw<ArgumentException>();
    }

    [Fact]
    public async Task JsonEventSerializer_RoundTrips_EnvelopeAndData()
    {
        var serializer = new JsonEventSerializer();
        var envelope = EventEnvelope<CourseCompletedData>.Create(
            "CourseCompleted", "learner-42", "enrollment", new CourseCompletedData("learner-42", "course-7"),
            correlationId: "corr-1");

        var context = new EventSerializationContext("public.learning.enrollment.courses.v1", "CourseCompleted");
        var bytes = await serializer.SerializeAsync(envelope, context);
        var roundTripped = await serializer.DeserializeAsync<CourseCompletedData>(bytes, context);

        roundTripped.Should().Be(envelope);
    }
}
