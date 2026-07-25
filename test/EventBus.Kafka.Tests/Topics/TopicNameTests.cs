using EventBus.Kafka.Topics;
using FluentAssertions;
using Xunit;

namespace EventBus.Kafka.Tests.Topics;

public class TopicNameTests
{
    [Fact]
    public void Parse_RoundTrips_ValidTopicName()
    {
        var topic = TopicName.Parse("public.learning.enrollment.courses.v1");

        topic.Visibility.Should().Be(TopicVisibility.Public);
        topic.Group.Should().Be("learning");
        topic.Service.Should().Be("enrollment");
        topic.Resource.Should().Be("courses");
        topic.Version.Should().Be(1);
        topic.ToString().Should().Be("public.learning.enrollment.courses.v1");
    }

    [Fact]
    public void Create_BuildsSameStringAs_Parse()
    {
        var created = TopicName.Create(TopicVisibility.Private, "learning", "enrollment", "courses", 2);
        var parsed = TopicName.Parse("private.learning.enrollment.courses.v2");

        created.Should().Be(parsed);
        created.ToString().Should().Be("private.learning.enrollment.courses.v2");
    }

    [Theory]
    [InlineData("public.learning.enrollment.courses")]
    [InlineData("public.learning.enrollment.courses.v1.extra")]
    public void Parse_Throws_WhenSegmentCountIsWrong(string value)
    {
        var act = () => TopicName.Parse(value);
        act.Should().Throw<FormatException>().WithMessage("*5 segments*");
    }

    [Fact]
    public void Parse_Throws_OnInvalidVisibility()
    {
        var act = () => TopicName.Parse("internal.learning.enrollment.courses.v1");
        act.Should().Throw<FormatException>().WithMessage("*visibility*");
    }

    [Theory]
    [InlineData("public.Learning.enrollment.courses.v1")]
    [InlineData("public.learning.enrollment.courses_bad.v1")]
    [InlineData("public..enrollment.courses.v1")]
    public void Parse_Throws_OnInvalidSegment(string value)
    {
        var act = () => TopicName.Parse(value);
        act.Should().Throw<FormatException>();
    }

    [Theory]
    [InlineData("public.learning.enrollment.courses.1")]
    [InlineData("public.learning.enrollment.courses.v0")]
    [InlineData("public.learning.enrollment.courses.version1")]
    public void Parse_Throws_OnInvalidVersion(string value)
    {
        var act = () => TopicName.Parse(value);
        act.Should().Throw<FormatException>().WithMessage("*version*");
    }

    [Fact]
    public void TryParse_ReturnsFalse_WithoutThrowing_OnInvalidInput()
    {
        var result = TopicName.TryParse("not-a-topic-name", out var topicName);

        result.Should().BeFalse();
        topicName.Should().BeNull();
    }

    [Fact]
    public void Create_Throws_OnInvalidSegment()
    {
        var act = () => TopicName.Create(TopicVisibility.Public, "Learning", "enrollment", "courses");
        act.Should().Throw<FormatException>();
    }

    [Fact]
    public void Equals_ComparesByValue()
    {
        var a = TopicName.Parse("public.learning.enrollment.courses.v1");
        var b = TopicName.Parse("public.learning.enrollment.courses.v1");

        a.Should().Be(b);
        (a == b).Should().BeTrue();
        a.Equals(b).Should().BeTrue();
    }

    [Fact]
    public void DeadLetterTopic_SuffixesResourceSegment_KeepingFiveSegments()
    {
        var topic = TopicName.Parse("public.learning.enrollment.courses.v1");

        var deadLetterTopic = topic.DeadLetterTopic();

        deadLetterTopic.ToString().Should().Be("public.learning.enrollment.courses-dlq.v1");
        deadLetterTopic.Visibility.Should().Be(topic.Visibility);
        deadLetterTopic.Group.Should().Be(topic.Group);
        deadLetterTopic.Service.Should().Be(topic.Service);
        deadLetterTopic.Version.Should().Be(topic.Version);
    }

    [Fact]
    public void DeadLetterTopic_RoundTrips_ThroughParse()
    {
        var deadLetterTopic = TopicName.Parse("public.learning.enrollment.courses.v1").DeadLetterTopic();

        TopicName.Parse(deadLetterTopic.ToString()).Should().Be(deadLetterTopic);
    }
}
