using EventBus.Kafka.Events;
using FluentAssertions;
using Xunit;

namespace EventBus.Kafka.Tests.Events;

public class EventNamingConventionTests
{
    [Theory]
    [InlineData("CourseCompleted")]
    [InlineData("CertificateGenerated")]
    [InlineData("UserSignedUp")]
    public void Validate_ReturnsNoWarnings_ForPastTenseFacts(string eventType)
    {
        EventNamingConvention.Validate(eventType).Should().BeEmpty();
    }

    [Theory]
    [InlineData("CompleteCourse")]
    [InlineData("SendNotification")]
    [InlineData("CreateUser")]
    [InlineData("CancelSubscription")]
    public void Validate_WarnsOnImperativeCommandNames(string eventType)
    {
        var warnings = EventNamingConvention.Validate(eventType);

        warnings.Should().ContainSingle();
        warnings[0].Should().Contain("reads as a command");
    }

    [Fact]
    public void Validate_WarnsOnLowercaseStart()
    {
        var warnings = EventNamingConvention.Validate("courseCompleted");

        warnings.Should().Contain(w => w.Contains("PascalCase"));
    }

    [Fact]
    public void Validate_WarnsOnEmptyEventType()
    {
        EventNamingConvention.Validate(string.Empty).Should().ContainSingle();
    }
}
