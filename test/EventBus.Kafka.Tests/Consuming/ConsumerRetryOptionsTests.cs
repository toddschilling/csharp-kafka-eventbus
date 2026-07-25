using EventBus.Kafka.Consuming;
using FluentAssertions;
using Xunit;

namespace EventBus.Kafka.Tests.Consuming;

public class ConsumerRetryOptionsTests
{
    [Fact]
    public void DelayForAttempt_AppliesExponentialBackoff()
    {
        var options = new ConsumerRetryOptions
        {
            InitialDelay = TimeSpan.FromSeconds(1),
            BackoffMultiplier = 2.0,
            MaxDelay = TimeSpan.FromMinutes(10),
        };

        options.DelayForAttempt(1).Should().Be(TimeSpan.FromSeconds(1));
        options.DelayForAttempt(2).Should().Be(TimeSpan.FromSeconds(2));
        options.DelayForAttempt(3).Should().Be(TimeSpan.FromSeconds(4));
        options.DelayForAttempt(4).Should().Be(TimeSpan.FromSeconds(8));
    }

    [Fact]
    public void DelayForAttempt_CapsAtMaxDelay()
    {
        var options = new ConsumerRetryOptions
        {
            InitialDelay = TimeSpan.FromSeconds(1),
            BackoffMultiplier = 2.0,
            MaxDelay = TimeSpan.FromSeconds(5),
        };

        options.DelayForAttempt(10).Should().Be(TimeSpan.FromSeconds(5));
    }

    [Fact]
    public void Defaults_MatchDocumentedValues()
    {
        var options = new ConsumerRetryOptions();

        options.MaxAttempts.Should().Be(3);
        options.InitialDelay.Should().Be(TimeSpan.FromSeconds(1));
        options.MaxDelay.Should().Be(TimeSpan.FromSeconds(30));
        options.BackoffMultiplier.Should().Be(2.0);
    }
}
