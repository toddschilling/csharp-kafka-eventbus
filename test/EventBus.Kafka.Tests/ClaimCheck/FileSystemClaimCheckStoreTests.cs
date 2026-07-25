using System.Text;
using EventBus.Kafka.ClaimCheck;
using FluentAssertions;
using Xunit;

namespace EventBus.Kafka.Tests.ClaimCheck;

public class FileSystemClaimCheckStoreTests : IDisposable
{
    private readonly string _rootDirectory =
        Path.Combine(Path.GetTempPath(), "eventbus-kafka-tests-" + Guid.NewGuid().ToString("N"));

    [Fact]
    public async Task StoreAsync_ThenRetrieveAsync_RoundTripsPayload()
    {
        var store = new FileSystemClaimCheckStore(_rootDirectory);
        var payload = Encoding.UTF8.GetBytes("a payload too large to publish inline");
        var eventId = Guid.NewGuid();

        var reference = await store.StoreAsync("public.learning.enrollment.courses.v1", eventId, payload, "application/json");

        reference.SizeBytes.Should().Be(payload.Length);
        reference.ContentType.Should().Be("application/json");

        var retrieved = await store.RetrieveAsync(reference);
        retrieved.Should().Equal(payload);
    }

    [Fact]
    public void Constructor_CreatesRootDirectory_WhenMissing()
    {
        Directory.Exists(_rootDirectory).Should().BeFalse();

        _ = new FileSystemClaimCheckStore(_rootDirectory);

        Directory.Exists(_rootDirectory).Should().BeTrue();
    }

    public void Dispose()
    {
        if (Directory.Exists(_rootDirectory))
        {
            Directory.Delete(_rootDirectory, recursive: true);
        }
    }
}
