using EventBus.Kafka.Idempotency;
using FluentAssertions;
using Xunit;

namespace EventBus.Kafka.Tests.Idempotency;

public class InMemoryProcessedEventStoreTests
{
    [Fact]
    public async Task IsProcessedAsync_ReturnsFalse_ForUnknownEvent()
    {
        var store = new InMemoryProcessedEventStore();

        (await store.IsProcessedAsync(Guid.NewGuid())).Should().BeFalse();
    }

    [Fact]
    public async Task MarkProcessedAsync_MakesSubsequentChecksTrue()
    {
        var store = new InMemoryProcessedEventStore();
        var eventId = Guid.NewGuid();

        await store.MarkProcessedAsync(eventId);

        (await store.IsProcessedAsync(eventId)).Should().BeTrue();
    }

    [Fact]
    public async Task MarkProcessedAsync_DoesNotAffectOtherEvents()
    {
        var store = new InMemoryProcessedEventStore();
        await store.MarkProcessedAsync(Guid.NewGuid());

        (await store.IsProcessedAsync(Guid.NewGuid())).Should().BeFalse();
    }
}
