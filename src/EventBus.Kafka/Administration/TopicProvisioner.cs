using Confluent.Kafka;
using Confluent.Kafka.Admin;
using EventBus.Kafka.Topics;

namespace EventBus.Kafka.Administration;

/// <summary>
/// Creates topics that follow the naming convention, with retention defaults chosen by
/// visibility: a public topic (a cross-team contract) defaults to a longer retention window
/// than a private one, since a newly onboarded subscriber may need to catch up on more
/// history. See docs/architecture/0002-topic-naming-convention.md.
/// </summary>
public sealed class TopicProvisioner
{
    private static readonly TimeSpan DefaultPublicRetention = TimeSpan.FromDays(7);
    private static readonly TimeSpan DefaultPrivateRetention = TimeSpan.FromDays(1);

    private readonly IAdminClient _adminClient;

    public TopicProvisioner(IAdminClient adminClient)
    {
        _adminClient = adminClient;
    }

    /// <summary>Creates <paramref name="topic"/> if it doesn't already exist; a no-op otherwise.</summary>
    public async Task EnsureTopicExistsAsync(
        TopicName topic,
        int partitions = 6,
        short replicationFactor = 3,
        TimeSpan? retention = null,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var retentionMs = (long)(retention ?? DefaultRetention(topic.Visibility)).TotalMilliseconds;

        var topicSpecification = new TopicSpecification
        {
            Name = topic.ToString(),
            NumPartitions = partitions,
            ReplicationFactor = replicationFactor,
            Configs = new Dictionary<string, string>
            {
                ["retention.ms"] = retentionMs.ToString(),
            },
        };

        try
        {
            await _adminClient.CreateTopicsAsync([topicSpecification]).ConfigureAwait(false);
        }
        catch (CreateTopicsException ex) when (ex.Results.All(r => r.Error.Code == ErrorCode.TopicAlreadyExists))
        {
            // Another instance already provisioned this topic; treat as success.
        }
    }

    private static TimeSpan DefaultRetention(TopicVisibility visibility) => visibility switch
    {
        TopicVisibility.Public => DefaultPublicRetention,
        TopicVisibility.Private => DefaultPrivateRetention,
        _ => DefaultPrivateRetention,
    };
}
