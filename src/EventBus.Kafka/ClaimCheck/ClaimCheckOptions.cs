namespace EventBus.Kafka.ClaimCheck;

/// <summary>Controls when the producer offloads a payload to the claim-check store instead of publishing it inline.</summary>
public sealed class ClaimCheckOptions
{
    /// <summary>
    /// Serialized payloads at or above this size are stored via <see cref="IClaimCheckStore"/> and
    /// replaced with a reference. Defaults to 1,000,000 bytes, comfortably under Kafka brokers'
    /// common 1 MB <c>message.max.bytes</c> default so a claim-checked message never risks rejection.
    /// </summary>
    public int ThresholdBytes { get; init; } = 1_000_000;
}
