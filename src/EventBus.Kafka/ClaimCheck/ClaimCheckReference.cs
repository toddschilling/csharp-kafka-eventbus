namespace EventBus.Kafka.ClaimCheck;

/// <summary>
/// Points to a payload stored out-of-band because it was too large to publish inline.
/// See docs/architecture/0005-claim-check-for-large-payloads.md.
/// </summary>
public sealed record ClaimCheckReference(string Location, long SizeBytes, string ContentType);
