using System.Text.RegularExpressions;

namespace EventBus.Kafka.Topics;

/// <summary>
/// A topic name that follows the org-wide convention:
/// <c>{visibility}.{group}.{service}.{resource}.{version}</c>, e.g.
/// <c>public.learning.enrollment.courses.v1</c>.
/// </summary>
/// <remarks>
/// The convention is deliberately fixed at five segments so tooling (access control,
/// dashboards, discovery) can depend on segment position instead of special-casing
/// topic names. One topic represents one resource; the action or event type belongs
/// in the message envelope (see <see cref="Events.EventEnvelope{T}.EventType"/>), not
/// in the topic name. See docs/architecture/0002-topic-naming-convention.md.
/// </remarks>
public sealed partial class TopicName : IEquatable<TopicName>
{
    private static readonly Regex SegmentPattern = MakeSegmentPattern();
    private static readonly Regex VersionPattern = MakeVersionPattern();

    public TopicVisibility Visibility { get; }
    public string Group { get; }
    public string Service { get; }
    public string Resource { get; }
    public int Version { get; }

    private TopicName(TopicVisibility visibility, string group, string service, string resource, int version)
    {
        Visibility = visibility;
        Group = group;
        Service = service;
        Resource = resource;
        Version = version;
    }

    /// <summary>
    /// Builds a <see cref="TopicName"/> from its parts, validating each segment against the
    /// naming convention. Throws <see cref="FormatException"/> on an invalid segment.
    /// </summary>
    public static TopicName Create(TopicVisibility visibility, string group, string service, string resource, int version = 1)
    {
        RequireValidSegment(group, nameof(group));
        RequireValidSegment(service, nameof(service));
        RequireValidSegment(resource, nameof(resource));

        if (version < 1)
        {
            throw new FormatException($"Topic version must be 1 or greater, got {version}.");
        }

        return new TopicName(visibility, group, service, resource, version);
    }

    /// <summary>
    /// Parses a fully qualified topic name such as <c>public.learning.enrollment.courses.v1</c>.
    /// Throws <see cref="FormatException"/> if the value does not have exactly five segments
    /// or any segment is invalid.
    /// </summary>
    public static TopicName Parse(string value)
    {
        if (!TryParse(value, out var topicName, out var error))
        {
            throw new FormatException(error);
        }

        return topicName!;
    }

    public static bool TryParse(string value, out TopicName? topicName) => TryParse(value, out topicName, out _);

    private static bool TryParse(string value, out TopicName? topicName, out string? error)
    {
        topicName = null;

        if (string.IsNullOrWhiteSpace(value))
        {
            error = "Topic name must not be empty.";
            return false;
        }

        var segments = value.Split('.');
        if (segments.Length != 5)
        {
            error = $"Topic name '{value}' must have exactly 5 segments in the form " +
                     "'{visibility}.{group}.{service}.{resource}.{version}', but had {segments.Length}.";
            return false;
        }

        var (visibilityText, group, service, resource, versionText) =
            (segments[0], segments[1], segments[2], segments[3], segments[4]);

        if (!TryParseVisibility(visibilityText, out var visibility))
        {
            error = $"Topic name '{value}' has an invalid visibility segment '{visibilityText}'; " +
                     "expected 'public' or 'private'.";
            return false;
        }

        if (!SegmentPattern.IsMatch(group))
        {
            error = $"Topic name '{value}' has an invalid group segment '{group}'; " +
                     "expected lowercase, kebab-case (e.g. 'learning').";
            return false;
        }

        if (!SegmentPattern.IsMatch(service))
        {
            error = $"Topic name '{value}' has an invalid service segment '{service}'; " +
                     "expected lowercase, kebab-case (e.g. 'enrollment').";
            return false;
        }

        if (!SegmentPattern.IsMatch(resource))
        {
            error = $"Topic name '{value}' has an invalid resource segment '{resource}'; " +
                     "expected lowercase, kebab-case (e.g. 'courses').";
            return false;
        }

        var versionMatch = VersionPattern.Match(versionText);
        if (!versionMatch.Success)
        {
            error = $"Topic name '{value}' has an invalid version segment '{versionText}'; " +
                     "expected 'v' followed by a positive integer (e.g. 'v1').";
            return false;
        }

        var version = int.Parse(versionMatch.Groups[1].Value);
        topicName = new TopicName(visibility, group, service, resource, version);
        error = null;
        return true;
    }

    private static bool TryParseVisibility(string text, out TopicVisibility visibility)
    {
        switch (text)
        {
            case "public":
                visibility = TopicVisibility.Public;
                return true;
            case "private":
                visibility = TopicVisibility.Private;
                return true;
            default:
                visibility = default;
                return false;
        }
    }

    private static void RequireValidSegment(string segment, string paramName)
    {
        if (!SegmentPattern.IsMatch(segment))
        {
            throw new FormatException(
                $"'{segment}' is not a valid {paramName} segment; expected lowercase, kebab-case " +
                "(e.g. 'enrollment'), matching ^[a-z][a-z0-9]*(-[a-z0-9]+)*$.");
        }
    }

    public override string ToString()
    {
        var visibilityText = Visibility == TopicVisibility.Public ? "public" : "private";
        return $"{visibilityText}.{Group}.{Service}.{Resource}.v{Version}";
    }

    public bool Equals(TopicName? other) =>
        other is not null && ToString() == other.ToString();

    public override bool Equals(object? obj) => Equals(obj as TopicName);

    public override int GetHashCode() => ToString().GetHashCode(StringComparison.Ordinal);

    public static bool operator ==(TopicName? left, TopicName? right) =>
        left is null ? right is null : left.Equals(right);

    public static bool operator !=(TopicName? left, TopicName? right) => !(left == right);

    public static implicit operator string(TopicName topicName) => topicName.ToString();

    [GeneratedRegex("^[a-z][a-z0-9]*(-[a-z0-9]+)*$")]
    private static partial Regex MakeSegmentPattern();

    [GeneratedRegex("^v([1-9][0-9]*)$")]
    private static partial Regex MakeVersionPattern();
}
