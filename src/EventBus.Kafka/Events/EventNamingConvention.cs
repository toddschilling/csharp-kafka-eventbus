namespace EventBus.Kafka.Events;

/// <summary>
/// Advisory checks for <see cref="EventEnvelope{T}.EventType"/> naming: events should read as
/// facts about something that already happened, not as instructions. This never blocks a
/// publish on its own; see <see cref="Producing.EventProducerOptions.EnforceEventNamingConvention"/>
/// to make violations throw instead of just warning.
/// </summary>
public static class EventNamingConvention
{
    private static readonly string[] ImperativeVerbPrefixes =
    [
        "Create", "Update", "Delete", "Remove", "Add", "Send", "Notify", "Process",
        "Handle", "Set", "Get", "Fetch", "Cancel", "Start", "Stop", "Make", "Do",
        "Complete", "Generate", "Build", "Run", "Execute", "Validate", "Approve", "Reject",
    ];

    /// <summary>Returns human-readable warnings for an event type; empty if none apply.</summary>
    public static IReadOnlyList<string> Validate(string eventType)
    {
        var warnings = new List<string>();

        if (string.IsNullOrWhiteSpace(eventType))
        {
            warnings.Add("Event type must not be empty.");
            return warnings;
        }

        if (!char.IsUpper(eventType[0]))
        {
            warnings.Add($"Event type '{eventType}' should be PascalCase, e.g. 'CourseCompleted'.");
        }

        foreach (var verb in ImperativeVerbPrefixes)
        {
            var startsWithVerb = eventType.StartsWith(verb, StringComparison.Ordinal) &&
                                  (eventType.Length == verb.Length || char.IsUpper(eventType[verb.Length]));

            if (startsWithVerb)
            {
                warnings.Add(
                    $"Event type '{eventType}' reads as a command ('{verb}...'), not a fact. " +
                    "Events describe something that already happened, e.g. 'CourseCompleted', " +
                    "not 'CompleteCourse'.");
                break;
            }
        }

        return warnings;
    }
}
