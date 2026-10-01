using System.Collections;
using System.Globalization;

namespace KafkaVsRabbitMQ.RabbitMq;

/// <summary>
/// Header helpers for RabbitMQ messages.
///
/// Two sources of truth for "how many times has this failed":
///
/// * our own <c>attempt</c> header, which we set when we publish;
/// * RabbitMQ's <c>x-death</c> header, which the **broker** adds every time a
///   message is dead-lettered. It is a list of records, each with a <c>count</c>.
///
/// Preferring <c>x-death</c> when it is present is the idiomatic choice: the broker
/// already counted the round trips through the retry queue, so the application does
/// not have to track anything. That is a capability Kafka simply does not have —
/// there is no broker-maintained history for a record.
/// </summary>
public static class RabbitHeaders
{
    /// <summary>Our own delivery attempt header.</summary>
    public const string Attempt = "attempt";

    /// <summary>Logical event name.</summary>
    public const string EventType = "event-type";

    /// <summary>Why the message ended up where it did.</summary>
    public const string Reason = "reason";

    /// <summary>The broker-maintained dead-letter history header.</summary>
    public const string DeadLetterHistory = "x-death";

    /// <summary>
    /// Delivery attempt: the broker's dead-letter count when there is one, otherwise
    /// our own header, otherwise zero.
    /// </summary>
    public static int ReadAttempt(IDictionary<string, object?>? headers)
    {
        if (headers is null)
        {
            return 0;
        }

        if (headers.TryGetValue(DeadLetterHistory, out var history))
        {
            var deadLetters = CountDeadLetters(history);
            if (deadLetters > 0)
            {
                return deadLetters;
            }
        }

        return headers.TryGetValue(Attempt, out var raw) && TryToInt(raw, out var attempt) ? attempt : 0;
    }

    /// <summary>Sum the <c>count</c> fields in an <c>x-death</c> header.</summary>
    public static int CountDeadLetters(object? deadLetterHistory)
    {
        if (deadLetterHistory is not IEnumerable records || deadLetterHistory is string)
        {
            return 0;
        }

        var total = 0;
        foreach (var record in records)
        {
            if (record is IDictionary<string, object> fields
                && fields.TryGetValue("count", out var count)
                && TryToInt(count, out var value))
            {
                total += value;
            }
        }

        return total;
    }

    /// <summary>
    /// Coerce a header value to an integer. AMQP header values arrive as whatever the
    /// sender chose: a number, a string, or raw bytes.
    /// </summary>
    public static bool TryToInt(object? value, out int result)
    {
        switch (value)
        {
            case null:
                result = 0;
                return false;
            case int i:
                result = i;
                return true;
            case long l:
                result = (int)Math.Clamp(l, int.MinValue, int.MaxValue);
                return true;
            case byte[] bytes when int.TryParse(System.Text.Encoding.UTF8.GetString(bytes), NumberStyles.Integer, CultureInfo.InvariantCulture, out var fromBytes):
                result = fromBytes;
                return true;
            case string text when int.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out var fromText):
                result = fromText;
                return true;
            default:
                result = 0;
                return false;
        }
    }
}
