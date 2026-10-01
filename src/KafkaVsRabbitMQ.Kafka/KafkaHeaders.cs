using System.Globalization;
using System.Text;
using Confluent.Kafka;

namespace KafkaVsRabbitMQ.Kafka;

/// <summary>
/// Kafka record headers — the only place besides the key and the value where you can
/// put metadata. Everything here is application-defined: the broker understands none
/// of it, which is why the retry count and the delay time have to be enforced by the
/// consumer rather than by the broker.
/// </summary>
public static class KafkaHeaders
{
    /// <summary>Delivery attempt number.</summary>
    public const string Attempt = "attempt";

    /// <summary>Unix milliseconds before which a retry must not be delivered.</summary>
    public const string NotBefore = "not-before";

    /// <summary>Logical event name.</summary>
    public const string EventType = "event-type";

    /// <summary>Read the delivery attempt, defaulting to zero.</summary>
    public static int ReadAttempt(this Headers headers)
    {
        ArgumentNullException.ThrowIfNull(headers);

        if (!headers.TryGetLastBytes(Attempt, out var bytes))
        {
            return 0;
        }

        return int.TryParse(Encoding.UTF8.GetString(bytes), NumberStyles.Integer, CultureInfo.InvariantCulture, out var value)
            ? value
            : 0;
    }

    /// <summary>Read the not-before time, if the record carries one.</summary>
    public static DateTimeOffset? ReadNotBefore(this Headers headers)
    {
        ArgumentNullException.ThrowIfNull(headers);

        if (!headers.TryGetLastBytes(NotBefore, out var bytes)
            || !long.TryParse(Encoding.UTF8.GetString(bytes), NumberStyles.Integer, CultureInfo.InvariantCulture, out var millis))
        {
            return null;
        }

        return DateTimeOffset.FromUnixTimeMilliseconds(millis);
    }

    /// <summary>Build the headers for an order record.</summary>
    public static Headers Build(int attempt, DateTimeOffset? notBefore = null)
    {
        var headers = new Headers
        {
            { Attempt, Encoding.UTF8.GetBytes(attempt.ToString(CultureInfo.InvariantCulture)) },
            { EventType, Encoding.UTF8.GetBytes(nameof(Contracts.OrderPlaced)) },
        };

        if (notBefore is not null)
        {
            headers.Add(NotBefore, Encoding.UTF8.GetBytes(notBefore.Value.ToUnixTimeMilliseconds().ToString(CultureInfo.InvariantCulture)));
        }

        return headers;
    }
}
