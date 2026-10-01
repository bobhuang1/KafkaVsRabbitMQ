namespace KafkaVsRabbitMQ.Kafka;

/// <summary>
/// The Kafka topology: topics, and how many partitions each has.
///
/// Kafka's model is a **log**. A producer appends to a topic; the broker decides
/// which partition from the message key. There is no queue, no binding and no
/// routing rule — a topic is a thing you write to and read from, and everything else
/// is a convention between the applications.
/// </summary>
public static class KafkaTopology
{
    /// <summary>Where orders are published.</summary>
    public const string Placed = "orders.placed";

    /// <summary>Transient failures, waiting out their backoff.</summary>
    public const string Retry = "orders.placed.retry";

    /// <summary>Orders nobody could process.</summary>
    public const string DeadLetter = "orders.placed.dlq";

    /// <summary>Processed orders.</summary>
    public const string Confirmed = "orders.confirmed";

    /// <summary>Declined orders.</summary>
    public const string Rejected = "orders.rejected";

    /// <summary>Everything the sample creates on startup.</summary>
    public static IReadOnlyList<(string Name, int Partitions)> All { get; } =
    [
        (Placed, 3),
        (Retry, 3),
        (DeadLetter, 1),
        (Confirmed, 3),
        (Rejected, 1),
    ];

    /// <summary>How a key decides placement, for the README's benefit and for tests.</summary>
    public static string PartitionKeyFor(string customerId) => customerId.Trim().ToLowerInvariant();
}
