namespace KafkaVsRabbitMQ.RabbitMq;

/// <summary>
/// The RabbitMQ topology: exchanges, queues, bindings and the arguments that make
/// retries and dead-lettering work.
///
/// This is the biggest structural difference from Kafka. Here the *broker* holds the
/// routing rules. A message is published to an exchange with a routing key; the
/// exchange looks up which queues are bound with that key and copies the message to
/// each of them. Retries and dead-lettering are then just more bindings plus queue
/// arguments — the broker does the work that Kafka forces your consumer to do.
///
/// The wiring:
///
/// <code>
///   orders (direct exchange)
///     ├─ routing key "placed"         → orders.placed          (the pipeline)
///     │      nack → dead-letters to "retried" ↓
///     ├─ routing key "retried"        → orders.placed.retry    (x-message-ttl 5s)
///     │      TTL expires → dead-letters back to "placed" ↑     (the delay!)
///     ├─ routing key "confirmed"      → orders.confirmed
///     └─ routing key "rejected"       → orders.rejected
///
///   orders.dlx (direct exchange)
///     └─ routing key "dead-lettered"  → orders.placed.dlq
/// </code>
///
/// The loop in the middle is the whole trick: a rejected message sits in a queue
/// whose only feature is a TTL, and when that expires RabbitMQ dead-letters it back
/// to the main exchange. No consumer is involved while it waits.
/// </summary>
public static class RabbitTopology
{
    /// <summary>Main direct exchange.</summary>
    public const string Exchange = "orders";

    /// <summary>Exchange used to park messages nobody could process.</summary>
    public const string DeadLetterExchange = "orders.dlx";

    /// <summary>Main work queue.</summary>
    public const string PlacedQueue = "orders.placed";

    /// <summary>Where a rejected message waits out its retry delay.</summary>
    public const string RetryQueue = "orders.placed.retry";

    /// <summary>Parking queue for exhausted messages.</summary>
    public const string DeadLetterQueue = "orders.placed.dlq";

    /// <summary>Processed orders.</summary>
    public const string ConfirmedQueue = "orders.confirmed";

    /// <summary>Declined orders.</summary>
    public const string RejectedQueue = "orders.rejected";

    /// <summary>Routing keys used on the two exchanges.</summary>
    public static class RoutingKeys
    {
        /// <summary>Publish an order to the pipeline.</summary>
        public const string Placed = "placed";

        /// <summary>Send a message to the retry queue.</summary>
        public const string Retried = "retried";

        /// <summary>Send a message to the dead-letter queue.</summary>
        public const string DeadLettered = "dead-lettered";

        /// <summary>Send an event to the confirmed queue.</summary>
        public const string Confirmed = "confirmed";

        /// <summary>Send an event to the rejected queue.</summary>
        public const string Rejected = "rejected";
    }

    /// <summary>
    /// Arguments for the main queue. A nacked message is dead-lettered to
    /// <see cref="RoutingKeys.Retried"/> on the main exchange, which lands it on the
    /// retry queue.
    /// </summary>
    public static IReadOnlyDictionary<string, object?> MainQueueArguments { get; } = new Dictionary<string, object?>
    {
        ["x-dead-letter-exchange"] = Exchange,
        ["x-dead-letter-routing-key"] = RoutingKeys.Retried,
    };

    /// <summary>
    /// Arguments for the retry queue. It holds a message for
    /// <c>x-message-ttl</c> milliseconds and then dead-letters it back to the main
    /// exchange under the <c>placed</c> key — RabbitMQ's built-in delayed delivery.
    /// </summary>
    public static IReadOnlyDictionary<string, object?> RetryQueueArguments(int retryDelayMs) => new Dictionary<string, object?>
    {
        ["x-message-ttl"] = retryDelayMs,
        ["x-dead-letter-exchange"] = Exchange,
        ["x-dead-letter-routing-key"] = RoutingKeys.Placed,
    };

    /// <summary>Every queue paired with the exchange, routing key and arguments it needs.</summary>
    public static IReadOnlyList<QueueBinding> Bindings(int retryDelayMs) =>
    [
        new(PlacedQueue, Exchange, RoutingKeys.Placed, MainQueueArguments),
        new(RetryQueue, Exchange, RoutingKeys.Retried, RetryQueueArguments(retryDelayMs)),
        new(ConfirmedQueue, Exchange, RoutingKeys.Confirmed, null),
        new(RejectedQueue, Exchange, RoutingKeys.Rejected, null),
        new(DeadLetterQueue, DeadLetterExchange, RoutingKeys.DeadLettered, null),
    ];
}

/// <summary>One queue and how it is bound to an exchange.</summary>
/// <param name="Queue">Queue name.</param>
/// <param name="Exchange">Exchange it binds to.</param>
/// <param name="RoutingKey">Routing key of the binding.</param>
/// <param name="Arguments">Queue arguments (TTL, dead-letter routing), or null.</param>
public sealed record QueueBinding(
    string Queue,
    string Exchange,
    string RoutingKey,
    IReadOnlyDictionary<string, object?>? Arguments);
