namespace KafkaVsRabbitMQ.Contracts;

/// <summary>What the simulated business logic decided about one delivery.</summary>
public enum SimulationOutcome
{
    /// <summary>Processed.</summary>
    Succeed,

    /// <summary>Failed in a way that may clear up — worth retrying.</summary>
    TransientFailure,

    /// <summary>Failed in a way that never will be fixed by trying again.</summary>
    PermanentFailure,
}

/// <summary>
/// The business rule, in exactly one place. Both the Kafka and the RabbitMQ sample
/// call this, so any difference you see in behaviour is a difference in the broker or
/// in the client code — never in the business logic. That is the whole point of the
/// comparison.
/// </summary>
public static class OrderSimulation
{
    /// <summary>Any order containing this SKU fails permanently.</summary>
    public const string UnavailableSku = "OUT-OF-STOCK";

    /// <summary>Orders above this total fail their first delivery.</summary>
    public const decimal LargeOrderThreshold = 5_000m;

    /// <summary>How long the "inventory and payment" call pretends to take.</summary>
    public static readonly TimeSpan WorkDuration = TimeSpan.FromMilliseconds(20);

    /// <summary>Decide what happens to <paramref name="order"/> on delivery <paramref name="attempt"/>.</summary>
    public static (SimulationOutcome Outcome, string? Reason) Decide(OrderPlaced order, int attempt)
    {
        ArgumentNullException.ThrowIfNull(order);

        var unavailable = order.Items.FirstOrDefault(
            item => string.Equals(item.Sku, UnavailableSku, StringComparison.OrdinalIgnoreCase));

        if (unavailable is not null)
        {
            return (SimulationOutcome.PermanentFailure, $"SKU {unavailable.Sku} is out of stock");
        }

        if (order.TotalAmount > LargeOrderThreshold && attempt == 0)
        {
            return (SimulationOutcome.TransientFailure, "payment provider timed out");
        }

        return (SimulationOutcome.Succeed, null);
    }
}
