namespace KafkaVsRabbitMQ.Contracts;

/// <summary>One line item on an order.</summary>
public sealed record OrderItem(string Sku, int Quantity, decimal UnitPrice)
{
    /// <summary>Quantity times unit price.</summary>
    public decimal LineTotal => Quantity * UnitPrice;
}

/// <summary>The event both brokers carry.</summary>
public sealed record OrderPlaced(
    Guid OrderId,
    string CustomerId,
    IReadOnlyList<OrderItem> Items,
    decimal TotalAmount,
    DateTimeOffset OccurredAt);

/// <summary>Emitted when an order is processed successfully.</summary>
public sealed record OrderConfirmed(
    Guid OrderId,
    string CustomerId,
    decimal TotalAmount,
    DateTimeOffset ConfirmedAt);

/// <summary>Emitted when an order is declined for a business reason.</summary>
public sealed record OrderRejected(
    Guid OrderId,
    string CustomerId,
    string Reason,
    DateTimeOffset RejectedAt);
