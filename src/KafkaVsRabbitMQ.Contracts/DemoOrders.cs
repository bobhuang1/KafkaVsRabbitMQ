namespace KafkaVsRabbitMQ.Contracts;

/// <summary>
/// The same generated workload for both brokers, so a throughput comparison is not
/// measuring two different sets of data.
/// </summary>
public static class DemoOrders
{
    /// <summary>Build <paramref name="count"/> deterministic orders.</summary>
    public static IReadOnlyList<OrderPlaced> Create(int count)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(count, 1);

        var orders = new List<OrderPlaced>(count);
        for (var index = 0; index < count; index++)
        {
            orders.Add(CreateOne(index, count));
        }

        return orders;
    }

    /// <summary>
    /// Order <paramref name="index"/>: the first carries a SKU that is permanently
    /// rejected, the second is over the large-order threshold and fails once, and the
    /// rest succeed. Customers repeat with a period of three.
    /// </summary>
    public static OrderPlaced CreateOne(int index, int count)
    {
        var customer = $"customer-{index % 3}";
        var isRejectable = index == 0;
        var isLarge = index == 1 && count > 1;

        var sku = isRejectable ? OrderSimulation.UnavailableSku : $"SKU-{index:D5}";
        var unitPrice = isLarge ? 6_000m : 10m + (index % 100);
        var quantity = isLarge ? 2 : 1;

        var items = new[] { new OrderItem(sku, quantity, unitPrice) };

        return new OrderPlaced(
            Guid.NewGuid(),
            customer,
            items,
            items.Sum(item => item.LineTotal),
            DateTimeOffset.UtcNow);
    }
}
