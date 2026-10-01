using System.Text;
using KafkaVsRabbitMQ.Contracts;

namespace KafkaVsRabbitMQ.Tests;

public sealed class JsonAndSimulationTests
{
    private static OrderPlaced Order(string sku = "SKU-001", decimal total = 25m) => new(
        Guid.Parse("11111111-2222-3333-4444-555555555555"),
        "customer-1",
        [new OrderItem(sku, 2, total / 2)],
        total,
        new DateTimeOffset(2026, 9, 30, 12, 0, 0, TimeSpan.Zero));

    [Fact]
    public void Json_round_trips_an_order()
    {
        var original = Order();

        var restored = JsonDefaults.Deserialize<OrderPlaced>(JsonDefaults.Serialize(original));

        Assert.Equal(original.OrderId, restored.OrderId);
        Assert.Equal(original.CustomerId, restored.CustomerId);
        Assert.Equal(original.TotalAmount, restored.TotalAmount);
        Assert.Equal(original.Items[0].Sku, restored.Items[0].Sku);
    }

    [Fact]
    public void Json_uses_camel_case_on_the_wire()
    {
        var json = Encoding.UTF8.GetString(JsonDefaults.Serialize(Order()));

        Assert.Contains("\"orderId\"", json);
        Assert.DoesNotContain("\"OrderId\"", json);
    }

    [Fact]
    public void A_normal_order_succeeds()
        => Assert.Equal(SimulationOutcome.Succeed, OrderSimulation.Decide(Order(), attempt: 0).Outcome);

    [Fact]
    public void An_unavailable_sku_fails_permanently()
    {
        var (outcome, reason) = OrderSimulation.Decide(Order(OrderSimulation.UnavailableSku), attempt: 0);

        Assert.Equal(SimulationOutcome.PermanentFailure, outcome);
        Assert.Contains("out of stock", reason);
    }

    [Fact]
    public void A_large_order_fails_once_then_succeeds()
    {
        var order = Order(total: OrderSimulation.LargeOrderThreshold + 1);

        Assert.Equal(SimulationOutcome.TransientFailure, OrderSimulation.Decide(order, attempt: 0).Outcome);
        Assert.Equal(SimulationOutcome.Succeed, OrderSimulation.Decide(order, attempt: 1).Outcome);
    }

    [Fact]
    public void Demo_orders_are_deterministic_in_shape()
    {
        var first = DemoOrders.Create(6);
        var second = DemoOrders.Create(6);

        Assert.Equal(6, first.Count);
        // Same SKUs and totals every run; only the generated order ids differ.
        Assert.Equal(first.Select(o => o.Items[0].Sku), second.Select(o => o.Items[0].Sku));
        Assert.Equal(first.Select(o => o.TotalAmount), second.Select(o => o.TotalAmount));
    }

    [Fact]
    public void The_workload_reaches_every_branch()
    {
        var orders = DemoOrders.Create(6);

        var outcomes = orders
            .Select((order, index) => OrderSimulation.Decide(order, attempt: 0).Outcome)
            .ToList();

        Assert.Contains(SimulationOutcome.PermanentFailure, outcomes);
        Assert.Contains(SimulationOutcome.TransientFailure, outcomes);
        Assert.Contains(SimulationOutcome.Succeed, outcomes);
    }

    [Fact]
    public void Customers_repeat_so_partitioning_is_observable()
    {
        var orders = DemoOrders.Create(9);

        Assert.Equal(3, orders.Select(o => o.CustomerId).Distinct().Count());
    }

    [Fact]
    public void Demo_orders_reject_a_non_positive_count()
        => Assert.Throws<ArgumentOutOfRangeException>(() => DemoOrders.Create(0));
}
