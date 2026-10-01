using KafkaVsRabbitMQ.RabbitMq;

namespace KafkaVsRabbitMQ.Tests;

public sealed class RabbitTransportTests
{
    [Fact]
    public void Every_queue_is_bound_exactly_once()
    {
        var queues = RabbitTopology.Bindings(5000).Select(b => b.Queue).ToList();
        Assert.Equal(queues.Count, queues.Distinct().Count());
    }

    [Fact]
    public void The_pipeline_queue_is_bound_to_the_main_exchange()
    {
        var placed = RabbitTopology.Bindings(5000).Single(b => b.Queue == RabbitTopology.PlacedQueue);

        Assert.Equal(RabbitTopology.Exchange, placed.Exchange);
        Assert.Equal(RabbitTopology.RoutingKeys.Placed, placed.RoutingKey);
    }

    [Fact]
    public void A_rejected_message_is_routed_to_the_retry_queue()
    {
        // The main queue's dead-letter routing is what a nack follows.
        var arguments = RabbitTopology.MainQueueArguments;

        Assert.Equal(RabbitTopology.Exchange, arguments["x-dead-letter-exchange"]);
        Assert.Equal(RabbitTopology.RoutingKeys.Retried, arguments["x-dead-letter-routing-key"]);
    }

    [Fact]
    public void The_retry_queue_delays_then_returns_to_the_pipeline()
    {
        var arguments = RabbitTopology.RetryQueueArguments(retryDelayMs: 7000);

        Assert.Equal(7000, arguments["x-message-ttl"]);
        Assert.Equal(RabbitTopology.Exchange, arguments["x-dead-letter-exchange"]);
        Assert.Equal(RabbitTopology.RoutingKeys.Placed, arguments["x-dead-letter-routing-key"]);
    }

    [Fact]
    public void The_dead_letter_queue_sits_on_its_own_exchange()
    {
        var binding = RabbitTopology.Bindings(5000).Single(b => b.Queue == RabbitTopology.DeadLetterQueue);

        Assert.Equal(RabbitTopology.DeadLetterExchange, binding.Exchange);
        Assert.Equal(RabbitTopology.RoutingKeys.DeadLettered, binding.RoutingKey);
    }

    [Fact]
    public void Routing_keys_are_distinct()
    {
        var keys = new[]
        {
            RabbitTopology.RoutingKeys.Placed,
            RabbitTopology.RoutingKeys.Retried,
            RabbitTopology.RoutingKeys.DeadLettered,
            RabbitTopology.RoutingKeys.Confirmed,
            RabbitTopology.RoutingKeys.Rejected,
        };

        Assert.Equal(keys.Length, keys.Distinct().Count());
    }

    [Fact]
    public void No_headers_means_no_attempts()
        => Assert.Equal(0, RabbitHeaders.ReadAttempt(null));

    [Fact]
    public void The_application_attempt_header_is_read()
        => Assert.Equal(2, RabbitHeaders.ReadAttempt(new Dictionary<string, object?> { [RabbitHeaders.Attempt] = 2L }));

    [Theory]
    [InlineData(3L, 3)]
    [InlineData(4, 4)]
    [InlineData("5", 5)]
    public void Attempt_headers_are_coerced_from_whatever_the_sender_chose(object raw, int expected)
    {
        var headers = new Dictionary<string, object?> { [RabbitHeaders.Attempt] = raw };

        Assert.Equal(expected, RabbitHeaders.ReadAttempt(headers));
    }

    [Fact]
    public void The_broker_dead_letter_count_is_read_from_x_death()
    {
        var headers = new Dictionary<string, object?>
        {
            [RabbitHeaders.Attempt] = 1L,
            [RabbitHeaders.DeadLetterHistory] = new List<object>
            {
                new Dictionary<string, object> { ["count"] = 2L, ["reason"] = "rejected" },
            },
        };

        // The broker's history wins over our own header.
        Assert.Equal(2, RabbitHeaders.ReadAttempt(headers));
    }

    [Fact]
    public void Dead_letter_counts_are_summed_across_records()
    {
        var history = new List<object>
        {
            new Dictionary<string, object> { ["count"] = 2L },
            new Dictionary<string, object> { ["count"] = 3L },
        };

        Assert.Equal(5, RabbitHeaders.CountDeadLetters(history));
    }

    [Fact]
    public void A_malformed_x_death_header_is_ignored()
    {
        var headers = new Dictionary<string, object?>
        {
            [RabbitHeaders.Attempt] = 4L,
            [RabbitHeaders.DeadLetterHistory] = "not-a-list",
        };

        Assert.Equal(4, RabbitHeaders.ReadAttempt(headers));
        Assert.Equal(0, RabbitHeaders.CountDeadLetters("not-a-list"));
    }

    [Fact]
    public void The_connection_factory_enables_automatic_recovery()
    {
        var factory = new RabbitMqOptions().ToConnectionFactory();

        Assert.True(factory.AutomaticRecoveryEnabled);
        Assert.True(factory.TopologyRecoveryEnabled);
    }
}
