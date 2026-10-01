using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using KafkaVsRabbitMQ.Contracts;

namespace KafkaVsRabbitMQ.RabbitMq;

/// <summary>What the consumer did with one message, for logging and for the benchmark.</summary>
public sealed record RabbitConsumeOutcome(string Event, string Detail);

/// <summary>
/// The RabbitMQ consumer.
///
/// The contrast with Kafka is concentrated in one line: a transient failure is
/// handled by
///
/// <code>await channel.BasicNackAsync(deliveryTag, multiple: false, requeue: false);</code>
///
/// and that is *all*. The nack dead-letters the message to the retry queue, whose
/// only feature is a TTL; when the TTL expires RabbitMQ dead-letters it back to the
/// main exchange. The broker performs the delay while no consumer is involved.
/// Kafka's consumer has to hold the record and pause its own partition.
///
/// What you give up for that convenience: the delay is a property of the queue, so it
/// is the same for every message in it. Per-attempt exponential backoff needs one
/// retry queue per delay (5s, 30s, 5m) and a routing rule that picks between them.
/// </summary>
public sealed class RabbitOrderConsumer
{
    private readonly RabbitMqOptions _options;
    private readonly RabbitOrderPublisher _publisher;
    private readonly Action<RabbitConsumeOutcome>? _report;

    public RabbitOrderConsumer(RabbitMqOptions options, RabbitOrderPublisher publisher, Action<RabbitConsumeOutcome>? report = null)
    {
        _options = options;
        _publisher = publisher;
        _report = report;
    }

    /// <summary>Consume the pipeline queue until cancelled.</summary>
    public async Task RunAsync(CancellationToken cancellationToken)
    {
        var connection = await _options.ToConnectionFactory().CreateConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using (connection.ConfigureAwait(false))
        {
            // A dedicated channel for consuming. The publisher has its own.
            var channel = await connection.CreateChannelAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
            await using (channel.ConfigureAwait(false))
            {
                await RabbitOrderPublisher.DeclareTopologyAsync(channel, _options, cancellationToken).ConfigureAwait(false);

                // One unacknowledged message at a time: this is what makes a worker's
                // memory bounded, and it is also what lets two workers share the queue
                // fairly. RabbitMQ pushes; this asks it to slow down.
                await channel.BasicQosAsync(prefetchSize: 0, prefetchCount: _options.PrefetchCount, global: false,
                    cancellationToken: cancellationToken).ConfigureAwait(false);

                var consumer = new AsyncEventingBasicConsumer(channel);
                consumer.ReceivedAsync += (_, args) => HandleAsync(channel, args);

                await channel.BasicConsumeAsync(
                    queue: RabbitTopology.PlacedQueue,
                    autoAck: false,
                    consumer: consumer,
                    cancellationToken: cancellationToken).ConfigureAwait(false);

                try
                {
                    await Task.Delay(Timeout.Infinite, cancellationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    // Shutting down.
                }
            }
        }
    }

    private async Task HandleAsync(IChannel channel, BasicDeliverEventArgs args)
    {
        // The broker maintains x-death for us across retry-queue round trips.
        var attempt = RabbitHeaders.ReadAttempt(args.BasicProperties.Headers);

        OrderPlaced order;
        try
        {
            order = JsonDefaults.Deserialize<OrderPlaced>(args.Body.Span);
        }
        catch (Exception ex)
        {
            // Poison message: keep the bytes, park them, and do not retry forever.
            await _publisher.PublishRawAsync(args.Body, RabbitTopology.DeadLetterExchange,
                RabbitTopology.RoutingKeys.DeadLettered, attempt, $"malformed payload: {ex.Message}").ConfigureAwait(false);
            await channel.BasicAckAsync(args.DeliveryTag, multiple: false).ConfigureAwait(false);
            Report("dead-letter", $"malformed payload: {ex.Message}");
            return;
        }

        var (outcome, reason) = OrderSimulation.Decide(order, attempt);

        switch (outcome)
        {
            case SimulationOutcome.Succeed:
                await _publisher.PublishAsync(
                    new OrderConfirmed(order.OrderId, order.CustomerId, order.TotalAmount, DateTimeOffset.UtcNow),
                    RabbitTopology.Exchange, RabbitTopology.RoutingKeys.Confirmed, attempt).ConfigureAwait(false);
                await channel.BasicAckAsync(args.DeliveryTag, multiple: false).ConfigureAwait(false);
                Report("confirmed", $"{order.OrderId} (attempt {attempt})");
                break;

            case SimulationOutcome.PermanentFailure:
                await _publisher.PublishAsync(
                    new OrderRejected(order.OrderId, order.CustomerId, reason ?? "rejected", DateTimeOffset.UtcNow),
                    RabbitTopology.Exchange, RabbitTopology.RoutingKeys.Rejected, attempt, reason).ConfigureAwait(false);
                await channel.BasicAckAsync(args.DeliveryTag, multiple: false).ConfigureAwait(false);
                Report("rejected", $"{order.OrderId}: {reason}");
                break;

            case SimulationOutcome.TransientFailure:
                var failureCount = attempt + 1;
                if (failureCount < _options.MaxAttempts)
                {
                    // requeue: false → the queue's dead-letter routing sends it to the
                    // retry queue, where the TTL does the waiting.
                    await channel.BasicNackAsync(args.DeliveryTag, multiple: false, requeue: false).ConfigureAwait(false);
                    Report("retry", $"{order.OrderId} back in {_options.RetryDelayMs / 1000.0:0.#}s (attempt {failureCount}): {reason}");
                }
                else
                {
                    await _publisher.PublishRawAsync(args.Body, RabbitTopology.DeadLetterExchange,
                        RabbitTopology.RoutingKeys.DeadLettered, failureCount, reason).ConfigureAwait(false);
                    await channel.BasicAckAsync(args.DeliveryTag, multiple: false).ConfigureAwait(false);
                    Report("dead-letter", $"{order.OrderId} after {failureCount} attempts: {reason}");
                }

                break;
        }
    }

    private void Report(string @event, string detail) => _report?.Invoke(new RabbitConsumeOutcome(@event, detail));
}
