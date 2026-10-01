using Confluent.Kafka;
using KafkaVsRabbitMQ.Contracts;

namespace KafkaVsRabbitMQ.Kafka;

/// <summary>What the consumer did with one record, for logging and for the benchmark.</summary>
public sealed record ConsumeOutcome(string Event, string Detail);

/// <summary>
/// The Kafka consumer.
///
/// Two things are worth watching here, because RabbitMQ does neither of them the
/// same way:
///
/// 1. **Delayed retry is hand-rolled.** Kafka has no delay. A transient failure is
///    written to a retry topic with a <c>not-before</c> header, and this consumer
///    enforces the delay itself: when the head of a partition is not due, it seeks
///    back onto the record and pauses the partition. Compare that with RabbitMQ,
///    where a queue's <c>x-message-ttl</c> plus a dead-letter exchange does the same
///    job inside the broker.
///
/// 2. **Offsets are committed by hand, per record.** The commit is the
///    acknowledgement, and it is a cursor rather than a per-message flag.
/// </summary>
public sealed class KafkaOrderConsumer
{
    private readonly KafkaOptions _options;
    private readonly KafkaOrderProducer _producer;
    private readonly Action<ConsumeOutcome>? _report;

    public KafkaOrderConsumer(KafkaOptions options, KafkaOrderProducer producer, Action<ConsumeOutcome>? report = null)
    {
        _options = options;
        _producer = producer;
        _report = report;
    }

    /// <summary>Consume until cancelled.</summary>
    public async Task RunAsync(CancellationToken cancellationToken)
    {
        using var consumer = new ConsumerBuilder<string, byte[]>(_options.ToConsumerConfig()).Build();

        // One subscription, two topics: the live pipeline and the retry lane.
        consumer.Subscribe([KafkaTopology.Placed, KafkaTopology.Retry]);

        var parkedUntil = new Dictionary<TopicPartition, DateTimeOffset>();

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                ResumeDuePartitions(consumer, parkedUntil);

                ConsumeResult<string, byte[]>? record;
                try
                {
                    record = consumer.Consume(TimeSpan.FromMilliseconds(_options.PollTimeoutMs));
                }
                catch (ConsumeException ex)
                {
                    Report("kafka", $"consume error: {ex.Error.Reason}");
                    continue;
                }

                if (record?.Message is null)
                {
                    continue;
                }

                var notBefore = record.Message.Headers.ReadNotBefore();
                if (notBefore is not null && DateTimeOffset.UtcNow < notBefore)
                {
                    // Hold the record at the head of its partition until it is due.
                    consumer.Seek(record.TopicPartitionOffset);
                    consumer.Pause([record.TopicPartition]);
                    parkedUntil[record.TopicPartition] = notBefore.Value;
                    continue;
                }

                await HandleAsync(record, cancellationToken).ConfigureAwait(false);
                consumer.Commit(record);
            }
        }
        catch (OperationCanceledException)
        {
            // Shutting down.
        }
        finally
        {
            consumer.Close();
        }
    }

    private async Task HandleAsync(ConsumeResult<string, byte[]> record, CancellationToken cancellationToken)
    {
        var attempt = record.Message.Headers.ReadAttempt();

        OrderPlaced order;
        try
        {
            order = JsonDefaults.Deserialize<OrderPlaced>(record.Message.Value);
        }
        catch (Exception ex)
        {
            Report("kafka", $"poison record: {ex.Message}");
            return;
        }

        var (outcome, reason) = OrderSimulation.Decide(order, attempt);

        switch (outcome)
        {
            case SimulationOutcome.Succeed:
                await _producer.PublishAsync(
                    new OrderConfirmed(order.OrderId, order.CustomerId, order.TotalAmount, DateTimeOffset.UtcNow),
                    KafkaTopology.Confirmed, order.CustomerId, attempt, cancellationToken: cancellationToken).ConfigureAwait(false);
                Report("confirmed", $"{order.OrderId} (attempt {attempt})");
                break;

            case SimulationOutcome.PermanentFailure:
                await _producer.PublishAsync(
                    new OrderRejected(order.OrderId, order.CustomerId, reason ?? "rejected", DateTimeOffset.UtcNow),
                    KafkaTopology.Rejected, order.CustomerId, attempt, cancellationToken: cancellationToken).ConfigureAwait(false);
                Report("rejected", $"{order.OrderId}: {reason}");
                break;

            case SimulationOutcome.TransientFailure:
                var failureCount = attempt + 1;
                if (failureCount < _options.MaxAttempts)
                {
                    var delay = _options.RetryDelay * Math.Pow(2, failureCount - 1);
                    await _producer.PublishAsync(order, KafkaTopology.Retry, order.CustomerId,
                        failureCount, DateTimeOffset.UtcNow + delay, cancellationToken).ConfigureAwait(false);
                    Report("retry", $"{order.OrderId} in {delay.TotalSeconds:0}s (attempt {failureCount}): {reason}");
                }
                else
                {
                    await _producer.PublishAsync(order, KafkaTopology.DeadLetter, order.CustomerId,
                        failureCount, cancellationToken: cancellationToken).ConfigureAwait(false);
                    Report("dead-letter", $"{order.OrderId} after {failureCount} attempts: {reason}");
                }

                break;
        }
    }

    private static void ResumeDuePartitions(IConsumer<string, byte[]> consumer, Dictionary<TopicPartition, DateTimeOffset> parkedUntil)
    {
        if (parkedUntil.Count == 0)
        {
            return;
        }

        var now = DateTimeOffset.UtcNow;
        foreach (var (partition, due) in parkedUntil.ToArray())
        {
            if (now >= due)
            {
                consumer.Resume([partition]);
                parkedUntil.Remove(partition);
            }
        }
    }

    private void Report(string @event, string detail) => _report?.Invoke(new ConsumeOutcome(@event, detail));
}
