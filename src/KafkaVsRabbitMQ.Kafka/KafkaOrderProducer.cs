using Confluent.Kafka;
using KafkaVsRabbitMQ.Contracts;

namespace KafkaVsRabbitMQ.Kafka;

/// <summary>Where a record landed, as reported by the broker.</summary>
public sealed record PublishReceipt(string Topic, int Partition, long Offset);

/// <summary>
/// Publishing to Kafka, in full.
///
/// Note what is *not* here: no exchange, no queue, no binding, no topology
/// declaration. You name a topic and append. Placement is the partitioner's job
/// (hash of the key) and durability comes from <c>Acks.All</c> plus replication —
/// not from anything the producer asks for.
/// </summary>
public sealed class KafkaOrderProducer : IDisposable
{
    private readonly IProducer<string, byte[]> _producer;

    public KafkaOrderProducer(KafkaOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        _producer = new ProducerBuilder<string, byte[]>(options.ToProducerConfig()).Build();
    }

    /// <summary>Append one order, keyed by customer, and wait for the acknowledgement.</summary>
    public Task<PublishReceipt> PublishAsync(
        OrderPlaced order,
        int attempt = 0,
        DateTimeOffset? notBefore = null,
        string topic = KafkaTopology.Placed,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(order);

        // The key decides the partition, so every order from one customer is appended
        // to the same partition and stays in order relative to that customer.
        return PublishAsync(order, topic, KafkaTopology.PartitionKeyFor(order.CustomerId), attempt, notBefore, cancellationToken);
    }

    /// <summary>Append any event to any topic.</summary>
    public async Task<PublishReceipt> PublishAsync<T>(
        T value,
        string topic,
        string key,
        int attempt = 0,
        DateTimeOffset? notBefore = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        ArgumentNullException.ThrowIfNull(value);

        var record = new Message<string, byte[]>
        {
            Key = key,
            Value = JsonDefaults.Serialize(value),
            Headers = KafkaHeaders.Build(attempt, notBefore),
        };

        var result = await _producer.ProduceAsync(topic, record, cancellationToken).ConfigureAwait(false);
        return new PublishReceipt(result.Topic, result.Partition.Value, result.Offset.Value);
    }

    /// <summary>Flush anything still buffered. Producers batch, so a clean shutdown waits for it.</summary>
    public void Flush(TimeSpan timeout) => _producer.Flush(timeout);

    /// <inheritdoc />
    public void Dispose() => _producer.Dispose();
}
