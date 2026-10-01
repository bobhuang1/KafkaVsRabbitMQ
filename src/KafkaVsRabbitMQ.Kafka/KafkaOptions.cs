using Confluent.Kafka;

namespace KafkaVsRabbitMQ.Kafka;

/// <summary>Connection and retry settings for the Kafka side.</summary>
public sealed class KafkaOptions
{
    /// <summary>Broker bootstrap list.</summary>
    public string BootstrapServers { get; set; } = "localhost:9092";

    /// <summary>Consumer group id.</summary>
    public string GroupId { get; set; } = "orders-compare-kafka";

    /// <summary>Client name shown in broker metrics.</summary>
    public string ClientId { get; set; } = "kafka-compare";

    /// <summary>How long a poll waits before looping.</summary>
    public int PollTimeoutMs { get; set; } = 500;

    /// <summary>Total deliveries before a message is dead-lettered.</summary>
    public int MaxAttempts { get; set; } = 4;

    /// <summary>Base delay for the exponential backoff between retries.</summary>
    public TimeSpan RetryDelay { get; set; } = TimeSpan.FromSeconds(2);

    /// <summary>
    /// Producer settings. <c>Acks.All</c> and idempotence are the same choices a real
    /// system would make: a confirmed write survives a broker failure, and the
    /// producer's own retries cannot duplicate a write.
    /// </summary>
    public ProducerConfig ToProducerConfig() => new()
    {
        BootstrapServers = BootstrapServers,
        ClientId = ClientId,
        Acks = Acks.All,
        EnableIdempotence = true,
        MessageTimeoutMs = 10_000,
        LingerMs = 5,
    };

    /// <summary>
    /// Consumer settings. Offsets are committed by hand, which is what gives
    /// at-least-once delivery.
    /// </summary>
    public ConsumerConfig ToConsumerConfig() => new()
    {
        BootstrapServers = BootstrapServers,
        ClientId = ClientId,
        GroupId = GroupId,
        AutoOffsetReset = AutoOffsetReset.Earliest,
        EnableAutoCommit = false,
        SessionTimeoutMs = 10_000,
    };
}
