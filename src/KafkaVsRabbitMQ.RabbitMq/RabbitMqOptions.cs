using RabbitMQ.Client;

namespace KafkaVsRabbitMQ.RabbitMq;

/// <summary>Connection and retry settings for the RabbitMQ side.</summary>
public sealed class RabbitMqOptions
{
    /// <summary>Broker host.</summary>
    public string HostName { get; set; } = "localhost";

    /// <summary>AMQP port.</summary>
    public int Port { get; set; } = 5672;

    /// <summary>User name.</summary>
    public string UserName { get; set; } = "guest";

    /// <summary>Password.</summary>
    public string Password { get; set; } = "guest";

    /// <summary>Virtual host.</summary>
    public string VirtualHost { get; set; } = "/";

    /// <summary>Total deliveries before a message is dead-lettered.</summary>
    public int MaxAttempts { get; set; } = 4;

    /// <summary>
    /// How long a message waits in the retry queue. RabbitMQ's TTL queue gives a
    /// *fixed* delay; see the README for the multi-queue pattern that recovers
    /// per-attempt backoff.
    /// </summary>
    public int RetryDelayMs { get; set; } = 5_000;

    /// <summary>How many unacknowledged messages this consumer will hold at once.</summary>
    public ushort PrefetchCount { get; set; } = 1;

    /// <summary>Build the client factory.</summary>
    public ConnectionFactory ToConnectionFactory() => new()
    {
        HostName = HostName,
        Port = Port,
        UserName = UserName,
        Password = Password,
        VirtualHost = VirtualHost,
        ClientProvidedName = "rabbitmq-compare",
        // Reconnect and restore topology after a network blip. Note it does *not*
        // cover the initial connection, which the caller must retry.
        AutomaticRecoveryEnabled = true,
        TopologyRecoveryEnabled = true,
    };
}
