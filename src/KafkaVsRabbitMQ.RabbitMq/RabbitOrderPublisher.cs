using RabbitMQ.Client;
using KafkaVsRabbitMQ.Contracts;

namespace KafkaVsRabbitMQ.RabbitMq;

/// <summary>Where a message was published from the client's point of view.</summary>
public sealed record RabbitPublishReceipt(string Exchange, string RoutingKey);

/// <summary>
/// Publishing to RabbitMQ.
///
/// Compare this with <c>KafkaOrderProducer</c>. Two extra ideas show up immediately:
///
/// * a message goes to an **exchange** with a **routing key**, never to a queue. The
///   broker decides which queues get a copy;
/// * the client **declares the topology** — exchanges, queues, bindings, and the
///   queue arguments that implement retry and dead-lettering. In Kafka there is
///   nothing to declare except the topic.
///
/// This class owns its own channel, and the consumer owns a different one: the client
/// documentation requires that a channel is not shared between concurrently
/// publishing threads, and consuming is not thread-safe either.
/// </summary>
public sealed class RabbitOrderPublisher : IAsyncDisposable
{
    private readonly IConnection _connection;
    private readonly IChannel _channel;
    private readonly RabbitMqOptions _options;

    private RabbitOrderPublisher(IConnection connection, IChannel channel, RabbitMqOptions options)
    {
        _connection = connection;
        _channel = channel;
        _options = options;
    }

    /// <summary>Connect, then declare the whole topology.</summary>
    public static async Task<RabbitOrderPublisher> CreateAsync(RabbitMqOptions options, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(options);

        var connection = await options.ToConnectionFactory().CreateConnectionAsync(cancellationToken).ConfigureAwait(false);
        var channel = await connection.CreateChannelAsync(cancellationToken: cancellationToken).ConfigureAwait(false);

        var publisher = new RabbitOrderPublisher(connection, channel, options);
        await DeclareTopologyAsync(channel, options, cancellationToken).ConfigureAwait(false);
        return publisher;
    }

    /// <summary>
    /// Declare every exchange, queue and binding. Idempotent: declaring something that
    /// already exists with the same shape is not an error, which is what makes it safe
    /// for both the publisher and the consumer to do it on startup.
    /// </summary>
    public static async Task DeclareTopologyAsync(IChannel channel, RabbitMqOptions options, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(channel);
        ArgumentNullException.ThrowIfNull(options);

        await channel.ExchangeDeclareAsync(RabbitTopology.Exchange, ExchangeType.Direct,
            durable: true, autoDelete: false, arguments: null, cancellationToken: cancellationToken).ConfigureAwait(false);

        await channel.ExchangeDeclareAsync(RabbitTopology.DeadLetterExchange, ExchangeType.Direct,
            durable: true, autoDelete: false, arguments: null, cancellationToken: cancellationToken).ConfigureAwait(false);

        foreach (var binding in RabbitTopology.Bindings(options.RetryDelayMs))
        {
            await channel.QueueDeclareAsync(binding.Queue,
                durable: true, exclusive: false, autoDelete: false,
                arguments: binding.Arguments?.ToDictionary(kv => kv.Key, kv => kv.Value),
                cancellationToken: cancellationToken).ConfigureAwait(false);

            await channel.QueueBindAsync(binding.Queue, binding.Exchange, binding.RoutingKey,
                arguments: null, cancellationToken: cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>Publish an order to the pipeline.</summary>
    public async Task<RabbitPublishReceipt> PublishOrderAsync(OrderPlaced order, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(order);

        var properties = new BasicProperties
        {
            ContentType = "application/json",
            // Persistent: survive a broker restart. RabbitMQ's equivalent of a
            // replicated Kafka topic — and, like it, only as good as the queue's own
            // durability setting.
            DeliveryMode = DeliveryModes.Persistent,
            MessageId = order.OrderId.ToString(),
            Timestamp = new AmqpTimestamp(DateTimeOffset.UtcNow.ToUnixTimeSeconds()),
            Headers = new Dictionary<string, object?>
            {
                [RabbitHeaders.Attempt] = 0,
                [RabbitHeaders.EventType] = nameof(OrderPlaced),
            },
        };

        await _channel.BasicPublishAsync(
            RabbitTopology.Exchange,
            RabbitTopology.RoutingKeys.Placed,
            mandatory: false,
            basicProperties: properties,
            body: JsonDefaults.Serialize(order),
            cancellationToken: cancellationToken).ConfigureAwait(false);

        return new RabbitPublishReceipt(RabbitTopology.Exchange, RabbitTopology.RoutingKeys.Placed);
    }

    /// <summary>Publish any event to an exchange with a routing key.</summary>
    public async Task<RabbitPublishReceipt> PublishAsync<T>(
        T value,
        string exchange,
        string routingKey,
        int attempt = 0,
        string? reason = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(value);

        return await PublishRawAsync(JsonDefaults.Serialize(value), exchange, routingKey, attempt, reason, cancellationToken)
            .ConfigureAwait(false);
    }

    /// <summary>
    /// Publish raw bytes. Used for a message that could not be deserialized: there is
    /// no object to re-serialize, but the original payload should still be preserved
    /// rather than dropped.
    /// </summary>
    public async Task<RabbitPublishReceipt> PublishRawAsync(
        ReadOnlyMemory<byte> body,
        string exchange,
        string routingKey,
        int attempt = 0,
        string? reason = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(exchange);

        var headers = new Dictionary<string, object?>
        {
            [RabbitHeaders.Attempt] = attempt,
        };

        if (!string.IsNullOrWhiteSpace(reason))
        {
            headers[RabbitHeaders.Reason] = reason;
        }

        var properties = new BasicProperties
        {
            ContentType = "application/json",
            DeliveryMode = DeliveryModes.Persistent,
            Headers = headers,
        };

        await _channel.BasicPublishAsync(exchange, routingKey, mandatory: false,
            basicProperties: properties, body: body, cancellationToken: cancellationToken).ConfigureAwait(false);

        return new RabbitPublishReceipt(exchange, routingKey);
    }

    /// <summary>Options this publisher was built with.</summary>
    public RabbitMqOptions Options => _options;

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        await _channel.DisposeAsync().ConfigureAwait(false);
        await _connection.DisposeAsync().ConfigureAwait(false);
    }
}
