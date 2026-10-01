using System.Diagnostics;
using System.Globalization;
using KafkaVsRabbitMQ.Contracts;
using KafkaVsRabbitMQ.Kafka;
using KafkaVsRabbitMQ.RabbitMq;

namespace KafkaVsRabbitMQ.App;

/// <summary>
/// The comparison harness. Same events, same business rule, same workload — two
/// brokers. Everything the two implementations do *differently* is in the two
/// transport projects; everything here is just plumbing.
/// </summary>
public static class Commands
{
    /// <summary>Dispatch a command line.</summary>
    public static async Task<int> RunAsync(string[] args)
    {
        if (args.Length == 0 || args[0] is "help" or "-h" or "--help")
        {
            PrintHelp();
            return 0;
        }

        var transport = args[0].ToLowerInvariant();
        var action = args.Length > 1 ? args[1].ToLowerInvariant() : "help";
        var count = ParseCount(args);

        using var shutdown = new CancellationTokenSource();
        Console.CancelKeyPress += (_, eventArgs) =>
        {
            eventArgs.Cancel = true;
            Console.WriteLine();
            Console.WriteLine("  stopping…");
            shutdown.Cancel();
        };

        var kafkaOptions = new KafkaOptions();
        var rabbitOptions = new RabbitMqOptions();

        try
        {
            return (transport, action) switch
            {
                ("kafka", "produce") => await ProduceKafkaAsync(kafkaOptions, count, shutdown.Token).ConfigureAwait(false),
                ("kafka", "consume") => await ConsumeKafkaAsync(kafkaOptions, shutdown.Token).ConfigureAwait(false),
                ("rabbit", "produce") => await ProduceRabbitAsync(rabbitOptions, count, shutdown.Token).ConfigureAwait(false),
                ("rabbit", "consume") => await ConsumeRabbitAsync(rabbitOptions, shutdown.Token).ConfigureAwait(false),
                ("benchmark", _) => await BenchmarkAsync(kafkaOptions, rabbitOptions, count, shutdown.Token).ConfigureAwait(false),
                _ => Help(),
            };
        }
        catch (OperationCanceledException)
        {
            return 0;
        }
    }

    private static async Task<int> ProduceKafkaAsync(KafkaOptions options, int count, CancellationToken cancellationToken)
    {
        Console.WriteLine($"kafka → {options.BootstrapServers}: ensuring topics");
        await KafkaTopicBootstrapper.EnsureTopicsAsync(options, cancellationToken).ConfigureAwait(false);

        using var producer = new KafkaOrderProducer(options);
        var orders = DemoOrders.Create(count);
        var stopwatch = Stopwatch.StartNew();

        foreach (var order in orders)
        {
            await producer.PublishAsync(order, cancellationToken: cancellationToken).ConfigureAwait(false);
        }

        stopwatch.Stop();
        producer.Flush(TimeSpan.FromSeconds(10));

        Console.WriteLine($"kafka: published {orders.Count} orders in {stopwatch.Elapsed.TotalMilliseconds:0} ms " +
                          $"({Rate(orders.Count, stopwatch.Elapsed):0} msg/s)");
        return 0;
    }

    private static async Task<int> ProduceRabbitAsync(RabbitMqOptions options, int count, CancellationToken cancellationToken)
    {
        Console.WriteLine($"rabbitmq → {options.HostName}:{options.Port}: declaring topology");
        await using var publisher = await RabbitOrderPublisher.CreateAsync(options, cancellationToken).ConfigureAwait(false);

        var orders = DemoOrders.Create(count);
        var stopwatch = Stopwatch.StartNew();

        foreach (var order in orders)
        {
            await publisher.PublishOrderAsync(order, cancellationToken).ConfigureAwait(false);
        }

        stopwatch.Stop();

        Console.WriteLine($"rabbitmq: published {orders.Count} orders in {stopwatch.Elapsed.TotalMilliseconds:0} ms " +
                          $"({Rate(orders.Count, stopwatch.Elapsed):0} msg/s)");
        return 0;
    }

    private static async Task<int> ConsumeKafkaAsync(KafkaOptions options, CancellationToken cancellationToken)
    {
        await KafkaTopicBootstrapper.EnsureTopicsAsync(options, cancellationToken).ConfigureAwait(false);

        using var producer = new KafkaOrderProducer(options);
        var consumer = new KafkaOrderConsumer(options, producer, outcome => Print("kafka   ", outcome.Event, outcome.Detail));

        Console.WriteLine($"kafka: consuming as group '{options.GroupId}' (Ctrl+C to stop)");
        await consumer.RunAsync(cancellationToken).ConfigureAwait(false);
        return 0;
    }

    private static async Task<int> ConsumeRabbitAsync(RabbitMqOptions options, CancellationToken cancellationToken)
    {
        await using var publisher = await RabbitOrderPublisher.CreateAsync(options, cancellationToken).ConfigureAwait(false);
        var consumer = new RabbitOrderConsumer(options, publisher, outcome => Print("rabbitmq", outcome.Event, outcome.Detail));

        Console.WriteLine($"rabbitmq: consuming {RabbitTopology.PlacedQueue} (Ctrl+C to stop)");
        await consumer.RunAsync(cancellationToken).ConfigureAwait(false);
        return 0;
    }

    /// <summary>
    /// Publish the same workload to both brokers and time it. Read the numbers as
    /// indicative only: the client settings differ (Kafka batches, RabbitMQ confirms
    /// nothing here), and this measures the client library as much as the broker.
    /// </summary>
    private static async Task<int> BenchmarkAsync(KafkaOptions kafkaOptions, RabbitMqOptions rabbitOptions, int count, CancellationToken cancellationToken)
    {
        Console.WriteLine($"Benchmark: {count} messages, identical payloads, identical business rule.");
        Console.WriteLine();

        await KafkaTopicBootstrapper.EnsureTopicsAsync(kafkaOptions, cancellationToken).ConfigureAwait(false);

        var orders = DemoOrders.Create(count);

        using (var kafkaProducer = new KafkaOrderProducer(kafkaOptions))
        {
            var stopwatch = Stopwatch.StartNew();
            foreach (var order in orders)
            {
                await kafkaProducer.PublishAsync(order, cancellationToken: cancellationToken).ConfigureAwait(false);
            }

            kafkaProducer.Flush(TimeSpan.FromSeconds(30));
            stopwatch.Stop();
            Console.WriteLine($"  kafka    publish: {stopwatch.ElapsedMilliseconds,6:0} ms  {Rate(count, stopwatch.Elapsed),8:0} msg/s");
        }

        await using (var rabbitPublisher = await RabbitOrderPublisher.CreateAsync(rabbitOptions, cancellationToken).ConfigureAwait(false))
        {
            var stopwatch = Stopwatch.StartNew();
            foreach (var order in orders)
            {
                await rabbitPublisher.PublishOrderAsync(order, cancellationToken).ConfigureAwait(false);
            }

            stopwatch.Stop();
            Console.WriteLine($"  rabbitmq publish: {stopwatch.ElapsedMilliseconds,6:0} ms  {Rate(count, stopwatch.Elapsed),8:0} msg/s");
        }

        Console.WriteLine();
        Console.WriteLine("  Delivery semantics are the real difference — see README.md.");
        return 0;
    }

    private static double Rate(int count, TimeSpan elapsed)
        => elapsed.TotalSeconds <= 0 ? 0 : count / elapsed.TotalSeconds;

    private static void Print(string source, string @event, string detail)
        => Console.WriteLine($"  {DateTimeOffset.Now:HH:mm:ss.fff} {source} {(@event + ":").PadRight(12)} {detail}");

    private static int ParseCount(string[] args)
    {
        for (var i = 0; i < args.Length - 1; i++)
        {
            if (args[i] is "--count" or "-n"
                && int.TryParse(args[i + 1], NumberStyles.Integer, CultureInfo.InvariantCulture, out var value))
            {
                return Math.Clamp(value, 1, 500_000);
            }
        }

        return 50;
    }

    private static int Help()
    {
        PrintHelp();
        return 1;
    }

    private static void PrintHelp() => Console.WriteLine("""
        KafkaVsRabbitMQ — the same pipeline on two brokers.

        Usage:
          dotnet run --project src/KafkaVsRabbitMQ.App -- <command> [options]

        Commands:
          kafka consume                 consume the order pipeline on Kafka (Ctrl+C to stop)
          kafka produce [--count N]     publish N demo orders to Kafka
          rabbit consume                consume the order pipeline on RabbitMQ (Ctrl+C to stop)
          rabbit produce [--count N]    publish N demo orders to RabbitMQ
          benchmark [--count N]         publish the same workload to both and time it
          help                          this text

        Start the brokers first (see README.md):
          docker compose up -d

        Suggested order: run a consumer in one terminal, a producer in another, and
        watch where retries and rejections end up. The first two demo orders always
        fail once and fail permanently, so every branch is reachable in one run.
        """);
}
