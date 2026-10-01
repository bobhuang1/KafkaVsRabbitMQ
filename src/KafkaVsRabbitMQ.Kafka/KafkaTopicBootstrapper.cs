using Confluent.Kafka;
using Confluent.Kafka.Admin;

namespace KafkaVsRabbitMQ.Kafka;

/// <summary>
/// Creates the sample's topics, because Kafka will not invent one for you
/// (auto-creation is usually disabled in production, and here you want explicit
/// partition counts anyway).
///
/// This is the first structural difference from RabbitMQ: there, declaring the
/// topology is something the *client* does as part of declaring the queues it needs.
/// Here it is a separate admin operation, and producing to a topic that does not
/// exist is simply an error.
/// </summary>
public static class KafkaTopicBootstrapper
{
    /// <summary>Create every topic in <see cref="KafkaTopology.All"/> that does not already exist.</summary>
    public static async Task EnsureTopicsAsync(KafkaOptions options, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(options);

        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = options.BootstrapServers }).Build();

        var specifications = KafkaTopology.All
            .Select(topic => new TopicSpecification
            {
                Name = topic.Name,
                NumPartitions = topic.Partitions,
                ReplicationFactor = 1,
            })
            .ToList();

        try
        {
            await admin.CreateTopicsAsync(specifications).WaitAsync(cancellationToken).ConfigureAwait(false);
            Console.WriteLine($"  kafka: created {specifications.Count} topic(s)");
        }
        catch (CreateTopicsException ex)
        {
            var unexpected = ex.Results.Where(r => r.Error.Code != ErrorCode.TopicAlreadyExists && r.Error.Code != ErrorCode.NoError).ToList();
            foreach (var report in unexpected)
            {
                Console.WriteLine($"  kafka: topic {report.Topic} failed: {report.Error.Reason}");
            }

            if (unexpected.Count == 0)
            {
                Console.WriteLine("  kafka: topics already existed");
            }
        }
    }
}
