using Confluent.Kafka;
using KafkaVsRabbitMQ.Kafka;

namespace KafkaVsRabbitMQ.Tests;

public sealed class KafkaTransportTests
{
    [Fact]
    public void Topic_names_are_unique()
    {
        var names = KafkaTopology.All.Select(t => t.Name).ToList();
        Assert.Equal(names.Count, names.Distinct().Count());
    }

    [Fact]
    public void Every_topic_has_at_least_one_partition()
        => Assert.All(KafkaTopology.All, topic => Assert.True(topic.Partitions >= 1));

    [Fact]
    public void Partition_keys_are_normalised()
        => Assert.Equal("customer-1", KafkaTopology.PartitionKeyFor("  Customer-1 "));

    [Fact]
    public void Headers_default_to_attempt_zero()
        => Assert.Equal(0, new Headers().ReadAttempt());

    [Fact]
    public void Attempt_round_trips_through_headers()
        => Assert.Equal(3, KafkaHeaders.Build(3).ReadAttempt());

    [Fact]
    public void NotBefore_round_trips_with_millisecond_precision()
    {
        var due = DateTimeOffset.FromUnixTimeMilliseconds(1_900_000_000_000);

        Assert.Equal(due, KafkaHeaders.Build(1, due).ReadNotBefore());
        Assert.Null(KafkaHeaders.Build(1).ReadNotBefore());
    }

    [Fact]
    public void The_producer_waits_for_all_replicas_and_is_idempotent()
    {
        var config = new KafkaOptions().ToProducerConfig();

        Assert.Equal(Acks.All, config.Acks);
        Assert.True(config.EnableIdempotence);
    }

    [Fact]
    public void The_consumer_commits_by_hand()
    {
        var config = new KafkaOptions().ToConsumerConfig();

        Assert.False(config.EnableAutoCommit);
        Assert.Equal(AutoOffsetReset.Earliest, config.AutoOffsetReset);
    }
}
