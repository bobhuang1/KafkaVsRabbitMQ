using System.Text.Json;
using System.Text.Json.Serialization;

namespace KafkaVsRabbitMQ.Contracts;

/// <summary>
/// One JSON configuration for both transports. Kafka stores raw bytes and RabbitMQ
/// stores raw bytes, so the encoding is entirely the application's business — and a
/// comparison is only fair if both sides serialise identically.
/// </summary>
public static class JsonDefaults
{
    /// <summary>Web defaults: camelCase names, case-insensitive reading, numbers as numbers.</summary>
    public static readonly JsonSerializerOptions Options = new(JsonSerializerDefaults.Web)
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        WriteIndented = false,
    };

    /// <summary>Serialize an event to UTF-8 JSON.</summary>
    public static byte[] Serialize<T>(T value) => JsonSerializer.SerializeToUtf8Bytes(value, Options);

    /// <summary>Deserialize an event from UTF-8 JSON.</summary>
    public static T Deserialize<T>(ReadOnlySpan<byte> utf8Json)
        => JsonSerializer.Deserialize<T>(utf8Json, Options)
           ?? throw new InvalidOperationException($"Payload deserialized to null; expected {typeof(T).Name}.");
}
