using System.Text.Json;

namespace Beskar.Jobs.Common.Serialization;

/// <summary>
/// Default <see cref="System.Text.Json"/> implementation of <see cref="IJobPayloadSerializer"/>.
/// </summary>
public sealed class JsonJobPayloadSerializer(JsonSerializerOptions? options = null) : IJobPayloadSerializer
{
   private static readonly JsonSerializerOptions DefaultOptions = new()
   {
      PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
      PropertyNameCaseInsensitive = true
   };

   public static JsonJobPayloadSerializer Default { get; } = new();

   public JsonSerializerOptions Options { get; } = options ?? DefaultOptions;

   public byte[] Serialize<T>(T value)
   {
      if (value is null)
      {
         return [];
      }

      if (value is byte[] rawBytes)
      {
         return rawBytes;
      }

      return JsonSerializer.SerializeToUtf8Bytes(value, Options);
   }

   public T? Deserialize<T>(ReadOnlySpan<byte> payload)
   {
      if (payload.IsEmpty)
      {
         return default;
      }

      if (typeof(T) == typeof(byte[]))
      {
         return (T)(object)payload.ToArray();
      }

      return JsonSerializer.Deserialize<T>(payload, Options);
   }
}
