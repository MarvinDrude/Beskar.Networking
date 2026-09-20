using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

namespace Beskar.Mqtt.Common.Serialization;

/// <summary>
/// A JSON-based payload decoder utilizing <see cref="System.Text.Json.JsonSerializer"/>.
/// Supports custom <see cref="JsonSerializerOptions"/> or source-generated <see cref="JsonTypeInfo{T}"/>.
/// </summary>
/// <typeparam name="T">The decoded target type.</typeparam>
public sealed class JsonPayloadDecoder<T> : IMqttPayloadDecoder<T>
{
   private readonly JsonSerializerOptions? _options;
   private readonly JsonTypeInfo<T>? _typeInfo;

   public JsonPayloadDecoder(JsonSerializerOptions? options = null)
   {
      _options = options;
   }

   public JsonPayloadDecoder(JsonTypeInfo<T> typeInfo)
   {
      _typeInfo = typeInfo ?? throw new ArgumentNullException(nameof(typeInfo));
   }

   public T Decode(ReadOnlyMemory<byte> payload)
   {
      if (_typeInfo is not null)
      {
         return JsonSerializer.Deserialize(payload.Span, _typeInfo)!;
      }

      return JsonSerializer.Deserialize<T>(payload.Span, _options)!;
   }
}
