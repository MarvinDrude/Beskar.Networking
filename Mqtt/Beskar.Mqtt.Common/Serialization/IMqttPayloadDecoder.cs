namespace Beskar.Mqtt.Common.Serialization;

/// <summary>
/// Defines a contract for decoding MQTT payload bytes into a strongly-typed instance.
/// </summary>
/// <typeparam name="T">The decoded target type.</typeparam>
public interface IMqttPayloadDecoder<out T>
{
   /// <summary>
   /// Decodes the given raw payload bytes into an instance of <typeparamref name="T"/>.
   /// </summary>
   /// <param name="payload">The raw MQTT payload memory.</param>
   /// <returns>The decoded instance of <typeparamref name="T"/>.</returns>
   public T Decode(ReadOnlyMemory<byte> payload);
}
