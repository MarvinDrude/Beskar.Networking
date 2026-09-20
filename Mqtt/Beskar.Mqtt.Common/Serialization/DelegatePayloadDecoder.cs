namespace Beskar.Mqtt.Common.Serialization;

/// <summary>
/// An adapter that implements <see cref="IMqttPayloadDecoder{T}"/> using a delegate function.
/// </summary>
/// <typeparam name="T">The decoded target type.</typeparam>
public sealed class DelegatePayloadDecoder<T>(Func<ReadOnlyMemory<byte>, T> decoder) : IMqttPayloadDecoder<T>
{
   private readonly Func<ReadOnlyMemory<byte>, T> _decoder = decoder ?? throw new ArgumentNullException(nameof(decoder));

   public T Decode(ReadOnlyMemory<byte> payload) => _decoder(payload);
}
