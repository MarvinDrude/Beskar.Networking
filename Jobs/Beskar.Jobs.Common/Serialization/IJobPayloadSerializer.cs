namespace Beskar.Jobs.Common.Serialization;

/// <summary>
/// Pluggable serializer abstraction for encoding and decoding job arguments, results, and control payloads.
/// </summary>
public interface IJobPayloadSerializer
{
   /// <summary>
   /// Serializes a value of type <typeparamref name="T"/> into a UTF-8 or binary byte array.
   /// </summary>
   public byte[] Serialize<T>(T value);

   /// <summary>
   /// Deserializes a span of bytes into an instance of <typeparamref name="T"/>.
   /// </summary>
   public T? Deserialize<T>(ReadOnlySpan<byte> payload);
}
