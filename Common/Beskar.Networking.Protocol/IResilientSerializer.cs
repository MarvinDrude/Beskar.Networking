using System.Buffers;
using Beskar.Networking.Protocol.Utilities;

namespace Beskar.Networking.Protocol;

/// <summary>
/// Defines a high-performance, zero-allocation serializer interface for encoding and decoding generic payloads or messages.
/// </summary>
public interface IResilientSerializer
{
   /// <summary>
   /// Serializes an object of type <typeparamref name="T"/> directly into an <see cref="IBufferWriter{Byte}"/>.
   /// </summary>
   void Serialize<T>(T value, IBufferWriter<byte> writer);

   /// <summary>
   /// Deserializes an object of type <typeparamref name="T"/> from a read-only sequence of bytes.
   /// </summary>
   T? Deserialize<T>(in ReadOnlySequence<byte> sequence);

   /// <summary>
   /// Attempts to serialize an object of type <typeparamref name="T"/> into a destination span.
   /// </summary>
   bool TrySerialize<T>(T value, Span<byte> destination, out int bytesWritten)
   {
      using var writer = new PooledBufferWriter(destination.Length);
      Serialize(value, writer);

      if (writer.WrittenCount > destination.Length)
      {
         bytesWritten = 0;
         return false;
      }

      writer.WrittenSpan.CopyTo(destination);
      bytesWritten = writer.WrittenCount;
      return true;
   }

   /// <summary>
   /// Deserializes an object of type <typeparamref name="T"/> from a byte sequence reader.
   /// </summary>
   T? Deserialize<T>(ref SequenceReader<byte> reader)
   {
      var seq = reader.UnreadSequence;
      var result = Deserialize<T>(in seq);
      reader.Advance(seq.Length);
      return result;
   }
}
