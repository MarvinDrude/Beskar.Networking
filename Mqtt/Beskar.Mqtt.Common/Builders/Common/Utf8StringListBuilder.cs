using System.Buffers;
using System.Buffers.Binary;
using System.Text;

namespace Beskar.Mqtt.Common.Builders.Common;

public sealed class Utf8StringListBuilder(IBufferWriter<byte> writer)
{
   private readonly IBufferWriter<byte> _writer = writer;

   public Utf8StringListBuilder()
      : this(new ArrayBufferWriter<byte>())
   {
   }

   public Utf8StringListBuilder(int capacity)
      : this(new ArrayBufferWriter<byte>(capacity))
   {
   }

   public void Add(ReadOnlySpan<byte> utf8Bytes)
   {
      if (utf8Bytes.Length == 0)
      {
         WriteEmpty();
      }

      var totalSize = 2 + utf8Bytes.Length;
      var buffer = _writer.GetSpan(totalSize);

      BinaryPrimitives.WriteUInt16LittleEndian(buffer, (ushort)utf8Bytes.Length);
      utf8Bytes.CopyTo(buffer[2..]);

      _writer.Advance(totalSize);
   }

   public void Add(ReadOnlySpan<char> str)
   {
      var maxByteCount = Encoding.UTF8.GetMaxByteCount(str.Length);
      var totalReservation = 2 + maxByteCount;

      var destination = _writer.GetSpan(totalReservation);
      var bytesWritten = Encoding.UTF8.GetBytes(str, destination[2..]);

      BinaryPrimitives.WriteUInt16LittleEndian(destination, (ushort)bytesWritten);
      _writer.Advance(2 + bytesWritten);
   }

   public void Add(string value)
   {
      Add(value.AsSpan());
   }

   private void WriteEmpty()
   {
      var destination = _writer.GetSpan(2);
      BinaryPrimitives.WriteUInt16LittleEndian(destination, 0);

      _writer.Advance(2);
   }

   public Utf8StringListEnumerator GetEnumerator() => _writer is ArrayBufferWriter<byte> buffer
      ? new Utf8StringListEnumerator(buffer.WrittenSpan)
      : throw new InvalidOperationException("Backing writer does not support automatic enumerator.");

   public ref struct Utf8StringListEnumerator(ReadOnlySpan<byte> buffer)
   {
      private ReadOnlySpan<byte> _remaining = buffer;
      private ReadOnlySpan<byte> _current = default;

      public readonly ReadOnlySpan<byte> Current => _current;

      public bool MoveNext()
      {
         if (_remaining.IsEmpty)
         {
            return false;
         }

         var payloadLength = BinaryPrimitives.ReadUInt16LittleEndian(_remaining);

         _current = payloadLength == 0
            ? ReadOnlySpan<byte>.Empty
            : _remaining.Slice(2, payloadLength);

         _remaining = _remaining[(2 + payloadLength)..];

         return true;
      }

      public readonly Utf8StringListEnumerator GetEnumerator() => this;
   }
}
