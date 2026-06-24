using System.Buffers;
using System.Buffers.Binary;
using System.Text;

namespace Beskar.Mqtt.Common.Builders.Common;

public sealed class Utf8StringListBuilder(IBufferWriter<byte> writer)
{
   public int Count { get; private set; }
   public int ByteCount { get; private set; }

   public ReadOnlySpan<byte> WrittenSpan => _writer is ArrayBufferWriter<byte> buffer
      ? buffer.WrittenSpan
      : throw new InvalidOperationException("Backing writer does not support written span.");

   private readonly IBufferWriter<byte> _writer = writer;

   public Utf8StringListBuilder()
      : this(new ArrayBufferWriter<byte>())
   {
   }

   public Utf8StringListBuilder(int capacity)
      : this(new ArrayBufferWriter<byte>(capacity))
   {
   }

   public Utf8StringListBuilder Add(ReadOnlySpan<byte> utf8Bytes)
   {
      if (utf8Bytes.Length == 0)
      {
         WriteEmpty();
         return this;
      }

      var totalSize = 2 + utf8Bytes.Length;
      var buffer = _writer.GetSpan(totalSize);

      BinaryPrimitives.WriteUInt16LittleEndian(buffer, (ushort)utf8Bytes.Length);
      utf8Bytes.CopyTo(buffer[2..]);

      _writer.Advance(totalSize);
      Count++;
      ByteCount += totalSize;

      return this;
   }

   public Utf8StringListBuilder Add(ReadOnlySpan<char> str)
   {
      var maxByteCount = Encoding.UTF8.GetMaxByteCount(str.Length);
      var totalReservation = 2 + maxByteCount;

      var destination = _writer.GetSpan(totalReservation);
      var bytesWritten = Encoding.UTF8.GetBytes(str, destination[2..]);

      BinaryPrimitives.WriteUInt16LittleEndian(destination, (ushort)bytesWritten);
      _writer.Advance(2 + bytesWritten);

      Count++;
      ByteCount += 2 + bytesWritten;

      return this;
   }

   public Utf8StringListBuilder Add(string value)
   {
      Add(value.AsSpan());
      return this;
   }

   public void Clear()
   {
      if (_writer is ArrayBufferWriter<byte> buffer)
      {
         buffer.Clear();
         Count = 0;
      }
      else
      {
         throw new InvalidOperationException("Backing writer does not support clearing.");
      }
   }

   private void WriteEmpty()
   {
      var destination = _writer.GetSpan(2);
      BinaryPrimitives.WriteUInt16LittleEndian(destination, 0);

      _writer.Advance(2);
      Count++;
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
