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

      BinaryPrimitives.WriteUInt16LittleEndian(buffer, (ushort)totalSize);
      utf8Bytes.CopyTo(buffer[2..]);

      _writer.Advance(totalSize);
   }

   public void Add(ReadOnlySpan<char> str)
   {
      var maxByteCount = Encoding.UTF8.GetByteCount(str);
      var totalReservation = 2 + maxByteCount;

      var destination = _writer.GetSpan(totalReservation);
      var bytesWritten = Encoding.UTF8.GetBytes(str, destination[2..]);

      BinaryPrimitives.WriteUInt16BigEndian(destination, (ushort)bytesWritten);
      _writer.Advance(2 + bytesWritten);
   }

   public void Add(string value)
   {
      Add(value.AsSpan());
   }

   private void WriteEmpty()
   {
      var destination = _writer.GetSpan(2);
      BinaryPrimitives.WriteUInt16BigEndian(destination, 0);

      _writer.Advance(2);
   }
}
