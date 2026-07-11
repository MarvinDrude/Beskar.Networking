using System.Buffers;
using System.Buffers.Binary;
using System.Text;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Models;

namespace Beskar.Mqtt.Common.Builders.Common;

public sealed class UserPropertyListBuilder(IBufferWriter<byte> writer)
{
   public int Count { get; private set; }
   public int ByteCount { get; private set; }

   public ReadOnlySpan<byte> WrittenSpan => _writer is ArrayBufferWriter<byte> buffer
      ? buffer.WrittenSpan
      : throw new InvalidOperationException("Backing writer does not support written span.");

   public ReadOnlyMemory<byte> WrittenMemory => _writer is ArrayBufferWriter<byte> buffer
      ? buffer.WrittenMemory
      : throw new InvalidOperationException("Backing writer does not support written span.");

   private readonly IBufferWriter<byte> _writer = writer;

   public UserPropertyListBuilder(int capacity)
      : this(new ArrayBufferWriter<byte>(capacity))
   {

   }

   public UserPropertyListBuilder()
      : this(new ArrayBufferWriter<byte>())
   {

   }

   public UserPropertyListEnumerator GetEnumerator() => _writer is ArrayBufferWriter<byte> buffer
      ? new UserPropertyListEnumerator(buffer.WrittenSpan)
      : throw new InvalidOperationException("Backing writer does not support automatic enumerator.");

   public UserPropertyListBuilder Add(string key, string value)
   {
      return Add(key.AsSpan(), value.AsSpan());
   }

   public UserPropertyListBuilder Add(string key, ReadOnlySpan<byte> valueBytes)
   {
      return Add(key.AsSpan(), valueBytes);
   }

   public UserPropertyListBuilder Add(string key, ReadOnlyMemory<byte> valueBytes)
   {
      return Add(key.AsSpan(), valueBytes);
   }

   public UserPropertyListBuilder Add(ReadOnlySpan<char> key, ReadOnlyMemory<byte> valueBytes)
   {
      return Add(key, valueBytes.Span);
   }

   public UserPropertyListBuilder Add(ReadOnlySpan<char> key, ReadOnlySpan<char> value)
   {
      var maxCount = Encoding.UTF8.GetMaxByteCount(key.Length);
      var maxCountValue = Encoding.UTF8.GetMaxByteCount(value.Length);

      var count = 1 + sizeof(ushort) + maxCount + sizeof(ushort) + maxCountValue;
      var span = _writer.GetSpan(count);

      span[0] = (byte)PropertyIdentifier.UserProperty;
      span = span[1..];

      var wrote = Encoding.UTF8.GetBytes(key, span[sizeof(ushort)..]);
      BinaryPrimitives.WriteUInt16BigEndian(span, (ushort)wrote);

      span = span[(sizeof(ushort) + wrote)..];

      var wroteValue = Encoding.UTF8.GetBytes(value, span[sizeof(ushort)..]);
      BinaryPrimitives.WriteUInt16BigEndian(span, (ushort)wroteValue);

      var byteCount = 1 + sizeof(ushort) + wrote + sizeof(ushort) + wroteValue;
      _writer.Advance(byteCount);

      Count++;
      ByteCount += byteCount;

      return this;
   }

   public UserPropertyListBuilder Add(ReadOnlySpan<char> key, ReadOnlySpan<byte> valueBytes)
   {
      var maxCount = Encoding.UTF8.GetMaxByteCount(key.Length);

      var count = 1 + sizeof(ushort) + maxCount + sizeof(ushort) + valueBytes.Length;
      var span = _writer.GetSpan(count);

      span[0] = (byte)PropertyIdentifier.UserProperty;
      span = span[1..];

      var wrote = Encoding.UTF8.GetBytes(key, span[sizeof(ushort)..]);
      BinaryPrimitives.WriteUInt16BigEndian(span, (ushort)wrote);

      span = span[(sizeof(ushort) + wrote)..];

      BinaryPrimitives.WriteUInt16BigEndian(span, (ushort)valueBytes.Length);
      valueBytes.CopyTo(span[sizeof(ushort)..]);

      var byteCount = 1 + sizeof(ushort) + wrote + sizeof(ushort) + valueBytes.Length;
      _writer.Advance(byteCount);

      Count++;
      ByteCount += byteCount;

      return this;
   }

   public UserPropertyListBuilder Add(ReadOnlySpan<byte> keyUtf8Bytes, ReadOnlySpan<byte> valueBytes)
   {
      var count = 1 + sizeof(ushort) + keyUtf8Bytes.Length + sizeof(ushort) + valueBytes.Length;
      var span = _writer.GetSpan(count);

      span[0] = (byte)PropertyIdentifier.UserProperty;
      span = span[1..];

      BinaryPrimitives.WriteUInt16BigEndian(span, (ushort)keyUtf8Bytes.Length);
      keyUtf8Bytes.CopyTo(span[sizeof(ushort)..]);

      span = span[(sizeof(ushort) + keyUtf8Bytes.Length)..];

      BinaryPrimitives.WriteUInt16BigEndian(span, (ushort)valueBytes.Length);
      valueBytes.CopyTo(span[sizeof(ushort)..]);

      _writer.Advance(count);

      Count++;
      ByteCount += count;

      return this;
   }

   public void Clear()
   {
      if (_writer is ArrayBufferWriter<byte> buffer)
      {
         buffer.Clear();
         Count = 0;
         ByteCount = 0;
      }
      else
      {
         throw new InvalidOperationException("Backing writer does not support clearing.");
      }
   }

   public ref struct UserPropertyListEnumerator(ReadOnlySpan<byte> buffer)
   {
      private ReadOnlySpan<byte> _buffer = buffer;

      public UserProperty Current { get; private set; } = default;

      public bool MoveNext()
      {
         if (_buffer.Length == 0)
         {
            return false;
         }

         var identifier = (PropertyIdentifier)_buffer[0];
         _buffer = _buffer[1..];

         if (identifier != PropertyIdentifier.UserProperty)
         {
            throw new InvalidOperationException($"Expected UserProperty (0x26), but found 0x{(byte)identifier:X2}.");
         }

         if (_buffer.Length < sizeof(ushort))
            throw new FormatException("Malformed payload: missing key length.");

         var keyLength = BinaryPrimitives.ReadUInt16BigEndian(_buffer);
         _buffer = _buffer[sizeof(ushort)..];

         if (_buffer.Length < keyLength)
            throw new FormatException("Malformed payload: incomplete key.");

         var key = _buffer[..keyLength];
         _buffer = _buffer[keyLength..];

         if (_buffer.Length < sizeof(ushort))
            throw new FormatException("Malformed payload: missing value length.");

         var valueLength = BinaryPrimitives.ReadUInt16BigEndian(_buffer);
         _buffer = _buffer[sizeof(ushort)..];

         if (_buffer.Length < valueLength)
            throw new FormatException("Malformed payload: incomplete value.");

         var value = _buffer[..valueLength];
         _buffer = _buffer[valueLength..];

         Current = new UserProperty(key, value);
         return true;
      }
   }
}
