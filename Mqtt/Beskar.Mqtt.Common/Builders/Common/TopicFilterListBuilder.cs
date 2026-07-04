using System.Buffers;
using System.Buffers.Binary;
using System.Text;
using Beskar.Mqtt.Protocol.Enumerators;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Models;

namespace Beskar.Mqtt.Common.Builders.Common;

public sealed class TopicFilterListBuilder(IBufferWriter<byte> writer)
{
   public int Count { get; private set; }

   private readonly IBufferWriter<byte> _writer = writer;

   public TopicFilterListBuilder(int capacity)
      : this(new ArrayBufferWriter<byte>(capacity))
   {
   }

   public TopicFilterListBuilder()
      : this(new ArrayBufferWriter<byte>())
   {
   }

   public ReadOnlySequence<byte> WrittenSequence => _writer is ArrayBufferWriter<byte> buffer
      ? new ReadOnlySequence<byte>(buffer.WrittenMemory)
      : throw new InvalidOperationException("Backing writer is not an ArrayBufferWriter.");

   public ReadOnlySpan<byte> WrittenSpan => _writer is ArrayBufferWriter<byte> buffer
      ? buffer.WrittenSpan
      : throw new InvalidOperationException("Backing writer does not support written span.");

   public TopicFilterListEnumerator GetEnumerator() => _writer is ArrayBufferWriter<byte> buffer
      ? new TopicFilterListEnumerator(buffer.WrittenMemory)
      : throw new InvalidOperationException("Backing writer does not support automatic enumerator.");

   public TopicFilterListBuilder Add(
      string topic,
      QualityOfServiceType qos,
      bool noLocal = false,
      bool retainAsPublished = false,
      RetainHandlingType retainHandling = RetainHandlingType.SendAtSubscription)
   {
      return Add(topic.AsSpan(), qos, noLocal, retainAsPublished, retainHandling);
   }

   public TopicFilterListBuilder Add(
      ReadOnlySpan<char> topic,
      QualityOfServiceType qos,
      bool noLocal = false,
      bool retainAsPublished = false,
      RetainHandlingType retainHandling = RetainHandlingType.SendAtSubscription)
   {
      var maxByteCount = Encoding.UTF8.GetMaxByteCount(topic.Length);
      var totalReservation = sizeof(ushort) + maxByteCount + 1; // 2 bytes length, max bytes topic, 1 byte option
      var span = _writer.GetSpan(totalReservation);

      var bytesWritten = Encoding.UTF8.GetBytes(topic, span[sizeof(ushort)..]);
      BinaryPrimitives.WriteUInt16BigEndian(span, (ushort)bytesWritten);

      var optionByte = (byte)((byte)qos & 0x03);
      if (noLocal) optionByte |= 0x04;
      if (retainAsPublished) optionByte |= 0x08;
      optionByte |= (byte)(((byte)retainHandling & 0x03) << 4);

      span[sizeof(ushort) + bytesWritten] = optionByte;

      _writer.Advance(sizeof(ushort) + bytesWritten + 1);
      Count++;

      return this;
   }

   public TopicFilterListBuilder Add(
      ReadOnlySpan<byte> topicUtf8Bytes,
      QualityOfServiceType qos,
      bool noLocal = false,
      bool retainAsPublished = false,
      RetainHandlingType retainHandling = RetainHandlingType.SendAtSubscription)
   {
      var totalReservation = sizeof(ushort) + topicUtf8Bytes.Length + 1;
      var span = _writer.GetSpan(totalReservation);

      BinaryPrimitives.WriteUInt16BigEndian(span, (ushort)topicUtf8Bytes.Length);
      topicUtf8Bytes.CopyTo(span[sizeof(ushort)..]);

      var optionByte = (byte)((byte)qos & 0x03);
      if (noLocal) optionByte |= 0x04;
      if (retainAsPublished) optionByte |= 0x08;
      optionByte |= (byte)(((byte)retainHandling & 0x03) << 4);

      span[sizeof(ushort) + topicUtf8Bytes.Length] = optionByte;

      _writer.Advance(totalReservation);
      Count++;

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

   public ref struct TopicFilterListEnumerator(ReadOnlyMemory<byte> buffer)
   {
      private ReadOnlyMemory<byte> _remaining = buffer;
      private TopicFilter _current = default;

      public readonly TopicFilter Current => _current;

      public bool MoveNext()
      {
         if (_remaining.IsEmpty)
         {
            return false;
         }

         var span = _remaining.Span;
         var topicLength = BinaryPrimitives.ReadUInt16BigEndian(span);
         var totalLength = sizeof(ushort) + topicLength + 1;

         var topicUtf8Bytes = _remaining.Slice(sizeof(ushort), topicLength);
         var optionByte = span[sizeof(ushort) + topicLength];

         var qos = (QualityOfServiceType)(optionByte & 0x03);
         var noLocal = (optionByte & 0x04) != 0;
         var retainAsPublished = (optionByte & 0x08) != 0;
         var retainHandling = (RetainHandlingType)((optionByte & 0x30) >> 4);

         _current = new TopicFilter(new ReadOnlySequence<byte>(topicUtf8Bytes), qos, noLocal, retainAsPublished, retainHandling);
         _remaining = _remaining[totalLength..];

         return true;
      }

      public readonly TopicFilterListEnumerator GetEnumerator() => this;
   }

}
