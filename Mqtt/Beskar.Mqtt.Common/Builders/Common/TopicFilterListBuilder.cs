using System.Buffers;
using System.Buffers.Binary;
using System.Text;
using Beskar.Mqtt.Protocol.Enumerators;
using Beskar.Mqtt.Protocol.Enums;

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

   public FilterEnumerator GetEnumerator() => _writer is ArrayBufferWriter<byte> buffer
      ? new FilterEnumerator(new ReadOnlySequence<byte>(buffer.WrittenMemory))
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
}
