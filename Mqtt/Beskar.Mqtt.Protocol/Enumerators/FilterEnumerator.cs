using System.Buffers;
using System.Runtime.InteropServices;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Models;

namespace Beskar.Mqtt.Protocol.Enumerators;

[StructLayout(LayoutKind.Auto)]
public ref struct FilterEnumerator(ReadOnlySequence<byte> sequence)
{
   private SequenceReader<byte> _reader = new(sequence);

   public TopicFilter Current { get; private set; } = default;

   public bool MoveNext()
   {
      if (_reader.End)
      {
         return false;
      }

      if (!_reader.TryReadBigEndian(out short topicLengthShort))
      {
         return false;
      }

      var topicLength = (ushort)topicLengthShort;
      if (_reader.Remaining < topicLength)
      {
         return false;
      }

      var topicUtf8Bytes = _reader.Sequence.Slice(_reader.Position, topicLength);
      _reader.Advance(topicLength);

      if (!_reader.TryRead(out var qosByte))
      {
         return false;
      }

      if ((qosByte & 0xC0) != 0)
      {
         return false;
      }

      var qos = (QualityOfServiceType)(qosByte & 0x03);
      var noLocal = (qosByte & 0x04) != 0;
      var retainAsPublished = (qosByte & 0x08) != 0;
      var retainHandling = (RetainHandlingType)((qosByte & 0x30) >> 4);

      Current = new TopicFilter(topicUtf8Bytes, qos, noLocal, retainAsPublished, retainHandling);
      return true;
   }
}
