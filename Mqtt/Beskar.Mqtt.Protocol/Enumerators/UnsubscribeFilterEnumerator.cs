using System.Buffers;
using System.Runtime.InteropServices;

namespace Beskar.Mqtt.Protocol.Enumerators;


[StructLayout(LayoutKind.Auto)]
public ref struct UnsubscribeFilterEnumerator(ReadOnlySequence<byte> sequence)
{
   private SequenceReader<byte> _reader = new(sequence);

   public ReadOnlySequence<byte> Current { get; private set; } = default;

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

      Current = _reader.Sequence.Slice(_reader.Position, topicLength);
      _reader.Advance(topicLength);

      return true;
   }
}
