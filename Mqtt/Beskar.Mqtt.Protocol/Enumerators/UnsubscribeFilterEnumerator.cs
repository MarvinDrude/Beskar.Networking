using System.Buffers;
using System.Runtime.InteropServices;

namespace Beskar.Mqtt.Protocol.Enumerators;


[StructLayout(LayoutKind.Auto)]
public ref struct UnsubscribeFilterEnumerator(ReadOnlySequence<byte> sequence)
{
   private SequenceReader<byte> _reader = new(sequence);

   public ReadOnlySequence<byte> Current { get; private set; } = default;

   public bool IsMalformed { get; private set; }
   public string? ErrorMessage { get; private set; }

   private bool Fail(string message)
   {
      IsMalformed = true;
      ErrorMessage = message;

      return false;
   }

   public bool MoveNext()
   {
      if (_reader.End)
      {
         return false;
      }

      if (!_reader.TryReadBigEndian(out short topicLengthShort))
      {
         return Fail("Malformed packet: Could not read topic filter length.");
      }

      var topicLength = (ushort)topicLengthShort;
      if (_reader.Remaining < topicLength)
      {
         return Fail("Malformed packet: Truncated topic filter.");
      }

      Current = _reader.Sequence.Slice(_reader.Position, topicLength);
      _reader.Advance(topicLength);

      return true;
   }
}
