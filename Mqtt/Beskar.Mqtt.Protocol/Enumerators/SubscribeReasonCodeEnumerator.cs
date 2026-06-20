using System.Buffers;
using System.Runtime.InteropServices;
using Beskar.Mqtt.Protocol.Enums;

namespace Beskar.Mqtt.Protocol.Enumerators;

[StructLayout(LayoutKind.Auto)]
public ref struct SubscribeReasonCodeEnumerator(ReadOnlySequence<byte> sequence)
{
   private SequenceReader<byte> _reader = new(sequence);

   public SubscribeReasonCode Current { get; private set; } = default;

   public bool MoveNext()
   {
      if (_reader.End)
      {
         return false;
      }

      if (!_reader.TryRead(out var reasonCodeByte))
      {
         return false;
      }

      Current = (SubscribeReasonCode)reasonCodeByte;
      return true;
   }
}
