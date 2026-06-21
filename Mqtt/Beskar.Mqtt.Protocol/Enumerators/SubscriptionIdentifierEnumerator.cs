using System.Buffers;
using System.Runtime.InteropServices;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Extensions;
using Beskar.Mqtt.Protocol.Parsing.Results;

namespace Beskar.Mqtt.Protocol.Enumerators;

[StructLayout(LayoutKind.Auto)]
public ref struct SubscriptionIdentifierEnumerator(ReadOnlySequence<byte> sequence)
{
   private SequenceReader<byte> _reader = new(sequence);
   public uint Current { get; private set; }

   public bool MoveNext()
   {
      while (!_reader.End)
      {
         if (_reader.TryReadVariableByteInteger(out var id) is not VariableByteIntegerResult.Success)
         {
            return false;
         }

         var identifier = (PropertyIdentifier)id;
         if (identifier is PropertyIdentifier.SubscriptionIdentifier)
         {
            if (_reader.TryReadVariableByteInteger(out var subId) != VariableByteIntegerResult.Success)
               return false;

            Current = subId;
            return true;
         }

         // Skip other properties
         if (!MqttPropertyParsingHelper.TrySkipProperty(ref _reader, identifier))
         {
            return false;
         }
      }

      return false;
   }
}
