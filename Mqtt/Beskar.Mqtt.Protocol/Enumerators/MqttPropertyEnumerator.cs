using System.Buffers;
using System.Runtime.InteropServices;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Extensions;
using Beskar.Mqtt.Protocol.Models;
using Beskar.Mqtt.Protocol.Parsing.Results;

namespace Beskar.Mqtt.Protocol.Enumerators;

[StructLayout(LayoutKind.Auto)]
public ref struct MqttPropertyEnumerator(ReadOnlySequence<byte> sequence)
{
   private SequenceReader<byte> _reader = new (sequence);
   private int _length;

   public MqttProperty Current { get; private set; } = default;

   public bool MoveNext()
   {
      if (_reader.End) return false;
      if (_reader.TryReadVariableByteInteger(out var id)
          is not VariableByteIntegerResult.Success)
      {
         return false;
      }

      var identifier = (PropertyIdentifier)id;
      var valueStart = _reader.Position;

      if (!MqttPropertyParsingHelper.TrySkipProperty(ref _reader, identifier))
      {
         return false;
      }

      var valueBytes = _reader.Sequence.Slice(valueStart, _reader.Position);
      Current = new MqttProperty(identifier, valueBytes);

      return true;
   }
}

internal static class MqttPropertyParsingHelper
{
   public static bool TrySkipProperty(ref SequenceReader<byte> reader, PropertyIdentifier identifier)
   {
      switch (identifier)
      {
         case PropertyIdentifier.PayloadFormatIndicator:
         case PropertyIdentifier.RequestProblemInformation:
         case PropertyIdentifier.RequestResponseInformation:
         case PropertyIdentifier.MaximumQos:
         case PropertyIdentifier.RetainAvailable:
         case PropertyIdentifier.WildcardSubscriptionAvailable:
         case PropertyIdentifier.SubscriptionIdentifierAvailable:
         case PropertyIdentifier.SharedSubscriptionAvailable:
            if (reader.Remaining < 1) return false;
            reader.Advance(1);
            return true;
         case PropertyIdentifier.ServerKeepAlive:
         case PropertyIdentifier.ReceiveMaximum:
         case PropertyIdentifier.TopicAliasMaximum:
         case PropertyIdentifier.TopicAlias:
            if (reader.Remaining < 2) return false;
            reader.Advance(2);
            return true;
         case PropertyIdentifier.MessageExpiryInterval:
         case PropertyIdentifier.SessionExpiryInterval:
         case PropertyIdentifier.WillDelayInterval:
         case PropertyIdentifier.MaximumPacketSize:
            if (reader.Remaining < 4) return false;
            reader.Advance(4);
            return true;
         case PropertyIdentifier.SubscriptionIdentifier:
            return reader.TryReadVariableByteInteger(out _) == VariableByteIntegerResult.Success;
         case PropertyIdentifier.ContentType:
         case PropertyIdentifier.ResponseTopic:
         case PropertyIdentifier.AssignedClientIdentifier:
         case PropertyIdentifier.AuthenticationMethod:
         case PropertyIdentifier.ResponseInformation:
         case PropertyIdentifier.ServerReference:
         case PropertyIdentifier.ReasonString:
         case PropertyIdentifier.CorrelationData:
         case PropertyIdentifier.AuthenticationData:
            if (!reader.TryReadUInt16BigEndian(out var len)) return false;
            if (reader.Remaining < len) return false;
            reader.Advance(len);

            return true;
         case PropertyIdentifier.UserProperty:
            if (!reader.TryReadUInt16BigEndian(out var keyLen)) return false;
            if (reader.Remaining < keyLen) return false;
            reader.Advance(keyLen);

            if (!reader.TryReadUInt16BigEndian(out var valLen)) return false;
            if (reader.Remaining < valLen) return false;
            reader.Advance(valLen);

            return true;
         default:
            return false;
      }
   }
}
