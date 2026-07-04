using System.Buffers;
using Beskar.Memory.Results;
using Beskar.Memory.Results.Errors;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Extensions;
using Beskar.Mqtt.Protocol.Packets;
using Beskar.Mqtt.Protocol.Parsing;
using Beskar.Mqtt.Protocol.Parsing.Results;

namespace Beskar.Mqtt.Common.Parsers.Version5;

public readonly ref partial struct PacketVersion5Parser
{
   /// <summary>
   /// Parses the SUBACK packet variable header and payload.
   /// </summary>
   private static Result<PacketDispatchResult, StringError> TryParseSubAckPacket(
      ref RawPacket rawPacket,
      ref SubAckPacket packet)
   {
      if ((rawPacket.FixedHeader & 0x0F) != 0)
      {
         return new StringError("Protocol Violation: Reserved bits in SUBACK fixed header must be 0.");
      }

      var initialConsumed = rawPacket.Reader.Consumed;

      if (!rawPacket.Reader.TryReadUInt16BigEndian(out packet.PacketIdentifier))
      {
         return new StringError("Could not read packet identifier.");
      }

      if (!rawPacket.Reader.TryReadProperties(out var propertiesSequence))
      {
         return new StringError("Could not read properties.");
      }
      packet.PropertiesBytes = propertiesSequence.ToArray();

      var enumerator = packet.GetProperties();
      var foundReasonString = false;

      while (enumerator.MoveNext())
      {
         switch (enumerator.Current.Identifier)
         {
            case PropertyIdentifier.ReasonString:
               packet.ReasonStringUtf8Bytes = enumerator.Current.AsReasonString().ToArray();
               foundReasonString = true;
               break;
            case PropertyIdentifier.UserProperty:
               break;
            default:
               return new StringError($"Invalid property identifier. ({enumerator.Current.Identifier})");
         }

         if (foundReasonString)
         {
            break;
         }
      }

      var consumedSoFar = rawPacket.Reader.Consumed - initialConsumed;
      var payloadLength = rawPacket.BodyLength - consumedSoFar;

      if (payloadLength < 0)
      {
         return new StringError("Malformed packet: variable header length exceeds body length.");
      }

      if (payloadLength > 0)
      {
         if (rawPacket.Reader.Remaining < payloadLength)
         {
            return new StringError("Could not read return codes payload.");
         }

         packet.ReturnCodesBytes = rawPacket.Reader.Sequence.Slice(rawPacket.Reader.Position, payloadLength).ToArray();
         rawPacket.Reader.Advance(payloadLength);
      }
      else
      {
         packet.ReturnCodesBytes = ReadOnlyMemory<byte>.Empty;
      }

      return PacketDispatchResult.Success;
   }
}
