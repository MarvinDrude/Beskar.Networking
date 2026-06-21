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
   /// Parses the DISCONNECT packet variable header.
   /// </summary>
   private static Result<PacketDispatchResult, StringError> TryParseDisconnectPacket(
      ref RawPacket rawPacket,
      ref DisconnectPacket packet)
   {
      if ((rawPacket.FixedHeader & 0x0F) != 0)
      {
         return new StringError("Protocol Violation: Reserved bits in DISCONNECT fixed header must be 0.");
      }

      if (rawPacket.BodyLength > 0)
      {
         if (!rawPacket.Reader.TryRead(out var reasonCodeByte))
         {
            return new StringError("Could not read reason code.");
         }
         packet.ReasonCode = (DisconnectReasonCode)reasonCodeByte;

         if (rawPacket.BodyLength > 1)
         {
            if (!rawPacket.Reader.TryReadProperties(out packet.PropertiesBytes))
            {
               return new StringError("Could not read properties.");
            }

            var enumerator = packet.GetProperties();
            byte foundFlags = 0;

            while (enumerator.MoveNext())
            {
               switch (enumerator.Current.Identifier)
               {
                  case PropertyIdentifier.SessionExpiryInterval:
                     packet.SessionExpiryInterval = enumerator.Current.AsSessionExpiryInterval();
                     foundFlags |= 0b001;
                     break;
                  case PropertyIdentifier.ReasonString:
                     packet.ReasonUtf8Bytes = enumerator.Current.AsReasonString();
                     foundFlags |= 0b010;
                     break;
                  case PropertyIdentifier.ServerReference:
                     packet.ServerReferenceUtf8Bytes = enumerator.Current.AsServerReference();
                     foundFlags |= 0b100;
                     break;
                  case PropertyIdentifier.UserProperty:
                     break;
                  default:
                     return new StringError($"Invalid property identifier. ({enumerator.Current.Identifier})");
               }

               if (foundFlags == 0b111)
               {
                  break;
               }
            }
         }
         else
         {
            packet.PropertiesBytes = System.Buffers.ReadOnlySequence<byte>.Empty;
         }
      }
      else
      {
         packet.ReasonCode = DisconnectReasonCode.NormalDisconnection;
         packet.PropertiesBytes = System.Buffers.ReadOnlySequence<byte>.Empty;
      }

      return PacketDispatchResult.Success;
   }
}
