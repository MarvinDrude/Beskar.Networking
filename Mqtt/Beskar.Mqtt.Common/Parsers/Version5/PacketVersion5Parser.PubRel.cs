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
   /// Parses the PUBREL packet variable header.
   /// </summary>
   private static Result<PacketDispatchResult, StringError> TryParsePubRelPacket(
      ref RawPacket rawPacket,
      ref PubRelPacket packet)
   {
      if ((rawPacket.FixedHeader & 0x0F) != 0x02)
      {
         return new StringError("Protocol Violation: Reserved bits in PUBREL fixed header must be 0010.");
      }

      if (rawPacket.BodyLength < 2)
      {
         return new StringError("Protocol Violation: PUBREL packet body length must be at least 2.");
      }

      if (!rawPacket.Reader.TryReadUInt16BigEndian(out packet.PacketIdentifier))
      {
         return new StringError("Could not read packet identifier.");
      }

      if (rawPacket.BodyLength > 2)
      {
         if (!rawPacket.Reader.TryRead(out var reasonCodeByte))
         {
            return new StringError("Could not read reason code.");
         }
         packet.ReasonCode = (PubRelReasonCode)reasonCodeByte;

         if (rawPacket.BodyLength > 3)
         {
            if (!rawPacket.Reader.TryReadProperties(out packet.PropertiesBytes))
            {
               return new StringError("Could not read properties.");
            }

            var enumerator = packet.GetProperties();
            var foundReasonString = false;

            while (enumerator.MoveNext())
            {
               switch (enumerator.Current.Identifier)
               {
                  case PropertyIdentifier.ReasonString:
                     packet.ReasonStringUtf8Bytes = enumerator.Current.AsReasonString();
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
         }
         else
         {
            packet.PropertiesBytes = System.Buffers.ReadOnlySequence<byte>.Empty;
         }
      }
      else
      {
         packet.ReasonCode = PubRelReasonCode.Success;
         packet.PropertiesBytes = System.Buffers.ReadOnlySequence<byte>.Empty;
      }

      return PacketDispatchResult.Success;
   }
}
