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
   /// Parses the PUBREC packet variable header.
   /// </summary>
   private static Result<PacketDispatchResult, StringError> TryParsePubRecPacket(
      ref RawPacket rawPacket,
      ref PubRecPacket packet)
   {
      if ((rawPacket.FixedHeader & 0x0F) != 0)
      {
         return new StringError("Protocol Violation: Reserved bits in PUBREC fixed header must be 0.");
      }

      if (rawPacket.BodyLength < 2)
      {
         return new StringError("Protocol Violation: PUBREC packet body length must be at least 2.");
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
         packet.ReasonCode = (PubRecReasonCode)reasonCodeByte;

         if (rawPacket.BodyLength > 3)
         {
            if (!rawPacket.Reader.TryReadProperties(out packet.PropertiesBytes))
            {
               return new StringError("Could not read properties.");
            }
         }
         else
         {
            packet.PropertiesBytes = System.Buffers.ReadOnlySequence<byte>.Empty;
         }
      }
      else
      {
         packet.ReasonCode = PubRecReasonCode.Success;
         packet.PropertiesBytes = System.Buffers.ReadOnlySequence<byte>.Empty;
      }

      return PacketDispatchResult.Success;
   }
}
