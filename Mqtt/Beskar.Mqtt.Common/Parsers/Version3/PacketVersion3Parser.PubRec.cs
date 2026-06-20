using Beskar.Memory.Results;
using Beskar.Memory.Results.Errors;
using Beskar.Mqtt.Common.Extensions;
using Beskar.Mqtt.Protocol.Packets;
using Beskar.Mqtt.Protocol.Parsing;
using Beskar.Mqtt.Protocol.Parsing.Results;
using Beskar.Mqtt.Protocol.Validators.Version3;

namespace Beskar.Mqtt.Common.Parsers.Version3;

public readonly ref partial struct PacketVersion3Parser
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

      if (rawPacket.BodyLength != 2)
      {
         return new StringError("Protocol Violation: PUBREC packet body length must be exactly 2.");
      }

      if (!rawPacket.Reader.TryReadUInt16BigEndian(out packet.PacketIdentifier))
      {
         return new StringError("Could not read packet identifier.");
      }

      var validateResult = PubRecPacketVersion3Validator.Validate(ref packet);
      if (validateResult.Failed) return validateResult.Error;

      return PacketDispatchResult.Success;
   }
}
