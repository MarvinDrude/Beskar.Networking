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

      if (rawPacket.BodyLength != 2)
      {
         return new StringError("Protocol Violation: PUBREL packet body length must be exactly 2.");
      }

      if (!rawPacket.Reader.TryReadUInt16BigEndian(out packet.PacketIdentifier))
      {
         return new StringError("Could not read packet identifier.");
      }

      var validateResult = PubRelPacketVersion3Validator.Validate(ref packet);
      if (validateResult.Failed) return validateResult.Error;

      return PacketDispatchResult.Success;
   }
}
