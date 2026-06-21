using Beskar.Memory.Results;
using Beskar.Memory.Results.Errors;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Packets;
using Beskar.Mqtt.Protocol.Parsing;
using Beskar.Mqtt.Protocol.Parsing.Results;
using Beskar.Mqtt.Protocol.Validators.Version3;

namespace Beskar.Mqtt.Common.Parsers.Version3;

public readonly ref partial struct PacketVersion3Parser
{
   /// <summary>
   /// Parses the CONNACK packet variable header.
   /// </summary>
   private Result<PacketDispatchResult, StringError> TryParseConnAckPacket(
      ref RawPacket rawPacket,
      ref ConnAckPacket packet)
   {
      if ((rawPacket.FixedHeader & 0x0F) != 0)
      {
         return new StringError("Protocol Violation: Reserved bits in CONNACK fixed header must be 0.");
      }

      if (rawPacket.BodyLength != 2)
      {
         return new StringError("Protocol Violation: CONNACK packet body length must be exactly 2.");
      }

      if (!rawPacket.Reader.TryRead(out var connAckFlags))
      {
         return new StringError("Could not read connect acknowledge flags.");
      }

      if (_protocolVersion is MqttProtocolVersion.V31)
      {
         if (connAckFlags != 0)
         {
            return new StringError("Protocol Violation: Connect acknowledge flags must be 0 in MQTT v3.1.");
         }
         packet.IsSessionPresent = false;
      }
      else // MQTT v3.1.1
      {
         if ((connAckFlags & 0xFE) != 0)
         {
            return new StringError("Protocol Violation: Reserved bits in connect acknowledge flags must be 0 in MQTT v3.1.1.");
         }
         packet.IsSessionPresent = (connAckFlags & 0x01) > 0;
      }

      if (!rawPacket.Reader.TryRead(out var returnCodeByte))
      {
         return new StringError("Could not read return code.");
      }

      packet.ReturnCode = (ConnectReturnCode)returnCodeByte;

      var validateResult = ConnAckPacketVersion3Validator.Validate(ref packet);
      if (validateResult.Failed) return validateResult.Error;

      return PacketDispatchResult.Success;
   }
}
