using Beskar.Memory.Results;
using Beskar.Memory.Results.Errors;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Packets;

namespace Beskar.Mqtt.Protocol.Validators.Version5;

public static class PublishPacketVersion5Validator
{
   public static VoidResult<StringError> Validate(ref PublishPacket packet)
   {
      if (packet.TopicUtf8Bytes.IsEmpty)
      {
         return new StringError("Topic name cannot be empty.");
      }

      if (packet.QualityOfService == (QualityOfServiceType)3)
      {
         return new StringError("Protocol Violation: QoS 3 is not allowed.");
      }

      if (packet.QualityOfService == QualityOfServiceType.AtMostOnce && packet.Dup)
      {
         return new StringError("Protocol Violation: DUP flag must be set to 0 for QoS 0 messages.");
      }

      if (packet.QualityOfService > QualityOfServiceType.AtMostOnce && packet.PacketIdentifier == 0)
      {
         return new StringError("Protocol Violation: Packet Identifier cannot be 0.");
      }

      if (packet.QualityOfService == QualityOfServiceType.AtMostOnce && packet.PacketIdentifier != 0)
      {
         return new StringError("Protocol Violation: Packet Identifier must be 0 or omitted for QoS 0 messages.");
      }

      return true;
   }
}
