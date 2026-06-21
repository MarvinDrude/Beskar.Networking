using Beskar.Memory.Results;
using Beskar.Memory.Results.Errors;
using Beskar.Mqtt.Protocol.Packets;

namespace Beskar.Mqtt.Protocol.Validators.Version5;

public static class SubscribePacketVersion5Validator
{
   public static VoidResult<StringError> Validate(ref SubscribePacket packet)
   {
      if (packet.PacketIdentifier == 0)
      {
         return new StringError("Protocol Violation: Packet Identifier cannot be 0.");
      }

      return true;
   }
}
