using Beskar.Memory.Results;
using Beskar.Memory.Results.Errors;
using Beskar.Mqtt.Protocol.Packets;

namespace Beskar.Mqtt.Protocol.Validators.Version3;

public static class PubRecPacketVersion3Validator
{
   public static VoidResult<StringError> Validate(ref PubRecPacket packet)
   {
      if (packet.PacketIdentifier == 0)
      {
         return new StringError("Protocol Violation: Packet Identifier cannot be 0.");
      }

      return true;
   }
}
