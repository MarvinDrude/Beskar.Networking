using Beskar.Memory.Results;
using Beskar.Memory.Results.Errors;
using Beskar.Mqtt.Protocol.Packets;

namespace Beskar.Mqtt.Protocol.Validators.Version3;

public static class ConnectPacketVersion3Validator
{
   public static VoidResult<StringError> Validate(ref ConnectPacket packet)
   {
      if (!packet.IsCleanSession && packet.ClientIdUtf8Bytes.IsEmpty)
      {
         return new StringError("Without a clean session, you need to provide a client id.");
      }

      return true;
   }
}
