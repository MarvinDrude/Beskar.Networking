using Beskar.Memory.Results;
using Beskar.Memory.Results.Errors;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Packets;

namespace Beskar.Mqtt.Protocol.Validators.Version3;

public static class ConnAckPacketVersion3Validator
{
   public static VoidResult<StringError> Validate(ref ConnAckPacket packet)
   {
      if ((byte)packet.ReturnCode > 5)
      {
         return new StringError($"Protocol Violation: Invalid return code 0x{(byte)packet.ReturnCode:X2} in CONNACK packet.");
      }

      return true;
   }
}
