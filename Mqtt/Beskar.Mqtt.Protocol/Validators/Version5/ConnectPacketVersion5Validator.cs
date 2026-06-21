using Beskar.Memory.Results;
using Beskar.Memory.Results.Errors;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Packets;

namespace Beskar.Mqtt.Protocol.Validators.Version5;

public static class ConnectPacketVersion5Validator
{
   public static VoidResult<StringError> Validate(ref ConnectPacket packet)
   {
      if (!packet.HasWill)
      {
         if (packet.WillQualityOfService != 0 || packet.WillRetain)
         {
            return new StringError(
               "Protocol Violation: Will Flag is set to 0, but Will QoS and/or Will Retain are not set to 0.");
         }
      }
      else
      {
         if (packet.WillQualityOfService == QualityOfServiceType.ExactlyOnce + 1) // i.e. 3
         {
            return new StringError("Protocol Violation: Will QoS cannot be 3.");
         }
      }

      return true;
   }
}
