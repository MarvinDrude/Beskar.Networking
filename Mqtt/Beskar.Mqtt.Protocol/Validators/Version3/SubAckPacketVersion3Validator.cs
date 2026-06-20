using Beskar.Memory.Results;
using Beskar.Memory.Results.Errors;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Packets;

namespace Beskar.Mqtt.Protocol.Validators.Version3;

public static class SubAckPacketVersion3Validator
{
   public static VoidResult<StringError> Validate(ref SubAckPacket packet)
   {
      if (packet.PacketIdentifier == 0)
      {
         return new StringError("Protocol Violation: Packet Identifier cannot be 0.");
      }

      var enumerator = packet.GetReturnCodes();
      if (!enumerator.MoveNext())
      {
         return new StringError("Protocol Violation: SUBACK packet payload must contain at least one return code.");
      }

      do
      {
         var code = enumerator.Current;
         if (code is not (SubscribeReasonCode.GrantedQos0 or SubscribeReasonCode.GrantedQos1 or SubscribeReasonCode.GrantedQos2 or SubscribeReasonCode.UnspecifiedError))
         {
            return new StringError($"Protocol Violation: Invalid return code 0x{(byte)code:X2} in SUBACK packet.");
          }
      }
      while (enumerator.MoveNext());

      return true;
   }
}
