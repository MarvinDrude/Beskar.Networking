using Beskar.Memory.Results;
using Beskar.Memory.Results.Errors;
using Beskar.Mqtt.Protocol.Enums;
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

      var enumerator = packet.GetFilters();
      if (!enumerator.MoveNext())
      {
         return new StringError("Protocol Violation: SUBSCRIBE packet payload must contain at least one topic filter.");
      }

      do
      {
         var filter = enumerator.Current;
         if (filter.TopicUtf8Bytes.IsEmpty)
         {
            return new StringError("Protocol Violation: Topic filter in SUBSCRIBE packet cannot be empty.");
         }

         if (filter.QualityOfService is < 0 or > QualityOfServiceType.ExactlyOnce)
         {
            return new StringError("Protocol Violation: Invalid QoS level in SUBSCRIBE packet.");
         }
      }
      while (enumerator.MoveNext());

      return true;
   }
}
