using Beskar.Memory.Results;
using Beskar.Memory.Results.Errors;
using Beskar.Mqtt.Protocol.Packets;

namespace Beskar.Mqtt.Protocol.Validators.Version3;

public static class UnsubscribePacketVersion3Validator
{
   public static VoidResult<StringError> Validate(ref UnsubscribePacket packet)
   {
      if (packet.PacketIdentifier == 0)
      {
         return new StringError("Protocol Violation: Packet Identifier cannot be 0.");
      }

      var enumerator = packet.GetFilters();
      if (!enumerator.MoveNext())
      {
         return new StringError("Protocol Violation: UNSUBSCRIBE packet payload must contain at least one topic filter.");
      }

      do
      {
         var filter = enumerator.Current;
         if (filter.IsEmpty)
         {
            return new StringError("Protocol Violation: Topic filter in UNSUBSCRIBE packet cannot be empty.");
         }
      }
      while (enumerator.MoveNext());

      return true;
   }
}
