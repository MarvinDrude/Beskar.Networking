using Beskar.Memory.Results;
using Beskar.Memory.Results.Errors;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Packets;

namespace Beskar.Mqtt.Protocol.Validators.Common;

internal static class PacketFilterValidator
{
   public static VoidResult<StringError> ValidateSubscribe(ref SubscribePacket packet, bool isVersion3)
   {
      if (packet.PacketIdentifier == 0)
      {
         return new StringError("Protocol Violation: Packet Identifier cannot be 0.");
      }

      var enumerator = packet.GetFilters();
      if (!enumerator.MoveNext())
      {
         return enumerator.IsMalformed
            ? new StringError(enumerator.ErrorMessage!)
            : new StringError("Protocol Violation: SUBSCRIBE packet payload must contain at least one topic filter.");
      }

      do
      {
         var filter = enumerator.Current;
         if (filter.TopicUtf8Bytes.IsEmpty)
         {
            return new StringError("Protocol Violation: Topic filter in SUBSCRIBE packet cannot be empty.");
         }

         var topicResult = TopicStringValidator.ValidateForSubscribe(filter.TopicUtf8Bytes);
         if (topicResult.Failed)
         {
            return topicResult.Error;
         }

         if (filter.QualityOfService is < 0 or > QualityOfServiceType.ExactlyOnce)
         {
            return new StringError("Protocol Violation: Invalid QoS level in SUBSCRIBE packet.");
         }

         if (isVersion3)
         {
            if (filter.NoLocal || filter.RetainAsPublished || filter.RetainHandling != RetainHandlingType.SendAtSubscription)
            {
               return new StringError("Protocol Violation: Reserved bits in SUBSCRIBE packet options byte must be 0 under MQTT v3.1.1.");
            }
         }
         else if (filter.RetainHandling is < 0 or > RetainHandlingType.DoNotSend)
         {
            return new StringError("Protocol Violation: Invalid Retain Handling level in SUBSCRIBE packet.");
         }
      }
      while (enumerator.MoveNext());

      if (enumerator.IsMalformed)
      {
         return new StringError(enumerator.ErrorMessage!);
      }

      return true;
   }

   public static VoidResult<StringError> ValidateUnsubscribe(ref UnsubscribePacket packet)
   {
      if (packet.PacketIdentifier == 0)
      {
         return new StringError("Protocol Violation: Packet Identifier cannot be 0.");
      }

      var enumerator = packet.GetFilters();
      if (!enumerator.MoveNext())
      {
         return enumerator.IsMalformed
            ? new StringError(enumerator.ErrorMessage!)
            : new StringError("Protocol Violation: UNSUBSCRIBE packet payload must contain at least one topic filter.");
      }

      do
      {
         var filter = enumerator.Current;
         if (filter.IsEmpty)
         {
            return new StringError("Protocol Violation: Topic filter in UNSUBSCRIBE packet cannot be empty.");
         }

         var topicResult = TopicStringValidator.ValidateForSubscribe(filter);
         if (topicResult.Failed)
         {
            return topicResult.Error;
         }
      }
      while (enumerator.MoveNext());

      if (enumerator.IsMalformed)
      {
         return new StringError(enumerator.ErrorMessage!);
      }

      return true;
   }
}
