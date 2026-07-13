using System.Buffers;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Models;
using Beskar.Mqtt.Server.Enumerators;
using Beskar.Mqtt.Server.Internal;

namespace Beskar.Mqtt.Server;

public sealed partial class MqttServer
{
   public SubscribeReasonCode Subscribe(MqttSession session, in TopicFilter filter)
   {
      return Subscribe(session, in filter, 0);
   }

   public SubscribeReasonCode Subscribe(MqttSession session, in TopicFilter filter, uint subscriptionIdentifier)
   {
      var topicFilterBytes = filter.TopicUtf8Bytes.ToArray();
      if (!ValidateTopicFilter(topicFilterBytes))
      {
         return SubscribeReasonCode.TopicFilterInvalid;
      }

      var qos = filter.QualityOfService;

      SubscriptionRouter.Subscribe(
         session,
         topicFilterBytes,
         qos,
         filter.NoLocal,
         filter.RetainAsPublished,
         filter.RetainHandling,
         subscriptionIdentifier);

      return qos switch
      {
         QualityOfServiceType.AtMostOnce => SubscribeReasonCode.GrantedQos0,
         QualityOfServiceType.AtLeastOnce => SubscribeReasonCode.GrantedQos1,
         QualityOfServiceType.ExactlyOnce => SubscribeReasonCode.GrantedQos2,
         _ => SubscribeReasonCode.UnspecifiedError
      };
   }

   public UnsubscribeReasonCode Unsubscribe(MqttSession session, byte[] topicFilter)
   {
      if (!session.Subscriptions.ContainsKey(topicFilter))
      {
         return UnsubscribeReasonCode.NoSubscriptionExisted;
      }

      SubscriptionRouter.Unsubscribe(session, topicFilter);
      return UnsubscribeReasonCode.Success;
   }

   private static bool ValidateTopicFilter(byte[] topicFilter)
   {
      var enumerator = new TopicLevelEnumerator(topicFilter);
      var hasHash = false;

      while (enumerator.MoveNext())
      {
         if (hasHash)
         {
            return false;
         }

         var level = enumerator.Current;
         if (level.Contains((byte)0x23)) // '#'
         {
            if (level.Length != 1) return false;
            hasHash = true;
         }

         if (level.Contains((byte)0x2B)) // '+'
         {
            if (level.Length != 1) return false;
         }
      }

      return true;
   }
}
