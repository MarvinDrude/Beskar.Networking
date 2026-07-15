using System.Buffers;
using System.Text;
using Beskar.Memory.Owners;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Models;
using Beskar.Mqtt.Server.Contexts;
using Beskar.Memory.Threading;
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
      var alternateLookup = session.Subscriptions.GetAlternateLookup<ReadOnlySpan<byte>>();
      var subscriptionExisted = alternateLookup.ContainsKey(topicFilterBytes);

      SubscriptionRouter.Subscribe(
         session,
         topicFilterBytes,
         qos,
         filter.NoLocal,
         filter.RetainAsPublished,
         filter.RetainHandling,
         subscriptionIdentifier);

      if (Events.OnSubscribe.Count > 0)
      {
         var topicFilterString = Encoding.UTF8.GetString(topicFilterBytes);
         var filterQos = filter.QualityOfService;
         var filterNoLocal = filter.NoLocal;
         var filterRetainAsPublished = filter.RetainAsPublished;
         var filterRetainHandling = filter.RetainHandling;

         _ = Task.Run(async () =>
         {
            try
            {
               await Events.OnSubscribe.ExecuteAsync(new MqttSubscribeContext()
               {
                  Session = session,
                  TopicFilter = topicFilterString,
                  QualityOfService = filterQos,
                  NoLocal = filterNoLocal,
                  RetainAsPublished = filterRetainAsPublished,
                  RetainHandling = filterRetainHandling
               }, HandlerExecutionStrategy.SequentialContinueOnError);
            }
            catch (Exception)
            {
               // ignored
            }
         });
      }

      if (filter.RetainHandling != RetainHandlingType.DoNotSend)
      {
         if (filter.RetainHandling == RetainHandlingType.SendAtSubscription ||
             (filter.RetainHandling == RetainHandlingType.SendOnNewSubscriptionOnly && !subscriptionExisted))
         {
            var matched = new List<MqttPublishMessage>();
            RetainedMessages.GetMatchingMessages(topicFilterBytes, matched);

            if (matched.Count > 0 && session.Client is { IsConnected: true } client)
            {
               var subId = subscriptionIdentifier;
               var localRetainAsPublished = filter.RetainAsPublished;
               _ = Task.Run(async () =>
               {
                  try
                  {
                     var propertiesBuffer = new byte[128];
                     foreach (var message in matched)
                     {
                        if (!client.IsConnected) break;

                        var targetQos = (QualityOfServiceType)Math.Min((int)qos, (int)message.QualityOfService);
                        var packetId = targetQos > 0 ? session.GenerateNextPacketIdentifier() : (ushort)0;

                        if (targetQos > 0)
                        {
                           session.AddUnacknowledgedPublish(new MqttPendingPublish
                           {
                              PacketIdentifier = packetId,
                              Message = message,
                              QualityOfService = targetQos,
                              RetainAsPublished = localRetainAsPublished,
                              SubscriptionIdentifier = subId
                           });
                        }

                        var retainAsPublished = client.ProtocolVersion is not MqttProtocolVersion.V50 || localRetainAsPublished;
                        await SendPublishMessageAsync(
                           client,
                           message,
                           targetQos,
                           retainAsPublished,
                           subId,
                           packetId,
                           dup: false,
                           propertiesBuffer,
                           CancellationToken.None);
                     }
                  }
                  catch (Exception)
                  {
                     // ignored
                  }
               });
            }
         }
      }

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
      return Unsubscribe(session, new ReadOnlySpan<byte>(topicFilter));
   }

   public UnsubscribeReasonCode Unsubscribe(MqttSession session, ReadOnlySpan<byte> topicFilter)
   {
      var alternateLookup = session.Subscriptions.GetAlternateLookup<ReadOnlySpan<byte>>();
      if (!alternateLookup.ContainsKey(topicFilter))
      {
         return UnsubscribeReasonCode.NoSubscriptionExisted;
      }

      SubscriptionRouter.Unsubscribe(session, topicFilter);

      if (Events.OnUnsubscribe.Count > 0)
      {
         var filterString = Encoding.UTF8.GetString(topicFilter);
         _ = Task.Run(async () =>
         {
            try
            {
               await Events.OnUnsubscribe.ExecuteAsync(
                  new MqttUnsubscribeContext() { Session = session, TopicFilter = filterString },
                  HandlerExecutionStrategy.SequentialContinueOnError);
            }
            catch (Exception)
            {
               // ignored
            }
         });
      }

      return UnsubscribeReasonCode.Success;
   }

   public UnsubscribeReasonCode Unsubscribe(MqttSession session, ReadOnlySequence<byte> topicFilter)
   {
      if (topicFilter.IsSingleSegment)
      {
         return Unsubscribe(session, topicFilter.FirstSpan);
      }

      var length = (int)topicFilter.Length;

      using var spanOwner = new SpanOwner<byte>(length);
      var span = spanOwner.Span;

      topicFilter.CopyTo(span);
      return Unsubscribe(session, span);
   }

   private static bool ValidateTopicFilter(ReadOnlySpan<byte> topicFilter)
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
