using System.Buffers;
using Beskar.Memory.Owners;
using Beskar.Memory.Threading;
using Beskar.Memory.Writers;
using Beskar.Mqtt.Common.Builders.Disconnecting;
using Beskar.Mqtt.Common.Encoders.Properties;
using Beskar.Mqtt.Common.Telemetry;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Extensions;
using Beskar.Mqtt.Protocol.Models;
using Beskar.Mqtt.Protocol.Packets;
using Beskar.Mqtt.Server.Contexts;
using Beskar.Mqtt.Server.Enumerators;
using Beskar.Mqtt.Server.Extensions;
using Beskar.Mqtt.Server.Internal;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Utilities.Tracing;

namespace Beskar.Mqtt.Server.Handlers;

public sealed partial class ServerPacketHandler
{
   public ValueTask ExecuteAsync(INetworkStream stream, in PublishPacket packet, CancellationToken ct = default)
   {
      if (!IsValid) return ValueTask.CompletedTask;

      var session = _client.MqttSession;
      if (session is null) return ValueTask.CompletedTask;

      return Awaited(_server, _client, stream, packet, session, ct);

      static async ValueTask Awaited(
         MqttServer server,
         MqttServerClient client,
         INetworkStream stream,
         PublishPacket packet,
         MqttSession session,
         CancellationToken ct)
      {
         if (packet.Dup)
         {
            MqttMetrics.RecordQosRetry();
         }

         var isQos1Or2 = packet.QualityOfService > QualityOfServiceType.AtMostOnce;
         var wasIncremented = false;

         if (client.ProtocolVersion is MqttProtocolVersion.V50 && isQos1Or2)
         {
            var receiveMax = server.Options.ReceiveMaximum;
            if (!session.TryIncrementIncomingInFlight(receiveMax, out var currentCount))
            {
               TraceLogger.LogServerError(
                  "ServerPacketHandler.Publish: Incoming in-flight QoS 1/2 publishes count {0} exceeds ReceiveMaximum {1} for client {2}.",
                  currentCount, receiveMax, client.ClientIdUtf8Bytes.GetUtf8String());

               await client.DisconnectAsync(new DisconnectOptions
                  { ReasonCode = DisconnectReasonCode.ReceiveMaximumExceeded });
               return;
            }

            wasIncremented = true;
         }

         var isNewRegistered = false;
         try
         {
            byte[] resolvedTopicBytes;
            if (client.ProtocolVersion is MqttProtocolVersion.V50)
            {
               if (packet.TopicAlias > 0)
               {
                  if (packet.TopicAlias > server.Options.TopicAliasMaximum)
                  {
                     await client.DisconnectAsync(new DisconnectOptions
                        { ReasonCode = DisconnectReasonCode.TopicAliasInvalid });
                     return;
                  }

                  if (packet.TopicUtf8Bytes.IsEmpty)
                  {
                     if (!client.TryGetTopicAlias(packet.TopicAlias, out var topicBytes))
                     {
                        await client.DisconnectAsync(new DisconnectOptions
                           { ReasonCode = DisconnectReasonCode.TopicAliasInvalid });
                        return;
                     }

                     MqttMetrics.RecordTopicAliasHit();
                     resolvedTopicBytes = topicBytes;
                  }
                  else
                  {
                     resolvedTopicBytes = packet.TopicUtf8Bytes.ToArray();
                     client.SetTopicAlias(packet.TopicAlias, resolvedTopicBytes);
                  }
               }
               else
               {
                  if (packet.TopicUtf8Bytes.IsEmpty)
                  {
                     await client.DisconnectAsync(
                        new DisconnectOptions { ReasonCode = DisconnectReasonCode.ProtocolError });
                     return;
                  }

                  resolvedTopicBytes = packet.TopicUtf8Bytes.ToArray();
               }
            }
            else // MQTT v3.x
            {
               if (packet.TopicUtf8Bytes.IsEmpty)
               {
                  await client.DisconnectAsync(
                     new DisconnectOptions { ReasonCode = DisconnectReasonCode.ProtocolError });
                  return;
               }

               resolvedTopicBytes = packet.TopicUtf8Bytes.ToArray();
            }

            var resolvedTopicSpan = new ReadOnlySpan<byte>(resolvedTopicBytes);
            if (resolvedTopicSpan.IsEmpty || resolvedTopicSpan.Contains((byte)0x23) ||
                resolvedTopicSpan.Contains((byte)0x2B))
            {
               await client.DisconnectAsync(
                  new DisconnectOptions { ReasonCode = DisconnectReasonCode.TopicNameInvalid });
               return;
            }

            var topicEnumerator = new TopicLevelEnumerator(resolvedTopicSpan);
            var levels = 0;
            var isDepthValid = true;

            while (topicEnumerator.MoveNext())
            {
               levels++;
               if (levels <= 64) continue;

               isDepthValid = false;
               break;
            }

            if (!isDepthValid)
            {
               await client.DisconnectAsync(
                  new DisconnectOptions { ReasonCode = DisconnectReasonCode.TopicNameInvalid });
               return;
            }

            switch (packet.QualityOfService)
            {
               case QualityOfServiceType.AtMostOnce: // QoS 0
               {
                  if (packet.Retain)
                  {
                     UpdateRetainedMessage(server, client, in packet, resolvedTopicBytes);
                  }

                  DispatchToSubscribers(server, client, in packet, resolvedTopicBytes, ct);
                  break;
               }
               case QualityOfServiceType.AtLeastOnce: // QoS 1
               {
                  if (packet.Retain)
                  {
                     UpdateRetainedMessage(server, client, in packet, resolvedTopicBytes);
                  }

                  DispatchToSubscribers(server, client, in packet, resolvedTopicBytes, ct);

                  var pubAck = new PubAckPacket
                  {
                     PacketIdentifier = packet.PacketIdentifier,
                     ReasonCode = PubAckReasonCode.Success
                  };
                  await stream.Send(in pubAck, client.ProtocolVersion, ct);

                  if (server.Events.OnAcknowledgePub.Count > 0)
                  {
                     await server.Events.OnAcknowledgePub.ExecuteAsync(
                        new MqttAcknowledgePubContext()
                           { Session = session, PublishMessage = new MqttPublishMessage(packet) },
                        HandlerExecutionStrategy.SequentialContinueOnError, ct);
                  }

                  break;
               }
               case QualityOfServiceType.ExactlyOnce: // QoS 2
               {
                  var isNew = session.TryAddQos2Packet(packet.PacketIdentifier);
                  if (isNew)
                  {
                     isNewRegistered = true;
                     if (packet.Retain)
                     {
                        UpdateRetainedMessage(server, client, in packet, resolvedTopicBytes);
                     }

                     DispatchToSubscribers(server, client, in packet, resolvedTopicBytes, ct);
                  }

                  var pubRec = new PubRecPacket
                  {
                     PacketIdentifier = packet.PacketIdentifier,
                     ReasonCode = PubRecReasonCode.Success
                  };

                  try
                  {
                     await stream.Send(in pubRec, client.ProtocolVersion, ct);
                  }
                  catch
                  {
                     if (isNewRegistered)
                     {
                        session.RemoveQos2Packet(packet.PacketIdentifier);
                        isNewRegistered = false;
                     }
                     throw;
                  }

                  if (server.Events.OnAcknowledgePub.Count > 0)
                  {
                     await server.Events.OnAcknowledgePub.ExecuteAsync(
                        new MqttAcknowledgePubContext()
                           { Session = session, PublishMessage = new MqttPublishMessage(packet) },
                        HandlerExecutionStrategy.SequentialContinueOnError, ct);
                  }

                  break;
               }
            }
         }
         finally
         {
            if (wasIncremented)
            {
               var qos2Duplicate = packet.QualityOfService == QualityOfServiceType.ExactlyOnce && !isNewRegistered;
               if (packet.QualityOfService == QualityOfServiceType.AtLeastOnce || qos2Duplicate)
               {
                  session.DecrementIncomingInFlight();
               }
            }
         }
      }

      static void DispatchToSubscribers(
         MqttServer server,
         MqttServerClient publisherClient,
         in PublishPacket packet,
         byte[] resolvedTopicBytes,
         CancellationToken ct)
      {
         var publishMessage = CreatePublishMessage(in packet, resolvedTopicBytes);

         var visitor = new PublishMessageDispatcherVisitor(publisherClient, publishMessage);
         server.SubscriptionRouter.Route(resolvedTopicBytes, ref visitor);

         if (visitor.MatchCount != 0 || publisherClient.MqttSession is null) return;
         var session = publisherClient.MqttSession;

         if (server.Events.OnNoSubscriberMessage.Count <= 0) return;
         var publishMessageContext = new MqttPublishMessage(packet);

         _ = Task.Run(async () =>
         {
            try
            {
               await server.Events.OnNoSubscriberMessage.ExecuteAsync(
                  new MqttNoSubscriberMessageContext() { Session = session, PublishMessage = publishMessageContext },
                  HandlerExecutionStrategy.SequentialContinueOnError, ct);
            }
            catch (Exception)
            {
               // ignored
            }
         }, ct);
      }
   }

   private static MqttPublishMessage CreatePublishMessage(in PublishPacket packet, byte[] resolvedTopicBytes)
   {
      var topicSequence = new System.Buffers.ReadOnlySequence<byte>(resolvedTopicBytes);
      return new MqttPublishMessage(new PublishPacket
      {
         Dup = packet.Dup,
         QualityOfService = packet.QualityOfService,
         Retain = packet.Retain,
         TopicUtf8Bytes = topicSequence,
         Payload = packet.Payload,
         PacketIdentifier = packet.PacketIdentifier,
         PayloadFormat = packet.PayloadFormat,
         MessageExpiryInterval = packet.MessageExpiryInterval,
         TopicAlias = packet.TopicAlias,
         ResponseTopicUtf8Bytes = packet.ResponseTopicUtf8Bytes,
         CorrelationDataBytes = packet.CorrelationDataBytes,
         ContentTypeUtf8Bytes = packet.ContentTypeUtf8Bytes,
         PropertiesBytes = packet.PropertiesBytes
      });
   }

   private static void UpdateRetainedMessage(MqttServer server, MqttServerClient client, in PublishPacket packet,
      byte[] resolvedTopicBytes)
   {
      var msg = CreatePublishMessage(in packet, resolvedTopicBytes);
      var clientIdStr = System.Text.Encoding.UTF8.GetString(client.ClientIdUtf8Bytes.Span);
      var changed = server.RetainedMessages.UpdateMessage(clientIdStr, msg);

      if (changed && server.Events.OnRetainedMessageChanged.Count > 0)
      {
         var stored = server.RetainedMessages.GetMessages();
         _ = Task.Run(async () =>
         {
            try
            {
               await server.Events.OnRetainedMessageChanged.ExecuteAsync(new MqttRetainedMessageChangedContext
               {
                  ClientId = clientIdStr,
                  ChangedRetainedMessage = msg.Payload.IsEmpty ? null : msg,
                  StoredRetainedMessages = stored
               }, HandlerExecutionStrategy.SequentialContinueOnError);
            }
            catch (Exception)
            {
               // ignored
            }
         });
      }
   }

   internal struct PublishMessageDispatcherVisitor(
      MqttServerClient publisherClient,
      MqttPublishMessage message)
      : ISubscriptionVisitor
   {
      private readonly MqttServerClient _publisherClient = publisherClient;
      private readonly MqttPublishMessage _message = message;

      public int MatchCount;

      public void Visit(in MqttSubscription subscription)
      {
         MatchCount++;
         var session = subscription.Session;
         if (subscription.NoLocal && _publisherClient is not null && session == _publisherClient.MqttSession)
         {
            return;
         }

         var targetQos =
            (QualityOfServiceType)Math.Min((int)_message.QualityOfService, (int)subscription.QualityOfService);

         var subscriberClient = session.Client;
         if (subscriberClient is not null && subscriberClient.IsConnected)
         {
            var localClient = subscriberClient;
            var localSession = session;
            var localQos = targetQos;
            var localRetain = subscription.RetainAsPublished && _message.Retain;
            var localRetainAsPublished = subscription.RetainAsPublished;
            var localSubId = subscription.SubscriptionIdentifier;
            var localMsg = _message;

            ushort packetId = 0;
            if (localQos > 0)
            {
               if (localSession.GetUnacknowledgedPublishCount() >= localSession.ClientReceiveMaximum)
               {
                  var queuedMessage = new MqttQueuedMessage(localMsg, localQos, localRetainAsPublished, localSubId);
                  localSession.EnqueueOfflineMessage(queuedMessage);

                  return;
               }

               packetId = localSession.GenerateNextPacketIdentifier();
               localSession.AddUnacknowledgedPublish(new MqttPendingPublish
               {
                  PacketIdentifier = packetId,
                  Message = localMsg,
                  QualityOfService = localQos,
                  RetainAsPublished = localRetainAsPublished,
                  SubscriptionIdentifier = localSubId
               });
            }

            try
            {
               var remainingExpiry = localMsg.MessageExpiryInterval;
               if (localMsg.MessageExpiryInterval > 0)
               {
                  var timeSpent = (uint)(DateTimeOffset.UtcNow - localMsg.CreatedAt).TotalSeconds;
                  if (timeSpent >= localMsg.MessageExpiryInterval)
                  {
                     if (localQos > 0)
                     {
                        localSession.AcknowledgePublish(packetId);
                     }

                     return; // Expired, do not deliver
                  }

                  remainingExpiry = localMsg.MessageExpiryInterval - timeSpent;
               }

               var topicBytes = System.Text.Encoding.UTF8.GetBytes(localMsg.Topic);
               var responseTopicBytes = string.IsNullOrEmpty(localMsg.ResponseTopic)
                  ? ReadOnlyMemory<byte>.Empty
                  : System.Text.Encoding.UTF8.GetBytes(localMsg.ResponseTopic);
               var contentTypeBytes = string.IsNullOrEmpty(localMsg.ContentType)
                  ? ReadOnlyMemory<byte>.Empty
                  : System.Text.Encoding.UTF8.GetBytes(localMsg.ContentType);

               var publishPacket = new PublishPacket
               {
                  Dup = false,
                  QualityOfService = localQos,
                  Retain = localRetain,
                  TopicUtf8Bytes = new ReadOnlySequence<byte>(topicBytes),
                  Payload = new ReadOnlySequence<byte>(localMsg.Payload),
                  PacketIdentifier = packetId,
                  PayloadFormat = localMsg.PayloadFormat,
                  MessageExpiryInterval = remainingExpiry,
                  TopicAlias = 0,
                  ResponseTopicUtf8Bytes = new ReadOnlySequence<byte>(responseTopicBytes),
                  CorrelationDataBytes = localMsg.CorrelationData.HasValue
                     ? new ReadOnlySequence<byte>(localMsg.CorrelationData.Value)
                     : ReadOnlySequence<byte>.Empty,
                  ContentTypeUtf8Bytes = new ReadOnlySequence<byte>(contentTypeBytes),
                  PropertiesBytes = ReadOnlySequence<byte>.Empty
               };

               if (localClient.ProtocolVersion is MqttProtocolVersion.V50)
               {
                  Span<byte> buffer = stackalloc byte[32];
                  var writer = new ByteWriter(buffer);

                  try
                  {
                     var propEncoder = writer.AsPublishPropertyEncoder();
                     try
                     {
                        if (localSubId > 0)
                        {
                           propEncoder.WriteSubscriptionIdentifier(localSubId);
                        }

                        if (localMsg.PayloadFormat is not PayloadFormat.Unspecified)
                        {
                           propEncoder.WritePayloadFormatIndicator(localMsg.PayloadFormat);
                        }

                        if (remainingExpiry > 0)
                        {
                           propEncoder.WriteMessageExpiryInterval(remainingExpiry);
                        }

                        if (!responseTopicBytes.IsEmpty)
                        {
                           propEncoder.WriteResponseTopic(responseTopicBytes.Span);
                        }

                        if (localMsg.CorrelationData.HasValue)
                        {
                           propEncoder.WriteCorrelationData(localMsg.CorrelationData.Value.Span);
                        }

                        if (!contentTypeBytes.IsEmpty)
                        {
                           propEncoder.WriteContentType(contentTypeBytes.Span);
                        }

                        if (localMsg.UserProperties.Count > 0)
                        {
                           var enumerator = localMsg.UserProperties.GetDirectEnumerator();
                           while (enumerator.MoveNext())
                           {
                              if (enumerator.Current.Identifier is not PropertyIdentifier.UserProperty)
                                 continue;

                              var userProperty = enumerator.Current.AsUserProperty();
                              propEncoder.WriteUserProperty(userProperty.KeyBytes, userProperty.ValueBytes);
                           }
                        }
                     }
                     finally
                     {
                        writer = propEncoder.Encoder.Writer;
                     }

                     publishPacket.PropertiesBytes = new ReadOnlySequence<byte>([.. writer.WrittenSpan]);
                  }
                  finally
                  {
                     writer.Dispose();
                  }
               }

               localClient.QueueOutgoingPublish(in publishPacket);
            }
            catch (Exception ex)
            {
               TraceLogger.LogServerError("MqttServer: Error dispatching publish message to client '{0}': {1}",
                  localClient.ClientIdUtf8Bytes.GetUtf8String(), ex.Message);
            }
         }
         else if (targetQos > 0)
         {
            if (session.IsExpired)
            {
               _ = Task.Run(async () =>
               {
                  try
                  {
                     await session.Server.ClientSessions.RemoveSessionAsync(session);
                  }
                  catch (Exception)
                  {
                     /* ignored */
                  }
               });
            }
            else if (session.ExpiryInterval > 0)
            {
               var queuedMessage = new MqttQueuedMessage(_message, targetQos, subscription.RetainAsPublished,
                  subscription.SubscriptionIdentifier);

               session.EnqueueOfflineMessage(queuedMessage);
            }
         }
      }
   }
}
