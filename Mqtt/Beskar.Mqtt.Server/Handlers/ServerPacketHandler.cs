using System.Buffers;
using System.Diagnostics.CodeAnalysis;
using System.Net;
using Beskar.Memory.Owners;
using Beskar.Mqtt.Common.Builders.Connecting;
using Beskar.Mqtt.Common.Builders.Disconnecting;
using Beskar.Mqtt.Common.Handlers;
using Beskar.Mqtt.Common.Encoders.Properties;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Packets;
using Beskar.Mqtt.Protocol.Results;
using Beskar.Mqtt.Protocol.Models;
using Beskar.Mqtt.Protocol.Extensions;
using Beskar.Mqtt.Server.Extensions;
using Beskar.Mqtt.Server.Internal;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Abstractions.Interfaces.Pools;
using Beskar.Memory.Writers;
using Beskar.Utilities.Tracing;

namespace Beskar.Mqtt.Server.Handlers;

public sealed class ServerPacketHandler
   : IPacketHandler, IPooledObject
{
   [MemberNotNullWhen(true, nameof(_server), nameof(_client))]
   public bool IsValid => _server is not null && _client is not null && _client.IsConnected;

   private MqttServer? _server;
   private MqttServerClient? _client;

   public void Initialize(
      MqttServer server, MqttServerClient client)
   {
      _server = server;
      _client = client;
   }

   public bool TryResetState()
   {
      _server = null;
      _client = null;

      return true;
   }

   public ValueTask ExecuteAsync(INetworkStream stream, in AuthPacket packet, CancellationToken ct = default)
   {
      throw new NotImplementedException();
   }

   public ValueTask ExecuteAsync(INetworkStream stream, in ConnAckPacket packet, CancellationToken ct = default)
   {
      throw new NotImplementedException();
   }

   public ValueTask ExecuteAsync(INetworkStream stream, in ConnectPacket packet, CancellationToken ct = default)
   {
      if (!IsValid) return ValueTask.CompletedTask;

      stream.Session.Properties.TryGet("MqttProtocolVersion", out MqttProtocolVersion protocolVersion);
      if (protocolVersion is MqttProtocolVersion.Unknown)
      {
         protocolVersion = MqttProtocolVersion.V50;
      }

      _client.ProtocolVersion = protocolVersion;

      var result = ConnectOptions.Create(in packet, protocolVersion, (IPEndPoint)stream.Session.RemoteAddress);
      _client.PushControlPacket(result);

      return ValueTask.CompletedTask;
   }

   public ValueTask ExecuteAsync(INetworkStream stream, in DisconnectPacket packet, CancellationToken ct = default)
   {
      if (!IsValid) return ValueTask.CompletedTask;

      _client.DisconnectOptions = DisconnectOptions.Create(in packet);
      return ValueTask.CompletedTask;
   }

   public ValueTask ExecuteAsync(INetworkStream stream, in PingReqPacket packet, CancellationToken ct = default)
   {
      if (!IsValid) return ValueTask.CompletedTask;
      return Awaited(_client.ProtocolVersion);

      async ValueTask Awaited(MqttProtocolVersion protocolVersion)
      {
         var pingResp = new PingRespPacket();
         await stream.Send(in pingResp, protocolVersion, ct);
      }
   }

   public ValueTask ExecuteAsync(INetworkStream stream, in PingRespPacket packet, CancellationToken ct = default)
   {
      // ping responses should not come from the client
      return ValueTask.CompletedTask;
   }

   public ValueTask ExecuteAsync(INetworkStream stream, in PubAckPacket packet, CancellationToken ct = default)
   {
      // should not be sent by the client
      return ValueTask.CompletedTask;
   }

   public ValueTask ExecuteAsync(INetworkStream stream, in PubCompPacket packet, CancellationToken ct = default)
   {
      // should not be sent by the client
      return ValueTask.CompletedTask;
   }

   public ValueTask ExecuteAsync(INetworkStream stream, in PubRecPacket packet, CancellationToken ct = default)
   {
      // should not be sent by the client
      return ValueTask.CompletedTask;
   }

   public ValueTask ExecuteAsync(INetworkStream stream, in PubRelPacket packet, CancellationToken ct = default)
   {
      if (!IsValid) return ValueTask.CompletedTask;

      var session = _client.MqttSession;
      if (session is null) return ValueTask.CompletedTask;

      return Awaited(_client, stream, packet, session, ct);

      static async ValueTask Awaited(
         MqttServerClient client,
         INetworkStream stream,
         PubRelPacket packet,
         MqttSession session,
         CancellationToken ct)
      {
         session.RemoveQos2Packet(packet.PacketIdentifier);

         var pubComp = new PubCompPacket
         {
            PacketIdentifier = packet.PacketIdentifier,
            ReasonCode = PubCompReasonCode.Success
         };

         await stream.Send(in pubComp, client.ProtocolVersion, ct);
      }
   }

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
         byte[] resolvedTopicBytes;
         if (client.ProtocolVersion is MqttProtocolVersion.V50)
         {
            if (packet.TopicAlias > 0)
            {
               if (packet.TopicUtf8Bytes.IsEmpty)
               {
                  if (!client.TryGetTopicAlias(packet.TopicAlias, out var topicBytes))
                  {
                     await client.DisconnectAsync(new DisconnectOptions
                        { ReasonCode = DisconnectReasonCode.TopicAliasInvalid });
                     return;
                  }

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
               await client.DisconnectAsync(new DisconnectOptions { ReasonCode = DisconnectReasonCode.ProtocolError });
               return;
            }

            resolvedTopicBytes = packet.TopicUtf8Bytes.ToArray();
         }

         var resolvedTopicSpan = new ReadOnlySpan<byte>(resolvedTopicBytes);
         if (resolvedTopicSpan.IsEmpty || resolvedTopicSpan.Contains((byte)0x23) ||
             resolvedTopicSpan.Contains((byte)0x2B))
         {
            await client.DisconnectAsync(new DisconnectOptions { ReasonCode = DisconnectReasonCode.TopicNameInvalid });
            return;
         }

         switch (packet.QualityOfService)
         {
            case QualityOfServiceType.AtMostOnce: // QoS 0
            {
               DispatchToSubscribers(server, client, in packet, resolvedTopicBytes);
               break;
            }
            case QualityOfServiceType.AtLeastOnce: // QoS 1
            {
               DispatchToSubscribers(server, client, in packet, resolvedTopicBytes);

               var pubAck = new PubAckPacket
               {
                  PacketIdentifier = packet.PacketIdentifier,
                  ReasonCode = PubAckReasonCode.Success
               };
               await stream.Send(in pubAck, client.ProtocolVersion, ct);
               break;
            }
            case QualityOfServiceType.ExactlyOnce: // QoS 2
            {
               var isNew = session.TryAddQos2Packet(packet.PacketIdentifier);
               if (isNew)
               {
                  DispatchToSubscribers(server, client, in packet, resolvedTopicBytes);
               }

               var pubRec = new PubRecPacket
               {
                  PacketIdentifier = packet.PacketIdentifier,
                  ReasonCode = PubRecReasonCode.Success
               };
               await stream.Send(in pubRec, client.ProtocolVersion, ct);
               break;
            }
         }
      }

      static void DispatchToSubscribers(
         MqttServer server,
         MqttServerClient publisherClient,
         in PublishPacket packet,
         byte[] resolvedTopicBytes)
      {
         var topicSequence = new ReadOnlySequence<byte>(resolvedTopicBytes);
         var publishMessage = new MqttPublishMessage(new PublishPacket
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

         var visitor = new PublishMessageDispatcherVisitor(publisherClient, publishMessage);
         server.SubscriptionRouter.Route(resolvedTopicBytes, ref visitor);
      }
   }

   public ValueTask ExecuteAsync(INetworkStream stream, in SubAckPacket packet, CancellationToken ct = default)
   {
      // should not be handled on the server
      return ValueTask.CompletedTask;
   }

   public ValueTask ExecuteAsync(INetworkStream stream, in SubscribePacket packet, CancellationToken ct = default)
   {
      if (!IsValid) return ValueTask.CompletedTask;

      var session = _client.MqttSession;
      if (session is null) return ValueTask.CompletedTask;

      return Awaited(_server, _client, stream, packet, session, ct);

      static async ValueTask Awaited(MqttServer server, MqttServerClient client, INetworkStream stream,
         SubscribePacket packet, MqttSession session, CancellationToken ct)
      {
         var filtersEnumerator = packet.GetFilters();
         var countEnumerator = filtersEnumerator;
         var filterCount = 0;

         while (countEnumerator.MoveNext())
         {
            filterCount++;
         }

         if (filterCount == 0) return;

         using var memoryOwner = new MemoryOwner<byte>(filterCount);
         var returnCodes = memoryOwner.Memory;
         var returnCodesSpan = returnCodes.Span;

         var index = 0;
         while (filtersEnumerator.MoveNext())
         {
            var filter = filtersEnumerator.Current;
            var reasonCode = server.Subscribe(session, in filter, packet.SubscriptionIdentifier);

            returnCodesSpan[index++] = (byte)reasonCode;
         }

         var subAck = new SubAckPacket
         {
            PacketIdentifier = packet.PacketIdentifier,
            ReturnCodesBytes = returnCodes,
            ReasonStringUtf8Bytes = ReadOnlyMemory<byte>.Empty,
            PropertiesBytes = ReadOnlyMemory<byte>.Empty
         };

         await stream.Send(in subAck, client.ProtocolVersion, ct);
      }
   }

   public ValueTask ExecuteAsync(INetworkStream stream, in UnsubAckPacket packet, CancellationToken ct = default)
   {
      // should not be handled on the server
      return ValueTask.CompletedTask;
   }

   public ValueTask ExecuteAsync(INetworkStream stream, in UnsubscribePacket packet, CancellationToken ct = default)
   {
      if (!IsValid) return ValueTask.CompletedTask;

      var session = _client.MqttSession;
      if (session is null) return ValueTask.CompletedTask;

      return Awaited(_server, _client, stream, packet, session, ct);

      static async ValueTask Awaited(MqttServer server, MqttServerClient client, INetworkStream stream,
         UnsubscribePacket packet, MqttSession session, CancellationToken ct)
      {
         var filtersEnumerator = packet.GetFilters();
         var countEnumerator = filtersEnumerator;
         var filterCount = 0;

         while (countEnumerator.MoveNext())
         {
            filterCount++;
         }

         if (filterCount == 0) return;

         using var memoryOwner = new MemoryOwner<byte>(filterCount);
         var reasonCodes = memoryOwner.Memory;
         var reasonCodeSpan = reasonCodes.Span;

         var index = 0;
         while (filtersEnumerator.MoveNext())
         {
            var filterSequence = filtersEnumerator.Current;
            var reasonCode = server.Unsubscribe(session, filterSequence);

            reasonCodeSpan[index++] = (byte)reasonCode;
         }

         var unsubAck = new UnsubAckPacket
         {
            PacketIdentifier = packet.PacketIdentifier,
            ReasonCodesBytes = reasonCodes,
            ReasonStringUtf8Bytes = ReadOnlyMemory<byte>.Empty,
            PropertiesBytes = ReadOnlyMemory<byte>.Empty
         };

         await stream.Send(in unsubAck, client.ProtocolVersion, ct);
      }
   }

   private readonly struct PublishMessageDispatcherVisitor(
      MqttServerClient publisherClient, MqttPublishMessage message)
      : ISubscriptionVisitor
   {
      private readonly MqttServerClient _publisherClient = publisherClient;
      private readonly MqttPublishMessage _message = message;

      public void Visit(in MqttSubscription subscription)
      {
         var session = subscription.Session;
         if (subscription.NoLocal && session == _publisherClient.MqttSession)
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
            var localSubId = subscription.SubscriptionIdentifier;
            var localMsg = _message;

            _ = Task.Run(async () =>
            {
               try
               {
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
                     PacketIdentifier = localQos > 0 ? localSession.GenerateNextPacketIdentifier() : (ushort)0,
                     PayloadFormat = localMsg.PayloadFormat,
                     MessageExpiryInterval = localMsg.MessageExpiryInterval,
                     TopicAlias = 0,
                     ResponseTopicUtf8Bytes = new ReadOnlySequence<byte>(responseTopicBytes),
                     CorrelationDataBytes = localMsg.CorrelationData.HasValue
                        ? new ReadOnlySequence<byte>(localMsg.CorrelationData.Value)
                        : ReadOnlySequence<byte>.Empty,
                     ContentTypeUtf8Bytes = new ReadOnlySequence<byte>(contentTypeBytes),
                     PropertiesBytes = ReadOnlySequence<byte>.Empty
                  };

                  if (localSubId > 0 && localClient.ProtocolVersion is MqttProtocolVersion.V50)
                  {
                     var buffer = new byte[16];
                     var writer = new ByteWriter(buffer);
                     var propEncoder = writer.AsPublishPropertyEncoder();
                     propEncoder.WriteSubscriptionIdentifier(localSubId);

                     var written = propEncoder.Encoder.Writer.Position;
                     publishPacket.PropertiesBytes = new ReadOnlySequence<byte>(buffer.AsMemory(0, written));
                  }

                  await localClient.Stream.Send(in publishPacket, localClient.ProtocolVersion);
               }
               catch (Exception ex)
               {
                  TraceLogger.LogServerError("MqttServer: Error dispatching publish message to client '{0}': {1}",
                     localClient.ClientIdUtf8Bytes.GetUtf8String(), ex.Message);
               }
            });
         }
         else if (targetQos > 0)
         {
            var queuedMessage = new MqttQueuedMessage(_message, targetQos, subscription.RetainAsPublished,
               subscription.SubscriptionIdentifier);

            session.EnqueueOfflineMessage(queuedMessage);
         }
      }
   }
}
