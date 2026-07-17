using System.Buffers;
using Beskar.Memory.Threading;
using Beskar.Mqtt.Common.Builders.Disconnecting;
using Beskar.Mqtt.Common.Handlers;
using Beskar.Mqtt.Common.Handlers.Contexts;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Extensions;
using Beskar.Mqtt.Protocol.Models;
using Beskar.Mqtt.Protocol.Packets;
using Beskar.Mqtt.Protocol.Results;
using Beskar.Utilities.Tracing;
using Beskar.Networking.Abstractions.Interfaces;

namespace Beskar.Mqtt.Client.Handlers;

public sealed class ClientPacketHandler(MqttClient client) : IPacketHandler
{
   private readonly MqttClient _client = client;

   public ValueTask ExecuteAsync(INetworkStream stream, in AuthPacket packet, CancellationToken ct = default)
   {
      TraceLogger.LogClientInfo("ClientPacketHandler: Received AUTH packet from server.");
      var result = AuthPacketResult.Create(in packet);
      _client.TryDispatch(in result, 0);

      return ValueTask.CompletedTask;
   }

   public ValueTask ExecuteAsync(INetworkStream stream, in ConnAckPacket packet, CancellationToken ct = default)
   {
      TraceLogger.LogClientInfo("ClientPacketHandler: Received CONNACK packet from server (ReasonCode: {0}).", packet.ReasonCode);
      var result = ClientConnectResult.Create(in packet);
      _client.TryDispatch(in result, 0);

      return ValueTask.CompletedTask;
   }

   public ValueTask ExecuteAsync(INetworkStream stream, in ConnectPacket packet, CancellationToken ct = default)
   {
      throw new InvalidOperationException("CONNECT received by client is not supported.");
   }

   public ValueTask ExecuteAsync(INetworkStream stream, in DisconnectPacket packet, CancellationToken ct = default)
   {
      TraceLogger.LogClientWarning("ClientPacketHandler: Received DISCONNECT packet from server (ReasonCode: {0}).", packet.ReasonCode);

      _client.UpdateDisconnectPacket(packet);
      return Awaited(packet);

      async ValueTask Awaited(DisconnectPacket packet)
      {
         await _client.HandleDisconnect(packet, ct);
      }
   }

   public ValueTask ExecuteAsync(INetworkStream stream, in PingReqPacket packet, CancellationToken ct = default)
   {
      throw new InvalidOperationException("PING_REQ received by client is not supported. (other way around)");
   }

   public ValueTask ExecuteAsync(INetworkStream stream, in PingRespPacket packet, CancellationToken ct = default)
   {
      TraceLogger.LogClientInfo("ClientPacketHandler: Received PINGRESP packet.");
      _client.TryDispatch(in packet, 0);
      return ValueTask.CompletedTask;
   }

   public ValueTask ExecuteAsync(INetworkStream stream, in PubAckPacket packet, CancellationToken ct = default)
   {
      TraceLogger.LogClientInfo("ClientPacketHandler: Received PUBACK packet (PacketId: {0}, ReasonCode: {1}).", packet.PacketIdentifier, packet.ReasonCode);
      _client.TryDispatch(in packet, packet.PacketIdentifier);
      return ValueTask.CompletedTask;
   }

   public ValueTask ExecuteAsync(INetworkStream stream, in PubCompPacket packet, CancellationToken ct = default)
   {
      TraceLogger.LogClientInfo("ClientPacketHandler: Received PUBCOMP packet (PacketId: {0}, ReasonCode: {1}).", packet.PacketIdentifier, packet.ReasonCode);
      _client.TryDispatch(in packet, packet.PacketIdentifier);
      return ValueTask.CompletedTask;
   }

   public ValueTask ExecuteAsync(INetworkStream stream, in PublishPacket packet, CancellationToken ct = default)
   {
      return Awaited(_client, packet, ct);

      static async ValueTask Awaited(MqttClient client, PublishPacket packet, CancellationToken ct)
      {
         var resolvedPacket = packet;

         if (client.ProtocolVersion is MqttProtocolVersion.V50)
         {
            if (packet.TopicAlias > 0)
            {
               var topicAliasMax = client.CurrentConnectOptions.TopicAliasMaximum ?? 0;
               if (packet.TopicAlias > topicAliasMax)
               {
                  TraceLogger.LogClientError("ClientPacketHandler: Received PUBLISH packet with TopicAlias {0} exceeding TopicAliasMaximum {1}.", packet.TopicAlias, topicAliasMax);
                  await client.DisconnectFromReceiveLoopAsync(new DisconnectOptions { ReasonCode = DisconnectReasonCode.TopicAliasInvalid }, ct);
                  return;
               }

               if (packet.TopicUtf8Bytes.IsEmpty)
               {
                  if (!client.TryGetTopicAlias(packet.TopicAlias, out var topicBytes))
                  {
                     TraceLogger.LogClientError("ClientPacketHandler: Received PUBLISH packet with unregistered TopicAlias {0}.", packet.TopicAlias);
                     await client.DisconnectFromReceiveLoopAsync(new DisconnectOptions { ReasonCode = DisconnectReasonCode.TopicAliasInvalid }, ct);
                     return;
                  }

                  resolvedPacket.TopicUtf8Bytes = new ReadOnlySequence<byte>(topicBytes);
               }
               else
               {
                  client.SetTopicAlias(packet.TopicAlias, packet.TopicUtf8Bytes.ToArray());
               }
            }
            else
            {
               if (packet.TopicUtf8Bytes.IsEmpty)
               {
                  TraceLogger.LogClientError("ClientPacketHandler: Received PUBLISH packet with empty topic and no TopicAlias.");
                  await client.DisconnectFromReceiveLoopAsync(new DisconnectOptions { ReasonCode = DisconnectReasonCode.ProtocolError }, ct);
                  return;
               }
            }
         }
         else // MQTT v3.x
         {
            if (packet.TopicUtf8Bytes.IsEmpty)
            {
               TraceLogger.LogClientError("ClientPacketHandler: Received PUBLISH packet with empty topic under v3.x.");
               await client.DisconnectFromReceiveLoopAsync(new DisconnectOptions { ReasonCode = DisconnectReasonCode.ProtocolError }, ct);
               return;
            }
         }

         TraceLogger.LogClientInfo("ClientPacketHandler: Received PUBLISH packet (PacketId: {0}, Topic: '{1}', QoS: {2}). Dispatching to receive handlers...", resolvedPacket.PacketIdentifier, resolvedPacket.TopicUtf8Bytes.GetUtf8String(), resolvedPacket.QualityOfService);

         var converted = new MqttPublishMessage(resolvedPacket);
         var context = new MessageReceiveContext()
         {
            Message = converted,
            PacketSender = client
         };

         // probably best here to defer actual handlers to run on new task?
         _ = Task.Run(async () =>
         {
            await client.Events.OnMessageReceive.ExecuteAsync(context, HandlerExecutionStrategy.SequentialContinueOnError, ct);

            if (context.AutoAcknowledge)
               await context.AcknowledgeAsync(ct);
         }, ct);
      }
   }

   public ValueTask ExecuteAsync(INetworkStream stream, in PubRecPacket packet, CancellationToken ct = default)
   {
      TraceLogger.LogClientInfo("ClientPacketHandler: Received PUBREC packet (PacketId: {0}, ReasonCode: {1}).", packet.PacketIdentifier, packet.ReasonCode);
      _client.TryDispatch(in packet, packet.PacketIdentifier);
      return ValueTask.CompletedTask;
   }

   public ValueTask ExecuteAsync(INetworkStream stream, in PubRelPacket packet, CancellationToken ct = default)
   {
      TraceLogger.LogClientInfo("ClientPacketHandler: Received PUBREL packet (PacketId: {0}, ReasonCode: {1}). Replying with PUBCOMP...", packet.PacketIdentifier, packet.ReasonCode);
      _client.TryDispatch(in packet, packet.PacketIdentifier);

      return Awaited(packet);

      async ValueTask Awaited(PubRelPacket packet)
      {
         var pubComp = new PubCompPacket
         {
            PacketIdentifier = packet.PacketIdentifier,
            ReasonCode = PubCompReasonCode.Success
         };
         await _client.SendAsync(pubComp, ct);
      }
   }

   public ValueTask ExecuteAsync(INetworkStream stream, in SubAckPacket packet, CancellationToken ct = default)
   {
      TraceLogger.LogClientInfo("ClientPacketHandler: Received SUBACK packet (PacketId: {0}).", packet.PacketIdentifier);
      _client.TryDispatch(in packet, packet.PacketIdentifier);
      return ValueTask.CompletedTask;
   }

   public ValueTask ExecuteAsync(INetworkStream stream, in SubscribePacket packet, CancellationToken ct = default)
   {
      throw new InvalidOperationException("SUB received by client is not supported.");
   }

   public ValueTask ExecuteAsync(INetworkStream stream, in UnsubAckPacket packet, CancellationToken ct = default)
   {
      TraceLogger.LogClientInfo("ClientPacketHandler: Received UNSUBACK packet (PacketId: {0}).", packet.PacketIdentifier);
      _client.TryDispatch(in packet, packet.PacketIdentifier);
      return ValueTask.CompletedTask;
   }

   public ValueTask ExecuteAsync(INetworkStream stream, in UnsubscribePacket packet, CancellationToken ct = default)
   {
      throw new InvalidOperationException("UNSUB received by client is not supported.");
   }
}
