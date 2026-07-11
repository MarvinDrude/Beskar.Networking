using System.Runtime.InteropServices;
using Beskar.Mqtt.Common.Handlers;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Packets;
using Beskar.Mqtt.Protocol.Parsing;
using Beskar.Mqtt.Protocol.Parsing.Results;
using Beskar.Utilities.Tracing;
using Beskar.Networking.Abstractions.Interfaces;

namespace Beskar.Mqtt.Common.Parsers.Version3;

[StructLayout(LayoutKind.Auto)]
public readonly ref partial struct PacketVersion3Parser(
   INetworkStream stream, IPacketHandler handler, MqttProtocolVersion protocolVersion)
{
   private readonly INetworkStream _stream = stream;
   private readonly IPacketHandler _packetHandler = handler;
   private readonly MqttProtocolVersion _protocolVersion = protocolVersion;

   public ValueTask<PacketDispatchResult> TryDispatch(
      ref RawPacket rawPacket,
      out int bytesConsumed,
      CancellationToken cancellation = default)
   {
      bytesConsumed = 0;

      var packetType = rawPacket.FixedHeader >> 4;
      if (packetType is < 1 or >= 15)
      {
         return ValueTask.FromResult(PacketDispatchResult.InvalidPacketType);
      }

      bytesConsumed = rawPacket.TotalLength;
      switch ((MqttPacketType)packetType)
      {
         // === Publishing
         case MqttPacketType.Publish:
            return DispatchPublish(ref rawPacket, cancellation);
         case MqttPacketType.PubAck:
            return DispatchPubAck(ref rawPacket, cancellation);
         case MqttPacketType.PubRec:
            return DispatchPubRec(ref rawPacket, cancellation);
         case MqttPacketType.PubRel:
            return DispatchPubRel(ref rawPacket, cancellation);
         case MqttPacketType.PubComp:
            return DispatchPubComp(ref rawPacket, cancellation);

         // === Pings
         case MqttPacketType.PingReq:
            return DispatchPingReq(ref rawPacket, cancellation);
         case MqttPacketType.PingResp:
            return DispatchPingResp(ref rawPacket, cancellation);

         // === Subscribing
         case MqttPacketType.Subscribe:
            return DispatchSubscribe(ref rawPacket, cancellation);
         case MqttPacketType.SubAck:
            return DispatchSubAck(ref rawPacket, cancellation);
         case MqttPacketType.Unsubscribe:
            return DispatchUnsubscribe(ref rawPacket, cancellation);
         case MqttPacketType.UnsubAck:
            return DispatchUnsubAck(ref rawPacket, cancellation);

         // === Connections
         case MqttPacketType.Connect:
            return DispatchConnect(ref rawPacket, cancellation);
         case MqttPacketType.ConnAck:
            return DispatchConnAck(ref rawPacket, cancellation);
         case MqttPacketType.Disconnect:
            return DispatchDisconnect(ref rawPacket, cancellation);
      }

      return ValueTask.FromResult(PacketDispatchResult.InvalidPacketType);
   }

   private ValueTask<PacketDispatchResult> DispatchPublish(
      ref RawPacket rawPacket, CancellationToken cancellation = default)
   {
      var packet = new PublishPacket();
      var result = TryParsePublishPacket(ref rawPacket, ref packet);

      if (result.Failed)
      {
         TraceLogger.LogNeutralError("Error at parsing PublishPacket: {0}", result.Error.Detail);
         return ValueTask.FromResult(PacketDispatchResult.ProtocolError);
      }

      var valueTask = _packetHandler.ExecuteAsync(_stream, in packet, cancellation);
      return valueTask.IsCompletedSuccessfully
         ? new ValueTask<PacketDispatchResult>(PacketDispatchResult.Success)
         : AwaitHandler(valueTask);
   }

   private ValueTask<PacketDispatchResult> DispatchPubAck(
      ref RawPacket rawPacket, CancellationToken cancellation = default)
   {
      var packet = new PubAckPacket();
      var result = TryParsePubAckPacket(ref rawPacket, ref packet);

      if (result.Failed)
      {
         TraceLogger.LogNeutralError("Error at parsing PubAckPacket: {0}", result.Error.Detail);
         return ValueTask.FromResult(PacketDispatchResult.ProtocolError);
      }

      var valueTask = _packetHandler.ExecuteAsync(_stream, in packet, cancellation);
      return valueTask.IsCompletedSuccessfully
         ? new ValueTask<PacketDispatchResult>(PacketDispatchResult.Success)
         : AwaitHandler(valueTask);
   }

   private ValueTask<PacketDispatchResult> DispatchPubRec(
      ref RawPacket rawPacket, CancellationToken cancellation = default)
   {
      var packet = new PubRecPacket();
      var result = TryParsePubRecPacket(ref rawPacket, ref packet);

      if (result.Failed)
      {
         TraceLogger.LogNeutralError("Error at parsing PubRecPacket: {0}", result.Error.Detail);
         return ValueTask.FromResult(PacketDispatchResult.ProtocolError);
      }

      var valueTask = _packetHandler.ExecuteAsync(_stream, in packet, cancellation);
      return valueTask.IsCompletedSuccessfully
         ? new ValueTask<PacketDispatchResult>(PacketDispatchResult.Success)
         : AwaitHandler(valueTask);
   }

   private ValueTask<PacketDispatchResult> DispatchPubRel(
      ref RawPacket rawPacket, CancellationToken cancellation = default)
   {
      var packet = new PubRelPacket();
      var result = TryParsePubRelPacket(ref rawPacket, ref packet);

      if (result.Failed)
      {
         TraceLogger.LogNeutralError("Error at parsing PubRelPacket: {0}", result.Error.Detail);
         return ValueTask.FromResult(PacketDispatchResult.ProtocolError);
      }

      var valueTask = _packetHandler.ExecuteAsync(_stream, in packet, cancellation);
      return valueTask.IsCompletedSuccessfully
         ? new ValueTask<PacketDispatchResult>(PacketDispatchResult.Success)
         : AwaitHandler(valueTask);
   }

   private ValueTask<PacketDispatchResult> DispatchPubComp(
      ref RawPacket rawPacket, CancellationToken cancellation = default)
   {
      var packet = new PubCompPacket();
      var result = TryParsePubCompPacket(ref rawPacket, ref packet);

      if (result.Failed)
      {
         TraceLogger.LogNeutralError("Error at parsing PubCompPacket: {0}", result.Error.Detail);
         return ValueTask.FromResult(PacketDispatchResult.ProtocolError);
      }

      var valueTask = _packetHandler.ExecuteAsync(_stream, in packet, cancellation);
      return valueTask.IsCompletedSuccessfully
         ? new ValueTask<PacketDispatchResult>(PacketDispatchResult.Success)
         : AwaitHandler(valueTask);
   }

   private ValueTask<PacketDispatchResult> DispatchSubscribe(
      ref RawPacket rawPacket, CancellationToken cancellation = default)
   {
      var packet = new SubscribePacket();
      var result = TryParseSubscribePacket(ref rawPacket, ref packet);

      if (result.Failed)
      {
         TraceLogger.LogNeutralError("Error at parsing SubscribePacket: {0}", result.Error.Detail);
         return ValueTask.FromResult(PacketDispatchResult.ProtocolError);
      }

      var valueTask = _packetHandler.ExecuteAsync(_stream, in packet, cancellation);
      return valueTask.IsCompletedSuccessfully
         ? new ValueTask<PacketDispatchResult>(PacketDispatchResult.Success)
         : AwaitHandler(valueTask);
   }

   private ValueTask<PacketDispatchResult> DispatchSubAck(
      ref RawPacket rawPacket, CancellationToken cancellation = default)
   {
      var packet = new SubAckPacket();
      var result = TryParseSubAckPacket(ref rawPacket, ref packet);

      if (result.Failed)
      {
         TraceLogger.LogNeutralError("Error at parsing SubAckPacket: {0}", result.Error.Detail);
         return ValueTask.FromResult(PacketDispatchResult.ProtocolError);
      }

      var valueTask = _packetHandler.ExecuteAsync(_stream, in packet, cancellation);
      return valueTask.IsCompletedSuccessfully
         ? new ValueTask<PacketDispatchResult>(PacketDispatchResult.Success)
         : AwaitHandler(valueTask);
   }

   private ValueTask<PacketDispatchResult> DispatchUnsubscribe(
      ref RawPacket rawPacket, CancellationToken cancellation = default)
   {
      var packet = new UnsubscribePacket();
      var result = TryParseUnsubscribePacket(ref rawPacket, ref packet);

      if (result.Failed)
      {
         TraceLogger.LogNeutralError("Error at parsing UnsubscribePacket: {0}", result.Error.Detail);
         return ValueTask.FromResult(PacketDispatchResult.ProtocolError);
      }

      var valueTask = _packetHandler.ExecuteAsync(_stream, in packet, cancellation);
      return valueTask.IsCompletedSuccessfully
         ? new ValueTask<PacketDispatchResult>(PacketDispatchResult.Success)
         : AwaitHandler(valueTask);
   }

   private ValueTask<PacketDispatchResult> DispatchUnsubAck(
      ref RawPacket rawPacket, CancellationToken cancellation = default)
   {
      var packet = new UnsubAckPacket();
      var result = TryParseUnsubAckPacket(ref rawPacket, ref packet);

      if (result.Failed)
      {
         TraceLogger.LogNeutralError("Error at parsing UnsubAckPacket: {0}", result.Error.Detail);
         return ValueTask.FromResult(PacketDispatchResult.ProtocolError);
      }

      var valueTask = _packetHandler.ExecuteAsync(_stream, in packet, cancellation);
      return valueTask.IsCompletedSuccessfully
         ? new ValueTask<PacketDispatchResult>(PacketDispatchResult.Success)
         : AwaitHandler(valueTask);
   }

   private ValueTask<PacketDispatchResult> DispatchPingReq(
      ref RawPacket rawPacket, CancellationToken cancellation = default)
   {
      var packet = new PingReqPacket();
      // ping request needs no parsing in v3

      var valueTask = _packetHandler.ExecuteAsync(_stream, in packet, cancellation);
      return valueTask.IsCompletedSuccessfully
         ? new ValueTask<PacketDispatchResult>(PacketDispatchResult.Success)
         : AwaitHandler(valueTask);
   }

   private ValueTask<PacketDispatchResult> DispatchPingResp(
      ref RawPacket rawPacket, CancellationToken cancellation = default)
   {
      var packet = new PingRespPacket();
      // ping response needs no parsing in v3

      var valueTask = _packetHandler.ExecuteAsync(_stream, in packet, cancellation);
      return valueTask.IsCompletedSuccessfully
         ? new ValueTask<PacketDispatchResult>(PacketDispatchResult.Success)
         : AwaitHandler(valueTask);
   }

   private ValueTask<PacketDispatchResult> DispatchConnect(
      ref RawPacket rawPacket, CancellationToken cancellation = default)
   {
      var packet = new ConnectPacket();
      var result = TryParseConnectPacket(ref rawPacket, ref packet);

      if (result.Failed)
      {
         TraceLogger.LogNeutralError("Error at parsing ConnectPacket: {0}", result.Error.Detail);
         return ValueTask.FromResult(PacketDispatchResult.ProtocolError);
      }

      var valueTask = _packetHandler.ExecuteAsync(_stream, in packet, cancellation);
      return valueTask.IsCompletedSuccessfully
         ? new ValueTask<PacketDispatchResult>(PacketDispatchResult.Success)
         : AwaitHandler(valueTask);
   }

   private ValueTask<PacketDispatchResult> DispatchConnAck(
      ref RawPacket rawPacket, CancellationToken cancellation = default)
   {
      var packet = new ConnAckPacket();
      var result = TryParseConnAckPacket(ref rawPacket, ref packet);

      if (result.Failed)
      {
         TraceLogger.LogNeutralError("Error at parsing ConnAckPacket: {0}", result.Error.Detail);
         return ValueTask.FromResult(PacketDispatchResult.ProtocolError);
      }

      var valueTask = _packetHandler.ExecuteAsync(_stream, in packet, cancellation);
      return valueTask.IsCompletedSuccessfully
         ? new ValueTask<PacketDispatchResult>(PacketDispatchResult.Success)
         : AwaitHandler(valueTask);
   }

   private ValueTask<PacketDispatchResult> DispatchDisconnect(
      ref RawPacket rawPacket, CancellationToken cancellation = default)
   {
      var packet = new DisconnectPacket();
      // disconnect needs no parsing in v3

      var valueTask = _packetHandler.ExecuteAsync(_stream, in packet, cancellation);
      return valueTask.IsCompletedSuccessfully
         ? new ValueTask<PacketDispatchResult>(PacketDispatchResult.Success)
         : AwaitHandler(valueTask);
   }

   private static async ValueTask<PacketDispatchResult> AwaitHandler(ValueTask task)
   {
      await task;
      return PacketDispatchResult.Success;
   }
}
