using Beskar.Mqtt.Common.Handlers;
using Beskar.Mqtt.Protocol.Packets;
using Beskar.Mqtt.Protocol.Results;

namespace Beskar.Mqtt.Client.Handlers;

public sealed class ClientPacketHandler(MqttClient client) : IPacketHandler
{
   private readonly MqttClient _client = client;

   public ValueTask ExecuteAsync(in AuthPacket packet, CancellationToken ct = default)
   {
      var result = AuthPacketResult.Create(in packet);
      _client.TryDispatch(in result, 0);

      return ValueTask.CompletedTask;
   }

   public ValueTask ExecuteAsync(in ConnAckPacket packet, CancellationToken ct = default)
   {
      var result = ClientConnectResult.Create(in packet);
      _client.TryDispatch(in result, 0);

      return ValueTask.CompletedTask;
   }

   public ValueTask ExecuteAsync(in ConnectPacket packet, CancellationToken ct = default)
   {
      throw new InvalidOperationException("CONNECT received by client is not supported.");
   }

   public ValueTask ExecuteAsync(in DisconnectPacket packet, CancellationToken ct = default)
   {
      throw new NotImplementedException();
   }

   public ValueTask ExecuteAsync(in PingReqPacket packet, CancellationToken ct = default)
   {
      _client.TryDispatch(in packet, 0);
      return ValueTask.CompletedTask;
   }

   public ValueTask ExecuteAsync(in PingRespPacket packet, CancellationToken ct = default)
   {
      _client.TryDispatch(in packet, 0);
      return ValueTask.CompletedTask;
   }

   public ValueTask ExecuteAsync(in PubAckPacket packet, CancellationToken ct = default)
   {
      _client.TryDispatch(in packet, packet.PacketIdentifier);
      return ValueTask.CompletedTask;
   }

   public ValueTask ExecuteAsync(in PubCompPacket packet, CancellationToken ct = default)
   {
      _client.TryDispatch(in packet, packet.PacketIdentifier);
      return ValueTask.CompletedTask;
   }

   public ValueTask ExecuteAsync(in PublishPacket packet, CancellationToken ct = default)
   {
      throw new NotImplementedException();
   }

   public ValueTask ExecuteAsync(in PubRecPacket packet, CancellationToken ct = default)
   {
      _client.TryDispatch(in packet, packet.PacketIdentifier);
      return ValueTask.CompletedTask;
   }

   public ValueTask ExecuteAsync(in PubRelPacket packet, CancellationToken ct = default)
   {
      _client.TryDispatch(in packet, packet.PacketIdentifier);
      return ValueTask.CompletedTask;
   }

   public ValueTask ExecuteAsync(in SubAckPacket packet, CancellationToken ct = default)
   {
      _client.TryDispatch(in packet, packet.PacketIdentifier);
      return ValueTask.CompletedTask;
   }

   public ValueTask ExecuteAsync(in SubscribePacket packet, CancellationToken ct = default)
   {
      throw new InvalidOperationException("SUB received by client is not supported.");
   }

   public ValueTask ExecuteAsync(in UnsubAckPacket packet, CancellationToken ct = default)
   {
      _client.TryDispatch(in packet, packet.PacketIdentifier);
      return ValueTask.CompletedTask;
   }

   public ValueTask ExecuteAsync(in UnsubscribePacket packet, CancellationToken ct = default)
   {
      throw new InvalidOperationException("UNSUB received by client is not supported.");
   }
}
