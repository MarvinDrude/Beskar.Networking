using System.Diagnostics.CodeAnalysis;
using System.Net;
using Beskar.Mqtt.Common.Builders.Connecting;
using Beskar.Mqtt.Common.Handlers;
using Beskar.Mqtt.Protocol.Packets;
using Beskar.Mqtt.Protocol.Results;
using Beskar.Mqtt.Server.Internal;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Abstractions.Interfaces.Pools;

namespace Beskar.Mqtt.Server.Handlers;

public sealed class ServerPacketHandler
   : IPacketHandler, IPooledObject
{
   [MemberNotNullWhen(true, nameof(_server), nameof(_client))]
   public bool IsValid => _server is not null && _client is not null;

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

      var result = ConnectOptions.Create(in packet, (IPEndPoint)stream.Session.RemoteAddress);
      _client.PushControlPacket(result);

      return ValueTask.CompletedTask;
   }

   public ValueTask ExecuteAsync(INetworkStream stream, in DisconnectPacket packet, CancellationToken ct = default)
   {
      throw new NotImplementedException();
   }

   public ValueTask ExecuteAsync(INetworkStream stream, in PingReqPacket packet, CancellationToken ct = default)
   {
      throw new NotImplementedException();
   }

   public ValueTask ExecuteAsync(INetworkStream stream, in PingRespPacket packet, CancellationToken ct = default)
   {
      throw new NotImplementedException();
   }

   public ValueTask ExecuteAsync(INetworkStream stream, in PubAckPacket packet, CancellationToken ct = default)
   {
      throw new NotImplementedException();
   }

   public ValueTask ExecuteAsync(INetworkStream stream, in PubCompPacket packet, CancellationToken ct = default)
   {
      throw new NotImplementedException();
   }

   public ValueTask ExecuteAsync(INetworkStream stream, in PublishPacket packet, CancellationToken ct = default)
   {
      throw new NotImplementedException();
   }

   public ValueTask ExecuteAsync(INetworkStream stream, in PubRecPacket packet, CancellationToken ct = default)
   {
      throw new NotImplementedException();
   }

   public ValueTask ExecuteAsync(INetworkStream stream, in PubRelPacket packet, CancellationToken ct = default)
   {
      throw new NotImplementedException();
   }

   public ValueTask ExecuteAsync(INetworkStream stream, in SubAckPacket packet, CancellationToken ct = default)
   {
      throw new NotImplementedException();
   }

   public ValueTask ExecuteAsync(INetworkStream stream, in SubscribePacket packet, CancellationToken ct = default)
   {
      throw new NotImplementedException();
   }

   public ValueTask ExecuteAsync(INetworkStream stream, in UnsubAckPacket packet, CancellationToken ct = default)
   {
      throw new NotImplementedException();
   }

   public ValueTask ExecuteAsync(INetworkStream stream, in UnsubscribePacket packet, CancellationToken ct = default)
   {
      throw new NotImplementedException();
   }
}
