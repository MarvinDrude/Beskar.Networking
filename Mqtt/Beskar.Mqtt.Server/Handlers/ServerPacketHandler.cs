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
using Beskar.Memory.Writers;
using Beskar.Mqtt.Common.Options;
using Beskar.Mqtt.Server.Options;
using Beskar.Memory.Threading;
using Beskar.Mqtt.Server.Contexts;
using Beskar.Utilities.Tracing;

namespace Beskar.Mqtt.Server.Handlers;

public sealed partial class ServerPacketHandler
   : IPacketHandler
{
   [MemberNotNullWhen(true, nameof(_server), nameof(_client))]
   public bool IsValid => _server is not null && _client is not null && _client.IsConnected && !_client.IsFenced;

   [MemberNotNullWhen(true, nameof(_server), nameof(_client))]
   private bool TryGetActiveSession([NotNullWhen(true)] out MqttSession? session)
   {
      if (!IsValid)
      {
         session = null;
         return false;
      }

      session = _client.MqttSession;
      return session is not null && session.IsActiveClient(_client);
   }

   private MqttServer? _server;
   private MqttServerClient? _client;

   public void Initialize(
      MqttServer server, MqttServerClient client)
   {
      _server = server;
      _client = client;
   }

   public ValueTask ExecuteAsync(INetworkStream stream, in AuthPacket packet, CancellationToken ct = default)
   {
      if (!IsValid) return ValueTask.CompletedTask;

      if (_client.IsHandshakeCompleted)
      {
         TraceLogger.LogServerWarning(
            "ServerPacketHandler: Received unsupported mid-session AUTH packet (ReasonCode: {0}). Disconnecting with ProtocolError.",
            packet.ReasonCode);
         return new ValueTask(_client.DisconnectAsync(new DisconnectOptions { ReasonCode = DisconnectReasonCode.ProtocolError }));
      }

      _client.PushControlPacket(new AuthPacketOptions(packet));
      return ValueTask.CompletedTask;
   }

   public ValueTask ExecuteAsync(INetworkStream stream, in ConnAckPacket packet, CancellationToken ct = default)
   {
      // should not be sent by a client.
      return ValueTask.CompletedTask;
   }

   public ValueTask ExecuteAsync(INetworkStream stream, in ConnectPacket packet, CancellationToken ct = default)
   {
      if (!IsValid) return ValueTask.CompletedTask;

      // MQTT §3.1.0: a client MUST only send CONNECT once over a network connection.
      if (!_client.TryMarkConnectReceived())
      {
         TraceLogger.LogServerWarning("ServerPacketHandler: Received duplicate CONNECT packet on existing connection. Disconnecting with ProtocolError.");
         return new ValueTask(_client.DisconnectAsync(new DisconnectOptions
         {
            ReasonCode = DisconnectReasonCode.ProtocolError
         }));
      }

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

      TraceLogger.LogServerInfo("ServerPacketHandler: Received DISCONNECT packet. SessionExpiryInterval: {0}",
         packet.SessionExpiryInterval);

      if (_client.ProtocolVersion is MqttProtocolVersion.V50 &&
          packet.SessionExpiryInterval is > 0 &&
          (_client.ConnectOptions?.SessionExpiryInterval ?? 0) == 0)
      {
         TraceLogger.LogServerWarning(
            "ServerPacketHandler: Client attempted to set non-zero SessionExpiryInterval ({0}) in DISCONNECT after zero session expiry.",
            packet.SessionExpiryInterval);

         var protocolError = new DisconnectOptions { ReasonCode = DisconnectReasonCode.ProtocolError };
         _client.DisconnectOptions = protocolError;
         return Awaited(_server, _client, protocolError);
      }

      _client.DisconnectOptions = DisconnectOptions.Create(in packet);
      return Awaited(_server, _client, null);

      static async ValueTask Awaited(MqttServer server, MqttServerClient client, DisconnectOptions? replyOptions)
      {
         await server.ClientSessions.HandleClientDisconnectAsync(client);
         await client.DisconnectAsync(replyOptions);
      }
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
        if (!TryGetActiveSession(out var session)) return ValueTask.CompletedTask;

        var pending = session.AcknowledgePublish(packet.PacketIdentifier);
        if (pending is not null)
        {
           _ = Task.Run(() => MqttServer.DeliverNextQueuedMessagesAsync(session), ct);
        }

        if (pending is null || _server.Events.OnPublishAcknowledged.Count <= 0)
           return ValueTask.CompletedTask;

        var server = _server;
        _ = Task.Run(async () =>
        {
           try
           {
              await server.Events.OnPublishAcknowledged.ExecuteAsync(new MqttPublishAcknowledgedContext
              {
                 Session = session,
                 PendingPublish = pending
              }, HandlerExecutionStrategy.SequentialContinueOnError, ct);
           }
           catch (Exception)
           {
              // ignored
           }
        }, ct);

        return ValueTask.CompletedTask;
    }

    public ValueTask ExecuteAsync(INetworkStream stream, in PubCompPacket packet, CancellationToken ct = default)
    {
        if (!IsValid) return ValueTask.CompletedTask;

        var session = _client.MqttSession;
        var pending = session?.AcknowledgePublish(packet.PacketIdentifier);

        if (session is not null && pending is not null)
        {
           _ = Task.Run(() => MqttServer.DeliverNextQueuedMessagesAsync(session), ct);
        }

        if (session is null || pending is null || _server.Events.OnPublishAcknowledged.Count <= 0)
           return ValueTask.CompletedTask;

        var server = _server;
        _ = Task.Run(async () =>
        {
           try
           {
              await server.Events.OnPublishAcknowledged.ExecuteAsync(new MqttPublishAcknowledgedContext
              {
                 Session = session,
                 PendingPublish = pending
              }, HandlerExecutionStrategy.SequentialContinueOnError, ct);
           }
           catch (Exception)
           {
              // ignored
           }
        }, ct);

        return ValueTask.CompletedTask;
    }

     public ValueTask ExecuteAsync(INetworkStream stream, in PubRecPacket packet, CancellationToken ct = default)
     {
        if (!TryGetActiveSession(out var session)) return ValueTask.CompletedTask;

        var pending = session.PeekUnacknowledgedPublish(packet.PacketIdentifier);
        if (pending is null) return ValueTask.CompletedTask;

        if (packet.ReasonCode >= PubRecReasonCode.UnspecifiedError)
        {
           TraceLogger.LogServerWarning("ServerPacketHandler: Received failed PUBREC (PacketId: {0}, ReasonCode: {1}). Aborting QoS 2 transaction.",
              packet.PacketIdentifier, packet.ReasonCode);

           session.AcknowledgePublish(packet.PacketIdentifier);
           _ = Task.Run(() => MqttServer.DeliverNextQueuedMessagesAsync(session), ct);

           if (_server.Events.OnPublishAcknowledged.Count > 0)
           {
              var server = _server;
              _ = Task.Run(async () =>
              {
                 try
                 {
                    await server.Events.OnPublishAcknowledged.ExecuteAsync(new MqttPublishAcknowledgedContext
                    {
                       Session = session,
                       PendingPublish = pending
                    }, HandlerExecutionStrategy.SequentialContinueOnError, ct);
                 }
                 catch (Exception)
                 {
                    // ignored
                 }
              }, ct);
           }

           return ValueTask.CompletedTask;
        }

        pending.MarkPubRecReceived();
        return Awaited(stream, packet, _client, ct);

        static async ValueTask Awaited(INetworkStream stream, PubRecPacket packet, MqttServerClient client, CancellationToken ct)
        {
           var pubRel = new PubRelPacket
           {
              PacketIdentifier = packet.PacketIdentifier,
              ReasonCode = PubRelReasonCode.Success
           };

           await stream.Send(in pubRel, client.ProtocolVersion, ct);
        }
     }

   public ValueTask ExecuteAsync(INetworkStream stream, in PubRelPacket packet, CancellationToken ct = default)
   {
      if (!TryGetActiveSession(out var session)) return ValueTask.CompletedTask;

      return Awaited(_client, stream, packet, session, ct);

      static async ValueTask Awaited(
         MqttServerClient client,
         INetworkStream stream,
         PubRelPacket packet,
         MqttSession session,
         CancellationToken ct)
      {
         var removed = session.TryRemoveQos2Packet(packet.PacketIdentifier);
         if (removed && client.ProtocolVersion is MqttProtocolVersion.V50)
         {
            session.DecrementIncomingInFlight();
         }

         var pubComp = new PubCompPacket
         {
            PacketIdentifier = packet.PacketIdentifier,
            ReasonCode = removed ? PubCompReasonCode.Success : PubCompReasonCode.PacketIdentifierNotFound
         };

         await stream.Send(in pubComp, client.ProtocolVersion, ct);
      }
   }

   public ValueTask ExecuteAsync(INetworkStream stream, in SubAckPacket packet, CancellationToken ct = default)
   {
      // should not be handled on the server
      return ValueTask.CompletedTask;
   }

   public ValueTask ExecuteAsync(INetworkStream stream, in SubscribePacket packet, CancellationToken ct = default)
   {
      if (!TryGetActiveSession(out var session)) return ValueTask.CompletedTask;

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
            if (!client.IsConnected || client.IsFenced || !session.IsActiveClient(client))
            {
               return;
            }

            var filter = filtersEnumerator.Current;
            var reasonCode = server.Subscribe(session, in filter, packet.SubscriptionIdentifier);

            returnCodesSpan[index++] = (byte)reasonCode;
         }

         if (!client.IsConnected || client.IsFenced || !session.IsActiveClient(client))
         {
            return;
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
      if (!TryGetActiveSession(out var session)) return ValueTask.CompletedTask;

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
            if (!client.IsConnected || client.IsFenced || !session.IsActiveClient(client))
            {
               return;
            }

            var filterSequence = filtersEnumerator.Current;
            var reasonCode = server.Unsubscribe(session, filterSequence);

            reasonCodeSpan[index++] = (byte)reasonCode;
         }

         if (!client.IsConnected || client.IsFenced || !session.IsActiveClient(client))
         {
            return;
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
}
