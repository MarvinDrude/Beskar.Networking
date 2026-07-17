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
using Beskar.Mqtt.Common.Options;
using Beskar.Mqtt.Server.Options;
using Beskar.Memory.Threading;
using Beskar.Mqtt.Server.Contexts;
using Beskar.Utilities.Tracing;

namespace Beskar.Mqtt.Server.Handlers;

public sealed partial class ServerPacketHandler
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
      if (!IsValid) return ValueTask.CompletedTask;

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
        if (!IsValid) return ValueTask.CompletedTask;

        var session = _client.MqttSession;
        if (session is null) return ValueTask.CompletedTask;

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
       if (!IsValid) return ValueTask.CompletedTask;

       var session = _client.MqttSession;
       var pending = session?.PeekUnacknowledgedPublish(packet.PacketIdentifier);

       if (pending is null) return ValueTask.CompletedTask;
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
         if (client.ProtocolVersion is MqttProtocolVersion.V50)
         {
            session.DecrementIncomingInFlight();
         }

         var pubComp = new PubCompPacket
         {
            PacketIdentifier = packet.PacketIdentifier,
            ReasonCode = PubCompReasonCode.Success
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
}
