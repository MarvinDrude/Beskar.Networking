using System.Buffers;
using System.IO.Pipelines;
using System.Net;
using System.Net.Sockets;
using System.Reflection;
using System.Text;
using Beskar.Memory.Results;
using Beskar.Memory.Threading;
using Beskar.Memory.Writers;
using Beskar.Mqtt.Client;
using Beskar.Mqtt.Common.Builders.Connecting;
using Beskar.Mqtt.Common.Builders.Disconnecting;
using Beskar.Mqtt.Common.Builders.Publishing;
using Beskar.Mqtt.Common.Builders.Subscribing;
using Beskar.Mqtt.Common.Encoders;
using Beskar.Mqtt.Common.Encoders.Properties;
using Beskar.Mqtt.Common.Handlers.Contexts;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Models;
using Beskar.Mqtt.Protocol.Packets;
using Beskar.Mqtt.Server;
using Beskar.Mqtt.Server.Handlers;
using Beskar.Mqtt.Server.Internal;
using Beskar.Mqtt.Server.Options;
using Beskar.Networking.Abstractions.Enums;
using Beskar.Networking.Abstractions.Errors;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Abstractions.Models;
using Beskar.Networking.Abstractions.Threading;

namespace Beskar.Mqtt.Common.Tests.Internal;

public class MqttServerSessionLifecycleTests
{
   [Test]
   [NotInParallel]
   public async Task Subscribe_RetainAsPublishedFalse_InitialRetainedMessageShouldPreserveRetainFlag()
   {
      await using var server = MqttServerFactory.CreateBuilder()
         .UseTcp(0)
         .WithDefaultClientIdGenerator()
         .Build();

      var startResult = await server.StartAsync();
      await Assert.That(startResult.Failed).IsFalse();

      var port = ((IPEndPoint)server.Listeners[0].LocalAddress).Port;

      var publisher = (MqttClient)MqttClientFactory.CreateTcp();
      await publisher.ConnectAsync(new ConnectOptions
      {
         EndPoint = new IPEndPoint(IPAddress.Loopback, port),
         ProtocolVersion = MqttProtocolVersion.V50,
         ClientIdUtf8Bytes = "rap-publisher"u8.ToArray()
      });

      var pubRes = await publisher.PublishAsync(new PublishOptionsBuilder()
         .WithTopic("rap/initial")
         .WithPayload("retained-value")
         .WithQualityOfService(QualityOfServiceType.AtLeastOnce)
         .WithRetain(true)
         .Build());
      await Assert.That(pubRes.Failed).IsFalse();
      await publisher.DisconnectAsync(new DisconnectOptions());

      var subscriber = (MqttClient)MqttClientFactory.CreateTcp();
      var receivedRetainTcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
      subscriber.Events.OnMessageReceive.Add((ctx, _) =>
      {
         receivedRetainTcs.TrySetResult(ctx.Message.Retain);
         return ValueTask.CompletedTask;
      });

      await subscriber.ConnectAsync(new ConnectOptions
      {
         EndPoint = new IPEndPoint(IPAddress.Loopback, port),
         ProtocolVersion = MqttProtocolVersion.V50,
         ClientIdUtf8Bytes = "rap-subscriber"u8.ToArray()
      });

      await subscriber.SubscribeAsync(new SubscribeOptionsBuilder()
         .WithTopicFilter("rap/initial", QualityOfServiceType.AtLeastOnce, retainAsPublished: false)
         .Build());

      var retainFlag = await receivedRetainTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
      await Assert.That(retainFlag).IsTrue();

      await subscriber.DisconnectAsync(new DisconnectOptions());
   }

   [Test]
   public async Task WillMessage_ZeroSessionExpiryWithWillDelayInterval_ShouldBePublishedWhenConnectionDrops()
   {
      await using var server = MqttServerFactory.CreateBuilder()
         .UseTcp(0)
         .WithDefaultClientIdGenerator()
         .Build();

      var startResult = await server.StartAsync();
      await Assert.That(startResult.Failed).IsFalse();

      var port = ((IPEndPoint)server.Listeners[0].LocalAddress).Port;

      var subscriber = (MqttClient)MqttClientFactory.CreateTcp();
      var willReceivedTcs = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
      subscriber.Events.OnMessageReceive.Add((ctx, _) =>
      {
         willReceivedTcs.TrySetResult(Encoding.UTF8.GetString(ctx.Message.Payload.Span));
         return ValueTask.CompletedTask;
      });

      await subscriber.ConnectAsync(new ConnectOptions
      {
         EndPoint = new IPEndPoint(IPAddress.Loopback, port),
         ProtocolVersion = MqttProtocolVersion.V50,
         ClientIdUtf8Bytes = "will-observer"u8.ToArray()
      });

      await subscriber.SubscribeAsync(new SubscribeOptionsBuilder()
         .WithTopicFilter("will/immediate", QualityOfServiceType.AtLeastOnce)
         .Build());

      // Connect publisher with SessionExpiryInterval = 0 and WillDelayInterval = 30
      var publisher = (MqttClient)MqttClientFactory.CreateTcp();
      await publisher.ConnectAsync(new ConnectOptionsBuilder(new IPEndPoint(IPAddress.Loopback, port))
         .WithProtocolVersion(MqttProtocolVersion.V50)
         .WithClientId("will-drop-client")
         .WithCleanSession(true)
         .WithSessionExpiryInterval(0)
         .WithWill("will/immediate", "offline"u8.ToArray(), QualityOfServiceType.AtLeastOnce)
         .WithWillDelayInterval(30)
         .Build());

      // Force ungraceful transport drop without sending DISCONNECT
      using (var clients = await server.ClientSessions.GetClients())
      {
         foreach (var sc in clients.WrittenSpan.ToArray())
         {
            if (sc.ClientIdUtf8Bytes.Span.SequenceEqual("will-drop-client"u8))
            {
               await sc.Session.DisposeAsync();
               break;
            }
         }
      }

      // Per MQTT 5.0 3.1.3.2.2, Will is published at min(WillDelayInterval, SessionExpiryInterval) = immediate
      var willPayload = await willReceivedTcs.Task.WaitAsync(TimeSpan.FromSeconds(3));
      await Assert.That(willPayload).IsEqualTo("offline");

      await subscriber.DisconnectAsync(new DisconnectOptions());
   }

   [Test]
   public async Task Disconnect_NonZeroSessionExpiryAfterZeroConnect_ShouldSignalProtocolError()
   {
      await using var server = MqttServerFactory.CreateBuilder()
         .UseTcp(0)
         .WithDefaultClientIdGenerator()
         .Build();

      var startResult = await server.StartAsync();
      await Assert.That(startResult.Failed).IsFalse();

      var port = ((IPEndPoint)server.Listeners[0].LocalAddress).Port;
      var serverDisconnectReasonTcs = new TaskCompletionSource<DisconnectReasonCode>(TaskCreationOptions.RunContinuationsAsynchronously);
      server.Events.OnDisconnect.Add((ctx, _) =>
      {
         serverDisconnectReasonTcs.TrySetResult(ctx.Reason);
         return ValueTask.CompletedTask;
      });

      using var tcpClient = new TcpClient();
      await tcpClient.ConnectAsync(IPAddress.Loopback, port);
      using var stream = tcpClient.GetStream();

      // CONNECT V5 with CleanStart = true, SessionExpiryInterval omitted (= 0)
      byte[] connectPacket =
      [
         0x10, 0x16,
         0x00, 0x04, (byte)'M', (byte)'Q', (byte)'T', (byte)'T',
         0x05, 0x02, 0x00, 0x3C,
         0x00,
         0x00, 0x09, (byte)'s', (byte)'e', (byte)'s', (byte)'s', (byte)'-', (byte)'p', (byte)'r', (byte)'o', (byte)'t'
      ];
      await stream.WriteAsync(connectPacket);
      await stream.FlushAsync();

      var buf = new byte[128];
      var connAckLen = await stream.ReadAsync(buf);
      await Assert.That(connAckLen).IsGreaterThanOrEqualTo(4);

      // DISCONNECT with ReasonCode=0x00 and SessionExpiryInterval(0x11)=60
      byte[] disconnectPacket =
      [
         0xE0, 0x07,
         0x00, // NormalDisconnection
         0x05, // Properties Length = 5
         0x11, 0x00, 0x00, 0x00, 0x3C // SessionExpiryInterval = 60
      ];
      await stream.WriteAsync(disconnectPacket);
      await stream.FlushAsync();

      var receivedProtocolErrorPacket = false;
      using (var readCts = new CancellationTokenSource(TimeSpan.FromSeconds(2)))
      {
         try
         {
            var n = await stream.ReadAsync(buf, readCts.Token);
            if (n >= 3 && (buf[0] >> 4) == (int)MqttPacketType.Disconnect && buf[2] == (byte)DisconnectReasonCode.ProtocolError)
            {
               receivedProtocolErrorPacket = true;
            }
         }
         catch (OperationCanceledException)
         {
         }
         catch (IOException)
         {
         }
      }

      var disconnectReason = await serverDisconnectReasonTcs.Task.WaitAsync(TimeSpan.FromSeconds(2));
      await Assert.That(receivedProtocolErrorPacket || disconnectReason == DisconnectReasonCode.ProtocolError).IsTrue();
   }

   [Test]
   public async Task DeliverOfflineMessagesAsync_WhileDeliverySemaphoreHeld_ShouldWaitForSemaphore()
   {
      var options = new MqttServerOptions();
      var server = new MqttServer([], options);
      var stream = new MockNetworkStream();
      var connContext = new NetworkServerConnectionContext(new DummyNetworkListener(), stream.Session);
      var streamContext = new NetworkServerStreamContext(connContext, stream);

      var client = new MqttServerClient();
      client.Initialize(streamContext, options);
      client.ProtocolVersion = MqttProtocolVersion.V50;

      var session = new MqttSession(server, client);
      client.MqttSession = session;

      var queuedMessage = new MqttPublishMessage(new PublishPacket
      {
         TopicUtf8Bytes = new ReadOnlySequence<byte>("offline/sem"u8.ToArray()),
         Payload = new ReadOnlySequence<byte>("hello"u8.ToArray()),
         QualityOfService = QualityOfServiceType.AtLeastOnce
      });
      session.EnqueueOfflineMessage(new MqttQueuedMessage(queuedMessage, QualityOfServiceType.AtLeastOnce, false, 0));

      var method = typeof(MqttServer).GetMethod("DeliverOfflineMessagesAsync", BindingFlags.NonPublic | BindingFlags.Static);
      await Assert.That(method).IsNotNull();

      await session.DeliverySemaphore.WaitAsync();
      try
      {
         var deliverTask = (Task)method!.Invoke(null, [client, session, CancellationToken.None])!;
         await Task.Delay(150);

         // DeliverOfflineMessagesAsync MUST NOT execute concurrently while DeliverySemaphore is held
         await Assert.That(deliverTask.IsCompleted).IsFalse();
      }
      finally
      {
         session.DeliverySemaphore.Release();
      }
   }

   [Test]
   public async Task DeliverNextQueuedMessagesAsync_WhenSemaphoreBrieflyContended_ShouldStillDrainQueue()
   {
      var options = new MqttServerOptions();
      var server = new MqttServer([], options);
      var stream = new MockNetworkStream();
      var connContext = new NetworkServerConnectionContext(new DummyNetworkListener(), stream.Session);
      var streamContext = new NetworkServerStreamContext(connContext, stream);

      var client = new MqttServerClient();
      client.Initialize(streamContext, options);
      client.ProtocolVersion = MqttProtocolVersion.V50;

      var session = new MqttSession(server, client);
      client.MqttSession = session;

      var queuedMessage = new MqttPublishMessage(new PublishPacket
      {
         TopicUtf8Bytes = new ReadOnlySequence<byte>("queue/wakeup"u8.ToArray()),
         Payload = new ReadOnlySequence<byte>("payload"u8.ToArray()),
         QualityOfService = QualityOfServiceType.AtLeastOnce
      });

      await session.DeliverySemaphore.WaitAsync();
      session.EnqueueOfflineMessage(new MqttQueuedMessage(queuedMessage, QualityOfServiceType.AtLeastOnce, false, 0));

      var triggerTask = MqttServer.DeliverNextQueuedMessagesAsync(session);
      await Task.Delay(30);
      session.DeliverySemaphore.Release();
      await triggerTask;

      await Task.Delay(150);

      // The queued message must eventually be dequeued and delivered once semaphore is released
      await Assert.That(session.OfflineQueueCount).IsEqualTo(0);
   }

   [Test]
   public async Task PersistentSession_QoS2AfterPubRecReconnect_ShouldResendPubRelInsteadOfPublish()
   {
      var serverOptions = new MqttServerOptions
      {
         SupportPersistentSessions = true
      };

      await using var server = MqttServerFactory.CreateBuilder(serverOptions)
         .UseTcp(0)
         .WithDefaultClientIdGenerator()
         .Build();

      var startResult = await server.StartAsync();
      await Assert.That(startResult.Failed).IsFalse();

      var port = ((IPEndPoint)server.Listeners[0].LocalAddress).Port;
      var clientId = "qos2-pubrec-resume";

      // 1. Connect persistent subscriber client via raw TCP and subscribe with QoS 2
      byte[] connectCleanStart = BuildConnectPacket(clientId, cleanStart: true, sessionExpiryInterval: 60);
      using (var tcp1 = new TcpClient())
      {
         await tcp1.ConnectAsync(IPAddress.Loopback, port);
         using var stream1 = tcp1.GetStream();

         await stream1.WriteAsync(connectCleanStart);
         await stream1.FlushAsync();
         var buf = new byte[1024];
         var n1 = await stream1.ReadAsync(buf);
         await Assert.That(n1).IsGreaterThanOrEqualTo(4);

         var subBytes = BuildSubscribePacket(packetId: 1, topicFilter: "qos2/resume", qosByte: 0x02);
         await stream1.WriteAsync(subBytes);
         await stream1.FlushAsync();
         var subAckLen = await stream1.ReadAsync(buf);
         await Assert.That(subAckLen).IsGreaterThanOrEqualTo(5);

         // 2. Another client publishes a QoS 2 message to "qos2/resume"
         var publisher = (MqttClient)MqttClientFactory.CreateTcp();
         await publisher.ConnectAsync(new ConnectOptions
         {
            EndPoint = new IPEndPoint(IPAddress.Loopback, port),
            ProtocolVersion = MqttProtocolVersion.V50,
            ClientIdUtf8Bytes = "qos2-pub-sender"u8.ToArray()
         });

         await publisher.PublishAsync(new PublishOptionsBuilder()
            .WithTopic("qos2/resume")
            .WithPayload("exact-once")
            .WithQualityOfService(QualityOfServiceType.ExactlyOnce)
            .Build());
         await publisher.DisconnectAsync(new DisconnectOptions());

         // 3. Receive incoming QoS 2 PUBLISH, send PUBREC ONLY, then drop socket
         var pubLen = await stream1.ReadAsync(buf);
         await Assert.That(pubLen).IsGreaterThan(0);
         await Assert.That((MqttPacketType)(buf[0] >> 4)).IsEqualTo(MqttPacketType.Publish);

         var packetId = ExtractPublishPacketId(buf.AsSpan(0, pubLen));
         byte[] pubRec = [0x50, 0x02, (byte)(packetId >> 8), (byte)packetId];
         await stream1.WriteAsync(pubRec);
         await stream1.FlushAsync();

         await Task.Delay(150);
      }

      // Wait briefly for server disconnect teardown while keeping session
      await Task.Delay(200);

      // 4. Reconnect with CleanStart = false and inspect the retransmitted packet
      using var tcp2 = new TcpClient();
      await tcp2.ConnectAsync(IPAddress.Loopback, port);
      using var stream2 = tcp2.GetStream();

      byte[] connectResume = BuildConnectPacket(clientId, cleanStart: false, sessionExpiryInterval: 60);
      await stream2.WriteAsync(connectResume);
      await stream2.FlushAsync();

      var recvBuf = new byte[1024];
      var totalRead = 0;
      using var readCts = new CancellationTokenSource(TimeSpan.FromSeconds(3));
      MqttPacketType? resumedPacketType = null;

      while (resumedPacketType is null && !readCts.IsCancellationRequested)
      {
         var n = await stream2.ReadAsync(recvBuf.AsMemory(totalRead), readCts.Token);
         if (n <= 0) break;
         totalRead += n;

         var pos = 0;
         while (pos < totalRead)
         {
            if (!TryReadPacketSlice(recvBuf.AsSpan(pos, totalRead - pos), out var type, out var consumed))
            {
               break;
            }

            if (type != MqttPacketType.ConnAck)
            {
               resumedPacketType = type;
               break;
            }

            pos += consumed;
         }
      }

      await Assert.That(resumedPacketType).IsEqualTo(MqttPacketType.PubRel);
   }

   [Test]
   public async Task Publish_WithTopicAliasAndEmptyTopic_ShouldResolveTopicForServerEvents()
   {
      var options = new MqttServerOptions
      {
         TopicAliasMaximum = 10
      };
      var server = new MqttServer([], options);
      var stream = new MockNetworkStream();
      var connContext = new NetworkServerConnectionContext(new DummyNetworkListener(), stream.Session);
      var streamContext = new NetworkServerStreamContext(connContext, stream);

      var client = new MqttServerClient();
      client.Initialize(streamContext, options);
      client.ProtocolVersion = MqttProtocolVersion.V50;

      var session = new MqttSession(server, client);
      client.MqttSession = session;

      var ackTopics = new List<string>();
      var noSubTopics = new List<string>();
      var noSubDone = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

      server.Events.OnAcknowledgePub.Add((ctx, _) =>
      {
         ackTopics.Add(ctx.PublishMessage.Topic);
         return ValueTask.CompletedTask;
      });
      server.Events.OnNoSubscriberMessage.Add((ctx, _) =>
      {
         lock (noSubTopics)
         {
            noSubTopics.Add(ctx.PublishMessage.Topic);
            if (noSubTopics.Count >= 2)
            {
               noSubDone.TrySetResult();
            }
         }
         return ValueTask.CompletedTask;
      });

      var handler = new ServerPacketHandler();
      handler.Initialize(server, client);

      var firstPacket = new PublishPacket
      {
         QualityOfService = QualityOfServiceType.AtLeastOnce,
         PacketIdentifier = 1,
         TopicUtf8Bytes = new ReadOnlySequence<byte>("sensors/alias"u8.ToArray()),
         Payload = new ReadOnlySequence<byte>("1"u8.ToArray()),
         TopicAlias = 1,
         PropertiesBytes = ReadOnlySequence<byte>.Empty
      };
      await handler.ExecuteAsync(stream, in firstPacket);

      var secondPacket = new PublishPacket
      {
         QualityOfService = QualityOfServiceType.AtLeastOnce,
         PacketIdentifier = 2,
         TopicUtf8Bytes = ReadOnlySequence<byte>.Empty,
         Payload = new ReadOnlySequence<byte>("2"u8.ToArray()),
         TopicAlias = 1,
         PropertiesBytes = ReadOnlySequence<byte>.Empty
      };
      await handler.ExecuteAsync(stream, in secondPacket);

      await noSubDone.Task.WaitAsync(TimeSpan.FromSeconds(3));

      await Assert.That(ackTopics).Count().IsEqualTo(2);
      await Assert.That(ackTopics[1]).IsEqualTo("sensors/alias");
      await Assert.That(noSubTopics).Count().IsEqualTo(2);
      await Assert.That(noSubTopics[1]).IsEqualTo("sensors/alias");
   }

   [Test]
   public async Task StopAsync_WithConnectedClient_ShouldNotifyWithServerShuttingDownAndDisconnect()
   {
      await using var server = MqttServerFactory.CreateBuilder()
         .UseTcp(0)
         .WithDefaultClientIdGenerator()
         .Build();

      var startResult = await server.StartAsync();
      await Assert.That(startResult.Failed).IsFalse();

      var port = ((IPEndPoint)server.Listeners[0].LocalAddress).Port;

      var client = (MqttClient)MqttClientFactory.CreateTcp();
      var disconnectedTcs = new TaskCompletionSource<ClientDisconnectedContext>(TaskCreationOptions.RunContinuationsAsynchronously);
      client.Events.OnClientDisconnected.Add((ctx, _) =>
      {
         disconnectedTcs.TrySetResult(ctx);
         return ValueTask.CompletedTask;
      });

      var connectRes = await client.ConnectAsync(new ConnectOptions
      {
         EndPoint = new IPEndPoint(IPAddress.Loopback, port),
         ProtocolVersion = MqttProtocolVersion.V50,
         ClientIdUtf8Bytes = "shutdown-notify-client"u8.ToArray()
      });
      await Assert.That(connectRes.Failed).IsFalse();

      await server.StopAsync(new DisconnectOptions
      {
         ReasonCode = DisconnectReasonCode.ServerShuttingDown
      });

      var disconnectedCtx = await disconnectedTcs.Task.WaitAsync(TimeSpan.FromSeconds(2));
      await Assert.That(disconnectedCtx.ReasonCode).IsEqualTo(DisconnectReasonCode.ServerShuttingDown);
   }

   private static byte[] BuildConnectPacket(string clientId, bool cleanStart, uint sessionExpiryInterval)
   {
      var propBuf = new byte[16];
      var pw = new ByteWriter(propBuf);
      pw.AsConnectPropertyEncoder().WriteSessionExpiryInterval(sessionExpiryInterval);

      var clientIdBytes = Encoding.UTF8.GetBytes(clientId);
      var bodyLen = 6 + 1 + 1 + 2 + 1 + 5 + 2 + clientIdBytes.Length;
      var packet = new byte[2 + bodyLen];
      var writer = new ByteWriter(packet);
      PacketEncoder.WriteFixedHeader(ref writer, MqttPacketType.Connect, 0, bodyLen);
      writer.WriteBigEndian((ushort)4);
      writer.WriteBytes("MQTT"u8);
      writer.WriteByte(5);
      writer.WriteByte(cleanStart ? (byte)0x02 : (byte)0x00);
      writer.WriteBigEndian((ushort)60);
      writer.WriteByte(5);
      writer.WriteByte((byte)PropertyIdentifier.SessionExpiryInterval);
      writer.WriteBigEndian(sessionExpiryInterval);
      writer.WriteBigEndian((ushort)clientIdBytes.Length);
      writer.WriteBytes(clientIdBytes);
      return packet[..writer.Position];
   }

   private static byte[] BuildSubscribePacket(ushort packetId, string topicFilter, byte qosByte)
   {
      var filterBytes = Encoding.UTF8.GetBytes(topicFilter);
      var bodyLen = 2 + 1 + 2 + filterBytes.Length + 1;
      var packet = new byte[2 + bodyLen];
      var writer = new ByteWriter(packet);
      PacketEncoder.WriteFixedHeader(ref writer, MqttPacketType.Subscribe, 0x02, bodyLen);
      writer.WriteBigEndian(packetId);
      writer.WriteByte(0); // Props length = 0
      writer.WriteBigEndian((ushort)filterBytes.Length);
      writer.WriteBytes(filterBytes);
      writer.WriteByte(qosByte);
      return packet[..writer.Position];
   }

   private static ushort ExtractPublishPacketId(ReadOnlySpan<byte> packet)
   {
      var offset = 1;
      byte b;
      do
      {
         b = packet[offset++];
      }
      while ((b & 0x80) != 0);

      var topicLen = (packet[offset] << 8) | packet[offset + 1];
      offset += 2 + topicLen;
      return (ushort)((packet[offset] << 8) | packet[offset + 1]);
   }

   private static bool TryReadPacketSlice(ReadOnlySpan<byte> buffer, out MqttPacketType packetType, out int consumed)
   {
      packetType = default;
      consumed = 0;
      if (buffer.Length < 2) return false;

      packetType = (MqttPacketType)(buffer[0] >> 4);
      var pos = 1;
      var remainingLen = 0;
      var multiplier = 1;
      byte b;
      do
      {
         if (pos >= buffer.Length) return false;
         b = buffer[pos++];
         remainingLen += (b & 0x7F) * multiplier;
         multiplier *= 128;
      }
      while ((b & 0x80) != 0);

      if (buffer.Length < pos + remainingLen) return false;
      consumed = pos + remainingLen;
      return true;
   }

   private sealed class DummyNetworkListener : INetworkListener
   {
      public EndPoint LocalAddress => new IPEndPoint(IPAddress.Loopback, 0);
      public TransportKind Transport => TransportKind.Unknown;
      public bool IsBound => true;
      public NetworkListenerStats Stats => default;
      public ValueTask<VoidResult<NetworkCodeError>> BindAsync(CancellationToken ct = default)
         => ValueTask.FromResult<VoidResult<NetworkCodeError>>(true);
      public ValueTask<VoidResult<NetworkCodeError>> UnbindAsync(CancellationToken ct = default)
         => ValueTask.FromResult<VoidResult<NetworkCodeError>>(true);
      public ValueTask<Result<INetworkSession, NetworkCodeError>> AcceptSessionAsync(CancellationToken ct = default)
         => throw new NotImplementedException();
      public ValueTask DisposeAsync() => ValueTask.CompletedTask;
   }

   private sealed class DummyNetworkSession : INetworkSession
   {
      public Guid Id { get; } = Guid.NewGuid();
      public EndPoint RemoteAddress { get; } = new IPEndPoint(IPAddress.Loopback, 0);
      public EndPoint LocalAddress { get; } = new IPEndPoint(IPAddress.Loopback, 0);
      public bool IsSupportingMultiplexing => false;
      public bool IsSupportingUnidirectional => false;
      public CancellationToken SessionClosedToken => CancellationToken.None;
      public INetworkPropertyStore Properties { get; } = new NetworkPropertyStore();
      public NetworkStats Stats => default;
      public DateTimeOffset CreatedAt { get; } = DateTimeOffset.UtcNow;
      public TransportKind Transport => TransportKind.Unknown;
      public NetworkSecurityInfo SecurityInfo => new(IsEncrypted: false);
      public NetworkSessionStats SessionStats => default;
      public IReadOnlyCollection<INetworkStream> ActiveStreams => [];
      public ValueTask<Result<INetworkStream, NetworkCodeError>> AcceptStreamAsync(CancellationToken ct = default)
         => throw new NotImplementedException();
      public ValueTask<Result<INetworkStream, NetworkCodeError>> OpenStreamAsync(
         NetworkStreamDirection direction = NetworkStreamDirection.Bidirectional, CancellationToken ct = default)
         => throw new NotImplementedException();
      public ValueTask DisposeAsync() => ValueTask.CompletedTask;
   }

   private sealed class MockDuplexPipe(PipeReader reader, PipeWriter writer) : IDuplexPipe
   {
      public PipeReader Input { get; } = reader;
      public PipeWriter Output { get; } = writer;
   }

   private sealed class MockNetworkStream : INetworkStream
   {
      private readonly AsyncLock _lock = new();
      private readonly Pipe _pipe = new();
      public long StreamId => 1;
      public INetworkSession Session { get; } = new DummyNetworkSession();
      public NetworkStreamDirection Direction => NetworkStreamDirection.Bidirectional;
      public IDuplexPipe Transport => new MockDuplexPipe(_pipe.Reader, _pipe.Writer);
      public NetworkStats Stats { get; set; }
      public DateTimeOffset CreatedAt { get; } = DateTimeOffset.UtcNow;
      public ValueTask<LockReleaser> AcquireWriterLock(CancellationToken cancellationToken = default)
         => _lock.LockAsync(cancellationToken);
      public ValueTask DisposeAsync() => ValueTask.CompletedTask;
   }
}
