using System.Buffers;
using System.IO.Pipelines;
using System.Net;
using System.Text;
using Beskar.Memory.Results;
using Beskar.Mqtt.Common.Builders.Connecting;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Packets;
using Beskar.Mqtt.Server;
using Beskar.Mqtt.Server.Enums;
using Beskar.Mqtt.Server.Handlers;
using Beskar.Mqtt.Server.Internal;
using Beskar.Mqtt.Server.Options;
using Beskar.Networking.Abstractions.Enums;
using Beskar.Networking.Abstractions.Errors;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Abstractions.Models;
using Beskar.Networking.Abstractions.Threading;

namespace Beskar.Mqtt.Common.Tests.Internal;

public class PublishHandlerTests
{
   private static (MqttServer, MqttServerClient, ServerPacketHandler, MockNetworkStream) SetupEnvironment(
      MqttProtocolVersion version)
   {
      var options = new MqttServerOptions();
      var server = new MqttServer([], options);
      var stream = new MockNetworkStream();
      var connContext = new NetworkServerConnectionContext(new DummyNetworkListener(), stream.Session);
      var streamContext = new NetworkServerStreamContext(connContext, stream);

      var client = new MqttServerClient();
      client.Initialize(streamContext, options);
      client.ProtocolVersion = version;

      var session = new MqttSession(server, client);
      client.MqttSession = session;

      var handler = new ServerPacketHandler();
      handler.Initialize(server, client);

      return (server, client, handler, stream);
   }

   [Test]
   public async Task Publish_WithTopicAlias_ShouldRegisterAndResolveCorrectly()
   {
      var (server, client, handler, stream) = SetupEnvironment(MqttProtocolVersion.V50);

      // Register topic alias "1" -> "test/alias"
      var regPacket = new PublishPacket
      {
         Dup = false,
         QualityOfService = QualityOfServiceType.AtMostOnce,
         Retain = false,
         TopicUtf8Bytes = new ReadOnlySequence<byte>("test/alias"u8.ToArray()),
         Payload = ReadOnlySequence<byte>.Empty,
         TopicAlias = 1
      };

      await handler.ExecuteAsync(stream, regPacket);

      // Verify alias was set on the client
      var hasAlias = client.TryGetTopicAlias(1, out var resolvedTopic);
      await Assert.That(hasAlias).IsTrue();
      await Assert.That(Encoding.UTF8.GetString(resolvedTopic!)).IsEqualTo("test/alias");

      // Publish using empty topic and alias 1
      var usePacket = new PublishPacket
      {
         Dup = false,
         QualityOfService = QualityOfServiceType.AtMostOnce,
         Retain = false,
         TopicUtf8Bytes = ReadOnlySequence<byte>.Empty,
         Payload = ReadOnlySequence<byte>.Empty,
         TopicAlias = 1
      };

      // Set up a subscriber session to verify routing works with the resolved alias
      var subscriberSession = new MqttSession(server, null!);
      server.SubscriptionRouter.Subscribe(subscriberSession, "test/alias"u8.ToArray(), QualityOfServiceType.AtMostOnce,
         false, false, RetainHandlingType.SendAtSubscription, 0);

      await handler.ExecuteAsync(stream, usePacket);

      // Wait a tiny bit since dispatch runs asynchronously
      await Task.Delay(10);
   }

   [Test]
   public async Task Publish_WithInvalidTopicAlias_ShouldDisconnect()
   {
      var (server, client, handler, stream) = SetupEnvironment(MqttProtocolVersion.V50);

      // Publish with alias 2 but no topic name and not registered
      var usePacket = new PublishPacket
      {
         Dup = false,
         QualityOfService = QualityOfServiceType.AtMostOnce,
         Retain = false,
         TopicUtf8Bytes = ReadOnlySequence<byte>.Empty,
         Payload = ReadOnlySequence<byte>.Empty,
         TopicAlias = 2
      };

      await handler.ExecuteAsync(stream, usePacket);

      await Assert.That(client.IsConnected).IsFalse();
      await Assert.That(client.DisconnectOptions).IsNotNull();
      await Assert.That(client.DisconnectOptions!.ReasonCode).IsEqualTo(DisconnectReasonCode.TopicAliasInvalid);
   }

   [Test]
   public async Task Publish_WithWildcards_ShouldDisconnect()
   {
      var (server, client, handler, stream) = SetupEnvironment(MqttProtocolVersion.V50);

      var wildPacket = new PublishPacket
      {
         Dup = false,
         QualityOfService = QualityOfServiceType.AtMostOnce,
         Retain = false,
         TopicUtf8Bytes = new ReadOnlySequence<byte>("test/+/topic"u8.ToArray()),
         Payload = ReadOnlySequence<byte>.Empty
      };

      await handler.ExecuteAsync(stream, wildPacket);

      await Assert.That(client.IsConnected).IsFalse();
      await Assert.That(client.DisconnectOptions).IsNotNull();
      await Assert.That(client.DisconnectOptions!.ReasonCode).IsEqualTo(DisconnectReasonCode.TopicNameInvalid);
   }

   [Test]
   public async Task Publish_QoS2_DeduplicationAndRelease()
   {
      var (server, client, handler, stream) = SetupEnvironment(MqttProtocolVersion.V50);
      var session = client.MqttSession;

      var pubPacket = new PublishPacket
      {
         Dup = false,
         QualityOfService = QualityOfServiceType.ExactlyOnce,
         Retain = false,
         TopicUtf8Bytes = new ReadOnlySequence<byte>("test/qos2"u8.ToArray()),
         Payload = ReadOnlySequence<byte>.Empty,
         PacketIdentifier = 42
      };

      // First publish: should be marked new
      var isNew1 = session!.TryAddQos2Packet(42);
      await Assert.That(isNew1).IsTrue();

      // Second publish: duplicate
      var isNew2 = session!.TryAddQos2Packet(42);
      await Assert.That(isNew2).IsFalse();

      // RelRel packet execution should remove tracking
      var relPacket = new PubRelPacket
      {
         PacketIdentifier = 42,
         ReasonCode = PubRelReasonCode.Success
      };

      await handler.ExecuteAsync(stream, relPacket);

      // Third publish (after rel): should be new again
      var isNew3 = session!.TryAddQos2Packet(42);
      await Assert.That(isNew3).IsTrue();
   }

   [Test]
   public async Task Publish_NoLocal_FiltersOutPublisher()
   {
      var (server, client, handler, stream) = SetupEnvironment(MqttProtocolVersion.V50);
      var session = client.MqttSession;

      // Subscribe publisher's own session with NoLocal = true
      server.SubscriptionRouter.Subscribe(session!, "test/topic"u8.ToArray(), QualityOfServiceType.AtMostOnce, true,
         false, RetainHandlingType.SendAtSubscription, 0);

      var pubPacket = new PublishPacket
      {
         Dup = false,
         QualityOfService = QualityOfServiceType.AtMostOnce,
         Retain = false,
         TopicUtf8Bytes = new ReadOnlySequence<byte>("test/topic"u8.ToArray()),
         Payload = ReadOnlySequence<byte>.Empty
      };

      await handler.ExecuteAsync(stream, pubPacket);

      // Wait a tiny bit since dispatch runs asynchronously
      await Task.Delay(10);

      // Verify no message was queued to offline queue of its own session
      await Assert.That(session!.OfflineQueueCount).IsEqualTo(0);
   }

   [Test]
   public async Task Publish_OfflineQueueing_QueuesAndDeliversOnReconnect()
   {
      var (server, clientA, handlerA, streamA) = SetupEnvironment(MqttProtocolVersion.V50);

      // Create Session B and subscribe to "test/topic" with QoS 1, then disconnect
      var clientB = new MqttServerClient();
      var sessionB = new MqttSession(server, clientB) { ExpiryInterval = 3600 };
      clientB.MqttSession = sessionB;
      server.SubscriptionRouter.Subscribe(sessionB, "test/topic"u8.ToArray(), QualityOfServiceType.AtLeastOnce, false,
         false, RetainHandlingType.SendAtSubscription, 0);

      // Disconnect Client B (simulating offline state)
      sessionB.Client = null;

      // Client A publishes QoS 1 message
      var pubPacket = new PublishPacket
      {
         Dup = false,
         QualityOfService = QualityOfServiceType.AtLeastOnce,
         Retain = false,
         TopicUtf8Bytes = new ReadOnlySequence<byte>("test/topic"u8.ToArray()),
         Payload = new ReadOnlySequence<byte>("hello offline"u8.ToArray()),
         PacketIdentifier = 99
      };

      await handlerA.ExecuteAsync(streamA, pubPacket);

      // Wait a tiny bit since routing is async
      await Task.Delay(10);

      // Verify session B offline queue contains the message
      await Assert.That(sessionB.OfflineQueueCount).IsEqualTo(1);

      var success = sessionB.TryDequeueOfflineMessage(out var queuedMsg);
      await Assert.That(success).IsTrue();
      await Assert.That(queuedMsg!.Message.Topic).IsEqualTo("test/topic");
      await Assert.That(Encoding.UTF8.GetString(queuedMsg.Message.Payload.Span)).IsEqualTo("hello offline");
   }

   [Test]
   public async Task Disconnect_ShouldSetDisconnectionTimestamp()
   {
      var (server, client, handler, stream) = SetupEnvironment(MqttProtocolVersion.V50);
      var session = client.MqttSession;

      await Assert.That(session!.DisconnectionTimestamp).IsNull();

      // Trigger disconnect
      await server.ClientSessions.HandleClientDisconnectAsync(client);

      await Assert.That(session.DisconnectionTimestamp).IsNotNull();
      await Assert.That(session.Client).IsNull();
   }

   [Test]
   public async Task Publish_OfflineQueueing_ShouldNotQueueForCleanSession()
   {
      var (server, clientA, handlerA, streamA) = SetupEnvironment(MqttProtocolVersion.V50);

      // Create Session B with ExpiryInterval = 0 (clean session / no persistence)
      var clientB = new MqttServerClient();
      var sessionB = new MqttSession(server, clientB) { ExpiryInterval = 0 };
      clientB.MqttSession = sessionB;
      server.SubscriptionRouter.Subscribe(sessionB, "test/topic"u8.ToArray(), QualityOfServiceType.AtLeastOnce, false,
         false, RetainHandlingType.SendAtSubscription, 0);

      // Disconnect Client B
      await server.ClientSessions.HandleClientDisconnectAsync(clientB);

      // Client A publishes QoS 1 message
      var pubPacket = new PublishPacket
      {
         Dup = false,
         QualityOfService = QualityOfServiceType.AtLeastOnce,
         Retain = false,
         TopicUtf8Bytes = new ReadOnlySequence<byte>("test/topic"u8.ToArray()),
         Payload = new ReadOnlySequence<byte>("hello offline clean"u8.ToArray()),
         PacketIdentifier = 101
      };

      await handlerA.ExecuteAsync(streamA, pubPacket);

      // Wait a tiny bit since routing is async
      await Task.Delay(10);

      // Verify session B was cleaned up immediately and does not receive the message
      await Assert.That(sessionB.OfflineQueueCount).IsEqualTo(0);
   }

   [Test]
   public async Task Publish_OfflineQueueing_ShouldNotQueueForExpiredSession()
   {
      var (server, clientA, handlerA, streamA) = SetupEnvironment(MqttProtocolVersion.V50);

      // Create Session B with ExpiryInterval = 1 second
      var clientB = new MqttServerClient();
      var sessionB = new MqttSession(server, clientB) { ExpiryInterval = 1 };
      clientB.MqttSession = sessionB;
      server.SubscriptionRouter.Subscribe(sessionB, "test/topic"u8.ToArray(), QualityOfServiceType.AtLeastOnce, false,
         false, RetainHandlingType.SendAtSubscription, 0);

      // Disconnect Client B
      await server.ClientSessions.HandleClientDisconnectAsync(clientB);

      // Artificially age the disconnection timestamp to expire it (e.g. set it to 2 seconds ago)
      sessionB.DisconnectionTimestamp = DateTimeOffset.UtcNow.AddSeconds(-2);
      await Assert.That(sessionB.IsExpired).IsTrue();

      // Client A publishes QoS 1 message
      var pubPacket = new PublishPacket
      {
         Dup = false,
         QualityOfService = QualityOfServiceType.AtLeastOnce,
         Retain = false,
         TopicUtf8Bytes = new ReadOnlySequence<byte>("test/topic"u8.ToArray()),
         Payload = new ReadOnlySequence<byte>("hello offline expired"u8.ToArray()),
         PacketIdentifier = 102
      };

      await handlerA.ExecuteAsync(streamA, pubPacket);

      // Wait a tiny bit since routing is async
      await Task.Delay(10);

      // Verify no message was queued since session is expired
      await Assert.That(sessionB.OfflineQueueCount).IsEqualTo(0);
   }

   [Test]
   public async Task Publish_OfflineQueueing_ShouldNotQueueWhenPersistentSessionsDisabled()
   {
      var options = new MqttServerOptions { SupportPersistentSessions = false };
      var server = new MqttServer([], options);
      var streamA = new MockNetworkStream();
      var connContextA = new NetworkServerConnectionContext(new DummyNetworkListener(), streamA.Session);
      var streamContextA = new NetworkServerStreamContext(connContextA, streamA);
      var clientA = new MqttServerClient();
      clientA.Initialize(streamContextA, options);
      clientA.ProtocolVersion = MqttProtocolVersion.V50;
      var sessionA = new MqttSession(server, clientA);
      clientA.MqttSession = sessionA;
      var handlerA = new ServerPacketHandler();
      handlerA.Initialize(server, clientA);

      // Create client B connecting with persistent options
      var clientB = new MqttServerClient();
      var connContextB = new NetworkServerConnectionContext(new DummyNetworkListener(), new DummyNetworkSession());
      clientB.Initialize(new NetworkServerStreamContext(connContextB, new MockNetworkStream()), options);
      clientB.ProtocolVersion = MqttProtocolVersion.V50;

      var connectOptions = new ConnectOptions
      {
         CleanSession = false,
         SessionExpiryInterval = 3600,
         EndPoint = new IPEndPoint(IPAddress.Loopback, 1883)
      };
      var sessionResult =
         await server.ClientSessions.GetOrCreateSession(clientB, connectOptions, CancellationToken.None);
      var sessionB = sessionResult.Session;

      // Verify that ExpiryInterval was forced to 0
      await Assert.That(sessionB.ExpiryInterval).IsEqualTo(0u);

      server.SubscriptionRouter.Subscribe(sessionB, "test/topic"u8.ToArray(), QualityOfServiceType.AtLeastOnce, false,
         false, RetainHandlingType.SendAtSubscription, 0);

      // Disconnect Client B
      await server.ClientSessions.HandleClientDisconnectAsync(clientB);

      // Client A publishes QoS 1 message
      var pubPacket = new PublishPacket
      {
         Dup = false,
         QualityOfService = QualityOfServiceType.AtLeastOnce,
         Retain = false,
         TopicUtf8Bytes = new ReadOnlySequence<byte>("test/topic"u8.ToArray()),
         Payload = new ReadOnlySequence<byte>("hello offline"u8.ToArray()),
         PacketIdentifier = 103
      };

      await handlerA.ExecuteAsync(streamA, pubPacket);

      // Wait a tiny bit since routing is async
      await Task.Delay(10);

      // Verify no message was queued since persistent sessions are disabled
      await Assert.That(sessionB.OfflineQueueCount).IsEqualTo(0);
   }

   [Test]
   public async Task Publish_OfflineQueueing_RespectsMaxPendingMessages_DropOldest()
   {
      var (server, clientA, handlerA, streamA) = SetupEnvironment(MqttProtocolVersion.V50);
      server.Options.MaxPendingMessagesPerConnection = 2;
      server.Options.PendingMessageOverflowBehavior = MessageOverflowBehavior.DropOldest;

      // Create Session B and subscribe to QoS 1, then disconnect
      var clientB = new MqttServerClient();
      var sessionB = new MqttSession(server, clientB) { ExpiryInterval = 3600 };
      clientB.MqttSession = sessionB;
      server.SubscriptionRouter.Subscribe(sessionB, "test/topic"u8.ToArray(), QualityOfServiceType.AtLeastOnce, false,
         false, RetainHandlingType.SendAtSubscription, 0);
      sessionB.Client = null;

      // Publish 3 QoS 1 messages
      for (var i = 1; i <= 3; i++)
      {
         var pubPacket = new PublishPacket
         {
            Dup = false,
            QualityOfService = QualityOfServiceType.AtLeastOnce,
            TopicUtf8Bytes = new ReadOnlySequence<byte>("test/topic"u8.ToArray()),
            Payload = new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes($"msg{i}")),
            PacketIdentifier = (ushort)i
         };
         await handlerA.ExecuteAsync(streamA, pubPacket);
      }

      await Task.Delay(15);

      // Verify size is capped at 2
      await Assert.That(sessionB.OfflineQueueCount).IsEqualTo(2);

      // Verify oldest ("msg1") was dropped, so queue contains "msg2" and "msg3"
      sessionB.TryDequeueOfflineMessage(out var msg2);
      await Assert.That(Encoding.UTF8.GetString(msg2!.Message.Payload.Span)).IsEqualTo("msg2");

      sessionB.TryDequeueOfflineMessage(out var msg3);
      await Assert.That(Encoding.UTF8.GetString(msg3!.Message.Payload.Span)).IsEqualTo("msg3");
   }

   [Test]
   public async Task Publish_OfflineQueueing_RespectsMaxPendingMessages_DropNewest()
   {
      var (server, clientA, handlerA, streamA) = SetupEnvironment(MqttProtocolVersion.V50);
      server.Options.MaxPendingMessagesPerConnection = 2;
      server.Options.PendingMessageOverflowBehavior = MessageOverflowBehavior.DropNewest;

      // Create Session B and subscribe to QoS 1, then disconnect
      var clientB = new MqttServerClient();
      var sessionB = new MqttSession(server, clientB) { ExpiryInterval = 3600 };
      clientB.MqttSession = sessionB;
      server.SubscriptionRouter.Subscribe(sessionB, "test/topic"u8.ToArray(), QualityOfServiceType.AtLeastOnce, false,
         false, RetainHandlingType.SendAtSubscription, 0);
      sessionB.Client = null;

      // Publish 3 QoS 1 messages
      for (var i = 1; i <= 3; i++)
      {
         var pubPacket = new PublishPacket
         {
            Dup = false,
            QualityOfService = QualityOfServiceType.AtLeastOnce,
            TopicUtf8Bytes = new ReadOnlySequence<byte>("test/topic"u8.ToArray()),
            Payload = new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes($"msg{i}")),
            PacketIdentifier = (ushort)i
         };
         await handlerA.ExecuteAsync(streamA, pubPacket);
      }

      await Task.Delay(15);

      // Verify size is capped at 2
      await Assert.That(sessionB.OfflineQueueCount).IsEqualTo(2);

      // Verify newest ("msg3") was dropped, so queue contains "msg1" and "msg2"
      sessionB.TryDequeueOfflineMessage(out var msg1);
      await Assert.That(Encoding.UTF8.GetString(msg1!.Message.Payload.Span)).IsEqualTo("msg1");

      sessionB.TryDequeueOfflineMessage(out var msg2);
      await Assert.That(Encoding.UTF8.GetString(msg2!.Message.Payload.Span)).IsEqualTo("msg2");
   }

   private class DummyNetworkListener : INetworkListener
   {
      public EndPoint LocalAddress => new IPEndPoint(IPAddress.Loopback, 0);

      public ValueTask<VoidResult<NetworkCodeError>> BindAsync(CancellationToken ct = default)
      {
         return ValueTask.FromResult<VoidResult<NetworkCodeError>>(true);
      }

      public ValueTask<VoidResult<NetworkCodeError>> UnbindAsync(CancellationToken ct = default)
      {
         return ValueTask.FromResult<VoidResult<NetworkCodeError>>(true);
      }

      public ValueTask<Result<INetworkSession, NetworkCodeError>> AcceptSessionAsync(CancellationToken ct = default)
      {
         throw new NotImplementedException();
      }

      public ValueTask DisposeAsync()
      {
         return ValueTask.CompletedTask;
      }
   }

   private class DummyNetworkSession : INetworkSession
   {
      public Guid Id { get; } = Guid.NewGuid();
      public EndPoint RemoteAddress { get; } = new IPEndPoint(IPAddress.Loopback, 0);
      public EndPoint LocalAddress { get; } = new IPEndPoint(IPAddress.Loopback, 0);
      public bool IsSupportingMultiplexing => false;
      public bool IsSupportingUnidirectional => false;
      public CancellationToken SessionClosedToken => CancellationToken.None;
      public INetworkPropertyStore Properties { get; } = new NetworkPropertyStore();
      public NetworkStats Stats => default;

      public ValueTask<Result<INetworkStream, NetworkCodeError>> AcceptStreamAsync(CancellationToken ct = default)
      {
         throw new NotImplementedException();
      }

      public ValueTask<Result<INetworkStream, NetworkCodeError>> OpenStreamAsync(
         NetworkStreamDirection direction = NetworkStreamDirection.Bidirectional, CancellationToken ct = default)
      {
         throw new NotImplementedException();
      }

      public ValueTask DisposeAsync()
      {
         return ValueTask.CompletedTask;
      }
   }

   private class MockDuplexPipe : IDuplexPipe
   {
      public MockDuplexPipe(PipeReader reader, PipeWriter writer)
      {
         Input = reader;
         Output = writer;
      }

      public PipeReader Input { get; }
      public PipeWriter Output { get; }
   }

   private class MockNetworkStream : INetworkStream
   {
      private readonly AsyncLock _lock = new();
      private readonly Pipe _pipe = new();
      public long StreamId => 1;
      public INetworkSession Session { get; } = new DummyNetworkSession();
      public NetworkStreamDirection Direction => NetworkStreamDirection.Bidirectional;
      public IDuplexPipe Transport => new MockDuplexPipe(_pipe.Reader, _pipe.Writer);
      public NetworkStats Stats { get; set; }

      public ValueTask<LockReleaser> AcquireWriterLock(CancellationToken cancellationToken = default)
      {
         return _lock.LockAsync(cancellationToken);
      }

      public ValueTask DisposeAsync()
      {
         return ValueTask.CompletedTask;
      }
   }
}
