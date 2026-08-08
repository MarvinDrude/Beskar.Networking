using System.Buffers;
using System.IO.Pipelines;
using System.Net;
using Beskar.Memory.Results;
using Beskar.Memory.Threading;
using Beskar.Mqtt.Common.Builders.Connecting;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Models;
using Beskar.Mqtt.Protocol.Packets;
using Beskar.Mqtt.Server;
using Beskar.Mqtt.Server.Contexts;
using Beskar.Mqtt.Server.Enums;
using Beskar.Mqtt.Server.Handlers;
using Beskar.Mqtt.Server.Internal;
using Beskar.Mqtt.Server.Options;
using Beskar.Networking.Abstractions.Enums;
using Beskar.Networking.Abstractions.Errors;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Abstractions.Models;
using Beskar.Networking.Abstractions.Threading;
using Beskar.Networking.Transports.Tcp;
using Beskar.Mqtt.Client;

namespace Beskar.Mqtt.Common.Tests.Internal;

public class MqttServerEventsTests
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
   public async Task StartAndStopEvents_ShouldFireCorrectly()
   {
      await using var server = new MqttServer([], new MqttServerOptions());

      var startCount = 0;
      var stopCount = 0;

      server.Events.OnStart.Add((ctx, ct) =>
      {
         startCount++;
         return ValueTask.CompletedTask;
      });

      server.Events.OnStop.Add((ctx, ct) =>
      {
         stopCount++;
         return ValueTask.CompletedTask;
      });

      var startResult = await server.StartAsync();
      await Assert.That(startResult.Failed).IsFalse();
      await Assert.That(startCount).IsEqualTo(1);
      await Assert.That(stopCount).IsEqualTo(0);

      await server.StopAsync();
      await Assert.That(startCount).IsEqualTo(1);
      await Assert.That(stopCount).IsEqualTo(1);
   }

   [Test]
   public async Task StartAsync_WhenOneListenerFails_RollsBackAndUnbindsStartedListeners()
   {
      var mockListener1 = new TrackingNetworkListener(failToBind: false);
      var mockListener2 = new TrackingNetworkListener(failToBind: true);

      await using var server = new MqttServer([mockListener1, mockListener2], new MqttServerOptions());

      var startResult = await server.StartAsync();

      await Assert.That(startResult.Failed).IsTrue();
      await Assert.That(startResult.Error.Detail).Contains("Failed to start one of the listener");

      // Verify that mockListener1 was bound and subsequently unbound/cleaned up
      await Assert.That(mockListener1.BindCalled).IsTrue();
      await Assert.That(mockListener1.UnbindCalled).IsTrue();

      // Verify that mockListener2 was attempted to be bound (and failed), and did not need cleanup
      await Assert.That(mockListener2.BindCalled).IsTrue();
      await Assert.That(mockListener2.UnbindCalled).IsFalse();
   }

   [Test]
   public async Task ConnectionEvents_ShouldFireCorrectly()
   {
      var options = new MqttServerOptions();
      await using var server = new MqttServer([], options);

      var interceptCount = 0;
      var newSessionCount = 0;
      var connectCount = 0;
      var disconnectCount = 0;
      var deleteSessionCount = 0;

      server.Events.OnConnectIntercept.Add((ctx, ct) =>
      {
         interceptCount++;
         return ValueTask.CompletedTask;
      });

      server.Events.OnNewSession.Add((ctx, ct) =>
      {
         newSessionCount++;
         return ValueTask.CompletedTask;
      });

      server.Events.OnConnect.Add((ctx, ct) =>
      {
         connectCount++;
         return ValueTask.CompletedTask;
      });

      server.Events.OnDisconnect.Add((ctx, ct) =>
      {
         disconnectCount++;
         return ValueTask.CompletedTask;
      });

      var deleteSessionTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
      server.Events.OnDeleteSession.Add((ctx, ct) =>
      {
         deleteSessionCount++;
         deleteSessionTcs.TrySetResult();
         return ValueTask.CompletedTask;
      });

      var stream = new MockNetworkStream();
      var client = new MqttServerClient();
      client.Initialize(new NetworkServerStreamContext(
         new NetworkServerConnectionContext(new DummyNetworkListener(), stream.Session), stream), options);
      client.ProtocolVersion = MqttProtocolVersion.V50;

      var connectOptions = new ConnectOptions
      {
         CleanSession = true,
         SessionExpiryInterval = 0,
         EndPoint = new IPEndPoint(IPAddress.Loopback, 1883),
         ClientIdUtf8Bytes = "test_client"u8.ToArray()
      };

      // 1. Intercept connection
      var interceptCtx = new MqttConnectInterceptContext(client)
      {
         ConnectOptions = connectOptions,
         NetworkSession = client.Session,
         CancellationToken = CancellationToken.None
      };
      await server.Events.OnConnectIntercept.ExecuteAsync(interceptCtx,
         HandlerExecutionStrategy.SequentialContinueOnError);
      await Assert.That(interceptCount).IsEqualTo(1);

      // 2. Session creation
      var sessionResult =
         await server.ClientSessions.GetOrCreateSession(client, connectOptions, CancellationToken.None);
      client.MqttSession = sessionResult.Session;
      await Assert.That(newSessionCount).IsEqualTo(1);

      // 3. Connect success
      // Manually trigger OnConnect since the accept loop normally calls it after GetOrCreateSession & CONNACK
      await server.Events.OnConnect.ExecuteAsync(new MqttConnectContext { Client = client },
         HandlerExecutionStrategy.SequentialContinueOnError);
      await Assert.That(connectCount).IsEqualTo(1);

      // 4. Disconnect
      await server.ClientSessions.HandleClientDisconnectAsync(client);
      // Manually trigger OnDisconnect since the listen loop is not running
      await server.Events.OnDisconnect.ExecuteAsync(new MqttDisconnectContext
      {
         ServerClient = client,
         Reason = DisconnectReasonCode.NormalDisconnection,
         DisconnectKind = ClientDisconnectKind.Graceful,
         IsSessionTakenOver = false
      }, HandlerExecutionStrategy.SequentialContinueOnError);
      await Assert.That(disconnectCount).IsEqualTo(1);

      // 5. Delete session (triggered asynchronously inside HandleClientDisconnectAsync)
      await deleteSessionTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
      await Assert.That(deleteSessionCount).IsEqualTo(1);
   }

   [Test]
   public async Task SubscriptionEvents_ShouldFireCorrectly()
   {
      var (server, client, _, _) = SetupEnvironment(MqttProtocolVersion.V50);
      var session = client.MqttSession;

      var subscribeCount = 0;
      var unsubscribeCount = 0;

      var subscribeTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
      server.Events.OnSubscribe.Add((ctx, ct) =>
      {
         subscribeCount++;
         subscribeTcs.TrySetResult();
         return ValueTask.CompletedTask;
      });

      var unsubscribeTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
      server.Events.OnUnsubscribe.Add((ctx, ct) =>
      {
         unsubscribeCount++;
         unsubscribeTcs.TrySetResult();
         return ValueTask.CompletedTask;
      });

      var filter = new TopicFilter(new ReadOnlySequence<byte>("test/filter"u8.ToArray()),
         QualityOfServiceType.AtLeastOnce);
      server.Subscribe(session!, filter);
      await subscribeTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
      await Assert.That(subscribeCount).IsEqualTo(1);

      server.Unsubscribe(session!, "test/filter"u8.ToArray());
      await unsubscribeTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
      await Assert.That(unsubscribeCount).IsEqualTo(1);
   }

   [Test]
   public async Task PublishAndAcknowledgeEvents_ShouldFireCorrectly()
   {
      var (server, client, handler, stream) = SetupEnvironment(MqttProtocolVersion.V50);
      var session = client.MqttSession;

      var acknowledgeCount = 0;
      var noSubscriberCount = 0;
      var publishAckedCount = 0;

      var acknowledgeTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
      server.Events.OnAcknowledgePub.Add((ctx, ct) =>
      {
         acknowledgeCount++;
         acknowledgeTcs.TrySetResult();
         return ValueTask.CompletedTask;
      });

      var noSubscriberTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
      server.Events.OnNoSubscriberMessage.Add((ctx, ct) =>
      {
         noSubscriberCount++;
         noSubscriberTcs.TrySetResult();
         return ValueTask.CompletedTask;
      });

      var publishAckedTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
      server.Events.OnPublishAcknowledged.Add((ctx, ct) =>
      {
         publishAckedCount++;
         publishAckedTcs.TrySetResult();
         return ValueTask.CompletedTask;
      });

      // 1. Publish QoS 1 message with no active subscribers
      var pubPacket = new PublishPacket
      {
         Dup = false,
         QualityOfService = QualityOfServiceType.AtLeastOnce,
         TopicUtf8Bytes = new ReadOnlySequence<byte>("test/no_sub"u8.ToArray()),
         Payload = new ReadOnlySequence<byte>("payload"u8.ToArray()),
         PacketIdentifier = 100
      };

      await handler.ExecuteAsync(stream, pubPacket);
      await Task.WhenAll(acknowledgeTcs.Task, noSubscriberTcs.Task).WaitAsync(TimeSpan.FromSeconds(5));

      // Verify OnNoSubscriberMessage and OnAcknowledgePub fired
      await Assert.That(noSubscriberCount).IsEqualTo(1);
      await Assert.That(acknowledgeCount).IsEqualTo(1);

      // 2. Client receives a publish message from server (simulated) and sends a PUBACK back to finalize
      session!.AddUnacknowledgedPublish(new MqttPendingPublish
      {
         PacketIdentifier = 42,
         Message = new MqttPublishMessage(pubPacket),
         QualityOfService = QualityOfServiceType.AtLeastOnce,
         RetainAsPublished = false,
         SubscriptionIdentifier = 0
      });

      var pubAckPacket = new PubAckPacket
      {
         PacketIdentifier = 42,
         ReasonCode = PubAckReasonCode.Success
      };

      await handler.ExecuteAsync(stream, pubAckPacket);
      await publishAckedTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));

      // Verify OnPublishAcknowledged fired
      await Assert.That(publishAckedCount).IsEqualTo(1);
   }

   [Test]
   public async Task Publish_WhenInterceptedAndBlocked_DoesNotRouteToSubscribersOrUpdateRetained()
   {
      var (server, client, handler, stream) = SetupEnvironment(MqttProtocolVersion.V50);
      var session = client.MqttSession;

      var interceptFired = false;
      server.Events.OnPublishIntercept.Add((ctx, ct) =>
      {
         interceptFired = true;
         if (ctx.PublishMessage.Topic == "blocked/topic")
         {
            ctx.Block(reasonCode: (byte)PubAckReasonCode.NotAuthorized);
         }
         return ValueTask.CompletedTask;
      });

      var noSubFired = false;
      server.Events.OnNoSubscriberMessage.Add((ctx, ct) =>
      {
         noSubFired = true;
         return ValueTask.CompletedTask;
      });

      // 1. Publish blocked message with Retain = true
      var pubPacket = new PublishPacket
      {
         Dup = false,
         QualityOfService = QualityOfServiceType.AtLeastOnce,
         Retain = true,
         TopicUtf8Bytes = new ReadOnlySequence<byte>("blocked/topic"u8.ToArray()),
         Payload = new ReadOnlySequence<byte>("secret payload"u8.ToArray()),
         PacketIdentifier = 101
      };

      await handler.ExecuteAsync(stream, pubPacket);

      // Verify intercept fired
      await Assert.That(interceptFired).IsTrue();
      // Verify message was blocked and not passed to OnNoSubscriberMessage
      await Assert.That(noSubFired).IsFalse();
      // Verify message was not saved to retained messages store
      var retained = server.RetainedMessages.GetMessages();
      await Assert.That(retained.Count).IsEqualTo(0);
   }

   [Test]
   public async Task RetainedMessageEvents_ShouldFireCorrectly()
   {
      var (server, client, handler, stream) = SetupEnvironment(MqttProtocolVersion.V50);

      var changedCount = 0;
      MqttPublishMessage? lastChangedMsg = null;

      var changedTcs1 = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
      var changedTcs2 = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

      server.Events.OnRetainedMessageChanged.Add((ctx, ct) =>
      {
         changedCount++;
         lastChangedMsg = ctx.ChangedRetainedMessage;
         if (changedCount == 1)
         {
            changedTcs1.TrySetResult();
         }
         else if (changedCount == 2)
         {
            changedTcs2.TrySetResult();
         }
         return ValueTask.CompletedTask;
      });

      // Publish with Retain = true
      var pubPacket = new PublishPacket
      {
         Dup = false,
         QualityOfService = QualityOfServiceType.AtMostOnce,
         Retain = true,
         TopicUtf8Bytes = new ReadOnlySequence<byte>("retained/topic"u8.ToArray()),
         Payload = new ReadOnlySequence<byte>("retained payload"u8.ToArray())
      };

      await handler.ExecuteAsync(stream, pubPacket);
      await changedTcs1.Task.WaitAsync(TimeSpan.FromSeconds(5));

      await Assert.That(changedCount).IsEqualTo(1);
      await Assert.That(lastChangedMsg).IsNotNull();
      await Assert.That(lastChangedMsg!.Topic).IsEqualTo("retained/topic");

      // Prune retained message
      var prunePacket = new PublishPacket
      {
         Dup = false,
         QualityOfService = QualityOfServiceType.AtMostOnce,
         Retain = true,
         TopicUtf8Bytes = new ReadOnlySequence<byte>("retained/topic"u8.ToArray()),
         Payload = ReadOnlySequence<byte>.Empty
      };

      await handler.ExecuteAsync(stream, prunePacket);
      await changedTcs2.Task.WaitAsync(TimeSpan.FromSeconds(5));

      await Assert.That(changedCount).IsEqualTo(2);
      await Assert.That(lastChangedMsg).IsNull(); // Pruned is null
   }

   [Test]
   public async Task CombinedBrokerScenario_ShouldCoordinateEventsInOrder()
   {
      var options = new MqttServerOptions();
      await using var server = new MqttServer([], options);

      var eventsFired = new List<string>();

      server.Events.OnStart.Add((ctx, ct) =>
      {
         lock (eventsFired)
         {
            eventsFired.Add("Start");
         }

         return ValueTask.CompletedTask;
      });
      server.Events.OnConnect.Add((ctx, ct) =>
      {
         lock (eventsFired)
         {
            eventsFired.Add("Connect");
         }

         return ValueTask.CompletedTask;
      });
      var retainedChangedTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
      var subscribeTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

      server.Events.OnSubscribe.Add((ctx, ct) =>
      {
         lock (eventsFired)
         {
            eventsFired.Add("Subscribe");
         }
         subscribeTcs.TrySetResult();
         return ValueTask.CompletedTask;
      });
      server.Events.OnRetainedMessageChanged.Add((ctx, ct) =>
      {
         lock (eventsFired)
         {
            eventsFired.Add("RetainedChanged");
         }
         retainedChangedTcs.TrySetResult();
         return ValueTask.CompletedTask;
      });
      server.Events.OnPublishAcknowledged.Add((ctx, ct) =>
      {
         lock (eventsFired)
         {
            eventsFired.Add("PublishAcked");
         }

         return ValueTask.CompletedTask;
      });

      // 1. Start Server
      await server.StartAsync();

      // 2. Setup Client A (Publisher) & Client B (Subscriber)
      var streamA = new MockNetworkStream();
      var clientA = new MqttServerClient();
      clientA.Initialize(new NetworkServerStreamContext(
         new NetworkServerConnectionContext(new DummyNetworkListener(), streamA.Session), streamA), options);
      clientA.ProtocolVersion = MqttProtocolVersion.V50;
      var connectOptionsA = new ConnectOptions
      {
         CleanSession = true, EndPoint = new IPEndPoint(IPAddress.Loopback, 1883),
         ClientIdUtf8Bytes = "client_a"u8.ToArray()
      };
      clientA.SetConnectOptions(connectOptionsA);
      var sessionResultA =
         await server.ClientSessions.GetOrCreateSession(clientA, connectOptionsA, CancellationToken.None);
      clientA.MqttSession = sessionResultA.Session;
      // Trigger Connect
      await server.Events.OnConnect.ExecuteAsync(new MqttConnectContext { Client = clientA },
         HandlerExecutionStrategy.SequentialContinueOnError);

      var streamB = new MockNetworkStream();
      var clientB = new MqttServerClient();
      clientB.Initialize(new NetworkServerStreamContext(
         new NetworkServerConnectionContext(new DummyNetworkListener(), streamB.Session), streamB), options);
      clientB.ProtocolVersion = MqttProtocolVersion.V50;
      var connectOptionsB = new ConnectOptions
      {
         CleanSession = true, EndPoint = new IPEndPoint(IPAddress.Loopback, 1883),
         ClientIdUtf8Bytes = "client_b"u8.ToArray()
      };
      clientB.SetConnectOptions(connectOptionsB);
      var sessionResultB =
         await server.ClientSessions.GetOrCreateSession(clientB, connectOptionsB, CancellationToken.None);
      clientB.MqttSession = sessionResultB.Session;

      // 3. Client A publishes a retained message
      var handlerA = new ServerPacketHandler();
      handlerA.Initialize(server, clientA);

      var pubPacket = new PublishPacket
      {
         Dup = false,
         QualityOfService = QualityOfServiceType.AtMostOnce,
         Retain = true,
         TopicUtf8Bytes = new ReadOnlySequence<byte>("combined/topic"u8.ToArray()),
         Payload = new ReadOnlySequence<byte>("combined payload"u8.ToArray())
      };
      await handlerA.ExecuteAsync(streamA, pubPacket);
      await retainedChangedTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));

      // 4. Client B subscribes to the topic filter
      var filter = new TopicFilter(new ReadOnlySequence<byte>("combined/#"u8.ToArray()),
         QualityOfServiceType.AtLeastOnce);
      server.Subscribe(sessionResultB.Session, filter);
      await subscribeTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));

      // Verify Start, Connect, RetainedChanged, Subscribe all fired
      List<string> firedCopy;
      lock (eventsFired)
      {
         firedCopy = [.. eventsFired];
      }

      await Assert.That(firedCopy).Contains("Start");
      await Assert.That(firedCopy).Contains("Connect");
      await Assert.That(firedCopy).Contains("RetainedChanged");
      await Assert.That(firedCopy).Contains("Subscribe");

      await server.StopAsync();
   }

   private class TrackingNetworkListener(bool failToBind) : INetworkListener
   {
      public bool BindCalled { get; private set; }
      public bool UnbindCalled { get; private set; }

      public EndPoint LocalAddress => new IPEndPoint(IPAddress.Loopback, 0);
      public TransportKind Transport => TransportKind.Unknown;
      public bool IsBound { get; private set; }
      public NetworkListenerStats Stats => default;

      public ValueTask<VoidResult<NetworkCodeError>> BindAsync(CancellationToken ct = default)
      {
         BindCalled = true;
         if (failToBind)
         {
            return ValueTask.FromResult<VoidResult<NetworkCodeError>>(new NetworkCodeError(-1, "Simulated bind failure"));
         }
         IsBound = true;
         return ValueTask.FromResult<VoidResult<NetworkCodeError>>(true);
      }

      public ValueTask<VoidResult<NetworkCodeError>> UnbindAsync(CancellationToken ct = default)
      {
         UnbindCalled = true;
         IsBound = false;
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

   private class DummyNetworkListener : INetworkListener
   {
      public EndPoint LocalAddress => new IPEndPoint(IPAddress.Loopback, 0);
      public TransportKind Transport => TransportKind.Unknown;

      public bool IsBound => true;
      public NetworkListenerStats Stats => default;

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
      public DateTimeOffset CreatedAt { get; } = DateTimeOffset.UtcNow;
      public TransportKind Transport => TransportKind.Unknown;
      public NetworkSecurityInfo SecurityInfo => new(IsEncrypted: false);
      public NetworkSessionStats SessionStats => default;
      public IReadOnlyCollection<INetworkStream> ActiveStreams => Array.Empty<INetworkStream>();

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

      public DateTimeOffset CreatedAt { get; } = DateTimeOffset.UtcNow;

      public ValueTask<LockReleaser> AcquireWriterLock(CancellationToken cancellationToken = default)
      {
         return _lock.LockAsync(cancellationToken);
      }

      public ValueTask DisposeAsync()
      {
         return ValueTask.CompletedTask;
      }
   }

   [Test]
   public async Task MqttServer_StartStopStartStop_SuccessiveCallsWork()
   {
      var options = new MqttServerOptions();
      var listener = new TcpNetworkListener(new IPEndPoint(IPAddress.Loopback, 0), new TcpTransportOptions());
      await using var server = new MqttServer([listener], options);

      for (var i = 0; i < 3; i++)
      {
         var startResult = await server.StartAsync();
         await Assert.That(startResult.Failed).IsFalse();
         await Assert.That(server.State).IsEqualTo(MqttServerState.Running);

         var stopResult = await server.StopAsync();
         await Assert.That(stopResult.Failed).IsFalse();
         await Assert.That(server.State).IsEqualTo(MqttServerState.Stopped);
      }
   }

   [Test]
   public async Task MqttServer_StopWithActiveClient_DisconnectsClientCleanly()
   {
      var options = new MqttServerOptions();
      var listener = new TcpNetworkListener(new IPEndPoint(IPAddress.Loopback, 0), new TcpTransportOptions());
      await using var server = new MqttServer([listener], options);
      await server.StartAsync();

      var client = MqttClientFactory.CreateTcp();
      var connectResult = await client.ConnectAsync(new ConnectOptions
      {
         EndPoint = (IPEndPoint)listener.LocalAddress,
         ClientIdUtf8Bytes = "test_client"u8.ToArray()
      });
      await Assert.That(connectResult.Failed).IsFalse();
      await Assert.That(client.IsConnected).IsTrue();

      // Stop server while client is connected
      var stopResult = await server.StopAsync();
      await Assert.That(stopResult.Failed).IsFalse();
      await Assert.That(server.State).IsEqualTo(MqttServerState.Stopped);

      // Wait a moment for client to detect disconnect
      await Task.Delay(100);

      // Verify client is disconnected
      await Assert.That(client.IsConnected).IsFalse();

      await client.DisposeAsync();
   }
}
