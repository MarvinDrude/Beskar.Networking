using System.Net;
using System.IO.Pipelines;
using Beskar.Memory.Results;
using Beskar.Mqtt.Client;
using Beskar.Mqtt.Common.Builders.Connecting;
using Beskar.Mqtt.Common.Builders.Disconnecting;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Server;
using Beskar.Mqtt.Server.Internal;
using Beskar.Mqtt.Server.Options;
using Beskar.Networking.Abstractions.Enums;
using Beskar.Networking.Abstractions.Errors;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Abstractions.Models;
using Beskar.Networking.Abstractions.Threading;
using Beskar.Networking.Transports.Tcp;
using Beskar.Utilities.Tracing;

namespace Beskar.Mqtt.Common.Tests.Internal;

public class MqttKeepAliveTests
{
   [Test]
   public async Task Server_ShouldDisconnectClient_WhenKeepAliveTimesOut()
   {
      TraceLogger.IsEnabled = true;

      // Arrange
      var options = new MqttServerOptions();
      options.KeepAlive.Interval = TimeSpan.FromMilliseconds(10); // Check frequently

      await using var server = new MqttServer([], options);
      await server.StartAsync();

      var serverSession = new KeepAliveMockNetworkSession();
      var connContext = new NetworkServerConnectionContext(new DummyNetworkListener(), serverSession);
      var streamContext = new NetworkServerStreamContext(connContext, new MockNetworkStream());

      var client = new MqttServerClient();
      client.Initialize(streamContext, options);
      client.ProtocolVersion = MqttProtocolVersion.V50;
      var connectOptions = new ConnectOptions
      {
         KeepAlivePeriod = 1, // 1 second keep alive period
         ClientIdUtf8Bytes = "keepalive-client"u8.ToArray(),
         CleanSession = true,
         EndPoint = new IPEndPoint(IPAddress.Loopback, 1883)
      };
      client.SetConnectOptions(connectOptions);

      // Register the client to the server session registry
      await server.ClientSessions.GetOrCreateSession(client, connectOptions, CancellationToken.None);

      // Simulate client last message was 2 seconds ago (timeout threshold is 1 * 1.5 = 1.5 seconds)
      serverSession.Stats = new NetworkStats
      {
         LastReceivedTimestamp = DateTimeOffset.UtcNow.AddSeconds(-2)
      };

      // Act & Assert
      // Wait for keep-alive background service to detect timeout
      var timeout = DateTimeOffset.UtcNow.AddSeconds(5);
      while ((client.IsConnected || client.DisconnectOptions is null) && DateTimeOffset.UtcNow < timeout)
      {
         await Task.Delay(10);
      }

      await Assert.That(client.IsConnected).IsFalse();
      await Assert.That(client.DisconnectOptions).IsNotNull();
      await Assert.That(client.DisconnectOptions!.ReasonCode).IsEqualTo(DisconnectReasonCode.KeepAliveTimeout);
   }

   private class KeepAliveMockNetworkSession : INetworkSession
   {
      public Guid Id { get; } = Guid.NewGuid();
      public DateTimeOffset CreatedAt { get; } = DateTimeOffset.UtcNow;
      public TransportKind Transport => TransportKind.Unknown;
      public NetworkSecurityInfo SecurityInfo => new(IsEncrypted: false);
      public NetworkSessionStats SessionStats => default;
      public IReadOnlyCollection<INetworkStream> ActiveStreams => Array.Empty<INetworkStream>();
      public EndPoint RemoteAddress { get; } = new IPEndPoint(IPAddress.Loopback, 0);
      public EndPoint LocalAddress { get; } = new IPEndPoint(IPAddress.Loopback, 0);
      public bool IsSupportingMultiplexing => false;
      public bool IsSupportingUnidirectional => false;
      public CancellationToken SessionClosedToken => CancellationToken.None;
      public INetworkPropertyStore Properties { get; } = new NetworkPropertyStore();
      public NetworkStats Stats { get; set; }

      public ValueTask<Result<INetworkStream, NetworkCodeError>> AcceptStreamAsync(CancellationToken ct = default) => throw new NotImplementedException();
      public ValueTask<Result<INetworkStream, NetworkCodeError>> OpenStreamAsync(NetworkStreamDirection direction = NetworkStreamDirection.Bidirectional, CancellationToken ct = default) => throw new NotImplementedException();
      public ValueTask DisposeAsync() => ValueTask.CompletedTask;
   }

   private class DummyNetworkListener : INetworkListener
   {
      public TransportKind Transport => TransportKind.Unknown;
      public EndPoint LocalAddress => new IPEndPoint(IPAddress.Loopback, 0);
      public bool IsBound => true;
      public NetworkListenerStats Stats => default;
      public ValueTask<VoidResult<NetworkCodeError>> BindAsync(CancellationToken ct = default) => ValueTask.FromResult<VoidResult<NetworkCodeError>>(true);
      public ValueTask<VoidResult<NetworkCodeError>> UnbindAsync(CancellationToken ct = default) => ValueTask.FromResult<VoidResult<NetworkCodeError>>(true);
      public ValueTask<Result<INetworkSession, NetworkCodeError>> AcceptSessionAsync(CancellationToken ct = default) => throw new NotImplementedException();
      public ValueTask DisposeAsync() => ValueTask.CompletedTask;
   }

   [Test]
   public async Task Server_ShouldCleanUpExpiredSession_AfterExpiryIntervalPassed()
   {
      // Arrange
      var options = new MqttServerOptions();
      options.SupportPersistentSessions = true;
      options.KeepAlive.Interval = TimeSpan.FromMilliseconds(10); // Run check frequently

      await using var server = new MqttServer([], options);
      await server.StartAsync();

      var serverSession = new KeepAliveMockNetworkSession();
      var connContext = new NetworkServerConnectionContext(new DummyNetworkListener(), serverSession);
      var streamContext = new NetworkServerStreamContext(connContext, new MockNetworkStream());

      var client = new MqttServerClient();
      client.Initialize(streamContext, options);
      client.ProtocolVersion = MqttProtocolVersion.V50;

      var connectOptions = new ConnectOptions
      {
         KeepAlivePeriod = 60,
         ClientIdUtf8Bytes = "expiry-client"u8.ToArray(),
         CleanSession = false,
         SessionExpiryInterval = 1, // 1 second session expiry
         EndPoint = new IPEndPoint(IPAddress.Loopback, 1883)
      };
      client.SetConnectOptions(connectOptions);

      // Create session (cleanSession is false, session starts active and connected)
      var createResult = await server.ClientSessions.GetOrCreateSession(client, connectOptions, CancellationToken.None);
      var session = createResult.Session;

      // Disconnect client (session transitions to offline and sets DisconnectionTimestamp)
      await server.ClientSessions.HandleClientDisconnectAsync(client);

      await Assert.That(session.IsConnected).IsFalse();
      await Assert.That(session.DisconnectionTimestamp).IsNotNull();
      await Assert.That(session.IsExpired).IsFalse(); // Not expired immediately

      // Wait 1.5 seconds so session expires and the background task gets a chance to clean it up
      await Task.Delay(1500);

      // Verify that the registry no longer contains the session
      var retrieveResult = await server.ClientSessions.GetOrCreateSession(client, connectOptions, CancellationToken.None);

      // Since it expired and was cleaned up, the registry should have initialized a NEW session
      // rather than returning the existing one.
      await Assert.That(retrieveResult.IsSessionPresent).IsFalse();
      await Assert.That(ReferenceEquals(retrieveResult.Session, session)).IsFalse();
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
      public INetworkSession Session { get; } = new KeepAliveMockNetworkSession();
      public NetworkStreamDirection Direction => NetworkStreamDirection.Bidirectional;
      public IDuplexPipe Transport => new MockDuplexPipe(_pipe.Reader, _pipe.Writer);
      public NetworkStats Stats { get; set; }
      public DateTimeOffset CreatedAt { get; } = DateTimeOffset.UtcNow;
      public ValueTask<LockReleaser> AcquireWriterLock(CancellationToken cancellationToken = default) => _lock.LockAsync(cancellationToken);
      public ValueTask DisposeAsync() => ValueTask.CompletedTask;
   }

   [Test]
   public async Task Client_KeepAliveFailure_ShouldTransitionToDisconnectedWithoutDeadlock()
   {
      // Start server
      await using var server = MqttServerFactory.CreateBuilder()
         .UseTcp(0)
         .WithDefaultClientIdGenerator()
         .Build();

      var startResult = await server.StartAsync();
      await Assert.That(startResult.Failed).IsFalse();

      var port = ((IPEndPoint)server.Listeners[0].LocalAddress).Port;

      // Create client with a very short keep alive (1 second)
      var client = (MqttClient)MqttClientFactory.CreateTcp();

      var connectOptions = new ConnectOptions
      {
         EndPoint = new IPEndPoint(IPAddress.Loopback, port),
         CleanSession = true,
         ClientIdUtf8Bytes = "keepalive-deadlock-client"u8.ToArray(),
         KeepAlivePeriod = 1
      };

      var connectResult = await client.ConnectAsync(connectOptions);
      await Assert.That(connectResult.Failed).IsFalse();
      await Assert.That(client.IsConnected).IsTrue();

      // Now shut down the server connection abruptly to cause next ping to fail
      await server.StopAsync();

      // Wait for keep-alive task to run its next ping (in 1 second), fail, and trigger disconnection.
      // We put a timeout of 5 seconds on the check loop.
      var timeout = DateTimeOffset.UtcNow.AddSeconds(5);
      while (client.IsConnected && DateTimeOffset.UtcNow < timeout)
      {
         await Task.Delay(100);
      }

      // Allow a brief moment for the disconnection to process
      await Task.Delay(500);

      // Check if it transitioned to Disconnected (1)
      var stateField = typeof(MqttClient).GetField("_state", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
      var state = (int)stateField!.GetValue(client)!;

      // 1 = Disconnected, 0 = Disconnecting (stuck in deadlock)
      await Assert.That(state).IsEqualTo(1);
   }

   [Test]
   public async Task ServerOptionsBuilder_DefaultKeepAliveInterval_ShouldStartWithoutThrowing()
   {
      var options = new MqttServerOptionsBuilder().Build();
      await using var server = MqttServerFactory.CreateBuilder(options)
         .UseTcp(0)
         .WithDefaultClientIdGenerator()
         .Build();

      var startResult = await server.StartAsync();
      await Assert.That(startResult.Failed).IsFalse();
   }

   [Test]
   public async Task KeepAliveTimeout_PartialNonPacketTransportBytes_ShouldDisconnectClient()
   {
      var options = new MqttServerOptions
      {
         KeepAlive = new MqttServerKeepAliveOptions
         {
            Interval = TimeSpan.FromMilliseconds(200)
         }
      };

      var tcpListener = new TcpNetworkListener(new IPEndPoint(IPAddress.Loopback, 0), new TcpTransportOptions());
      var trackingListener = new TransportReadTrackingListener(tcpListener);

      await using var server = MqttServerFactory.CreateBuilder(options)
         .Use(trackingListener)
         .WithDefaultClientIdGenerator()
         .Build();

      var startResult = await server.StartAsync();
      await Assert.That(startResult.Failed).IsFalse();

      var port = ((IPEndPoint)server.Listeners[0].LocalAddress).Port;
      var disconnectedReasonTcs = new TaskCompletionSource<DisconnectReasonCode>(TaskCreationOptions.RunContinuationsAsynchronously);
      server.Events.OnDisconnect.Add((ctx, _) =>
      {
         disconnectedReasonTcs.TrySetResult(ctx.Reason);
         return ValueTask.CompletedTask;
      });

      using var tcpClient = new System.Net.Sockets.TcpClient { NoDelay = true };
      await tcpClient.ConnectAsync(IPAddress.Loopback, port);
      using var stream = tcpClient.GetStream();

      // Send valid MQTT 5.0 CONNECT with KeepAlive = 1 second (1.5x timeout = 1.5s)
      byte[] connectPacket =
      [
         0x10, 0x16,
         0x00, 0x04, (byte)'M', (byte)'Q', (byte)'T', (byte)'T',
         0x05, 0x02, 0x00, 0x01, 0x00,
         0x00, 0x09,
         (byte)'k', (byte)'a', (byte)'-', (byte)'p', (byte)'a', (byte)'r', (byte)'t', (byte)'i', (byte)'a'
      ];
      await stream.WriteAsync(connectPacket);
      await stream.FlushAsync();

      var connAckBuf = new byte[16];
      var connAckRead = await stream.ReadAsync(connAckBuf);
      await Assert.That(connAckRead).IsGreaterThanOrEqualTo(4);

      // Send incomplete PUBLISH header chunks ([0x30, 0x7F] -> remaining length 127) every 350ms for ~2.45s.
      // Per MQTT 5.0 3.1.2.10, keep-alive requires a complete Control Packet within 1.5s,
      // not merely transport RX bytes updating stream activity timestamps.
      byte[] partialHeader = [0x30, 0x7F];
      for (var i = 0; i < 7 && !disconnectedReasonTcs.Task.IsCompleted; i++)
      {
         await Task.Delay(350);

         try
         {
            await stream.WriteAsync(partialHeader);
            await stream.FlushAsync();
         }
         catch (IOException)
         {
            break;
         }
      }

      if (!disconnectedReasonTcs.Task.IsCompleted)
      {
         await Task.WhenAny(disconnectedReasonTcs.Task, Task.Delay(300));
      }

      var timedOutWaitingForDisconnect = !disconnectedReasonTcs.Task.IsCompleted;
      await Assert.That(timedOutWaitingForDisconnect).IsFalse();
      await Assert.That(await disconnectedReasonTcs.Task).IsEqualTo(DisconnectReasonCode.KeepAliveTimeout);
   }

   private sealed class TransportReadTrackingListener(INetworkListener inner) : INetworkListener
   {
      public EndPoint LocalAddress => inner.LocalAddress;
      public bool IsBound => inner.IsBound;
      public TransportKind Transport => inner.Transport;
      public NetworkListenerStats Stats => inner.Stats;
      public ValueTask<VoidResult<NetworkCodeError>> BindAsync(CancellationToken ct = default) => inner.BindAsync(ct);
      public ValueTask<VoidResult<NetworkCodeError>> UnbindAsync(CancellationToken ct = default) => inner.UnbindAsync(ct);
      public ValueTask DisposeAsync() => inner.DisposeAsync();

      public async ValueTask<Result<INetworkSession, NetworkCodeError>> AcceptSessionAsync(CancellationToken ct = default)
      {
         var result = await inner.AcceptSessionAsync(ct);
         return result.Failed
            ? result
            : new TransportReadTrackingSession(result.Success);
      }
   }

   private sealed class TransportReadTrackingSession(INetworkSession inner) : INetworkSession
   {
      public Guid Id => inner.Id;
      public EndPoint RemoteAddress => inner.RemoteAddress;
      public EndPoint LocalAddress => inner.LocalAddress;
      public bool IsSupportingMultiplexing => inner.IsSupportingMultiplexing;
      public bool IsSupportingUnidirectional => inner.IsSupportingUnidirectional;
      public CancellationToken SessionClosedToken => inner.SessionClosedToken;
      public INetworkPropertyStore Properties => inner.Properties;
      public NetworkStats Stats => inner.Stats;
      public DateTimeOffset CreatedAt => inner.CreatedAt;
      public TransportKind Transport => inner.Transport;
      public NetworkSecurityInfo SecurityInfo => inner.SecurityInfo;
      public NetworkSessionStats SessionStats => inner.SessionStats;
      public IReadOnlyCollection<INetworkStream> ActiveStreams => inner.ActiveStreams;
      public ValueTask<Result<INetworkStream, NetworkCodeError>> OpenStreamAsync(
         NetworkStreamDirection direction = NetworkStreamDirection.Bidirectional, CancellationToken ct = default)
         => inner.OpenStreamAsync(direction, ct);
      public ValueTask DisposeAsync() => inner.DisposeAsync();

      public async ValueTask<Result<INetworkStream, NetworkCodeError>> AcceptStreamAsync(CancellationToken ct = default)
      {
         var result = await inner.AcceptStreamAsync(ct);
         return result.Failed
            ? result
            : new TransportReadTrackingStream(result.Success, this);
      }
   }

   private sealed class TransportReadTrackingStream(INetworkStream inner, INetworkSession session) : INetworkStream
   {
      public long StreamId => inner.StreamId;
      public INetworkSession Session { get; } = session;
      public NetworkStreamDirection Direction => inner.Direction;
      public IDuplexPipe Transport { get; } = new TransportReadTrackingDuplexPipe(inner.Transport, inner);
      public NetworkStats Stats { get => inner.Stats; set => inner.Stats = value; }
      public DateTimeOffset CreatedAt => inner.CreatedAt;
      public ValueTask<LockReleaser> AcquireWriterLock(CancellationToken cancellationToken = default) => inner.AcquireWriterLock(cancellationToken);
      public ValueTask DisposeAsync() => inner.DisposeAsync();
   }

   private sealed class TransportReadTrackingDuplexPipe(IDuplexPipe inner, INetworkStream statsTarget) : IDuplexPipe
   {
      public PipeReader Input { get; } = new TransportReadTrackingPipeReader(inner.Input, statsTarget);
      public PipeWriter Output => inner.Output;
   }

   private sealed class TransportReadTrackingPipeReader(PipeReader inner, INetworkStream statsTarget) : PipeReader
   {
      public override void AdvanceTo(SequencePosition consumed) => inner.AdvanceTo(consumed);
      public override void AdvanceTo(SequencePosition consumed, SequencePosition examined) => inner.AdvanceTo(consumed, examined);
      public override void CancelPendingRead() => inner.CancelPendingRead();
      public override void Complete(Exception? exception = null) => inner.Complete(exception);

      public override bool TryRead(out ReadResult result)
      {
         var read = inner.TryRead(out result);
         if (read && !result.Buffer.IsEmpty)
         {
            statsTarget.IncrementBytesReceived(result.Buffer.Length);
         }
         return read;
      }

      public override async ValueTask<ReadResult> ReadAsync(CancellationToken cancellationToken = default)
      {
         var result = await inner.ReadAsync(cancellationToken);
         if (!result.Buffer.IsEmpty)
         {
            statsTarget.IncrementBytesReceived(result.Buffer.Length);
         }
         return result;
      }
   }
}
