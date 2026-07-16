using System.Net;
using System.IO.Pipelines;
using Beskar.Memory.Results;
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
      while (client.IsConnected && DateTimeOffset.UtcNow < timeout)
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
}
