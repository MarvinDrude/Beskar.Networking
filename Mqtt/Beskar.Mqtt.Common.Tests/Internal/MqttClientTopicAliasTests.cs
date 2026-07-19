using System.Buffers;
using System.Net;
using System.Reflection;
using System.Text;
using Beskar.Memory.Results;
using Beskar.Memory.Threading;
using Beskar.Mqtt.Client;
using Beskar.Mqtt.Client.Handlers;
using Beskar.Mqtt.Client.States;
using Beskar.Mqtt.Common.Builders.Connecting;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Packets;
using Beskar.Networking.Abstractions.Enums;
using Beskar.Networking.Abstractions.Errors;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Abstractions.Models;
using Beskar.Networking.Abstractions.Threading;
using System.IO.Pipelines;

namespace Beskar.Mqtt.Common.Tests.Internal;

public class MqttClientTopicAliasTests
{
   private static (MqttClient, ClientPacketHandler, MockNetworkStream) SetupClientEnvironment(ushort? topicAliasMaximum = 10)
   {
      var mockNetworkClient = new MockNetworkClient();
      var client = new MqttClient(mockNetworkClient);

      // Force state to Connected (3)
      var stateField = typeof(MqttClient).GetField("_state", BindingFlags.NonPublic | BindingFlags.Instance);
      stateField?.SetValue(client, 3); // MqttClientConnectionState.Connected = 3

      // Set protocol version to V50
      var versionField = typeof(MqttClient).GetField("_protocolVersion", BindingFlags.NonPublic | BindingFlags.Instance);
      versionField?.SetValue(client, MqttProtocolVersion.V50);

      // Set connect options with TopicAliasMaximum
      var connectOptions = new ConnectOptions
      {
         EndPoint = new IPEndPoint(IPAddress.Loopback, 1883),
         TopicAliasMaximum = topicAliasMaximum
      };
      var optionsField = typeof(MqttClient).GetField("_connectOptions", BindingFlags.NonPublic | BindingFlags.Instance);
      optionsField?.SetValue(client, connectOptions);

      // Set mock control stream
      var stream = new MockNetworkStream();
      var streamField = typeof(MqttClient).GetField("_controlStream", BindingFlags.NonPublic | BindingFlags.Instance);
      streamField?.SetValue(client, stream);

      var handler = new ClientPacketHandler(client);

      return (client, handler, stream);
   }

   [Test]
   public async Task TopicAlias_RegisterAndResolve_ShouldSucceed()
   {
      var (client, handler, _) = SetupClientEnvironment();

      // Register topic alias "1" -> "test/client-alias"
      var regPacket = new PublishPacket
      {
         Dup = false,
         QualityOfService = QualityOfServiceType.AtMostOnce,
         Retain = false,
         TopicUtf8Bytes = new ReadOnlySequence<byte>("test/client-alias"u8.ToArray()),
         Payload = ReadOnlySequence<byte>.Empty,
         TopicAlias = 1
      };

      await handler.ExecuteAsync(null!, regPacket);

      // Verify alias was registered internally on the client
      var hasAlias = client.TryGetTopicAlias(1, out var resolvedTopic);
      await Assert.That(hasAlias).IsTrue();
      await Assert.That(Encoding.UTF8.GetString(resolvedTopic!)).IsEqualTo("test/client-alias");

      // Verify that subsequent Publish messages with empty topic are resolved
      var usePacket = new PublishPacket
      {
         Dup = false,
         QualityOfService = QualityOfServiceType.AtMostOnce,
         Retain = false,
         TopicUtf8Bytes = ReadOnlySequence<byte>.Empty,
         Payload = ReadOnlySequence<byte>.Empty,
         TopicAlias = 1
      };

      var messageReceivedTcs = new TaskCompletionSource<string>();
      client.AddMessageReceiveHandler((context, ct) =>
      {
         messageReceivedTcs.TrySetResult(context.Message.Topic);
         return ValueTask.CompletedTask;
      });

      await handler.ExecuteAsync(null!, usePacket);

      var resolvedTopicName = await messageReceivedTcs.Task.WaitAsync(TimeSpan.FromSeconds(2));
      await Assert.That(resolvedTopicName).IsEqualTo("test/client-alias");
   }

   [Test]
   public async Task TopicAlias_ExceedingMaximum_ShouldDisconnect()
   {
      var (client, handler, _) = SetupClientEnvironment(topicAliasMaximum: 5);

      var disconnectTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
      client.Events.OnClientDisconnected.Add((ctx, ct) =>
      {
         disconnectTcs.TrySetResult();
         return ValueTask.CompletedTask;
      });

      // Try registering alias 6 (exceeding maximum of 5)
      var packet = new PublishPacket
      {
         Dup = false,
         QualityOfService = QualityOfServiceType.AtMostOnce,
         Retain = false,
         TopicUtf8Bytes = new ReadOnlySequence<byte>("test/alias"u8.ToArray()),
         Payload = ReadOnlySequence<byte>.Empty,
         TopicAlias = 6
      };

      await handler.ExecuteAsync(null!, packet);

      // Wait for the disconnection to process
      await disconnectTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));

      await Assert.That(client.IsConnected).IsFalse();

      var disconnectReasonField = typeof(MqttClient).GetField("_disconnectReason", BindingFlags.NonPublic | BindingFlags.Instance);
      var reason = (MqttClientDisconnectReason?)disconnectReasonField?.GetValue(client);
      await Assert.That(reason).IsNotNull();
      if (reason is null) return;
      await Assert.That(reason.Value.ReasonCode).IsEqualTo((int)DisconnectReasonCode.TopicAliasInvalid);
   }

   [Test]
   public async Task TopicAlias_UnregisteredWithEmptyTopic_ShouldDisconnect()
   {
      var (client, handler, _) = SetupClientEnvironment();

      var disconnectTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
      client.Events.OnClientDisconnected.Add((ctx, ct) =>
      {
         disconnectTcs.TrySetResult();
         return ValueTask.CompletedTask;
      });

      // Receive empty topic with alias 2 without registering it first
      var packet = new PublishPacket
      {
         Dup = false,
         QualityOfService = QualityOfServiceType.AtMostOnce,
         Retain = false,
         TopicUtf8Bytes = ReadOnlySequence<byte>.Empty,
         Payload = ReadOnlySequence<byte>.Empty,
         TopicAlias = 2
      };

      await handler.ExecuteAsync(null!, packet);

      await disconnectTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));

      await Assert.That(client.IsConnected).IsFalse();

      var disconnectReasonField = typeof(MqttClient).GetField("_disconnectReason", BindingFlags.NonPublic | BindingFlags.Instance);
      var reason = (MqttClientDisconnectReason?)disconnectReasonField?.GetValue(client);
      await Assert.That(reason).IsNotNull();
      if (reason is null) return;
      await Assert.That(reason.Value.ReasonCode).IsEqualTo((int)DisconnectReasonCode.TopicAliasInvalid);
   }

   [Test]
   public async Task TopicAlias_ClearedOnDisconnect()
   {
      var (client, handler, _) = SetupClientEnvironment();

      // Register alias
      var regPacket = new PublishPacket
      {
         TopicUtf8Bytes = new ReadOnlySequence<byte>("test/alias"u8.ToArray()),
         TopicAlias = 1
      };
      await handler.ExecuteAsync(null!, regPacket);

      // Verify alias is set
      var hasAliasBefore = client.TryGetTopicAlias(1, out _);
      await Assert.That(hasAliasBefore).IsTrue();

      // Disconnect
      await client.DisconnectAsync(new Beskar.Mqtt.Common.Builders.Disconnecting.DisconnectOptions());

      // Verify alias is cleared
      var hasAliasAfter = client.TryGetTopicAlias(1, out _);
      await Assert.That(hasAliasAfter).IsFalse();
   }

   private class MockNetworkClient : INetworkClient
   {
      public TransportKind Transport => TransportKind.Unknown;
      public bool IsConnected => false;
      public NetworkClientStats Stats => default;
      public INetworkSession? Session => null;
      public EndPoint? LocalAddress => null;
      public EndPoint? RemoteAddress => null;
      public ValueTask<Result<INetworkSession, NetworkCodeError>> ConnectAsync(EndPoint endPoint, CancellationToken ct = default)
      {
         return ValueTask.FromResult<Result<INetworkSession, NetworkCodeError>>(new NetworkCodeError(1, "Mock connect not supported"));
      }
      public ValueTask DisconnectAsync(CancellationToken ct = default) => ValueTask.CompletedTask;
      public ValueTask DisposeAsync() => ValueTask.CompletedTask;
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
      public ValueTask<Result<INetworkStream, NetworkCodeError>> AcceptStreamAsync(CancellationToken ct = default) => throw new NotImplementedException();
      public ValueTask<Result<INetworkStream, NetworkCodeError>> OpenStreamAsync(NetworkStreamDirection direction = NetworkStreamDirection.Bidirectional, CancellationToken ct = default) => throw new NotImplementedException();
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
      public INetworkSession Session { get; } = new DummyNetworkSession();
      public NetworkStreamDirection Direction => NetworkStreamDirection.Bidirectional;
      public IDuplexPipe Transport => new MockDuplexPipe(_pipe.Reader, _pipe.Writer);
      public NetworkStats Stats { get; set; }
      public DateTimeOffset CreatedAt { get; } = DateTimeOffset.UtcNow;
      public ValueTask<LockReleaser> AcquireWriterLock(CancellationToken cancellationToken = default) => _lock.LockAsync(cancellationToken);
      public ValueTask DisposeAsync() => ValueTask.CompletedTask;
   }
}
