using System.Buffers;
using System.IO.Pipelines;
using System.Net;
using System.Net.Sockets;
using System.Reflection;
using Beskar.Memory.Results;
using Beskar.Memory.Results.Errors;
using Beskar.Memory.Threading;
using Beskar.Mqtt.Client;
using Beskar.Mqtt.Client.Handlers;
using Beskar.Mqtt.Client.States;
using Beskar.Mqtt.Common.Builders.Connecting;
using Beskar.Mqtt.Common.Builders.Disconnecting;
using Beskar.Mqtt.Common.Builders.Publishing;
using Beskar.Mqtt.Common.Tests.Helpers;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Packets;
using Beskar.Networking.Abstractions.Enums;
using Beskar.Networking.Abstractions.Errors;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Abstractions.Models;
using Beskar.Networking.Abstractions.Threading;

namespace Beskar.Mqtt.Common.Tests.Internal;

public class MqttClientFlowControlAndLifecycleTests
{
   [Test]
   public async Task PublishAsync_QoS1DisconnectedBeforePubAck_ShouldCompleteInsteadOfHanging()
   {
      using var listener = new TcpListener(IPAddress.Loopback, 0);
      listener.Start();
      var port = ((IPEndPoint)listener.LocalEndpoint).Port;

      var publishReceivedTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
      using var serverCts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

      var serverTask = Task.Run(async () =>
      {
         using var serverClient = await listener.AcceptTcpClientAsync(serverCts.Token);
         using var stream = serverClient.GetStream();
         var recvBuffer = new byte[1024];

         // Read CONNECT packet
         var read = await stream.ReadAsync(recvBuffer, serverCts.Token);
         if (read == 0) return;

         // Send MQTT 5.0 CONNACK (Success, no session present, props len = 0)
         byte[] connAck = [0x20, 0x03, 0x00, 0x00, 0x00];
         await stream.WriteAsync(connAck, serverCts.Token);
         await stream.FlushAsync(serverCts.Token);

         // Read incoming PUBLISH packet and intentionally do NOT reply with PUBACK
         var pubRead = await stream.ReadAsync(recvBuffer, serverCts.Token);
         if (pubRead > 0)
         {
            publishReceivedTcs.TrySetResult();
         }

         // Keep socket open briefly until client disconnects
         try
         {
            while (await stream.ReadAsync(recvBuffer, serverCts.Token) > 0)
            {
            }
         }
         catch (OperationCanceledException)
         {
         }
         catch (IOException)
         {
         }
      });

      var client = (MqttClient)MqttClientFactory.CreateTcp();
      var connectResult = await client.ConnectAsync(new ConnectOptions
      {
         EndPoint = new IPEndPoint(IPAddress.Loopback, port),
         ProtocolVersion = MqttProtocolVersion.V50,
         ClientIdUtf8Bytes = "disconnect-inflight-client"u8.ToArray(),
         KeepAlivePeriod = 60
      });

      await Assert.That(connectResult.Failed).IsFalse();

      var publishOptions = PublishOptions.Create()
         .WithTopic("flow/disconnect")
         .WithPayload("payload"u8.ToArray())
         .WithQualityOfService(QualityOfServiceType.AtLeastOnce)
         .Build();

      // Await without external CancellationToken (WaitOneAsync(CancellationToken.None))
      var publishTask = client.PublishAsync(publishOptions);

      await publishReceivedTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
      await client.DisconnectAsync(new DisconnectOptions());

      var completedResult = await publishTask.WaitAsync(TimeSpan.FromSeconds(2));
      await Assert.That(completedResult.Failed).IsTrue();

      await serverCts.CancelAsync();
      await serverTask;
   }

   [Test]
   public async Task HandleIncomingPublish_AutoAcknowledgeWriteThrows_ShouldNotDoubleDecrementIncomingInFlight()
   {
      var stream = new ThrowingOutputNetworkStream();
      var client = CreateInitializedConnectedClient(stream, receiveMaximum: 2);
      var handler = new ClientPacketHandler(client);

      // 1. Receive QoS 2 publish #1 (in-flight count should become 1; sends PUBREC while throw is disabled)
      var qos2Packet = new PublishPacket
      {
         PacketIdentifier = 10,
         QualityOfService = QualityOfServiceType.ExactlyOnce,
         TopicUtf8Bytes = new ReadOnlySequence<byte>("qos2/hold"u8.ToArray()),
         Payload = new ReadOnlySequence<byte>("p1"u8.ToArray()),
         PropertiesBytes = ReadOnlySequence<byte>.Empty
      };
      await handler.ExecuteAsync(stream, in qos2Packet);
      await Assert.That(GetIncomingInFlightCount(client)).IsEqualTo(1);

      // 2. Arm write failure on next FlushAsync and receive QoS 1 publish #2
      stream.ThrowOnFlush = true;
      var qos1Packet = new PublishPacket
      {
         PacketIdentifier = 11,
         QualityOfService = QualityOfServiceType.AtLeastOnce,
         TopicUtf8Bytes = new ReadOnlySequence<byte>("qos1/failack"u8.ToArray()),
         Payload = new ReadOnlySequence<byte>("p2"u8.ToArray()),
         PropertiesBytes = ReadOnlySequence<byte>.Empty
      };

      try
      {
         await handler.ExecuteAsync(stream, in qos1Packet);
      }
      catch (IOException)
      {
      }

      // QoS 2 #1 is still in flight awaiting PUBREL, so count MUST still be 1 (not 0 from double decrement)
      await Assert.That(GetIncomingInFlightCount(client)).IsEqualTo(1);
   }

   [Test]
   public async Task HandlePubRel_UnknownPacketIdentifier_ShouldNotDecrementIncomingInFlight()
   {
      var stream = new ThrowingOutputNetworkStream();
      var client = CreateInitializedConnectedClient(stream, receiveMaximum: 2);
      var handler = new ClientPacketHandler(client);

      // Receive QoS 2 publish id=10 so _incomingInFlightCount == 1
      var qos2Packet = new PublishPacket
      {
         PacketIdentifier = 10,
         QualityOfService = QualityOfServiceType.ExactlyOnce,
         TopicUtf8Bytes = new ReadOnlySequence<byte>("qos2/known"u8.ToArray()),
         Payload = new ReadOnlySequence<byte>("p1"u8.ToArray()),
         PropertiesBytes = ReadOnlySequence<byte>.Empty
      };
      await handler.ExecuteAsync(stream, in qos2Packet);
      await Assert.That(GetIncomingInFlightCount(client)).IsEqualTo(1);

      // Dispatch PUBREL for an unknown PacketIdentifier
      var unknownPubRel = new PubRelPacket
      {
         PacketIdentifier = 999,
         ReasonCode = PubRelReasonCode.Success,
         PropertiesBytes = ReadOnlyMemory<byte>.Empty
      };
      await handler.ExecuteAsync(stream, in unknownPubRel);

      // Unknown PUBREL must not consume the slot reserved for packet 10
      await Assert.That(GetIncomingInFlightCount(client)).IsEqualTo(1);
   }

   private static MqttClient CreateInitializedConnectedClient(INetworkStream controlStream, ushort receiveMaximum)
   {
      var client = new MqttClient(new DummyNetworkClient());
      var connectOptions = new ConnectOptions
      {
         EndPoint = new IPEndPoint(IPAddress.Loopback, 1883),
         ProtocolVersion = MqttProtocolVersion.V50,
         ReceiveMaximum = receiveMaximum
      };

      var flags = BindingFlags.NonPublic | BindingFlags.Instance;
      typeof(MqttClient).GetField("_state", flags)!.SetValue(client, (int)MqttClientConnectionState.Connected);
      typeof(MqttClient).GetField("_protocolVersion", flags)!.SetValue(client, MqttProtocolVersion.V50);
      typeof(MqttClient).GetField("_connectOptions", flags)!.SetValue(client, connectOptions);
      typeof(MqttClient).GetField("_controlStream", flags)!.SetValue(client, controlStream);

      return client;
   }

   private static int GetIncomingInFlightCount(MqttClient client)
   {
      var field = typeof(MqttClient).GetField("_incomingInFlightCount", BindingFlags.NonPublic | BindingFlags.Instance);
      return (int)field!.GetValue(client)!;
   }

   private sealed class DummyNetworkClient : INetworkClient
   {
      public TransportKind Transport => TransportKind.Unknown;
      public bool IsConnected => true;
      public NetworkClientStats Stats => default;
      public INetworkSession? Session => null;
      public EndPoint? LocalAddress => null;
      public EndPoint? RemoteAddress => null;
      public ValueTask<Result<INetworkSession, NetworkCodeError>> ConnectAsync(EndPoint endPoint, CancellationToken ct = default)
         => throw new NotSupportedException();
      public ValueTask DisconnectAsync(CancellationToken ct = default)
         => ValueTask.CompletedTask;
      public ValueTask DisposeAsync() => ValueTask.CompletedTask;
   }

   private sealed class ThrowingPipeWriter(PipeWriter inner, ThrowingOutputNetworkStream owner) : PipeWriter
   {
      public override void Advance(int bytes) => inner.Advance(bytes);
      public override void CancelPendingFlush() => inner.CancelPendingFlush();
      public override void Complete(Exception? exception = null) => inner.Complete(exception);
      public override Memory<byte> GetMemory(int sizeHint = 0) => inner.GetMemory(sizeHint);
      public override Span<byte> GetSpan(int sizeHint = 0) => inner.GetSpan(sizeHint);

      public override ValueTask<FlushResult> FlushAsync(CancellationToken cancellationToken = default)
      {
         if (owner.ThrowOnFlush)
         {
            return ValueTask.FromException<FlushResult>(new IOException("Simulated write failure."));
         }

         return inner.FlushAsync(cancellationToken);
      }
   }

   private sealed class DuplexPipe(PipeReader reader, PipeWriter writer) : IDuplexPipe
   {
      public PipeReader Input { get; } = reader;
      public PipeWriter Output { get; } = writer;
   }

   private sealed class ThrowingOutputNetworkStream : INetworkStream
   {
      private readonly AsyncLock _lock = new();
      private readonly Pipe _pipe = new();
      private readonly ThrowingPipeWriter _writer;

      public ThrowingOutputNetworkStream()
      {
         _writer = new ThrowingPipeWriter(_pipe.Writer, this);
         Transport = new DuplexPipe(_pipe.Reader, _writer);
      }

      public bool ThrowOnFlush { get; set; }
      public long StreamId => 1;
      public INetworkSession Session { get; } = new DummyNetworkSession();
      public NetworkStreamDirection Direction => NetworkStreamDirection.Bidirectional;
      public IDuplexPipe Transport { get; }
      public NetworkStats Stats { get; set; }
      public DateTimeOffset CreatedAt { get; } = DateTimeOffset.UtcNow;

      public ValueTask<LockReleaser> AcquireWriterLock(CancellationToken cancellationToken = default)
         => _lock.LockAsync(cancellationToken);

      public ValueTask DisposeAsync() => ValueTask.CompletedTask;
   }
}
