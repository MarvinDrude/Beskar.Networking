using System.Buffers;
using System.Net;
using System.Text;
using Beskar.Mqtt.Client;
using Beskar.Mqtt.Common.Builders.Connecting;
using Beskar.Mqtt.Common.Builders.Disconnecting;
using Beskar.Mqtt.Common.Builders.Publishing;
using Beskar.Mqtt.Common.Builders.Subscribing;
using Beskar.Mqtt.Common.Encoders.Version5;
using Beskar.Mqtt.Common.Generators;
using Beskar.Mqtt.Common.Parsers;
using Beskar.Mqtt.Common.Tests.Helpers;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Packets;
using Beskar.Mqtt.Server;
using Beskar.Mqtt.Server.Options;

namespace Beskar.Mqtt.Common.Tests.Internal;

[NotInParallel]
public class AuditMqttBugConfirmationTests
{
   [Test]
   [Timeout(5000)]
   public async Task SignalBroker_ConcurrentDisposeAndReuse_ShouldNotEvictLiveAwaiterOrDoublePool(CancellationToken ct)
   {
      using var broker = new SignalBroker();
      var corruptionDetected = false;
      var stop = 0;

      // Thread A: rents an awaiter on id=1, calls WaitOneAsync, spins until TryComplete sets _core.SetResult,
      // then immediately calls GetResult() + Dispose() while Thread B is between TryComplete() and OnPruned().
      var waiterAndRenterTasks = Enumerable.Range(0, 2).Select(_ => Task.Run(() =>
      {
         var spin = new SpinWait();
         for (var i = 0; i < 4000 && Volatile.Read(ref stop) == 0 && !ct.IsCancellationRequested; i++)
         {
            var awaiter = broker.AddAwaitable<int>(1);
            if ((!awaiter.IsPending && !awaiter.IsCompletedSuccessfully) ||
                (awaiter.IsPruned && !awaiter.IsCompletedSuccessfully))
            {
               corruptionDetected = true;
               Volatile.Write(ref stop, 1);
               break;
            }

            var vt = awaiter.WaitOneAsync(CancellationToken.None);
            for (var s = 0; s < 8 && !vt.IsCompleted; s++)
            {
               spin.SpinOnce(-1);
            }

            if (vt.IsCompletedSuccessfully)
            {
               _ = vt.GetAwaiter().GetResult();
            }

            awaiter.Dispose();
         }
      }, ct)).ToArray();

      var dispatchTasks = Enumerable.Range(0, 2).Select(_ => Task.Run(() =>
      {
         for (var i = 0; i < 8000 && Volatile.Read(ref stop) == 0 && !ct.IsCancellationRequested; i++)
         {
            broker.TryDispatch(i, 1);
         }
      }, ct)).ToArray();

      var verifierTask = Task.Run(() =>
      {
         for (var i = 0; i < 4000 && Volatile.Read(ref stop) == 0 && !ct.IsCancellationRequested; i++)
         {
            var a1 = broker.AddAwaitable<int>(1);
            var a2 = broker.AddAwaitable<int>(2);
            if (ReferenceEquals(a1, a2) ||
                (!a1.IsPending && !a1.IsCompletedSuccessfully) ||
                (a1.IsPruned && !a1.IsCompletedSuccessfully) ||
                a2.IsPruned || !a2.IsPending)
            {
               corruptionDetected = true;
               Volatile.Write(ref stop, 1);
               break;
            }
            a1.Dispose();
            a2.Dispose();
         }
      }, ct);

      await Task.WhenAll([.. waiterAndRenterTasks, .. dispatchTasks, verifierTask]);
      Volatile.Write(ref stop, 1);

      await Assert.That(corruptionDetected).IsFalse();
   }

   [Test]
   [Timeout(5000)]
   public async Task MqttSession_CleanSessionTakeover_ShouldNotDeleteNewClientSessionOrSubscriptions(CancellationToken ct)
   {
      await using var server = MqttServerFactory.CreateBuilder(new MqttServerOptions { SupportPersistentSessions = true })
         .UseTcp(new IPEndPoint(IPAddress.Loopback, 0))
         .WithDefaultClientIdGenerator()
         .Build();

      server.Events.OnDisconnect.Add(async (ctx, token) =>
      {
         if (ctx.IsSessionTakenOver)
         {
            // Give takenOverClient's receive loop a moment to enter HandleClientDisconnectAsync(takenOverClient)
            // before GetOrCreateSession reaches existing.DisposeAsync() at line 216.
            await Task.Delay(50, token);
         }
      });

      await server.StartAsync();
      try
      {
         var endPoint = (IPEndPoint)server.Listeners[0].LocalAddress!;
         var sharedClientId = $"takeover-clean-{Guid.NewGuid():N}";

         // Client 1 connects with CleanSession = true, SessionExpiryInterval = 0
         await using var client1 = MqttClientFactory.CreateTcp();
         var res1 = await client1.ConnectAsync(new ConnectOptionsBuilder(endPoint)
            .WithProtocolVersion(MqttProtocolVersion.V50)
            .WithClientId(sharedClientId)
            .WithCleanSession(true)
            .WithSessionExpiryInterval(0)
            .Build(), ct);
         await Assert.That(res1.Failed).IsFalse();

         // Client 2 takes over with CleanSession = true, SessionExpiryInterval = 300 (persistent session)
         await using var client2 = MqttClientFactory.CreateTcp();
         var res2 = await client2.ConnectAsync(new ConnectOptionsBuilder(endPoint)
            .WithProtocolVersion(MqttProtocolVersion.V50)
            .WithClientId(sharedClientId)
            .WithCleanSession(true)
            .WithSessionExpiryInterval(300)
            .Build(), ct);
         await Assert.That(res2.Failed).IsFalse();

         // Now Client 2 disconnects and reconnects with CleanSession = false to resume its persistent session (ExpiryInterval = 300)
         await client2.DisconnectAsync(new DisconnectOptionsBuilder()
            .WithReasonCode(DisconnectReasonCode.NormalDisconnection)
            .Build(), ct);

         await using var client3 = MqttClientFactory.CreateTcp();
         var res3 = await client3.ConnectAsync(new ConnectOptionsBuilder(endPoint)
            .WithProtocolVersion(MqttProtocolVersion.V50)
            .WithClientId(sharedClientId)
            .WithCleanSession(false)
            .WithSessionExpiryInterval(300)
            .Build(), ct);
         await Assert.That(res3.Failed).IsFalse();

         await Assert.That(res3.Success!.IsSessionPresent).IsTrue();
      }
      finally
      {
         await server.StopAsync();
      }
   }

   [Test]
   [Timeout(5000)]
   public async Task MqttServer_SessionTakeover_ShouldFireOnDisconnectExactlyOnceForTakenOverClient(CancellationToken ct)
   {
      await using var server = MqttServerFactory.CreateBuilder()
         .UseTcp(new IPEndPoint(IPAddress.Loopback, 0))
         .WithDefaultClientIdGenerator()
         .Build();

      var disconnectCountForSharedId = 0;
      var sharedClientId = $"takeover-dup-disconnect-{Guid.NewGuid():N}";

      server.Events.OnDisconnect.Add((ctx, _) =>
      {
         if (Encoding.UTF8.GetString(ctx.ServerClient.ClientIdUtf8Bytes.Span) == sharedClientId)
         {
            Interlocked.Increment(ref disconnectCountForSharedId);
         }
         return ValueTask.CompletedTask;
      });

      await server.StartAsync();
      try
      {
         var endPoint = (IPEndPoint)server.Listeners[0].LocalAddress!;

         var connectOpts = new ConnectOptionsBuilder(endPoint)
            .WithProtocolVersion(MqttProtocolVersion.V50)
            .WithClientId(sharedClientId)
            .WithCleanSession(true)
            .Build();

         await using var client1 = MqttClientFactory.CreateTcp();
         await client1.ConnectAsync(connectOpts, ct);

         await using var client2 = MqttClientFactory.CreateTcp();
         await client2.ConnectAsync(connectOpts, ct);

         await Task.Delay(150, ct);

         // Expected: OnDisconnect must fire exactly once for taken-over client1 (not twice).
         await Assert.That(Volatile.Read(ref disconnectCountForSharedId)).IsEqualTo(1);
      }
      finally
      {
         await server.StopAsync();
      }
   }

   [Test]
   [Timeout(5000)]
   public async Task MqttClient_RetransmittedQoS2PublishBeforePubRel_ShouldDeliverExactlyOnceAndNotLeakInFlightCount(CancellationToken ct)
   {
      await using var server = MqttServerFactory.CreateBuilder()
         .UseTcp(new IPEndPoint(IPAddress.Loopback, 0))
         .WithDefaultClientIdGenerator()
         .Build();

      await server.StartAsync();
      try
      {
         var endPoint = (IPEndPoint)server.Listeners[0].LocalAddress!;

         await using var client = (MqttClient)MqttClientFactory.CreateTcp();
         var deliveryCount = 0;
         client.AddMessageReceiveHandler((_, _) =>
         {
            Interlocked.Increment(ref deliveryCount);
            return ValueTask.CompletedTask;
         });

         await client.ConnectAsync(new ConnectOptionsBuilder(endPoint)
            .WithProtocolVersion(MqttProtocolVersion.V50)
            .WithCleanSession(true)
            .WithReceiveMaximum(2)
            .Build(), ct);

         // Invoke ClientPacketHandler directly with a QoS 2 PUBLISH (PacketId=42, Dup=false),
         // followed by a retransmitted QoS 2 PUBLISH (PacketId=42, Dup=true) BEFORE PUBREL arrives.
         var packetHandler = new Beskar.Mqtt.Client.Handlers.ClientPacketHandler(client);
         var dummyStream = new DummyNetworkStream();

         var qos2PublishInitial = new PublishPacket
         {
            PacketIdentifier = 42,
            QualityOfService = QualityOfServiceType.ExactlyOnce,
            Dup = false,
            TopicUtf8Bytes = new ReadOnlySequence<byte>("qos2/test"u8.ToArray()),
            Payload = new ReadOnlySequence<byte>("payload"u8.ToArray())
         };

         var qos2PublishRetransmit = new PublishPacket
         {
            PacketIdentifier = 42,
            QualityOfService = QualityOfServiceType.ExactlyOnce,
            Dup = true,
            TopicUtf8Bytes = new ReadOnlySequence<byte>("qos2/test"u8.ToArray()),
            Payload = new ReadOnlySequence<byte>("payload"u8.ToArray())
         };

         await packetHandler.ExecuteAsync(dummyStream, in qos2PublishInitial, ct);
         await packetHandler.ExecuteAsync(dummyStream, in qos2PublishRetransmit, ct);

         // Expected: QoS 2 message with PacketId=42 must be delivered to application handlers at most once (not twice!).
         await Assert.That(Volatile.Read(ref deliveryCount)).IsEqualTo(1);
      }
      finally
      {
         await server.StopAsync();
      }
   }

   [Test]
   [Timeout(5000)]
   public async Task PacketVersion5Parser_Disconnect_WithAllThreeStandardPropertiesAndTrailingInvalidProperty_ShouldReturnError(CancellationToken ct)
   {
      var buffer = new MemoryBuffer();
      var encoder = new PacketVersion5Encoder(buffer);

      // Build a raw MQTT v5 DISCONNECT packet that has:
      // - SessionExpiryInterval (0x11) = 10
      // - ReasonString (0x1F) = "bye"
      // - ServerReference (0x1C) = "srv"
      // - Trailing invalid property identifier (0xFE)
      var rawDisconnect = new byte[]
      {
         0xE0, // DISCONNECT fixed header
         19,   // Remaining Length = 1 (ReasonCode) + 1 (PropLen) + 17 (Properties)
         0x00, // ReasonCode = NormalDisconnection
         17,   // Property Length = 5 + 6 + 6 + 1 = 18 -> wait: 5 + 6 + 6 + 1 = 18, so RemainingLength = 20, PropLen = 18
      };

      // Construct exact bytes:
      // Prop 1: 0x11, 0x00, 0x00, 0x00, 0x0A (5 bytes)
      // Prop 2: 0x1F, 0x00, 0x03, 'b', 'y', 'e' (6 bytes)
      // Prop 3: 0x1C, 0x00, 0x03, 's', 'r', 'v' (6 bytes)
      // Prop 4 (invalid property ID 0xFE): 0xFE (1 byte)
      var packetBytes = new byte[]
      {
         0xE0, 20,
         0x00,
         18,
         0x11, 0x00, 0x00, 0x00, 0x0A,
         0x1F, 0x00, 0x03, (byte)'b', (byte)'y', (byte)'e',
         0x1C, 0x00, 0x03, (byte)'s', (byte)'r', (byte)'v',
         0xFE
      };

      var handler = new TestPacketHandler();
      var parser = new PacketParser(new DummyNetworkStream(), handler, MqttProtocolVersion.V50);
      var reader = new SequenceReader<byte>(new ReadOnlySequence<byte>(packetBytes));
      var result = await parser.TryDispatch(ref reader, out _);

      // Expected: Parser must reject the trailing invalid property 0xFE instead of breaking early at foundFlags == 0b111.
      await Assert.That(result.Failed || result.Success == Beskar.Mqtt.Protocol.Parsing.Results.PacketDispatchResult.ProtocolError).IsTrue();
   }
}
