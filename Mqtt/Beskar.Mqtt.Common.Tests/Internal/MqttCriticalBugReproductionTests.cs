using System.Buffers;
using System.Net;
using System.Net.Sockets;
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
using Beskar.Networking.Abstractions.Backoffs;
using Beskar.Networking.Abstractions.Options;

namespace Beskar.Mqtt.Common.Tests.Internal;

public partial class MqttCriticalBugReproductionTests
{
   [GeneratedMqttTopic("sensors/{id}/temperature")]
   public static partial bool TryParseTemperatureUtf8(ReadOnlySpan<byte> topic, out int id);

   [GeneratedMqttTopic("sensors/{id}/temperature")]
   public static partial bool TryParseTemperatureChar(ReadOnlySpan<char> topic, out int id);

   [Test]
   [Timeout(5000)]
   public async Task Mqtt5_NormalDisconnectWithoutSessionExpiryProperty_ShouldPreservePersistentSession(CancellationToken ct)
   {
      await using var server = MqttServerFactory.CreateBuilder(new MqttServerOptions { SupportPersistentSessions = true })
         .UseTcp(new IPEndPoint(IPAddress.Loopback, 0))
         .WithDefaultClientIdGenerator()
         .Build();

      await server.StartAsync();
      var endPoint = (IPEndPoint)server.Listeners[0].LocalAddress!;
      var clientId = $"mqtt5-persistent-{Guid.NewGuid():N}";

      // 1. Connect with CleanSession = false and SessionExpiryInterval = 300s
      var subClient = MqttClientFactory.CreateTcp();
      var connectOpts = new ConnectOptionsBuilder(endPoint)
         .WithProtocolVersion(MqttProtocolVersion.V50)
         .WithClientId(clientId)
         .WithCleanSession(false)
         .WithSessionExpiryInterval(300)
         .Build();

      var connRes1 = await subClient.ConnectAsync(connectOpts, ct);
      await Assert.That(connRes1.Failed).IsFalse();

      var subOpts = SubscribeOptions.Create()
         .WithTopicFilter("orders/persistent", QualityOfServiceType.AtLeastOnce)
         .Build();
      await subClient.SubscribeAsync(subOpts, ct);

      // 2. Normal DISCONNECT without overriding SessionExpiryInterval (omitted property on wire)
      await subClient.DisconnectAsync(new DisconnectOptions(), ct);
      await subClient.DisposeAsync();

      // 3. Publish a QoS 1 message while subscriber is offline
      var pubClient = MqttClientFactory.CreateTcp();
      await pubClient.ConnectAsync(new ConnectOptionsBuilder(endPoint)
         .WithProtocolVersion(MqttProtocolVersion.V50)
         .WithCleanSession(true)
         .Build(), ct);

      var pubOpts = new PublishOptionsBuilder()
         .WithTopic("orders/persistent")
         .WithPayload("offline-order-1")
         .WithQualityOfService(QualityOfServiceType.AtLeastOnce)
         .Build();
      await pubClient.PublishAsync(pubOpts, ct);
      await pubClient.DisconnectAsync(new DisconnectOptions(), ct);
      await pubClient.DisposeAsync();

      // 4. Reconnect subscriber with CleanSession = false and verify offline message is delivered
      var subClient2 = MqttClientFactory.CreateTcp();
      var msgTcs = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
      subClient2.AddMessageReceiveHandler((ctx, _) =>
      {
         msgTcs.TrySetResult(Encoding.UTF8.GetString(ctx.Message.Payload.Span));
         return ValueTask.CompletedTask;
      });

      var connRes2 = await subClient2.ConnectAsync(connectOpts, ct);
      await Assert.That(connRes2.Failed).IsFalse();
      await Assert.That(connRes2.Success!.IsSessionPresent).IsTrue();

      var receivedPayload = await msgTcs.Task.WaitAsync(TimeSpan.FromSeconds(2), ct);
      await Assert.That(receivedPayload).IsEqualTo("offline-order-1");

      await subClient2.DisposeAsync();
      await server.StopAsync();
   }

   [Test]
   [Timeout(5000)]
   public async Task Mqtt311_CleanSessionFalse_ShouldPreservePersistentSessionAcrossDisconnect(CancellationToken ct)
   {
      await using var server = MqttServerFactory.CreateBuilder(new MqttServerOptions { SupportPersistentSessions = true })
         .UseTcp(new IPEndPoint(IPAddress.Loopback, 0))
         .WithDefaultClientIdGenerator()
         .Build();

      await server.StartAsync();
      var endPoint = (IPEndPoint)server.Listeners[0].LocalAddress!;
      var clientId = $"mqtt311-persistent-{Guid.NewGuid():N}";

      var subClient = MqttClientFactory.CreateTcp();
      var connectOpts = new ConnectOptionsBuilder(endPoint)
         .WithProtocolVersion(MqttProtocolVersion.V311)
         .WithClientId(clientId)
         .WithCleanSession(false)
         .Build();

      var connRes1 = await subClient.ConnectAsync(connectOpts, ct);
      await Assert.That(connRes1.Failed).IsFalse();

      await subClient.SubscribeAsync(SubscribeOptions.Create()
         .WithTopicFilter("v311/persistent", QualityOfServiceType.AtLeastOnce)
         .Build(), ct);

      await subClient.DisconnectAsync(new DisconnectOptions(), ct);
      await subClient.DisposeAsync();

      // Reconnect with CleanSession = false in MQTT 3.1.1
      var subClient2 = MqttClientFactory.CreateTcp();
      var connRes2 = await subClient2.ConnectAsync(connectOpts, ct);
      await Assert.That(connRes2.Failed).IsFalse();
      await Assert.That(connRes2.Success!.IsSessionPresent).IsTrue();

      await subClient2.DisposeAsync();
      await server.StopAsync();
   }

   [Test]
   [Timeout(5000)]
   public async Task MqttServer_SendPublishMessageAsync_WithPropertiesLargerThan128Bytes_ShouldDeliverWithoutThrowing(CancellationToken ct)
   {
      await using var server = MqttServerFactory.CreateBuilder(new MqttServerOptions { SupportPersistentSessions = true })
         .UseTcp(new IPEndPoint(IPAddress.Loopback, 0))
         .WithDefaultClientIdGenerator()
         .Build();

      await server.StartAsync();
      var endPoint = (IPEndPoint)server.Listeners[0].LocalAddress!;

      // Publish a retained QoS 1 message with >128 bytes of UserProperties so Subscribe triggers SendPublishMessageAsync
      var pubClient = MqttClientFactory.CreateTcp();
      await pubClient.ConnectAsync(new ConnectOptionsBuilder(endPoint)
         .WithProtocolVersion(MqttProtocolVersion.V50)
         .WithCleanSession(true)
         .Build(), ct);

      var pubOpts = new PublishOptionsBuilder()
         .WithTopic("retained/large-props")
         .WithPayload("hello-retained")
         .WithQualityOfService(QualityOfServiceType.AtLeastOnce)
         .WithRetain(true)
         .WithResponseTopic("responses/very/long/topic/path/that/takes/up/plenty/of/bytes/in/mqtt5/properties")
         .WithUserProperty("custom-header-1", new string('A', 80))
         .WithUserProperty("custom-header-2", new string('B', 80))
         .Build();

      await pubClient.PublishAsync(pubOpts, ct);

      var subClient = MqttClientFactory.CreateTcp();
      var receivedTcs = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
      subClient.AddMessageReceiveHandler((ctx, _) =>
      {
         receivedTcs.TrySetResult(Encoding.UTF8.GetString(ctx.Message.Payload.Span));
         return ValueTask.CompletedTask;
      });

      await subClient.ConnectAsync(new ConnectOptionsBuilder(endPoint)
         .WithProtocolVersion(MqttProtocolVersion.V50)
         .WithCleanSession(true)
         .Build(), ct);

      // Subscribing to a topic with a matching retained message calls MqttServer.SendPublishMessageAsync
      await subClient.SubscribeAsync(SubscribeOptions.Create()
         .WithTopicFilter("retained/large-props", QualityOfServiceType.AtLeastOnce)
         .Build(), ct);

      var received = await receivedTcs.Task.WaitAsync(TimeSpan.FromSeconds(2), ct);
      await Assert.That(received).IsEqualTo("hello-retained");

      await pubClient.DisposeAsync();
      await subClient.DisposeAsync();
      await server.StopAsync();
   }

   [Test]
   [Timeout(5000)]
   public async Task MqttClient_RequestAsync_WithQoS1Responses_ShouldAcknowledgeResponsesAndNotExhaustReceiveMaximum(CancellationToken ct)
   {
      await using var server = MqttServerFactory.CreateBuilder()
         .UseTcp(new IPEndPoint(IPAddress.Loopback, 0))
         .WithDefaultClientIdGenerator()
         .Build();

      await server.StartAsync();
      var endPoint = (IPEndPoint)server.Listeners[0].LocalAddress!;

      // Requester configures ReceiveMaximum = 2
      var requester = MqttClientFactory.CreateTcp();
      await requester.ConnectAsync(new ConnectOptionsBuilder(endPoint)
         .WithProtocolVersion(MqttProtocolVersion.V50)
         .WithCleanSession(true)
         .WithReceiveMaximum(2)
         .Build(), ct);

      var responder = MqttClientFactory.CreateTcp();
      await responder.ConnectAsync(new ConnectOptionsBuilder(endPoint)
         .WithProtocolVersion(MqttProtocolVersion.V50)
         .WithCleanSession(true)
         .Build(), ct);

      responder.AddMessageReceiveHandler(async (ctx, token) =>
      {
         // Default QoS in RespondAsync is AtLeastOnce (QoS 1)
         await ctx.RespondAsync("pong", QualityOfServiceType.AtLeastOnce, token);
      });

      await responder.SubscribeAsync(SubscribeOptions.Create()
         .WithTopicFilter("rpc/ping", QualityOfServiceType.AtLeastOnce)
         .Build(), ct);

      // Execute 4 sequential requests (> ReceiveMaximum of 2)
      for (var i = 0; i < 4; i++)
      {
         var response = await requester.RequestAsync("rpc/ping", "ping"u8.ToArray(), TimeSpan.FromSeconds(2), ct);
         await Assert.That(response.Failed).IsFalse();
      }

      await Assert.That(requester.IsConnected).IsTrue();

      await requester.DisposeAsync();
      await responder.DisposeAsync();
      await server.StopAsync();
   }

   [Test]
   public async Task PacketVersion5Encoder_DisconnectWithServerReferenceAndReasonString_ShouldEncodeAndParseBothProperties()
   {
      var buffer = new MemoryBuffer();
      var encoder = new PacketVersion5Encoder(buffer);

      var options = new DisconnectOptions
      {
         ReasonCode = DisconnectReasonCode.UseAnotherServer,
         ReasonString = "Server maintenance",
         ServerReference = "backup.mqtt.local:1883"
      };

      encoder.WriteDisconnect(options);

      string? parsedReason = null;
      string? parsedServerRef = null;
      var handler = new TestPacketHandler
      {
         OnDisconnect = (in pkt) =>
         {
            parsedReason = Encoding.UTF8.GetString(pkt.ReasonUtf8Bytes);
            parsedServerRef = Encoding.UTF8.GetString(pkt.ServerReferenceUtf8Bytes);
         }
      };

      var parser = new PacketParser(new DummyNetworkStream(), handler, MqttProtocolVersion.V50);
      var reader = new SequenceReader<byte>(buffer.WrittenSequence);
      var result = await parser.TryDispatch(ref reader, out _);

      await Assert.That(result.Failed).IsFalse();
      await Assert.That(parsedReason).IsEqualTo("Server maintenance");
      await Assert.That(parsedServerRef).IsEqualTo("backup.mqtt.local:1883");
   }

   [Test]
   public async Task PacketVersion5Encoder_PubAckWithReasonStringUtf8Bytes_ShouldEncodeReasonStringOnWire()
   {
      var buffer = new MemoryBuffer();
      var encoder = new PacketVersion5Encoder(buffer);

      var packet = new PubAckPacket
      {
         PacketIdentifier = 77,
         ReasonCode = PubAckReasonCode.QuotaExceeded,
         ReasonStringUtf8Bytes = "Rate limit exceeded"u8.ToArray()
      };

      encoder.WritePubAck(packet);

      string? parsedReasonString = null;
      var handler = new TestPacketHandler
      {
         OnPubAck = (in p) =>
         {
            parsedReasonString = Encoding.UTF8.GetString(p.ReasonStringUtf8Bytes.Span);
         }
      };

      var parser = new PacketParser(new DummyNetworkStream(), handler, MqttProtocolVersion.V50);
      var reader = new SequenceReader<byte>(buffer.WrittenSequence);
      var dispatchRes = await parser.TryDispatch(ref reader, out _);

      await Assert.That(dispatchRes.Failed).IsFalse();
      await Assert.That(parsedReasonString).IsEqualTo("Rate limit exceeded");
   }

   [Test]
   [Timeout(5000)]
   public async Task Mqtt5_WillMessage_WithWillDelayAndZeroSessionExpiry_ShouldPublishWillAndWillUserPropertiesOnSessionEnd(CancellationToken ct)
   {
      await using var server = MqttServerFactory.CreateBuilder()
         .UseTcp(new IPEndPoint(IPAddress.Loopback, 0))
         .WithDefaultClientIdGenerator()
         .Build();

      await server.StartAsync();
      var endPoint = (IPEndPoint)server.Listeners[0].LocalAddress!;

      var watcher = MqttClientFactory.CreateTcp();
      var willReceivedTcs = new TaskCompletionSource<(string Payload, int UserPropCount)>(TaskCreationOptions.RunContinuationsAsynchronously);
      watcher.AddMessageReceiveHandler((ctx, _) =>
      {
         var payloadStr = Encoding.UTF8.GetString(ctx.Message.Payload.Span);
         willReceivedTcs.TrySetResult((payloadStr, ctx.Message.UserProperties.Count));
         return ValueTask.CompletedTask;
      });

      await watcher.ConnectAsync(new ConnectOptionsBuilder(endPoint)
         .WithProtocolVersion(MqttProtocolVersion.V50)
         .WithCleanSession(true)
         .Build(), ct);

      await watcher.SubscribeAsync(SubscribeOptions.Create()
         .WithTopicFilter("clients/will", QualityOfServiceType.AtLeastOnce)
         .Build(), ct);

      // Connect client with SessionExpiryInterval = 0 (default), WillDelayInterval = 10s, and WillUserProperty
      var doomedClientId = $"doomed-{Guid.NewGuid():N}";
      var doomedClient = MqttClientFactory.CreateTcp();
      await doomedClient.ConnectAsync(new ConnectOptionsBuilder(endPoint)
         .WithProtocolVersion(MqttProtocolVersion.V50)
         .WithClientId(doomedClientId)
         .WithCleanSession(true)
         .WithSessionExpiryInterval(0)
         .WithWill("clients/will", "client-died"u8.ToArray(), QualityOfServiceType.AtLeastOnce)
         .WithWillDelayInterval(10)
         .WithWillUserProperty("reason", "crash")
         .Build(), ct);

      // Simulate ungraceful disconnect by dropping the session on server
      using (var clients = await server.ClientSessions.GetClients())
      {
         foreach (var c in clients.WrittenSpan.ToArray())
         {
            if (Encoding.UTF8.GetString(c.ClientIdUtf8Bytes.Span) == doomedClientId)
            {
               await c.MqttSession!.DisposeAsync();
            }
         }
      }

      // Per MQTT 5.0 3.1.2.11.2, Will MUST be published when Session ends even if WillDelayInterval > 0
      var willResult = await willReceivedTcs.Task.WaitAsync(TimeSpan.FromSeconds(1), ct);
      await Assert.That(willResult.Payload).IsEqualTo("client-died");
      await Assert.That(willResult.UserPropCount).IsEqualTo(1);

      await watcher.DisposeAsync();
      await doomedClient.DisposeAsync();
      await server.StopAsync();
   }

   [Test]
   [Timeout(5000)]
   public async Task MqttClient_DisconnectAsync_WhileInReconnectingState_ShouldCancelReconnectLoopImmediately(CancellationToken ct)
   {
      var server1 = MqttServerFactory.CreateBuilder()
         .UseTcp(new IPEndPoint(IPAddress.Loopback, 0))
         .WithDefaultClientIdGenerator()
         .Build();

      await server1.StartAsync();
      var endPoint = (IPEndPoint)server1.Listeners[0].LocalAddress!;

      var attemptCount = 0;
      var firstBackoffEnteredTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
      var backoff = new CustomSignalBackoff(() =>
      {
         if (Interlocked.Increment(ref attemptCount) == 1)
         {
            firstBackoffEnteredTcs.TrySetResult();
         }
         return TimeSpan.FromMilliseconds(200);
      });

      var client = (MqttClient)MqttClientFactory.CreateTcp();
      await client.ConnectAsync(new ConnectOptionsBuilder(endPoint)
         .WithProtocolVersion(MqttProtocolVersion.V50)
         .WithCleanSession(true)
         .WithAutoReconnect(new AutoReconnectOptions
         {
            IsEnabled = true,
            MaxRetryAttempts = 10,
            BackoffPolicy = backoff
         })
         .Build(), ct);

      // Drop server1 so client enters Reconnecting state
      await server1.StopAsync();
      await server1.DisposeAsync();

      await firstBackoffEnteredTcs.Task.WaitAsync(TimeSpan.FromSeconds(2), ct);

      // Call DisconnectAsync while client is in Reconnecting state
      await client.DisconnectAsync(new DisconnectOptions(), ct);

      // Wait 500ms: if DisconnectAsync properly cancelled the reconnect loop, attemptCount stays at 1
      await Task.Delay(500, ct);
      await Assert.That(Volatile.Read(ref attemptCount)).IsEqualTo(1);

      await client.DisposeAsync();
   }

   [Test]
   public async Task MqttTopicGenerator_TryParseUtf8_ShouldRejectTrailingNonDigitsInIntegerSegment()
   {
      var charMatched = TryParseTemperatureChar("sensors/123abc/temperature", out _);
      var utf8Matched = TryParseTemperatureUtf8("sensors/123abc/temperature"u8, out _);

      await Assert.That(charMatched).IsFalse();
      await Assert.That(utf8Matched).IsFalse();
   }

   [Test]
   [Timeout(5000)]
   public async Task MqttClient_DisconnectAsync_InsideOnClientConnectingCallbackDuringAutoReconnect_ShouldNotDeadlock(CancellationToken ct)
   {
      var server1 = MqttServerFactory.CreateBuilder()
         .UseTcp(new IPEndPoint(IPAddress.Loopback, 0))
         .WithDefaultClientIdGenerator()
         .Build();

      await server1.StartAsync();
      var endPoint = (IPEndPoint)server1.Listeners[0].LocalAddress!;

      var client = (MqttClient)MqttClientFactory.CreateTcp();
      await client.ConnectAsync(new ConnectOptionsBuilder(endPoint)
         .WithProtocolVersion(MqttProtocolVersion.V50)
         .WithCleanSession(true)
         .WithAutoReconnect(new AutoReconnectOptions
         {
            IsEnabled = true,
            MaxRetryAttempts = 5,
            BackoffPolicy = new Beskar.Networking.Abstractions.Backoffs.ConstantBackoffPolicy(TimeSpan.FromMilliseconds(30))
         })
         .Build(), ct);

      var callbackCompletedTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
      client.Events.OnClientConnecting.Add(async (_, token) =>
      {
         await client.DisconnectAsync(new DisconnectOptions(), token);
         callbackCompletedTcs.TrySetResult();
      });

      await server1.StopAsync();
      await server1.DisposeAsync();

      var completed = await Task.WhenAny(callbackCompletedTcs.Task, Task.Delay(1500, ct));
      await Assert.That(completed == callbackCompletedTcs.Task).IsTrue();

      await client.DisposeAsync();
   }

   [Test]
   [Timeout(5000)]
   public async Task MqttServer_PublishLargerThan64KiB_ShouldNotStallReceivePipe(CancellationToken ct)
   {
      await using var server = MqttServerFactory.CreateBuilder()
         .UseTcp(new IPEndPoint(IPAddress.Loopback, 0))
         .WithDefaultClientIdGenerator()
         .Build();

      await server.StartAsync();
      var endPoint = (IPEndPoint)server.Listeners[0].LocalAddress!;

      var subClient = MqttClientFactory.CreateTcp();
      var receivedPayloadTcs = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
      subClient.AddMessageReceiveHandler((ctx, _) =>
      {
         receivedPayloadTcs.TrySetResult(ctx.Message.Payload.Length);
         return ValueTask.CompletedTask;
      });

      await subClient.ConnectAsync(new ConnectOptionsBuilder(endPoint)
         .WithProtocolVersion(MqttProtocolVersion.V50)
         .WithCleanSession(true)
         .Build(), ct);

      await subClient.SubscribeAsync(SubscribeOptions.Create()
         .WithTopicFilter("large/frame", QualityOfServiceType.AtLeastOnce)
         .Build(), ct);

      var pubClient = MqttClientFactory.CreateTcp();
      await pubClient.ConnectAsync(new ConnectOptionsBuilder(endPoint)
         .WithProtocolVersion(MqttProtocolVersion.V50)
         .WithCleanSession(true)
         .Build(), ct);

      var largePayload = new byte[100 * 1024];
      Array.Fill(largePayload, (byte)0x42);

      var pubRes = await pubClient.PublishAsync(new PublishOptionsBuilder()
         .WithTopic("large/frame")
         .WithPayload(largePayload)
         .WithQualityOfService(QualityOfServiceType.AtLeastOnce)
         .Build(), ct);

      await Assert.That(pubRes.Failed).IsFalse();

      var receivedLength = await receivedPayloadTcs.Task.WaitAsync(TimeSpan.FromSeconds(2), ct);
      await Assert.That(receivedLength).IsEqualTo(100 * 1024);

      await pubClient.DisposeAsync();
      await subClient.DisposeAsync();
      await server.StopAsync();
   }

   private sealed class CustomSignalBackoff(Func<TimeSpan> onDelay) : Beskar.Networking.Abstractions.Interfaces.Misc.IBackoffPolicy
   {
      public TimeSpan GetNextDelay(int attempt) => onDelay();
   }
}
