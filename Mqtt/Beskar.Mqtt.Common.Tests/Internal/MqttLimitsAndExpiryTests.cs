using System.Collections.Concurrent;
using System.Net;
using System.Text;
using Beskar.Mqtt.Client;
using Beskar.Mqtt.Common.Builders.Connecting;
using Beskar.Mqtt.Common.Builders.Disconnecting;
using Beskar.Mqtt.Common.Builders.Publishing;
using Beskar.Mqtt.Common.Builders.Subscribing;
using Beskar.Mqtt.Common.Interfaces;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Server;
using Beskar.Mqtt.Server.Enums;
using Beskar.Mqtt.Server.Options;

namespace Beskar.Mqtt.Common.Tests.Internal;

public class MqttLimitsAndExpiryTests
{
   [Test]
   public async Task MaxConnections_WhenReached_ShouldRejectAdditionalClientAndAllowAfterDisconnect()
   {
      var options = new MqttServerOptions { MaxConnections = 2 };
      await using var server = MqttServerFactory.CreateBuilder(options)
         .UseTcp(0)
         .WithDefaultClientIdGenerator()
         .Build();

      var startResult = await server.StartAsync();
      await Assert.That(startResult.Failed).IsFalse();

      var endpoint = new IPEndPoint(IPAddress.Loopback, ((IPEndPoint)server.Listeners[0].LocalAddress).Port);

      await using var client1 = MqttClientFactory.CreateTcp();
      await using var client2 = MqttClientFactory.CreateTcp();
      await using var client3 = MqttClientFactory.CreateTcp();
      await using var client4 = MqttClientFactory.CreateTcp();

      var c1 = await client1.ConnectAsync(new ConnectOptionsBuilder(endpoint)
         .WithClientId("limit-c1")
         .WithTimeout(TimeSpan.FromSeconds(3))
         .Build());
      var c2 = await client2.ConnectAsync(new ConnectOptionsBuilder(endpoint)
         .WithClientId("limit-c2")
         .WithTimeout(TimeSpan.FromSeconds(3))
         .Build());

      await Assert.That(c1.Failed).IsFalse();
      await Assert.That(c2.Failed).IsFalse();

      // 3rd client exceeds MaxConnections = 2 and must be rejected/closed by the server.
      var c3 = await client3.ConnectAsync(new ConnectOptionsBuilder(endpoint)
         .WithClientId("limit-c3")
         .WithTimeout(TimeSpan.FromSeconds(2))
         .Build());
      await Assert.That(c3.Failed).IsTrue();

      // Free one slot and wait briefly for server connection cleanup.
      await client1.DisconnectAsync(new DisconnectOptions());
      await WaitUntilAsync(async () =>
      {
         using var clients = await server.ClientSessions.GetClients();
         return clients.WrittenSpan.Length == 1;
      }, TimeSpan.FromSeconds(3));

      // 4th client should now be accepted.
      var c4 = await client4.ConnectAsync(new ConnectOptionsBuilder(endpoint)
         .WithClientId("limit-c4")
         .WithTimeout(TimeSpan.FromSeconds(3))
         .Build());
      await Assert.That(c4.Failed).IsFalse();

      await client2.DisconnectAsync(new DisconnectOptions());
      await client4.DisconnectAsync(new DisconnectOptions());
   }

   [Test]
   [Arguments(MessageOverflowBehavior.DropOldest, new[] { "msg-2", "msg-3", "msg-4" })]
   [Arguments(MessageOverflowBehavior.DropNewest, new[] { "msg-0", "msg-1", "msg-2" })]
   public async Task OfflineQueue_OverflowBehavior_ShouldKeepExpectedMessages(
      MessageOverflowBehavior overflowBehavior,
      string[] expectedPayloads)
   {
      var options = new MqttServerOptions
      {
         SupportPersistentSessions = true,
         MaxPendingMessagesPerConnection = 3,
         PendingMessageOverflowBehavior = overflowBehavior
      };

      await using var server = MqttServerFactory.CreateBuilder(options)
         .UseTcp(0)
         .WithDefaultClientIdGenerator()
         .Build();

      await server.StartAsync();
      var endpoint = new IPEndPoint(IPAddress.Loopback, ((IPEndPoint)server.Listeners[0].LocalAddress).Port);

      // 1. Connect persistent subscriber and subscribe QoS 1
      await using (var sub = MqttClientFactory.CreateTcp())
      {
         var res = await sub.ConnectAsync(new ConnectOptionsBuilder(endpoint)
            .WithProtocolVersion(MqttProtocolVersion.V50)
            .WithClientId("overflow-sub")
            .WithCleanSession(false)
            .WithSessionExpiryInterval(60)
            .Build());
         await Assert.That(res.Failed).IsFalse();

         await sub.SubscribeAsync(SubscribeOptions.Create()
            .WithTopicFilter("queue/overflow", QualityOfServiceType.AtLeastOnce)
            .Build());

         await sub.DisconnectAsync(new DisconnectOptions { SessionExpiryInterval = 60 });
      }

      // Wait until server registers session as disconnected
      await WaitUntilAsync(async () =>
      {
         using var clients = await server.ClientSessions.GetClients();
         return clients.WrittenSpan.Length == 0;
      }, TimeSpan.FromSeconds(3));

      // 2. Publish 5 QoS 1 messages while subscriber is offline (capacity = 3)
      await using (var pub = MqttClientFactory.CreateTcp())
      {
         await pub.ConnectAsync(new ConnectOptionsBuilder(endpoint)
            .WithClientId("overflow-pub")
            .Build());

         for (var i = 0; i < 5; i++)
         {
            var pubRes = await pub.PublishAsync(PublishOptions.Create()
               .WithTopic("queue/overflow")
               .WithPayload($"msg-{i}")
               .WithQualityOfService(QualityOfServiceType.AtLeastOnce)
               .Build());
            await Assert.That(pubRes.Failed).IsFalse();
         }

         await pub.DisconnectAsync(new DisconnectOptions());
      }

      // 3. Reconnect persistent subscriber and verify retained queue subset
      var received = new ConcurrentQueue<string>();
      await using var reconnectedSub = MqttClientFactory.CreateTcp();
      reconnectedSub.AddMessageReceiveHandler((ctx, _) =>
      {
         received.Enqueue(Encoding.UTF8.GetString(ctx.Message.Payload.Span));
         return ValueTask.CompletedTask;
      });

      var reconnRes = await reconnectedSub.ConnectAsync(new ConnectOptionsBuilder(endpoint)
         .WithProtocolVersion(MqttProtocolVersion.V50)
         .WithClientId("overflow-sub")
         .WithCleanSession(false)
         .WithSessionExpiryInterval(60)
         .Build());

      await Assert.That(reconnRes.Failed).IsFalse();
      await Assert.That(reconnRes.Success!.IsSessionPresent).IsTrue();

      await WaitUntilAsync(() => Task.FromResult(received.Count >= expectedPayloads.Length), TimeSpan.FromSeconds(5));
      await Task.Delay(150);

      await Assert.That(received.ToArray()).IsEquivalentTo(expectedPayloads);
      await reconnectedSub.DisconnectAsync(new DisconnectOptions());
   }

   [Test]
   public async Task MessageExpiryInterval_OfflineQueuedExpiredMessage_ShouldNotBeDeliveredOnReconnect()
   {
      var options = new MqttServerOptions { SupportPersistentSessions = true };
      await using var server = MqttServerFactory.CreateBuilder(options)
         .UseTcp(0)
         .WithDefaultClientIdGenerator()
         .Build();

      await server.StartAsync();
      var endpoint = new IPEndPoint(IPAddress.Loopback, ((IPEndPoint)server.Listeners[0].LocalAddress).Port);

      // 1. Setup persistent subscriber and disconnect
      await using (var sub = MqttClientFactory.CreateTcp())
      {
         await sub.ConnectAsync(new ConnectOptionsBuilder(endpoint)
            .WithProtocolVersion(MqttProtocolVersion.V50)
            .WithClientId("expiry-offline-sub")
            .WithCleanSession(false)
            .WithSessionExpiryInterval(60)
            .Build());

         await sub.SubscribeAsync(SubscribeOptions.Create()
            .WithTopicFilter("expiry/offline", QualityOfServiceType.AtLeastOnce)
            .Build());

         await sub.DisconnectAsync(new DisconnectOptions { SessionExpiryInterval = 60 });
      }

      await WaitUntilAsync(async () =>
      {
         using var clients = await server.ClientSessions.GetClients();
         return clients.WrittenSpan.Length == 0;
      }, TimeSpan.FromSeconds(3));

      // 2. Publish one short-TTL message (1s) and one long-TTL message (30s)
      await using (var pub = MqttClientFactory.CreateTcp())
      {
         await pub.ConnectAsync(new ConnectOptionsBuilder(endpoint).WithClientId("expiry-pub").Build());

         await pub.PublishAsync(PublishOptions.Create()
            .WithTopic("expiry/offline")
            .WithPayload("expired-message")
            .WithQualityOfService(QualityOfServiceType.AtLeastOnce)
            .WithMessageExpiryInterval(1)
            .Build());

         await pub.PublishAsync(PublishOptions.Create()
            .WithTopic("expiry/offline")
            .WithPayload("fresh-message")
            .WithQualityOfService(QualityOfServiceType.AtLeastOnce)
            .WithMessageExpiryInterval(30)
            .Build());

         await pub.DisconnectAsync(new DisconnectOptions());
      }

      // 3. Wait past the 1-second TTL
      await Task.Delay(1300);

      // 4. Reconnect subscriber; only "fresh-message" should arrive, with decremented TTL
      var received = new ConcurrentQueue<(string Payload, uint Expiry)>();
      await using var reconnectedSub = MqttClientFactory.CreateTcp();
      reconnectedSub.AddMessageReceiveHandler((ctx, _) =>
      {
         received.Enqueue((Encoding.UTF8.GetString(ctx.Message.Payload.Span), ctx.Message.MessageExpiryInterval));
         return ValueTask.CompletedTask;
      });

      await reconnectedSub.ConnectAsync(new ConnectOptionsBuilder(endpoint)
         .WithProtocolVersion(MqttProtocolVersion.V50)
         .WithClientId("expiry-offline-sub")
         .WithCleanSession(false)
         .WithSessionExpiryInterval(60)
         .Build());

      await WaitUntilAsync(() => Task.FromResult(received.Count >= 1), TimeSpan.FromSeconds(5));
      await Task.Delay(200);

      var items = received.ToArray();
      await Assert.That(items.Length).IsEqualTo(1);
      await Assert.That(items[0].Payload).IsEqualTo("fresh-message");
      await Assert.That(items[0].Expiry).IsGreaterThan(0u);
      await Assert.That(items[0].Expiry).IsLessThan(30u);

      await reconnectedSub.DisconnectAsync(new DisconnectOptions());
   }

   [Test]
   public async Task MessageExpiryInterval_RetainedMessage_ShouldDecrementRemainingExpiryOnDelivery()
   {
      await using var server = MqttServerFactory.CreateBuilder()
         .UseTcp(0)
         .WithDefaultClientIdGenerator()
         .Build();

      await server.StartAsync();
      var endpoint = new IPEndPoint(IPAddress.Loopback, ((IPEndPoint)server.Listeners[0].LocalAddress).Port);

      await using (var pub = MqttClientFactory.CreateTcp())
      {
         await pub.ConnectAsync(new ConnectOptionsBuilder(endpoint).WithClientId("retained-ttl-pub").Build());

         await pub.PublishAsync(PublishOptions.Create()
            .WithTopic("expiry/retained")
            .WithPayload("ttl-value")
            .WithQualityOfService(QualityOfServiceType.AtLeastOnce)
            .WithRetain(true)
            .WithMessageExpiryInterval(10)
            .Build());

         await pub.DisconnectAsync(new DisconnectOptions());
      }

      // Wait >1s so TotalSeconds >= 1
      await Task.Delay(1200);

      var tcs = new TaskCompletionSource<uint>(TaskCreationOptions.RunContinuationsAsynchronously);
      await using var sub = MqttClientFactory.CreateTcp();
      sub.AddMessageReceiveHandler((ctx, _) =>
      {
         tcs.TrySetResult(ctx.Message.MessageExpiryInterval);
         return ValueTask.CompletedTask;
      });

      await sub.ConnectAsync(new ConnectOptionsBuilder(endpoint).WithClientId("retained-ttl-sub").Build());
      await sub.SubscribeAsync(SubscribeOptions.Create()
         .WithTopicFilter("expiry/retained", QualityOfServiceType.AtLeastOnce)
         .Build());

      var deliveredExpiry = await tcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
      await Assert.That(deliveredExpiry).IsGreaterThan(0u);
      await Assert.That(deliveredExpiry).IsLessThan(10u);

      await sub.DisconnectAsync(new DisconnectOptions());
   }

   private static async Task WaitUntilAsync(Func<Task<bool>> condition, TimeSpan timeout)
   {
      var deadline = DateTime.UtcNow + timeout;
      while (DateTime.UtcNow < deadline)
      {
         if (await condition())
         {
            return;
         }

         await Task.Delay(25);
      }

      throw new TimeoutException($"Condition was not satisfied within {timeout.TotalSeconds:F1}s.");
   }
}
