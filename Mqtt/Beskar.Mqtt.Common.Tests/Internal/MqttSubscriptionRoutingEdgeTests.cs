using System.Collections.Concurrent;
using System.Net;
using System.Text;
using Beskar.Mqtt.Client;
using Beskar.Mqtt.Common.Builders.Connecting;
using Beskar.Mqtt.Common.Builders.Disconnecting;
using Beskar.Mqtt.Common.Builders.Publishing;
using Beskar.Mqtt.Common.Builders.Subscribing;
using Beskar.Mqtt.Common.Builders.Unsubscribing;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Models;
using Beskar.Mqtt.Server;
using Beskar.Mqtt.Server.Options;

namespace Beskar.Mqtt.Common.Tests.Internal;

public class MqttSubscriptionRoutingEdgeTests
{
   [Test]
   public async Task LivePublish_RetainAsPublished_TruePreservesFlag_FalseClearsFlag()
   {
      await using var server = MqttServerFactory.CreateBuilder()
         .UseTcp(0)
         .WithDefaultClientIdGenerator()
         .Build();

      await server.StartAsync();
      var endpoint = new IPEndPoint(IPAddress.Loopback, ((IPEndPoint)server.Listeners[0].LocalAddress).Port);

      var subATcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
      var subBTcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

      await using var subA = MqttClientFactory.CreateTcp();
      await using var subB = MqttClientFactory.CreateTcp();
      await using var pub = MqttClientFactory.CreateTcp();

      subA.AddMessageReceiveHandler((ctx, _) =>
      {
         subATcs.TrySetResult(ctx.Message.Retain);
         return ValueTask.CompletedTask;
      });
      subB.AddMessageReceiveHandler((ctx, _) =>
      {
         subBTcs.TrySetResult(ctx.Message.Retain);
         return ValueTask.CompletedTask;
      });

      await subA.ConnectAsync(new ConnectOptionsBuilder(endpoint).WithClientId("rap-true").Build());
      await subB.ConnectAsync(new ConnectOptionsBuilder(endpoint).WithClientId("rap-false").Build());
      await pub.ConnectAsync(new ConnectOptionsBuilder(endpoint).WithClientId("rap-pub").Build());

      await subA.SubscribeAsync(SubscribeOptions.Create()
         .WithTopicFilter("live/retain", QualityOfServiceType.AtLeastOnce, retainAsPublished: true)
         .Build());

      await subB.SubscribeAsync(SubscribeOptions.Create()
         .WithTopicFilter("live/retain", QualityOfServiceType.AtLeastOnce, retainAsPublished: false)
         .Build());

      await pub.PublishAsync(PublishOptions.Create()
         .WithTopic("live/retain")
         .WithPayload("hello")
         .WithQualityOfService(QualityOfServiceType.AtLeastOnce)
         .WithRetain(true)
         .Build());

      var retainA = await subATcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
      var retainB = await subBTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));

      await Assert.That(retainA).IsTrue();
      await Assert.That(retainB).IsFalse();

      await subA.DisconnectAsync(new DisconnectOptions());
      await subB.DisconnectAsync(new DisconnectOptions());
      await pub.DisconnectAsync(new DisconnectOptions());
   }

   [Test]
   public async Task OverlappingFilters_UnsubscribeSpecificFilter_ShouldKeepWildcardActive()
   {
      await using var server = MqttServerFactory.CreateBuilder()
         .UseTcp(0)
         .WithDefaultClientIdGenerator()
         .Build();

      await server.StartAsync();
      var endpoint = new IPEndPoint(IPAddress.Loopback, ((IPEndPoint)server.Listeners[0].LocalAddress).Port);

      var received = new ConcurrentQueue<string>();
      await using var sub = MqttClientFactory.CreateTcp();
      await using var pub = MqttClientFactory.CreateTcp();

      sub.AddMessageReceiveHandler((ctx, _) =>
      {
         received.Enqueue(Encoding.UTF8.GetString(ctx.Message.Payload.Span));
         return ValueTask.CompletedTask;
      });

      await sub.ConnectAsync(new ConnectOptionsBuilder(endpoint).WithClientId("overlap-sub").Build());
      await pub.ConnectAsync(new ConnectOptionsBuilder(endpoint).WithClientId("overlap-pub").Build());

      await sub.SubscribeAsync(SubscribeOptions.Create()
         .WithTopicFilter("sensors/#", QualityOfServiceType.AtMostOnce)
         .WithTopicFilter("sensors/temp", QualityOfServiceType.AtLeastOnce)
         .Build());

      await pub.PublishAsync(PublishOptions.Create()
         .WithTopic("sensors/temp")
         .WithPayload("before-unsub")
         .WithQualityOfService(QualityOfServiceType.AtLeastOnce)
         .Build());

      await WaitUntilAsync(() => received.Count >= 1, TimeSpan.FromSeconds(5));

      var unsubRes = await sub.UnsubscribeAsync(UnsubscribeOptions.Create()
         .WithTopicFilter("sensors/temp")
         .Build());
      await Assert.That(unsubRes.Failed).IsFalse();
      await Assert.That(unsubRes.Success!.Unsubscriptions[0].ReasonCode).IsEqualTo(UnsubscribeReasonCode.Success);

      await pub.PublishAsync(PublishOptions.Create()
         .WithTopic("sensors/temp")
         .WithPayload("after-unsub")
         .WithQualityOfService(QualityOfServiceType.AtLeastOnce)
         .Build());

      await WaitUntilAsync(() => received.Contains("after-unsub"), TimeSpan.FromSeconds(5));
      await Assert.That(received.ToArray()).Contains("after-unsub");

      await sub.DisconnectAsync(new DisconnectOptions());
      await pub.DisconnectAsync(new DisconnectOptions());
   }

   [Test]
   public async Task Subscribe_MixedValidAndSharedFilters_ShouldReturnPerFilterReasonCodes()
   {
      await using var server = MqttServerFactory.CreateBuilder()
         .UseTcp(0)
         .WithDefaultClientIdGenerator()
         .Build();

      await server.StartAsync();
      var endpoint = new IPEndPoint(IPAddress.Loopback, ((IPEndPoint)server.Listeners[0].LocalAddress).Port);

      await using var sub = MqttClientFactory.CreateTcp();
      await sub.ConnectAsync(new ConnectOptionsBuilder(endpoint)
         .WithProtocolVersion(MqttProtocolVersion.V50)
         .WithClientId("mixed-sub")
         .Build());

      var subRes = await sub.SubscribeAsync(SubscribeOptions.Create()
         .WithTopicFilter("valid/topic", QualityOfServiceType.AtLeastOnce)
         .WithTopicFilter("$share/workers/valid/topic", QualityOfServiceType.AtLeastOnce)
         .Build());

      await Assert.That(subRes.Failed).IsFalse();
      await Assert.That(subRes.Success!.Subscriptions.Count).IsEqualTo(2);
      await Assert.That(subRes.Success.Subscriptions[0].ReasonCode).IsEqualTo(SubscribeReasonCode.GrantedQos1);
      await Assert.That(subRes.Success.Subscriptions[1].ReasonCode).IsEqualTo(SubscribeReasonCode.SharedSubscriptionsNotSupported);

      await sub.DisconnectAsync(new DisconnectOptions());
   }

   [Test]
   public async Task Unsubscribe_NonExistentFilter_V5_ShouldReturnNoSubscriptionExistedAndKeepExisting()
   {
      await using var server = MqttServerFactory.CreateBuilder()
         .UseTcp(0)
         .WithDefaultClientIdGenerator()
         .Build();

      await server.StartAsync();
      var endpoint = new IPEndPoint(IPAddress.Loopback, ((IPEndPoint)server.Listeners[0].LocalAddress).Port);

      var tcs = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
      await using var sub = MqttClientFactory.CreateTcp();
      await using var pub = MqttClientFactory.CreateTcp();

      sub.AddMessageReceiveHandler((ctx, _) =>
      {
         tcs.TrySetResult(Encoding.UTF8.GetString(ctx.Message.Payload.Span));
         return ValueTask.CompletedTask;
      });

      await sub.ConnectAsync(new ConnectOptionsBuilder(endpoint).WithClientId("unsub-miss-sub").Build());
      await pub.ConnectAsync(new ConnectOptionsBuilder(endpoint).WithClientId("unsub-miss-pub").Build());

      await sub.SubscribeAsync(SubscribeOptions.Create()
         .WithTopicFilter("keep/active", QualityOfServiceType.AtLeastOnce)
         .Build());

      var unsubRes = await sub.UnsubscribeAsync(UnsubscribeOptions.Create()
         .WithTopicFilter("never/subscribed")
         .Build());

      await Assert.That(unsubRes.Failed).IsFalse();
      await Assert.That(unsubRes.Success!.Unsubscriptions[0].ReasonCode).IsEqualTo(UnsubscribeReasonCode.NoSubscriptionExisted);

      await pub.PublishAsync(PublishOptions.Create()
         .WithTopic("keep/active")
         .WithPayload("still-here")
         .WithQualityOfService(QualityOfServiceType.AtLeastOnce)
         .Build());

      var delivered = await tcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
      await Assert.That(delivered).IsEqualTo("still-here");

      await sub.DisconnectAsync(new DisconnectOptions());
      await pub.DisconnectAsync(new DisconnectOptions());
   }

   [Test]
   public async Task PersistentSession_ShouldPreserveNoLocalAndRetainAsPublishedAcrossReconnect()
   {
      var options = new MqttServerOptions { SupportPersistentSessions = true };
      await using var server = MqttServerFactory.CreateBuilder(options)
         .UseTcp(0)
         .WithDefaultClientIdGenerator()
         .Build();

      await server.StartAsync();
      var endpoint = new IPEndPoint(IPAddress.Loopback, ((IPEndPoint)server.Listeners[0].LocalAddress).Port);

      // 1. Connect, subscribe with NoLocal = true and RetainAsPublished = true, then disconnect
      await using (var sub = MqttClientFactory.CreateTcp())
      {
         await sub.ConnectAsync(new ConnectOptionsBuilder(endpoint)
            .WithProtocolVersion(MqttProtocolVersion.V50)
            .WithClientId("persist-flags-client")
            .WithCleanSession(false)
            .WithSessionExpiryInterval(60)
            .Build());

         await sub.SubscribeAsync(SubscribeOptions.Create()
            .WithTopicFilter("persist/flags", QualityOfServiceType.AtLeastOnce, noLocal: true, retainAsPublished: true)
            .Build());

         await sub.DisconnectAsync(new DisconnectOptions { SessionExpiryInterval = 60 });
      }

      // 2. Reconnect with CleanSession = false (session restored without re-subscribing)
      var received = new ConcurrentQueue<(string Payload, bool Retain)>();
      await using var reconnected = MqttClientFactory.CreateTcp();
      reconnected.AddMessageReceiveHandler((ctx, _) =>
      {
         received.Enqueue((Encoding.UTF8.GetString(ctx.Message.Payload.Span), ctx.Message.Retain));
         return ValueTask.CompletedTask;
      });

      var connRes = await reconnected.ConnectAsync(new ConnectOptionsBuilder(endpoint)
         .WithProtocolVersion(MqttProtocolVersion.V50)
         .WithClientId("persist-flags-client")
         .WithCleanSession(false)
         .WithSessionExpiryInterval(60)
         .Build());
      await Assert.That(connRes.Success!.IsSessionPresent).IsTrue();

      // Self-publish should NOT loop back (NoLocal persisted)
      await reconnected.PublishAsync(PublishOptions.Create()
         .WithTopic("persist/flags")
         .WithPayload("from-self")
         .WithQualityOfService(QualityOfServiceType.AtLeastOnce)
         .WithRetain(true)
         .Build());

      await Task.Delay(250);
      await Assert.That(received.Count).IsEqualTo(0);

      // External publish with Retain = true should arrive with Retain == true (RetainAsPublished persisted)
      await using var otherPub = MqttClientFactory.CreateTcp();
      await otherPub.ConnectAsync(new ConnectOptionsBuilder(endpoint).WithClientId("other-pub").Build());
      await otherPub.PublishAsync(PublishOptions.Create()
         .WithTopic("persist/flags")
         .WithPayload("from-other")
         .WithQualityOfService(QualityOfServiceType.AtLeastOnce)
         .WithRetain(true)
         .Build());

      await WaitUntilAsync(() => received.Count >= 1, TimeSpan.FromSeconds(5));
      var items = received.ToArray();
      await Assert.That(items.Length).IsEqualTo(1);
      await Assert.That(items[0].Payload).IsEqualTo("from-other");
      await Assert.That(items[0].Retain).IsTrue();

      await reconnected.DisconnectAsync(new DisconnectOptions());
      await otherPub.DisconnectAsync(new DisconnectOptions());
   }

   private static async Task WaitUntilAsync(Func<bool> condition, TimeSpan timeout)
   {
      var deadline = DateTime.UtcNow + timeout;
      while (DateTime.UtcNow < deadline)
      {
         if (condition()) return;
         await Task.Delay(25);
      }

      throw new TimeoutException($"Condition was not met within {timeout.TotalSeconds:F1}s.");
   }
}
