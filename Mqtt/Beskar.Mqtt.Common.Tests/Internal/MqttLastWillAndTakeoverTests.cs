using System.Buffers;
using System.Collections.Concurrent;
using System.Net;
using System.Text;
using Beskar.Mqtt.Client;
using Beskar.Mqtt.Common.Builders.Connecting;
using Beskar.Mqtt.Common.Builders.Disconnecting;
using Beskar.Mqtt.Common.Builders.Publishing;
using Beskar.Mqtt.Common.Builders.Subscribing;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Models;
using Beskar.Mqtt.Server;

namespace Beskar.Mqtt.Common.Tests.Internal;

public class MqttLastWillAndTakeoverTests
{
   [Test]
   public async Task WillRetain_EmptyPayload_OnCrash_ShouldClearRetainedMessageForLiveAndFutureSubscribers()
   {
      await using var server = MqttServerFactory.CreateBuilder()
         .UseTcp(0)
         .WithDefaultClientIdGenerator()
         .Build();

      await server.StartAsync();
      var endpoint = new IPEndPoint(IPAddress.Loopback, ((IPEndPoint)server.Listeners[0].LocalAddress).Port);

      const string nodeTopic = "registry/nodes/worker-1";
      var watcherEvents = new ConcurrentQueue<(string Topic, int Length, bool Retain)>();
      var initialReceived = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
      var tombstoneReceived = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

      await using var liveWatcher = MqttClientFactory.CreateTcp();
      liveWatcher.AddMessageReceiveHandler((ctx, _) =>
      {
         watcherEvents.Enqueue((ctx.Message.Topic, ctx.Message.Payload.Length, ctx.Message.Retain));
         if (ctx.Message.Payload.IsEmpty)
         {
            tombstoneReceived.TrySetResult();
         }
         else
         {
            initialReceived.TrySetResult();
         }

         return ValueTask.CompletedTask;
      });

      await liveWatcher.ConnectAsync(new ConnectOptionsBuilder(endpoint).WithClientId("live-watcher").Build());
      await liveWatcher.SubscribeAsync(SubscribeOptions.Create()
         .WithTopicFilter("registry/nodes/+", QualityOfServiceType.AtLeastOnce)
         .Build());

      // Node connects with a retained empty-payload Will and publishes a retained announcement
      var nodeClient = MqttClientFactory.CreateTcp();
      await nodeClient.ConnectAsync(new ConnectOptionsBuilder(endpoint)
         .WithProtocolVersion(MqttProtocolVersion.V50)
         .WithClientId("worker-1")
         .WithWill(nodeTopic, Array.Empty<byte>(), QualityOfServiceType.AtLeastOnce, retain: true)
         .Build());

      await nodeClient.PublishAsync(PublishOptions.Create()
         .WithTopic(nodeTopic)
         .WithPayload("{\"status\":\"online\"}")
         .WithQualityOfService(QualityOfServiceType.AtLeastOnce)
         .WithRetain(true)
         .Build());

      await initialReceived.Task.WaitAsync(TimeSpan.FromSeconds(5));
      await Assert.That(server.RetainedMessages.GetMessages().Count).IsEqualTo(1);

      // Simulate abrupt crash (dispose without DISCONNECT)
      await nodeClient.DisposeAsync();
      await tombstoneReceived.Task.WaitAsync(TimeSpan.FromSeconds(5));

      // Broker retained store must now be empty
      await Assert.That(server.RetainedMessages.GetMessages().Count).IsEqualTo(0);

      // A late-joining subscriber must receive NO retained messages
      var lateMessages = new ConcurrentQueue<MqttPublishMessage>();
      await using var lateWatcher = MqttClientFactory.CreateTcp();
      lateWatcher.AddMessageReceiveHandler((ctx, _) =>
      {
         lateMessages.Enqueue(ctx.Message);
         return ValueTask.CompletedTask;
      });

      await lateWatcher.ConnectAsync(new ConnectOptionsBuilder(endpoint).WithClientId("late-watcher").Build());
      await lateWatcher.SubscribeAsync(SubscribeOptions.Create()
         .WithTopicFilter("registry/nodes/+", QualityOfServiceType.AtLeastOnce)
         .Build());

      await Task.Delay(250);
      await Assert.That(lateMessages.Count).IsEqualTo(0);

      await liveWatcher.DisconnectAsync(new DisconnectOptions());
      await lateWatcher.DisconnectAsync(new DisconnectOptions());
   }

   [Test]
   public async Task WillRetain_NonEmptyPayload_OnCrash_ShouldPersistForLateSubscribers()
   {
      await using var server = MqttServerFactory.CreateBuilder()
         .UseTcp(0)
         .WithDefaultClientIdGenerator()
         .Build();

      await server.StartAsync();
      var endpoint = new IPEndPoint(IPAddress.Loopback, ((IPEndPoint)server.Listeners[0].LocalAddress).Port);

      var willFiredOnServer = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
      server.Events.OnRetainedMessageChanged.Add((ctx, _) =>
      {
         if (ctx.ChangedRetainedMessage is not null)
         {
            willFiredOnServer.TrySetResult();
         }
         return ValueTask.CompletedTask;
      });

      var device = MqttClientFactory.CreateTcp();
      await device.ConnectAsync(new ConnectOptionsBuilder(endpoint)
         .WithClientId("will-persist-device")
         .WithWill("devices/will-persist-device/state", "crashed"u8.ToArray(), QualityOfServiceType.AtLeastOnce, retain: true)
         .Build());

      await device.DisposeAsync();
      await willFiredOnServer.Task.WaitAsync(TimeSpan.FromSeconds(5));

      var lateTcs = new TaskCompletionSource<(string Payload, bool Retain)>(TaskCreationOptions.RunContinuationsAsynchronously);
      await using var lateSub = MqttClientFactory.CreateTcp();
      lateSub.AddMessageReceiveHandler((ctx, _) =>
      {
         lateTcs.TrySetResult((Encoding.UTF8.GetString(ctx.Message.Payload.Span), ctx.Message.Retain));
         return ValueTask.CompletedTask;
      });

      await lateSub.ConnectAsync(new ConnectOptionsBuilder(endpoint).WithClientId("will-late-sub").Build());
      await lateSub.SubscribeAsync(SubscribeOptions.Create()
         .WithTopicFilter("devices/+/state", QualityOfServiceType.AtLeastOnce)
         .Build());

      var (payload, retain) = await lateTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
      await Assert.That(payload).IsEqualTo("crashed");
      await Assert.That(retain).IsTrue();

      await lateSub.DisconnectAsync(new DisconnectOptions());
   }

   [Test]
   public async Task WillMessage_SessionTakeover_ShouldCancelOldClientWillAndPreserveNewClientWill()
   {
      await using var server = MqttServerFactory.CreateBuilder()
         .UseTcp(0)
         .WithDefaultClientIdGenerator()
         .Build();

      await server.StartAsync();
      var endpoint = new IPEndPoint(IPAddress.Loopback, ((IPEndPoint)server.Listeners[0].LocalAddress).Port);

      var receivedWills = new ConcurrentQueue<string>();
      var fromBTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

      await using var monitor = MqttClientFactory.CreateTcp();
      monitor.AddMessageReceiveHandler((ctx, _) =>
      {
         var text = Encoding.UTF8.GetString(ctx.Message.Payload.Span);
         receivedWills.Enqueue(text);
         if (text == "from-B")
         {
            fromBTcs.TrySetResult();
         }

         return ValueTask.CompletedTask;
      });

      await monitor.ConnectAsync(new ConnectOptionsBuilder(endpoint).WithClientId("takeover-monitor").Build());
      await monitor.SubscribeAsync(SubscribeOptions.Create()
         .WithTopicFilter("nodes/shared/will", QualityOfServiceType.AtLeastOnce)
         .Build());

      var clientA = MqttClientFactory.CreateTcp();
      var clientADisconnected = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
      clientA.AddDisconnectedHandler((_, _) =>
      {
         clientADisconnected.TrySetResult();
         return ValueTask.CompletedTask;
      });

      await clientA.ConnectAsync(new ConnectOptionsBuilder(endpoint)
         .WithClientId("shared-node")
         .WithWill("nodes/shared/will", "from-A"u8.ToArray(), QualityOfServiceType.AtLeastOnce)
         .Build());

      // Client B takes over the same ClientId with a different Will
      var clientB = MqttClientFactory.CreateTcp();
      await clientB.ConnectAsync(new ConnectOptionsBuilder(endpoint)
         .WithClientId("shared-node")
         .WithWill("nodes/shared/will", "from-B"u8.ToArray(), QualityOfServiceType.AtLeastOnce)
         .Build());

      await clientADisconnected.Task.WaitAsync(TimeSpan.FromSeconds(5));
      await clientA.DisposeAsync();

      // Wait briefly to confirm Client A's old Will does NOT fire
      await Task.Delay(350);
      await Assert.That(receivedWills.ToArray()).DoesNotContain("from-A");

      // Now crash Client B — only "from-B" should fire
      await clientB.DisposeAsync();
      await fromBTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));

      await Assert.That(receivedWills.ToArray()).IsEquivalentTo(["from-B"]);
      await monitor.DisconnectAsync(new DisconnectOptions());
   }

   [Test]
   public async Task WillMessage_AllV5Properties_ShouldBeDeliveredIntactOnCrash()
   {
      await using var server = MqttServerFactory.CreateBuilder()
         .UseTcp(0)
         .WithDefaultClientIdGenerator()
         .Build();

      await server.StartAsync();
      var endpoint = new IPEndPoint(IPAddress.Loopback, ((IPEndPoint)server.Listeners[0].LocalAddress).Port);

      var tcs = new TaskCompletionSource<MqttPublishMessage>(TaskCreationOptions.RunContinuationsAsynchronously);
      await using var monitor = MqttClientFactory.CreateTcp();
      monitor.AddMessageReceiveHandler((ctx, _) =>
      {
         tcs.TrySetResult(ctx.Message);
         return ValueTask.CompletedTask;
      });

      await monitor.ConnectAsync(new ConnectOptionsBuilder(endpoint).WithClientId("will-props-monitor").Build());
      await monitor.SubscribeAsync(SubscribeOptions.Create()
         .WithTopicFilter("will/props", QualityOfServiceType.AtLeastOnce)
         .Build());

      var correlation = new byte[] { 0xDE, 0xAD, 0xBE, 0xEF };
      var client = MqttClientFactory.CreateTcp();
      await client.ConnectAsync(new ConnectOptionsBuilder(endpoint)
         .WithProtocolVersion(MqttProtocolVersion.V50)
         .WithClientId("will-props-node")
         .WithWill("will/props", "{\"down\":true}"u8.ToArray(), QualityOfServiceType.AtLeastOnce)
         .WithWillPayloadFormat(PayloadFormat.CharacterData)
         .WithWillContentType("application/json")
         .WithWillResponseTopic("will/ack")
         .WithWillCorrelationData(correlation)
         .WithWillMessageExpiryInterval(60)
         .WithWillUserProperty("cause", "sigkill")
         .WithWillUserProperty("region", "eu-central")
         .Build());

      await client.DisposeAsync();

      var msg = await tcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
      await Assert.That(Encoding.UTF8.GetString(msg.Payload.Span)).IsEqualTo("{\"down\":true}");
      await Assert.That(msg.PayloadFormat).IsEqualTo(PayloadFormat.CharacterData);
      await Assert.That(msg.ContentType).IsEqualTo("application/json");
      await Assert.That(msg.ResponseTopic).IsEqualTo("will/ack");
      await Assert.That(msg.CorrelationData?.ToArray()).IsEquivalentTo(correlation);
      await Assert.That(msg.MessageExpiryInterval).IsGreaterThan(0u);

      var props = msg.UserProperties.ToDictionary(p => p.Name, p => Encoding.UTF8.GetString(p.Value.Span));
      await Assert.That(props["cause"]).IsEqualTo("sigkill");
      await Assert.That(props["region"]).IsEqualTo("eu-central");

      await monitor.DisconnectAsync(new DisconnectOptions());
   }
}
