using System.Net;
using System.Net.Sockets;
using System.Text;
using Beskar.Mqtt.Client;
using Beskar.Mqtt.Common.Builders.Connecting;
using Beskar.Mqtt.Common.Builders.Disconnecting;
using Beskar.Mqtt.Common.Builders.Publishing;
using Beskar.Mqtt.Common.Builders.Subscribing;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Server;
using Beskar.Mqtt.Server.Internal;
using Beskar.Mqtt.Server.Options;

namespace Beskar.Mqtt.Common.Tests.Internal;

public class MqttQosTests
{
   private static int GetFreePort()
   {
      using var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
      socket.Bind(new IPEndPoint(IPAddress.Loopback, 0));
      return ((IPEndPoint)socket.LocalEndPoint!).Port;
   }

   [Test]
   public async Task QoS0_PublishAndSubscribe_ShouldDeliverMessage()
   {
      var port = GetFreePort();

      await using var server = MqttServerFactory.CreateBuilder()
         .UseTcp(port)
         .WithDefaultClientIdGenerator()
         .Build();

      var startResult = await server.StartAsync();
      await Assert.That(startResult.Failed).IsFalse();

      var subscriber = MqttClientFactory.CreateTcp();
      await subscriber.ConnectAsync(new ConnectOptions
      {
         EndPoint = new IPEndPoint(IPAddress.Loopback, port)
      });

      var tcs = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
      subscriber.AddMessageReceiveHandler((ctx, ct) =>
      {
         tcs.TrySetResult(Encoding.UTF8.GetString(ctx.Message.Payload.Span));
         return ValueTask.CompletedTask;
      });

      var subOptions = new SubscribeOptionsBuilder()
         .WithTopicFilter("test/qos0"u8, QualityOfServiceType.AtMostOnce)
         .Build();

      await subscriber.SubscribeAsync(subOptions);

      var publisher = MqttClientFactory.CreateTcp();
      await publisher.ConnectAsync(new ConnectOptions
      {
         EndPoint = new IPEndPoint(IPAddress.Loopback, port)
      });

      var pubOptions = new PublishOptionsBuilder()
         .WithTopic("test/qos0"u8)
         .WithQualityOfService(QualityOfServiceType.AtMostOnce)
         .WithPayload("QoS0_Payload")
         .Build();

      var pubResult = await publisher.PublishAsync(pubOptions);
      await Assert.That(pubResult.Failed).IsFalse();

      var receivedPayload = await tcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
      await Assert.That(receivedPayload).IsEqualTo("QoS0_Payload");

      await subscriber.DisconnectAsync(new DisconnectOptions());
      await publisher.DisconnectAsync(new DisconnectOptions());
   }

   [Test]
   public async Task QoS1_PublishAndSubscribe_ShouldDeliverMessage()
   {
      var port = GetFreePort();

      await using var server = MqttServerFactory.CreateBuilder()
         .UseTcp(port)
         .WithDefaultClientIdGenerator()
         .Build();

      var startResult = await server.StartAsync();
      await Assert.That(startResult.Failed).IsFalse();

      var subscriber = MqttClientFactory.CreateTcp();
      await subscriber.ConnectAsync(new ConnectOptions
      {
         EndPoint = new IPEndPoint(IPAddress.Loopback, port)
      });

      var tcs = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
      subscriber.AddMessageReceiveHandler((ctx, ct) =>
      {
         tcs.TrySetResult(Encoding.UTF8.GetString(ctx.Message.Payload.Span));
         return ValueTask.CompletedTask;
      });

      var subOptions = new SubscribeOptionsBuilder()
         .WithTopicFilter("test/qos1"u8, QualityOfServiceType.AtLeastOnce)
         .Build();

      await subscriber.SubscribeAsync(subOptions);

      var publisher = MqttClientFactory.CreateTcp();
      await publisher.ConnectAsync(new ConnectOptions
      {
         EndPoint = new IPEndPoint(IPAddress.Loopback, port)
      });

      var pubOptions = new PublishOptionsBuilder()
         .WithTopic("test/qos1"u8)
         .WithQualityOfService(QualityOfServiceType.AtLeastOnce)
         .WithPayload("QoS1_Payload")
         .Build();

      var pubResult = await publisher.PublishAsync(pubOptions);
      await Assert.That(pubResult.Failed).IsFalse();

      var receivedPayload = await tcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
      await Assert.That(receivedPayload).IsEqualTo("QoS1_Payload");

      await subscriber.DisconnectAsync(new DisconnectOptions());
      await publisher.DisconnectAsync(new DisconnectOptions());
   }

   [Test]
   public async Task QoS2_PublishAndSubscribe_ShouldDeliverMessage()
   {
      var port = GetFreePort();

      await using var server = MqttServerFactory.CreateBuilder()
         .UseTcp(port)
         .WithDefaultClientIdGenerator()
         .Build();

      var startResult = await server.StartAsync();
      await Assert.That(startResult.Failed).IsFalse();

      var subscriber = MqttClientFactory.CreateTcp();
      await subscriber.ConnectAsync(new ConnectOptions
      {
         EndPoint = new IPEndPoint(IPAddress.Loopback, port)
      });

      var tcs = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
      subscriber.AddMessageReceiveHandler((ctx, ct) =>
      {
         tcs.TrySetResult(Encoding.UTF8.GetString(ctx.Message.Payload.Span));
         return ValueTask.CompletedTask;
      });

      var subOptions = new SubscribeOptionsBuilder()
         .WithTopicFilter("test/qos2"u8, QualityOfServiceType.ExactlyOnce)
         .Build();

      await subscriber.SubscribeAsync(subOptions);

      var publisher = MqttClientFactory.CreateTcp();
      await publisher.ConnectAsync(new ConnectOptions
      {
         EndPoint = new IPEndPoint(IPAddress.Loopback, port)
      });

      var pubOptions = new PublishOptionsBuilder()
         .WithTopic("test/qos2"u8)
         .WithQualityOfService(QualityOfServiceType.ExactlyOnce)
         .WithPayload("QoS2_Payload")
         .Build();

      var pubResult = await publisher.PublishAsync(pubOptions);
      await Assert.That(pubResult.Failed).IsFalse();

      var receivedPayload = await tcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
      await Assert.That(receivedPayload).IsEqualTo("QoS2_Payload");

      await subscriber.DisconnectAsync(new DisconnectOptions());
      await publisher.DisconnectAsync(new DisconnectOptions());
   }

   [Test]
   public async Task QoSDowngrade_PublishQoS2_SubscribeQoS0_ShouldDowngradeToQoS0()
   {
      var port = GetFreePort();

      await using var server = MqttServerFactory.CreateBuilder()
         .UseTcp(port)
         .WithDefaultClientIdGenerator()
         .Build();

      var startResult = await server.StartAsync();
      await Assert.That(startResult.Failed).IsFalse();

      var subscriber = MqttClientFactory.CreateTcp();
      await subscriber.ConnectAsync(new ConnectOptions
      {
         EndPoint = new IPEndPoint(IPAddress.Loopback, port)
      });

      var tcs = new TaskCompletionSource<QualityOfServiceType>(TaskCreationOptions.RunContinuationsAsynchronously);
      subscriber.AddMessageReceiveHandler((ctx, ct) =>
      {
         tcs.TrySetResult(ctx.Message.QualityOfService);
         return ValueTask.CompletedTask;
      });

      var subOptions = new SubscribeOptionsBuilder()
         .WithTopicFilter("test/downgrade"u8, QualityOfServiceType.AtMostOnce) // QoS 0 subscription
         .Build();

      await subscriber.SubscribeAsync(subOptions);

      var publisher = MqttClientFactory.CreateTcp();
      await publisher.ConnectAsync(new ConnectOptions
      {
         EndPoint = new IPEndPoint(IPAddress.Loopback, port)
      });

      var pubOptions = new PublishOptionsBuilder()
         .WithTopic("test/downgrade"u8)
         .WithQualityOfService(QualityOfServiceType.ExactlyOnce) // QoS 2 publish
         .WithPayload("Payload")
         .Build();

      var pubResult = await publisher.PublishAsync(pubOptions);
      await Assert.That(pubResult.Failed).IsFalse();

      var receivedQos = await tcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
      await Assert.That(receivedQos).IsEqualTo(QualityOfServiceType.AtMostOnce); // CAP to QoS 0

      await subscriber.DisconnectAsync(new DisconnectOptions());
      await publisher.DisconnectAsync(new DisconnectOptions());
   }

   [Test]
   public async Task QoSUpgradeNotAllowed_PublishQoS0_SubscribeQoS2_ShouldDeliverAsQoS0()
   {
      var port = GetFreePort();

      await using var server = MqttServerFactory.CreateBuilder()
         .UseTcp(port)
         .WithDefaultClientIdGenerator()
         .Build();

      var startResult = await server.StartAsync();
      await Assert.That(startResult.Failed).IsFalse();

      var subscriber = MqttClientFactory.CreateTcp();
      await subscriber.ConnectAsync(new ConnectOptions
      {
         EndPoint = new IPEndPoint(IPAddress.Loopback, port)
      });

      var tcs = new TaskCompletionSource<QualityOfServiceType>(TaskCreationOptions.RunContinuationsAsynchronously);
      subscriber.AddMessageReceiveHandler((ctx, ct) =>
      {
         tcs.TrySetResult(ctx.Message.QualityOfService);
         return ValueTask.CompletedTask;
      });

      var subOptions = new SubscribeOptionsBuilder()
         .WithTopicFilter("test/upgrade"u8, QualityOfServiceType.ExactlyOnce) // QoS 2 subscription
         .Build();

      await subscriber.SubscribeAsync(subOptions);

      var publisher = MqttClientFactory.CreateTcp();
      await publisher.ConnectAsync(new ConnectOptions
      {
         EndPoint = new IPEndPoint(IPAddress.Loopback, port)
      });

      var pubOptions = new PublishOptionsBuilder()
         .WithTopic("test/upgrade"u8)
         .WithQualityOfService(QualityOfServiceType.AtMostOnce) // QoS 0 publish
         .WithPayload("Payload")
         .Build();

      var pubResult = await publisher.PublishAsync(pubOptions);
      await Assert.That(pubResult.Failed).IsFalse();

      var receivedQos = await tcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
      await Assert.That(receivedQos).IsEqualTo(QualityOfServiceType.AtMostOnce); // remains QoS 0

      await subscriber.DisconnectAsync(new DisconnectOptions());
      await publisher.DisconnectAsync(new DisconnectOptions());
   }

   [Test]
   public async Task QoS_RetainedMessage_ShouldDeliverOnSubscribe()
   {
      var port = GetFreePort();

      await using var server = MqttServerFactory.CreateBuilder()
         .UseTcp(port)
         .WithDefaultClientIdGenerator()
         .Build();

      var startResult = await server.StartAsync();
      await Assert.That(startResult.Failed).IsFalse();

      var publisher = MqttClientFactory.CreateTcp();
      await publisher.ConnectAsync(new ConnectOptions
      {
         EndPoint = new IPEndPoint(IPAddress.Loopback, port)
      });

      var pubOptions = new PublishOptionsBuilder()
         .WithTopic("test/retained"u8)
         .WithQualityOfService(QualityOfServiceType.AtLeastOnce)
         .WithPayload("RetainedValue")
         .WithRetain()
         .Build();

      var pubResult = await publisher.PublishAsync(pubOptions);
      await Assert.That(pubResult.Failed).IsFalse();

      // Disconnect publisher to ensure it's not live delivery
      await publisher.DisconnectAsync(new DisconnectOptions());

      // Connect subscriber and subscribe with retainAsPublished: true
      var subscriber = MqttClientFactory.CreateTcp();
      await subscriber.ConnectAsync(new ConnectOptions
      {
         EndPoint = new IPEndPoint(IPAddress.Loopback, port)
      });

      var tcs = new TaskCompletionSource<(string, bool)>(TaskCreationOptions.RunContinuationsAsynchronously);
      subscriber.AddMessageReceiveHandler((ctx, ct) =>
      {
         tcs.TrySetResult((Encoding.UTF8.GetString(ctx.Message.Payload.Span), ctx.Message.Retain));
         return ValueTask.CompletedTask;
      });

      var subOptions = new SubscribeOptionsBuilder()
         .WithTopicFilter("test/retained"u8, QualityOfServiceType.AtLeastOnce, retainAsPublished: true)
         .Build();

      await subscriber.SubscribeAsync(subOptions);

      var (payload, isRetained) = await tcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
      await Assert.That(payload).IsEqualTo("RetainedValue");
      await Assert.That(isRetained).IsTrue();

      await subscriber.DisconnectAsync(new DisconnectOptions());
   }

   [Test]
   public async Task QoS_SubscriptionIdentifier_ShouldBePropagated()
   {
      var port = GetFreePort();

      await using var server = MqttServerFactory.CreateBuilder()
         .UseTcp(port)
         .WithDefaultClientIdGenerator()
         .Build();

      var startResult = await server.StartAsync();
      await Assert.That(startResult.Failed).IsFalse();

      var subscriber = MqttClientFactory.CreateTcp();
      await subscriber.ConnectAsync(new ConnectOptions
      {
         EndPoint = new IPEndPoint(IPAddress.Loopback, port)
      });

      var tcs = new TaskCompletionSource<uint>(TaskCreationOptions.RunContinuationsAsynchronously);
      subscriber.AddMessageReceiveHandler((ctx, ct) =>
      {
         if (ctx.Message.SubscriptionIdentifiers.Count > 0)
            tcs.TrySetResult(ctx.Message.SubscriptionIdentifiers[0]);
         else
            tcs.TrySetResult(0);
         return ValueTask.CompletedTask;
      });

      var subOptions = new SubscribeOptionsBuilder()
         .WithTopicFilter("test/subid"u8, QualityOfServiceType.AtLeastOnce)
         .WithSubscriptionIdentifier(123)
         .Build();

      await subscriber.SubscribeAsync(subOptions);

      var publisher = MqttClientFactory.CreateTcp();
      await publisher.ConnectAsync(new ConnectOptions
      {
         EndPoint = new IPEndPoint(IPAddress.Loopback, port)
      });

      var pubOptions = new PublishOptionsBuilder()
         .WithTopic("test/subid"u8)
         .WithQualityOfService(QualityOfServiceType.AtLeastOnce)
         .WithPayload("Payload")
         .Build();

      await publisher.PublishAsync(pubOptions);

      var subId = await tcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
      await Assert.That(subId).IsEqualTo(123u);

      await subscriber.DisconnectAsync(new DisconnectOptions());
      await publisher.DisconnectAsync(new DisconnectOptions());
   }

   [Test]
   public async Task QoS_UserProperties_ShouldBePropagated()
   {
      var port = GetFreePort();

      await using var server = MqttServerFactory.CreateBuilder()
         .UseTcp(port)
         .WithDefaultClientIdGenerator()
         .Build();

      var startResult = await server.StartAsync();
      await Assert.That(startResult.Failed).IsFalse();

      var subscriber = MqttClientFactory.CreateTcp();
      await subscriber.ConnectAsync(new ConnectOptions
      {
         EndPoint = new IPEndPoint(IPAddress.Loopback, port)
      });

      var tcs = new TaskCompletionSource<(string, string)>(TaskCreationOptions.RunContinuationsAsynchronously);
      subscriber.AddMessageReceiveHandler((ctx, ct) =>
      {
         string? key = null;
         string? val = null;
         if (ctx.Message.UserProperties.Count > 0)
         {
            var prop = ctx.Message.UserProperties[0];
            key = prop.Name;
            val = Encoding.UTF8.GetString(prop.Value.Span);
         }

         tcs.TrySetResult((key ?? "", val ?? ""));
         return ValueTask.CompletedTask;
      });

      var subOptions = new SubscribeOptionsBuilder()
         .WithTopicFilter("test/userprops"u8, QualityOfServiceType.AtLeastOnce)
         .Build();

      await subscriber.SubscribeAsync(subOptions);

      var publisher = MqttClientFactory.CreateTcp();
      await publisher.ConnectAsync(new ConnectOptions
      {
         EndPoint = new IPEndPoint(IPAddress.Loopback, port)
      });

      var pubOptions = new PublishOptionsBuilder()
         .WithTopic("test/userprops"u8)
         .WithQualityOfService(QualityOfServiceType.AtLeastOnce)
         .WithPayload("Payload")
         .WithUserProperty("customKey", "customValue")
         .Build();

      await publisher.PublishAsync(pubOptions);

      var (receivedKey, receivedValue) = await tcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
      await Assert.That(receivedKey).IsEqualTo("customKey");
      await Assert.That(receivedValue).IsEqualTo("customValue");

      await subscriber.DisconnectAsync(new DisconnectOptions());
      await publisher.DisconnectAsync(new DisconnectOptions());
   }

   [Test]
   public async Task QoS_WildcardSubscription_ShouldDeliverMessage()
   {
      var port = GetFreePort();

      await using var server = MqttServerFactory.CreateBuilder()
         .UseTcp(port)
         .WithDefaultClientIdGenerator()
         .Build();

      var startResult = await server.StartAsync();
      await Assert.That(startResult.Failed).IsFalse();

      var subscriber = MqttClientFactory.CreateTcp();
      await subscriber.ConnectAsync(new ConnectOptions
      {
         EndPoint = new IPEndPoint(IPAddress.Loopback, port)
      });

      var tcs = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
      subscriber.AddMessageReceiveHandler((ctx, ct) =>
      {
         tcs.TrySetResult(Encoding.UTF8.GetString(ctx.Message.Payload.Span));
         return ValueTask.CompletedTask;
      });

      var subOptions = new SubscribeOptionsBuilder()
         .WithTopicFilter("sensor/+/temperature/#"u8, QualityOfServiceType.AtLeastOnce)
         .Build();

      await subscriber.SubscribeAsync(subOptions);

      var publisher = MqttClientFactory.CreateTcp();
      await publisher.ConnectAsync(new ConnectOptions
      {
         EndPoint = new IPEndPoint(IPAddress.Loopback, port)
      });

      var pubOptions = new PublishOptionsBuilder()
         .WithTopic("sensor/kitchen/temperature/celsius"u8)
         .WithQualityOfService(QualityOfServiceType.AtLeastOnce)
         .WithPayload("23.5")
         .Build();

      await publisher.PublishAsync(pubOptions);

      var payload = await tcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
      await Assert.That(payload).IsEqualTo("23.5");

      await subscriber.DisconnectAsync(new DisconnectOptions());
      await publisher.DisconnectAsync(new DisconnectOptions());
   }

   [Test]
   public async Task QoS_noLocal_ShouldNotForwardBackToPublisher()
   {
      var port = GetFreePort();

      await using var server = MqttServerFactory.CreateBuilder()
         .UseTcp(port)
         .WithDefaultClientIdGenerator()
         .Build();

      var startResult = await server.StartAsync();
      await Assert.That(startResult.Failed).IsFalse();

      // Subscriber/Publisher A
      var clientA = MqttClientFactory.CreateTcp();
      await clientA.ConnectAsync(new ConnectOptions
      {
         EndPoint = new IPEndPoint(IPAddress.Loopback, port)
      });

      // Subscriber B
      var clientB = MqttClientFactory.CreateTcp();
      await clientB.ConnectAsync(new ConnectOptions
      {
         EndPoint = new IPEndPoint(IPAddress.Loopback, port)
      });

      var tcsA = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
      clientA.AddMessageReceiveHandler((ctx, ct) =>
      {
         tcsA.TrySetResult(Encoding.UTF8.GetString(ctx.Message.Payload.Span));
         return ValueTask.CompletedTask;
      });

      var tcsB = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
      clientB.AddMessageReceiveHandler((ctx, ct) =>
      {
         tcsB.TrySetResult(Encoding.UTF8.GetString(ctx.Message.Payload.Span));
         return ValueTask.CompletedTask;
      });

      // A subscribes with noLocal: true
      await clientA.SubscribeAsync(new SubscribeOptionsBuilder()
         .WithTopicFilter("test/nolocal"u8, QualityOfServiceType.AtLeastOnce, true)
         .Build());

      // B subscribes with noLocal: false (default)
      await clientB.SubscribeAsync(new SubscribeOptionsBuilder()
         .WithTopicFilter("test/nolocal"u8, QualityOfServiceType.AtLeastOnce)
         .Build());

      // A publishes to topic
      var pubOptions = new PublishOptionsBuilder()
         .WithTopic("test/nolocal"u8)
         .WithQualityOfService(QualityOfServiceType.AtLeastOnce)
         .WithPayload("NoLocalMsg")
         .Build();

      await clientA.PublishAsync(pubOptions);

      // Verify B receives it
      var receivedB = await tcsB.Task.WaitAsync(TimeSpan.FromSeconds(5));
      await Assert.That(receivedB).IsEqualTo("NoLocalMsg");

      // Verify A does NOT receive it
      var didAStart = Task.Run(async () => { await tcsA.Task; });
      var completed = await Task.WhenAny(didAStart, Task.Delay(500));
      await Assert.That(completed != didAStart).IsTrue(); // timed out, which means A did not receive it

      await clientA.DisconnectAsync(new DisconnectOptions());
      await clientB.DisconnectAsync(new DisconnectOptions());
   }

   [Test]
   public async Task QoS_CleanStartFalse_WithSessionExpiry_ShouldQueueOfflineMessages()
   {
      var port = GetFreePort();

      // Explicitly enable persistent session support on the server
      var serverOptions = new MqttServerOptions
      {
         SupportPersistentSessions = true
      };

      await using var server = MqttServerFactory.CreateBuilder(serverOptions)
         .UseTcp(port)
         .WithDefaultClientIdGenerator()
         .Build();

      var startResult = await server.StartAsync();
      await Assert.That(startResult.Failed).IsFalse();

      var clientId = Encoding.UTF8.GetBytes("client-persistent-id");
      MqttSession? sessionA = null;
      var disconnectEventFired = false;

      // Capture sessionA from server's OnConnect event
      server.Events.OnConnect.Add((ctx, ct) =>
      {
         if (ctx.Client.ClientIdUtf8Bytes.Span.SequenceEqual(clientId)) sessionA = ctx.Client.MqttSession;
         return ValueTask.CompletedTask;
      });

      server.Events.OnDisconnect.Add((ctx, ct) =>
      {
         if (ctx.ServerClient.ClientIdUtf8Bytes.Span.SequenceEqual(clientId)) disconnectEventFired = true;
         return ValueTask.CompletedTask;
      });

      // 1. First connection of Client A: cleanSession = true, sets up subscription
      var clientA1 = MqttClientFactory.CreateTcp();
      await clientA1.ConnectAsync(new ConnectOptions
      {
         EndPoint = new IPEndPoint(IPAddress.Loopback, port),
         ClientIdUtf8Bytes = clientId,
         CleanSession = true,
         SessionExpiryInterval = 300
      });

      await clientA1.SubscribeAsync(new SubscribeOptionsBuilder()
         .WithTopicFilter("test/offline"u8, QualityOfServiceType.AtLeastOnce)
         .Build());

      // Disconnect Client A1
      await clientA1.DisconnectAsync(new DisconnectOptions());

      // Wait for disconnect event to fire on the server
      var start = DateTime.UtcNow;
      while (!disconnectEventFired && DateTime.UtcNow - start < TimeSpan.FromSeconds(5)) await Task.Delay(50);

      // Add a tiny extra delay to make sure session cleanup completes
      await Task.Delay(100);

      // Verify server session state diagnostic info
      if (sessionA == null) throw new Exception("Diagnostic: sessionA is NULL");
      if (sessionA.Client != null)
         throw new Exception(
            $"Diagnostic: sessionA.Client is NOT NULL. ClientId={Encoding.UTF8.GetString(sessionA.Client.ClientIdUtf8Bytes.Span)}. IsConnected={sessionA.Client.IsConnected}. DisconnectEventFired={disconnectEventFired}");

      // 2. Client B (Publisher) publishes QoS 1 message while A is offline
      var publisher = MqttClientFactory.CreateTcp();
      await publisher.ConnectAsync(new ConnectOptions
      {
         EndPoint = new IPEndPoint(IPAddress.Loopback, port)
      });

      await publisher.PublishAsync(new PublishOptionsBuilder()
         .WithTopic("test/offline"u8)
         .WithQualityOfService(QualityOfServiceType.AtLeastOnce)
         .WithPayload("OfflinePayload")
         .Build());

      await publisher.DisconnectAsync(new DisconnectOptions());

      // 3. Client A reconnects with cleanSession = false to recover the session
      var clientA2 = MqttClientFactory.CreateTcp();
      var tcs = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
      clientA2.AddMessageReceiveHandler((ctx, ct) =>
      {
         tcs.TrySetResult(Encoding.UTF8.GetString(ctx.Message.Payload.Span));
         return ValueTask.CompletedTask;
      });

      await clientA2.ConnectAsync(new ConnectOptions
      {
         EndPoint = new IPEndPoint(IPAddress.Loopback, port),
         ClientIdUtf8Bytes = clientId,
         CleanSession = false
      });

      // Verify the queued offline message is delivered upon reconnection
      var received = await tcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
      await Assert.That(received).IsEqualTo("OfflinePayload");

      await clientA2.DisconnectAsync(new DisconnectOptions());
   }

   [Test]
   public async Task QoS_RetainHandling_DoNotSend_ShouldNotDeliverRetainedMessage()
   {
      var port = GetFreePort();

      await using var server = MqttServerFactory.CreateBuilder()
         .UseTcp(port)
         .WithDefaultClientIdGenerator()
         .Build();

      var startResult = await server.StartAsync();
      await Assert.That(startResult.Failed).IsFalse();

      // 1. Publish retained message
      var publisher = MqttClientFactory.CreateTcp();
      await publisher.ConnectAsync(new ConnectOptions { EndPoint = new IPEndPoint(IPAddress.Loopback, port) });
      await publisher.PublishAsync(new PublishOptionsBuilder()
         .WithTopic("test/retainhandling/nosend"u8)
         .WithQualityOfService(QualityOfServiceType.AtLeastOnce)
         .WithPayload("RetainedVal")
         .WithRetain(true)
         .Build());
      await publisher.DisconnectAsync(new DisconnectOptions());

      // 2. Subscribe with RetainHandlingType.DoNotSend
      var subscriber = MqttClientFactory.CreateTcp();
      await subscriber.ConnectAsync(new ConnectOptions { EndPoint = new IPEndPoint(IPAddress.Loopback, port) });

      var tcs = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
      subscriber.AddMessageReceiveHandler((ctx, ct) =>
      {
         tcs.TrySetResult(Encoding.UTF8.GetString(ctx.Message.Payload.Span));
         return ValueTask.CompletedTask;
      });

      await subscriber.SubscribeAsync(new SubscribeOptionsBuilder()
         .WithTopicFilter("test/retainhandling/nosend"u8, QualityOfServiceType.AtLeastOnce, retainHandling: RetainHandlingType.DoNotSend)
         .Build());

      // Verify no message is delivered
      var completed = await Task.WhenAny(tcs.Task, Task.Delay(500));
      await Assert.That(completed != tcs.Task).IsTrue(); // Timed out (correct)

      await subscriber.DisconnectAsync(new DisconnectOptions());
   }

   [Test]
   public async Task QoS_RetainHandling_SendOnNewSubscriptionOnly_ShouldDeliverOnNewSubscription()
   {
      var port = GetFreePort();

      await using var server = MqttServerFactory.CreateBuilder()
         .UseTcp(port)
         .WithDefaultClientIdGenerator()
         .Build();

      var startResult = await server.StartAsync();
      await Assert.That(startResult.Failed).IsFalse();

      // 1. Publish retained message
      var publisher = MqttClientFactory.CreateTcp();
      await publisher.ConnectAsync(new ConnectOptions { EndPoint = new IPEndPoint(IPAddress.Loopback, port) });
      await publisher.PublishAsync(new PublishOptionsBuilder()
         .WithTopic("test/retainhandling/newonly"u8)
         .WithQualityOfService(QualityOfServiceType.AtLeastOnce)
         .WithPayload("RetainedValNewOnly")
         .WithRetain(true)
         .Build());
      await publisher.DisconnectAsync(new DisconnectOptions());

      // 2. Subscribe with RetainHandlingType.SendOnNewSubscriptionOnly
      var subscriber = MqttClientFactory.CreateTcp();
      await subscriber.ConnectAsync(new ConnectOptions { EndPoint = new IPEndPoint(IPAddress.Loopback, port) });

      var tcs = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
      subscriber.AddMessageReceiveHandler((ctx, ct) =>
      {
         tcs.TrySetResult(Encoding.UTF8.GetString(ctx.Message.Payload.Span));
         return ValueTask.CompletedTask;
      });

      await subscriber.SubscribeAsync(new SubscribeOptionsBuilder()
         .WithTopicFilter("test/retainhandling/newonly"u8, QualityOfServiceType.AtLeastOnce, retainHandling: RetainHandlingType.SendOnNewSubscriptionOnly)
         .Build());

      // Verify message is delivered
      var received = await tcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
      await Assert.That(received).IsEqualTo("RetainedValNewOnly");

      await subscriber.DisconnectAsync(new DisconnectOptions());
   }

   [Test]
   public async Task QoS_TopicAlias_EndToEnd_ShouldDeliverMessagesWithAlias()
   {
      var port = GetFreePort();

      await using var server = MqttServerFactory.CreateBuilder()
         .UseTcp(port)
         .WithDefaultClientIdGenerator()
         .Build();

      var startResult = await server.StartAsync();
      await Assert.That(startResult.Failed).IsFalse();

      // 1. Subscribe
      var subscriber = MqttClientFactory.CreateTcp();
      await subscriber.ConnectAsync(new ConnectOptions { EndPoint = new IPEndPoint(IPAddress.Loopback, port) });

      var tcs1 = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
      var tcs2 = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
      int receiveCount = 0;
      subscriber.AddMessageReceiveHandler((ctx, ct) =>
      {
         var topic = ctx.Message.Topic;
         var payload = Encoding.UTF8.GetString(ctx.Message.Payload.Span);
         if (Interlocked.Increment(ref receiveCount) == 1)
         {
            tcs1.TrySetResult($"{topic}:{payload}");
         }
         else
         {
            tcs2.TrySetResult($"{topic}:{payload}");
         }
         return ValueTask.CompletedTask;
      });

      await subscriber.SubscribeAsync(new SubscribeOptionsBuilder()
         .WithTopicFilter("test/alias/topic"u8, QualityOfServiceType.AtMostOnce)
         .Build());

      // 2. Publish with Topic Alias
      var publisher = MqttClientFactory.CreateTcp();
      await publisher.ConnectAsync(new ConnectOptions { EndPoint = new IPEndPoint(IPAddress.Loopback, port) });

      // First publish registers alias 1
      await publisher.PublishAsync(new PublishOptionsBuilder()
         .WithTopic("test/alias/topic"u8)
         .WithTopicAlias(1)
         .WithQualityOfService(QualityOfServiceType.AtMostOnce)
         .WithPayload("FirstPayload")
         .Build());

      // Second publish uses alias 1 without topic name
      await publisher.PublishAsync(new PublishOptionsBuilder()
         .WithTopic(ReadOnlyMemory<byte>.Empty)
         .WithTopicAlias(1)
         .WithQualityOfService(QualityOfServiceType.AtMostOnce)
         .WithPayload("SecondPayload")
         .Build());

      var r1 = await tcs1.Task.WaitAsync(TimeSpan.FromSeconds(5));
      await Assert.That(r1).IsEqualTo("test/alias/topic:FirstPayload");

      var r2 = await tcs2.Task.WaitAsync(TimeSpan.FromSeconds(5));
      await Assert.That(r2).IsEqualTo("test/alias/topic:SecondPayload");

      await subscriber.DisconnectAsync(new DisconnectOptions());
      await publisher.DisconnectAsync(new DisconnectOptions());
   }
}
