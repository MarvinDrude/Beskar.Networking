using System.Net;
using System.Reflection;
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

public class MqttReceiveMaximumTests
{
   [Test]
   public async Task ReceiveMaximum_ClientThrottling_ShouldThrottleQoS1()
   {

      // Start server with ReceiveMaximum = 1
      var serverOptions = new MqttServerOptions
      {
         ReceiveMaximum = 1
      };
      await using var server = MqttServerFactory.CreateBuilder(serverOptions)
         .UseTcp(0)
         .WithDefaultClientIdGenerator()
         .Build();

      await server.StartAsync();

      var port = ((IPEndPoint)server.Listeners[0].LocalAddress).Port;

      var client = (MqttClient)MqttClientFactory.CreateTcp();
      await client.ConnectAsync(new ConnectOptions
      {
         EndPoint = new IPEndPoint(IPAddress.Loopback, port)
      });

      // Hook server OnAcknowledgePub to delay acknowledgements so we can catch the client in a throttled state
      var ackTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
      var proceedTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
      server.Events.OnAcknowledgePub.Add(async (ctx, ct) =>
      {
         ackTcs.TrySetResult();
         await proceedTcs.Task;
      });

      // Publish QoS 1 message
      var pubOptions = new PublishOptionsBuilder()
         .WithTopic("test/receivemax"u8)
         .WithQualityOfService(QualityOfServiceType.AtLeastOnce)
         .WithPayload("Payload1")
         .Build();

      var pub1Task = client.PublishAsync(pubOptions);

      // Wait until the server receives the publication and enters the hook (so pub1 is in-flight)
      await ackTcs.Task.WaitAsync(TimeSpan.FromSeconds(30));

      // Attempt to publish a second QoS 1 message. It should be throttled by the client because in-flight is 1 (equal to ReceiveMaximum).
      var pub2Task = client.PublishAsync(pubOptions);

      // Verify that pub2Task is indeed blocked/throttled
      var completedTask = await Task.WhenAny(pub2Task, Task.Delay(500));
      await Assert.That(completedTask != pub2Task).IsTrue(); // It timed out, meaning it's throttled

      // Release the hook to allow pub1 to be acknowledged
      proceedTcs.TrySetResult();

      // Now both publications should complete successfully
      var result1 = await pub1Task.WaitAsync(TimeSpan.FromSeconds(30));
      var result2 = await pub2Task.WaitAsync(TimeSpan.FromSeconds(30));

      await Assert.That(result1.Failed).IsFalse();
      await Assert.That(result2.Failed).IsFalse();

      await client.DisconnectAsync(new DisconnectOptions());
   }

   [Test]
   public async Task ReceiveMaximum_ServerThrottling_ShouldQueueAndDeliverSubsequentMessages()
   {

      await using var server = MqttServerFactory.CreateBuilder()
         .UseTcp(0)
         .WithDefaultClientIdGenerator()
         .Build();

      await server.StartAsync();

      var port = ((IPEndPoint)server.Listeners[0].LocalAddress).Port;

      // Subscriber connects with ReceiveMaximum = 1
      var subscriber = (MqttClient)MqttClientFactory.CreateTcp();
      await subscriber.ConnectAsync(new ConnectOptions
      {
         EndPoint = new IPEndPoint(IPAddress.Loopback, port),
         ReceiveMaximum = 1
      });

      // Track received messages on the subscriber
      var receivedMsgs = new List<string>();
      var receiveTcs1 = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
      var receiveTcs2 = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
      var ackTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

      subscriber.AddMessageReceiveHandler(async (ctx, ct) =>
      {
         lock (receivedMsgs)
         {
            receivedMsgs.Add(Encoding.UTF8.GetString(ctx.Message.Payload.Span));
         }

         if (receivedMsgs.Count == 1)
         {
            receiveTcs1.TrySetResult();
            await ackTcs.Task; // Hold acknowledgment of the first message
         }
         else if (receivedMsgs.Count == 2)
         {
            receiveTcs2.TrySetResult();
         }
      });

      await subscriber.SubscribeAsync(new SubscribeOptionsBuilder()
         .WithTopicFilter("test/serverthrottling"u8, QualityOfServiceType.AtLeastOnce)
         .Build());

      // Publisher publishes two messages
      var publisher = (MqttClient)MqttClientFactory.CreateTcp();
      await publisher.ConnectAsync(new ConnectOptions
      {
         EndPoint = new IPEndPoint(IPAddress.Loopback, port)
      });

      var pubOptions = new PublishOptionsBuilder()
         .WithTopic("test/serverthrottling"u8)
         .WithQualityOfService(QualityOfServiceType.AtLeastOnce)
         .WithPayload("Payload1")
         .Build();
      await publisher.PublishAsync(pubOptions);

      pubOptions = new PublishOptionsBuilder()
         .WithTopic("test/serverthrottling"u8)
         .WithQualityOfService(QualityOfServiceType.AtLeastOnce)
         .WithPayload("Payload2")
         .Build();
      await publisher.PublishAsync(pubOptions);

      // Wait until the first message is received by subscriber
      await receiveTcs1.Task.WaitAsync(TimeSpan.FromSeconds(30));

      // Verify that the second message is not received yet (because subscriber is holding ack of first,
      // and client receive maximum is 1, so server throttles and queues it)
      await Task.Delay(500);
      int count1;
      lock (receivedMsgs)
      {
         count1 = receivedMsgs.Count;
      }
      await Assert.That(count1).IsEqualTo(1);

      // Now release the acknowledgment of the first message
      ackTcs.TrySetResult();

      // The subscriber should now receive the second message
      await receiveTcs2.Task.WaitAsync(TimeSpan.FromSeconds(30));

      int count2;
      string p1;
      string p2;
      lock (receivedMsgs)
      {
         count2 = receivedMsgs.Count;
         p1 = receivedMsgs.Count > 0 ? receivedMsgs[0] : "";
         p2 = receivedMsgs.Count > 1 ? receivedMsgs[1] : "";
      }

      await Assert.That(count2).IsEqualTo(2);
      await Assert.That(p1).IsEqualTo("Payload1");
      await Assert.That(p2).IsEqualTo("Payload2");

      await subscriber.DisconnectAsync(new DisconnectOptions());
      await publisher.DisconnectAsync(new DisconnectOptions());
   }

   [Test]
   public async Task ReceiveMaximum_ServerEnforcement_Exceeded_ShouldDisconnectClient()
   {

      // Server ReceiveMaximum = 1
      var serverOptions = new MqttServerOptions
      {
         ReceiveMaximum = 1
      };
      await using var server = MqttServerFactory.CreateBuilder(serverOptions)
         .UseTcp(0)
         .WithDefaultClientIdGenerator()
         .Build();

      await server.StartAsync();

      var port = ((IPEndPoint)server.Listeners[0].LocalAddress).Port;

      var client = (MqttClient)MqttClientFactory.CreateTcp();

      var disconnectTcs = new TaskCompletionSource<DisconnectReasonCode>(TaskCreationOptions.RunContinuationsAsynchronously);
      client.Events.OnClientDisconnected.Add((ctx, ct) =>
      {
         disconnectTcs.TrySetResult(ctx.ReasonCode);
         return ValueTask.CompletedTask;
      });

      await client.ConnectAsync(new ConnectOptions
      {
         EndPoint = new IPEndPoint(IPAddress.Loopback, port)
      });

      // Bypass the client's own throttling semaphore using reflection
      var semField = typeof(MqttClient).GetField("_inFlightSemaphore", BindingFlags.NonPublic | BindingFlags.Instance);
      semField?.SetValue(client, null); // Disable client-side throttling

      // Publish two QoS 2 messages. The first will increment in-flight to 1.
      // The second will increment in-flight to 2, exceeding the server's ReceiveMaximum of 1.
      var pubOptions = new PublishOptionsBuilder()
         .WithTopic("test/serverexceed"u8)
         .WithQualityOfService(QualityOfServiceType.ExactlyOnce)
         .WithPayload("Payload")
         .Build();

      _ = client.PublishAsync(pubOptions);
      _ = client.PublishAsync(pubOptions);

      // The server should disconnect the client with ReceiveMaximumExceeded (0x93)
      var reason = await disconnectTcs.Task.WaitAsync(TimeSpan.FromSeconds(30));
      await Assert.That(reason).IsEqualTo(DisconnectReasonCode.ReceiveMaximumExceeded);
   }

   [Test]
   public async Task ReceiveMaximum_ClientEnforcement_Exceeded_ShouldDisconnect()
   {

      await using var server = MqttServerFactory.CreateBuilder()
         .UseTcp(0)
         .WithDefaultClientIdGenerator()
         .Build();

      await server.StartAsync();

      var port = ((IPEndPoint)server.Listeners[0].LocalAddress).Port;

      var subscriber = (MqttClient)MqttClientFactory.CreateTcp();
      var disconnectTcs = new TaskCompletionSource<DisconnectReasonCode>(TaskCreationOptions.RunContinuationsAsynchronously);
      subscriber.Events.OnClientDisconnected.Add((ctx, ct) =>
      {
         disconnectTcs.TrySetResult(ctx.ReasonCode);
         return ValueTask.CompletedTask;
      });

      // Connect with ReceiveMaximum = 1
      await subscriber.ConnectAsync(new ConnectOptions
      {
         EndPoint = new IPEndPoint(IPAddress.Loopback, port),
         ReceiveMaximum = 1
      });

      await subscriber.SubscribeAsync(new SubscribeOptionsBuilder()
         .WithTopicFilter("test/clientexceed"u8, QualityOfServiceType.ExactlyOnce)
         .Build());

      // Get the session on the server and bypass throttling using reflection
      using var clientsResult = await server.ClientSessions.GetClients();
      MqttSession? subSession = null;
      foreach (var c in clientsResult.WrittenSpan)
      {
         if (c.IsConnected)
         {
            subSession = c.MqttSession;
            break;
         }
      }
      await Assert.That(subSession).IsNotNull();
      subSession!.ClientReceiveMaximum = 5; // Disable server-side throttling for this session

      // Publish two QoS 2 messages from a publisher client
      var publisher = (MqttClient)MqttClientFactory.CreateTcp();
      await publisher.ConnectAsync(new ConnectOptions
      {
         EndPoint = new IPEndPoint(IPAddress.Loopback, port)
      });

      var pubOptions = new PublishOptionsBuilder()
         .WithTopic("test/clientexceed"u8)
         .WithQualityOfService(QualityOfServiceType.ExactlyOnce)
         .WithPayload("Payload")
         .Build();

      // Send both publishes concurrently
      _ = publisher.PublishAsync(pubOptions);
      _ = publisher.PublishAsync(pubOptions);

      // The subscriber client should detect that it has received more than 1 concurrent unacknowledged QoS 2 message
      // and disconnect with ReceiveMaximumExceeded (0x93)
      var reason = await disconnectTcs.Task.WaitAsync(TimeSpan.FromSeconds(30));
      await Assert.That(reason).IsEqualTo(DisconnectReasonCode.ReceiveMaximumExceeded);

      await publisher.DisconnectAsync(new DisconnectOptions());
   }

   [Test]
   public async Task ReceiveMaximum_V3_ShouldNotEnforceLimits()
   {

      // Start server with ReceiveMaximum = 1
      var serverOptions = new MqttServerOptions
      {
         ReceiveMaximum = 1
      };
      await using var server = MqttServerFactory.CreateBuilder(serverOptions)
         .UseTcp(0)
         .WithDefaultClientIdGenerator()
         .Build();

      await server.StartAsync();

      var port = ((IPEndPoint)server.Listeners[0].LocalAddress).Port;

      // Connect client with MQTT v3.1.1
      var client = (MqttClient)MqttClientFactory.CreateTcp();
      await client.ConnectAsync(new ConnectOptions
      {
         EndPoint = new IPEndPoint(IPAddress.Loopback, port),
         ProtocolVersion = MqttProtocolVersion.V311
      });

      // Hook server OnAcknowledgePub to delay acknowledgements so we can have multiple in-flight
      var ackTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
      var proceedTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
      server.Events.OnAcknowledgePub.Add(async (ctx, ct) =>
      {
         ackTcs.TrySetResult();
         await proceedTcs.Task;
      });

      // Publish QoS 1 message
      var pubOptions = new PublishOptionsBuilder()
         .WithTopic("test/receivemaxv3"u8)
         .WithQualityOfService(QualityOfServiceType.AtLeastOnce)
         .WithPayload("Payload1")
         .Build();

      var pub1Task = client.PublishAsync(pubOptions);

      // Wait until the server receives the publication and enters the hook (so pub1 is in-flight)
      await ackTcs.Task.WaitAsync(TimeSpan.FromSeconds(30));

      // Attempt to publish a second QoS 1 message.
      // Since it's MQTT V3, client should NOT throttle it even though ReceiveMaximum is 1 and 1 is already in-flight!
      var pub2Task = client.PublishAsync(pubOptions);

      // Verify that pub2Task is NOT throttled and both tasks are running (they both wait on proceedTcs inside the server hook)
      var completedTask = await Task.WhenAny(pub2Task, Task.Delay(500));
      await Assert.That(completedTask == pub2Task).IsFalse(); // it timed out because server hook is holding it, NOT client semaphore!

      // Let's release the hook on the server so both can complete
      proceedTcs.TrySetResult();

      var result1 = await pub1Task.WaitAsync(TimeSpan.FromSeconds(30));
      var result2 = await pub2Task.WaitAsync(TimeSpan.FromSeconds(30));

      await Assert.That(result1.Failed).IsFalse();
      await Assert.That(result2.Failed).IsFalse();

      await client.DisconnectAsync(new DisconnectOptions());
   }

   [Test]
   public async Task ReceiveMaximum_V3_ServerEnforcement_ShouldNotDisconnectClient()
   {

      // Server ReceiveMaximum = 1
      var serverOptions = new MqttServerOptions
      {
         ReceiveMaximum = 1
      };
      await using var server = MqttServerFactory.CreateBuilder(serverOptions)
         .UseTcp(0)
         .WithDefaultClientIdGenerator()
         .Build();

      await server.StartAsync();

      var port = ((IPEndPoint)server.Listeners[0].LocalAddress).Port;

      var client = (MqttClient)MqttClientFactory.CreateTcp();

      var disconnectTcs = new TaskCompletionSource<DisconnectReasonCode>(TaskCreationOptions.RunContinuationsAsynchronously);
      client.Events.OnClientDisconnected.Add((ctx, ct) =>
      {
         disconnectTcs.TrySetResult(ctx.ReasonCode);
         return ValueTask.CompletedTask;
      });

      await client.ConnectAsync(new ConnectOptions
      {
         EndPoint = new IPEndPoint(IPAddress.Loopback, port),
         ProtocolVersion = MqttProtocolVersion.V311
      });

      // Publish two QoS 2 messages concurrently.
      var pubOptions = new PublishOptionsBuilder()
         .WithTopic("test/serverexceedv3"u8)
         .WithQualityOfService(QualityOfServiceType.ExactlyOnce)
         .WithPayload("Payload")
         .Build();

      var pub1 = client.PublishAsync(pubOptions);
      var pub2 = client.PublishAsync(pubOptions);

      // Under V3, the server should not disconnect the client.
      // So both publishes should eventually complete when we wait for them.
      var result1 = await pub1.WaitAsync(TimeSpan.FromSeconds(30));
      var result2 = await pub2.WaitAsync(TimeSpan.FromSeconds(30));

      await Assert.That(result1.Failed).IsFalse();
      await Assert.That(result2.Failed).IsFalse();
      await Assert.That(disconnectTcs.Task.IsCompleted).IsFalse();

      await client.DisconnectAsync(new DisconnectOptions());
   }

   [Test]
   public async Task ReceiveMaximum_ClientEnforcement_ShouldSucceed_WhenReceivingManyMessagesConsecutively()
   {

      await using var server = MqttServerFactory.CreateBuilder()
         .UseTcp(0)
         .WithDefaultClientIdGenerator()
         .Build();

      await server.StartAsync();

      var port = ((IPEndPoint)server.Listeners[0].LocalAddress).Port;

      var subscriber = (MqttClient)MqttClientFactory.CreateTcp();
      var disconnectTcs = new TaskCompletionSource<DisconnectReasonCode>(TaskCreationOptions.RunContinuationsAsynchronously);
      subscriber.Events.OnClientDisconnected.Add((ctx, ct) =>
      {
         disconnectTcs.TrySetResult(ctx.ReasonCode);
         return ValueTask.CompletedTask;
      });

      // Connect with ReceiveMaximum = 2
      await subscriber.ConnectAsync(new ConnectOptions
      {
         EndPoint = new IPEndPoint(IPAddress.Loopback, port),
         ReceiveMaximum = 2
      });

      var messageCount = 0;
      var receiveFinishedTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

      subscriber.AddMessageReceiveHandler((ctx, ct) =>
      {
         var count = Interlocked.Increment(ref messageCount);
         if (count == 10)
         {
            receiveFinishedTcs.TrySetResult();
         }
         return ValueTask.CompletedTask;
      });

      await subscriber.SubscribeAsync(new SubscribeOptionsBuilder()
         .WithTopicFilter("test/clientmany"u8, QualityOfServiceType.AtLeastOnce)
         .Build());

      var publisher = (MqttClient)MqttClientFactory.CreateTcp();
      await publisher.ConnectAsync(new ConnectOptions
      {
         EndPoint = new IPEndPoint(IPAddress.Loopback, port)
      });

      var pubOptions = new PublishOptionsBuilder()
         .WithTopic("test/clientmany"u8)
         .WithQualityOfService(QualityOfServiceType.AtLeastOnce)
         .WithPayload("Payload")
         .Build();

      // Send 10 messages. Since ReceiveMaximum is 2, if the client did not decrement the count,
      // it would exceed the limit and disconnect. With the fix, the client should successfully receive all 10 messages.
      for (var i = 0; i < 10; i++)
      {
         await publisher.PublishAsync(pubOptions);
      }

      // Verify we received all 10 messages successfully without disconnecting
      await receiveFinishedTcs.Task.WaitAsync(TimeSpan.FromSeconds(30));
      await Assert.That(disconnectTcs.Task.IsCompleted).IsFalse();

      await subscriber.DisconnectAsync(new DisconnectOptions());
      await publisher.DisconnectAsync(new DisconnectOptions());
   }

   [Test]
   public async Task ReceiveMaximum_ClientEnforcement_QoS2_ShouldSucceed_WhenReceivingManyMessagesConsecutively()
   {

      await using var server = MqttServerFactory.CreateBuilder()
         .UseTcp(0)
         .WithDefaultClientIdGenerator()
         .Build();

      await server.StartAsync();

      var port = ((IPEndPoint)server.Listeners[0].LocalAddress).Port;

      var subscriber = (MqttClient)MqttClientFactory.CreateTcp();
      var disconnectTcs = new TaskCompletionSource<DisconnectReasonCode>(TaskCreationOptions.RunContinuationsAsynchronously);
      subscriber.Events.OnClientDisconnected.Add((ctx, ct) =>
      {
         disconnectTcs.TrySetResult(ctx.ReasonCode);
         return ValueTask.CompletedTask;
      });

      // Connect with ReceiveMaximum = 2
      await subscriber.ConnectAsync(new ConnectOptions
      {
         EndPoint = new IPEndPoint(IPAddress.Loopback, port),
         ReceiveMaximum = 2
      });

      var messageCount = 0;
      var receiveFinishedTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

      subscriber.AddMessageReceiveHandler((ctx, ct) =>
      {
         var count = Interlocked.Increment(ref messageCount);
         if (count == 10)
         {
            receiveFinishedTcs.TrySetResult();
         }
         return ValueTask.CompletedTask;
      });

      await subscriber.SubscribeAsync(new SubscribeOptionsBuilder()
         .WithTopicFilter("test/clientmanyqos2"u8, QualityOfServiceType.ExactlyOnce)
         .Build());

      var publisher = (MqttClient)MqttClientFactory.CreateTcp();
      await publisher.ConnectAsync(new ConnectOptions
      {
         EndPoint = new IPEndPoint(IPAddress.Loopback, port)
      });

      var pubOptions = new PublishOptionsBuilder()
         .WithTopic("test/clientmanyqos2"u8)
         .WithQualityOfService(QualityOfServiceType.ExactlyOnce)
         .WithPayload("Payload")
         .Build();

      // Send 10 messages. Since ReceiveMaximum is 2, if the client did not decrement the count correctly (double-decrement or leak),
      // it would get out of sync or exceed the limit. With the fix, it should successfully receive all 10 messages.
      for (var i = 0; i < 10; i++)
      {
         await publisher.PublishAsync(pubOptions);
      }

      await receiveFinishedTcs.Task.WaitAsync(TimeSpan.FromSeconds(30));
      await Assert.That(disconnectTcs.Task.IsCompleted).IsFalse();

      await subscriber.DisconnectAsync(new DisconnectOptions());
      await publisher.DisconnectAsync(new DisconnectOptions());
   }

   [Test]
   public async Task ReceiveMaximum_ClientEnforcement_QoS2_FailurePath_ShouldReleaseSlot()
   {

      await using var server = MqttServerFactory.CreateBuilder()
         .UseTcp(0)
         .WithDefaultClientIdGenerator()
         .Build();

      await server.StartAsync();

      var port = ((IPEndPoint)server.Listeners[0].LocalAddress).Port;

      var subscriber = (MqttClient)MqttClientFactory.CreateTcp();
      var disconnectTcs = new TaskCompletionSource<DisconnectReasonCode>(TaskCreationOptions.RunContinuationsAsynchronously);
      subscriber.Events.OnClientDisconnected.Add((ctx, ct) =>
      {
         disconnectTcs.TrySetResult(ctx.ReasonCode);
         return ValueTask.CompletedTask;
      });

      // Connect with ReceiveMaximum = 1
      await subscriber.ConnectAsync(new ConnectOptions
      {
         EndPoint = new IPEndPoint(IPAddress.Loopback, port),
         ReceiveMaximum = 1
      });

      var messageCount = 0;
      var receiveFinishedTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

      subscriber.AddMessageReceiveHandler((ctx, ct) =>
      {
         ctx.ReasonCode = PubAckReasonCode.UnspecifiedError;

         var count = Interlocked.Increment(ref messageCount);
         if (count == 5)
         {
            receiveFinishedTcs.TrySetResult();
         }
         return ValueTask.CompletedTask;
      });

      await subscriber.SubscribeAsync(new SubscribeOptionsBuilder()
         .WithTopicFilter("test/clientfailqos2"u8, QualityOfServiceType.ExactlyOnce)
         .Build());

      var publisher = (MqttClient)MqttClientFactory.CreateTcp();
      await publisher.ConnectAsync(new ConnectOptions
      {
         EndPoint = new IPEndPoint(IPAddress.Loopback, port)
      });

      var pubOptions = new PublishOptionsBuilder()
         .WithTopic("test/clientfailqos2"u8)
         .WithQualityOfService(QualityOfServiceType.ExactlyOnce)
         .WithPayload("Payload")
         .Build();

      // Send 5 messages sequentially. Since ReceiveMaximum is 1, if the client leaked the slot on failure,
      // it would disconnect on the second message because the slot remains occupied.
      // With the fix, the client should successfully receive and reject all 5 messages without disconnecting.
      for (var i = 0; i < 5; i++)
      {
         await publisher.PublishAsync(pubOptions);
      }

      await receiveFinishedTcs.Task.WaitAsync(TimeSpan.FromSeconds(30));
      await Assert.That(disconnectTcs.Task.IsCompleted).IsFalse();

      await subscriber.DisconnectAsync(new DisconnectOptions());
      await publisher.DisconnectAsync(new DisconnectOptions());
   }

   [Test]
   public async Task ReceiveMaximum_ServerEnforcement_QoS2_FailurePath_ShouldNotSendPubRel()
   {

      await using var server = MqttServerFactory.CreateBuilder()
         .UseTcp(0)
         .WithDefaultClientIdGenerator()
         .Build();

      await server.StartAsync();

      var port = ((IPEndPoint)server.Listeners[0].LocalAddress).Port;

      // Subscriber client
      var subscriber = (MqttClient)MqttClientFactory.CreateTcp();
      await subscriber.ConnectAsync(new ConnectOptions
      {
         EndPoint = new IPEndPoint(IPAddress.Loopback, port)
      });

      subscriber.AddMessageReceiveHandler((ctx, ct) =>
      {
         ctx.ReasonCode = PubAckReasonCode.UnspecifiedError; // Client returns failed PUBREC
         return ValueTask.CompletedTask;
      });

      await subscriber.SubscribeAsync(new SubscribeOptionsBuilder()
         .WithTopicFilter("test/serverfailqos2"u8, QualityOfServiceType.ExactlyOnce)
         .Build());

      // Publisher client
      var publisher = (MqttClient)MqttClientFactory.CreateTcp();
      await publisher.ConnectAsync(new ConnectOptions
      {
         EndPoint = new IPEndPoint(IPAddress.Loopback, port)
      });

      using var clientsResult = await server.ClientSessions.GetClients();
      MqttSession? subSession = null;
      foreach (var c in clientsResult.WrittenSpan)
      {
         if (c.IsConnected && !c.ClientIdUtf8Bytes.Span.SequenceEqual(publisher.CurrentConnectOptions.ClientIdUtf8Bytes.Span))
         {
            subSession = c.MqttSession;
            break;
         }
      }
      await Assert.That(subSession).IsNotNull();

      var pubOptions = new PublishOptionsBuilder()
         .WithTopic("test/serverfailqos2"u8)
         .WithQualityOfService(QualityOfServiceType.ExactlyOnce)
         .WithPayload("Payload")
         .Build();

      var pubResult = await publisher.PublishAsync(pubOptions);

      // Verify that the publish to server succeeded
      await Assert.That(pubResult.Failed).IsFalse();

      // Wait a moment for any potentially violating PUBREL to arrive
      await Task.Delay(200);

      // The server should have cleaned up the publish from its session without sending PUBREL
      await Assert.That(subSession!.GetUnacknowledgedPublishCount()).IsEqualTo(0);

      await subscriber.DisconnectAsync(new DisconnectOptions());
      await publisher.DisconnectAsync(new DisconnectOptions());
   }
}
