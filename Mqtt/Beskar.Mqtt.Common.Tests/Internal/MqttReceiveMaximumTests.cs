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
   private static int _nextPort = 13000;
   private static int GetFreePort()
   {
      return Interlocked.Increment(ref _nextPort);
   }

   [Test]
   public async Task ReceiveMaximum_ClientThrottling_ShouldThrottleQoS1()
   {
      var port = GetFreePort();

      // Start server with ReceiveMaximum = 1
      var serverOptions = new MqttServerOptions
      {
         ReceiveMaximum = 1
      };
      await using var server = MqttServerFactory.CreateBuilder(serverOptions)
         .UseTcp(port)
         .WithDefaultClientIdGenerator()
         .Build();

      await server.StartAsync();

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
      await ackTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));

      // Attempt to publish a second QoS 1 message. It should be throttled by the client because in-flight is 1 (equal to ReceiveMaximum).
      var pub2Task = client.PublishAsync(pubOptions);

      // Verify that pub2Task is indeed blocked/throttled
      var completedTask = await Task.WhenAny(pub2Task, Task.Delay(500));
      await Assert.That(completedTask != pub2Task).IsTrue(); // It timed out, meaning it's throttled

      // Release the hook to allow pub1 to be acknowledged
      proceedTcs.TrySetResult();

      // Now both publications should complete successfully
      var result1 = await pub1Task.WaitAsync(TimeSpan.FromSeconds(5));
      var result2 = await pub2Task.WaitAsync(TimeSpan.FromSeconds(5));

      await Assert.That(result1.Failed).IsFalse();
      await Assert.That(result2.Failed).IsFalse();

      await client.DisconnectAsync(new DisconnectOptions());
   }

   [Test]
   public async Task ReceiveMaximum_ServerThrottling_ShouldQueueAndDeliverSubsequentMessages()
   {
      var port = GetFreePort();

      await using var server = MqttServerFactory.CreateBuilder()
         .UseTcp(port)
         .WithDefaultClientIdGenerator()
         .Build();

      await server.StartAsync();

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
      await receiveTcs1.Task.WaitAsync(TimeSpan.FromSeconds(5));

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
      await receiveTcs2.Task.WaitAsync(TimeSpan.FromSeconds(5));
      
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
      var port = GetFreePort();

      // Server ReceiveMaximum = 1
      var serverOptions = new MqttServerOptions
      {
         ReceiveMaximum = 1
      };
      await using var server = MqttServerFactory.CreateBuilder(serverOptions)
         .UseTcp(port)
         .WithDefaultClientIdGenerator()
         .Build();

      await server.StartAsync();

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
      var reason = await disconnectTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
      await Assert.That(reason).IsEqualTo(DisconnectReasonCode.ReceiveMaximumExceeded);
   }

   [Test]
   public async Task ReceiveMaximum_ClientEnforcement_Exceeded_ShouldDisconnect()
   {
      var port = GetFreePort();

      await using var server = MqttServerFactory.CreateBuilder()
         .UseTcp(port)
         .WithDefaultClientIdGenerator()
         .Build();

      await server.StartAsync();

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
      var reason = await disconnectTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
      await Assert.That(reason).IsEqualTo(DisconnectReasonCode.ReceiveMaximumExceeded);

      await publisher.DisconnectAsync(new DisconnectOptions());
   }
}
