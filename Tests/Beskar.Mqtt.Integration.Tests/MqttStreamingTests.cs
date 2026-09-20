using System.Net;
using System.Text;
using System.Text.Json;
using System.Threading.Channels;
using Beskar.Mqtt.Client;
using Beskar.Mqtt.Common.Builders.Connecting;
using Beskar.Mqtt.Common.Builders.Disconnecting;
using Beskar.Mqtt.Common.Builders.Publishing;
using Beskar.Mqtt.Common.Extensions;
using Beskar.Mqtt.Common.Options;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Server;

namespace Beskar.Mqtt.Integration.Tests;

public record JobStatusUpdate(string JobId, string Status, int Progress);

public class MqttStreamingTests
{
   [Test]
   public async Task SubscribeStream_WithCustomDecoder_StreamsAndDecodesItemsSuccessfully()
   {
      var server = MqttServerFactory.CreateBuilder()
         .UseTcp(new IPEndPoint(IPAddress.Loopback, 0))
         .WithDefaultClientIdGenerator()
         .Build();

      var startResult = await server.StartAsync();
      await Assert.That(startResult.Failed).IsFalse();

      try
      {
         var localAddress = (IPEndPoint)server.Listeners[0].LocalAddress;
         await using var publisher = MqttClientFactory.CreateTcp();
         await using var subscriber = MqttClientFactory.CreateTcp();

         var connectOptions = new ConnectOptionsBuilder(localAddress)
            .WithProtocolVersion(MqttProtocolVersion.V50)
            .WithCleanSession(true)
            .WithTimeout(TimeSpan.FromSeconds(5))
            .Build();

         await Assert.That((await publisher.ConnectAsync(connectOptions)).Failed).IsFalse();
         await Assert.That((await subscriber.ConnectAsync(connectOptions)).Failed).IsFalse();

         var receivedItems = new List<string>();
         using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

         // Background publisher
         var pubTask = Task.Run(async () =>
         {
            await Task.Delay(100);
            for (var i = 1; i <= 3; i++)
            {
               var pubOptions = PublishOptions.Create()
                  .WithTopic("jobs/42/status")
                  .WithPayload($"STATUS-{i}")
                  .WithQualityOfService(QualityOfServiceType.AtLeastOnce)
                  .Build();

               await publisher.PublishAsync(pubOptions, cts.Token);
               await Task.Delay(50);
            }
         });

         // Consume stream with per-sub string decoder
         await foreach (var status in subscriber.SubscribeStream(
            "jobs/42/status",
            decoder: bytes => Encoding.UTF8.GetString(bytes.Span),
            ct: cts.Token))
         {
            receivedItems.Add(status);
            if (receivedItems.Count == 3)
            {
               break; // Automatically unsubscribes and disposes stream!
            }
         }

         await pubTask;

         await Assert.That(receivedItems.Count).IsEqualTo(3);
         await Assert.That(receivedItems[0]).IsEqualTo("STATUS-1");
         await Assert.That(receivedItems[1]).IsEqualTo("STATUS-2");
         await Assert.That(receivedItems[2]).IsEqualTo("STATUS-3");
      }
      finally
      {
         await server.StopAsync();
         await server.DisposeAsync();
      }
   }

   [Test]
   public async Task SubscribeJsonStream_StreamsTypedJsonObjects()
   {
      var server = MqttServerFactory.CreateBuilder()
         .UseTcp(new IPEndPoint(IPAddress.Loopback, 0))
         .WithDefaultClientIdGenerator()
         .Build();

      var startResult = await server.StartAsync();
      await Assert.That(startResult.Failed).IsFalse();

      try
      {
         var localAddress = (IPEndPoint)server.Listeners[0].LocalAddress;
         await using var publisher = MqttClientFactory.CreateTcp();
         await using var subscriber = MqttClientFactory.CreateTcp();

         var connectOptions = new ConnectOptionsBuilder(localAddress)
            .WithProtocolVersion(MqttProtocolVersion.V50)
            .WithCleanSession(true)
            .WithTimeout(TimeSpan.FromSeconds(5))
            .Build();

         await Assert.That((await publisher.ConnectAsync(connectOptions)).Failed).IsFalse();
         await Assert.That((await subscriber.ConnectAsync(connectOptions)).Failed).IsFalse();

         var receivedUpdates = new List<JobStatusUpdate>();
         using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

         var pubTask = Task.Run(async () =>
         {
            await Task.Delay(100);
            var updates = new[]
            {
               new JobStatusUpdate("job-1", "Queued", 0),
               new JobStatusUpdate("job-1", "Running", 50),
               new JobStatusUpdate("job-1", "Completed", 100),
            };

            foreach (var update in updates)
            {
               var json = JsonSerializer.Serialize(update);
               var pubOptions = PublishOptions.Create()
                  .WithTopic("jobs/job-1/status")
                  .WithPayload(json)
                  .WithQualityOfService(QualityOfServiceType.AtLeastOnce)
                  .Build();

               await publisher.PublishAsync(pubOptions, cts.Token);
               await Task.Delay(50);
            }
         });

         await foreach (var update in subscriber.SubscribeJsonStream<JobStatusUpdate>("jobs/job-1/status", ct: cts.Token))
         {
            receivedUpdates.Add(update);
            if (update.Status == "Completed")
            {
               break;
            }
         }

         await pubTask;

         await Assert.That(receivedUpdates.Count).IsEqualTo(3);
         await Assert.That(receivedUpdates[0].Status).IsEqualTo("Queued");
         await Assert.That(receivedUpdates[1].Progress).IsEqualTo(50);
         await Assert.That(receivedUpdates[2].Status).IsEqualTo("Completed");
      }
      finally
      {
         await server.StopAsync();
         await server.DisposeAsync();
      }
   }

   [Test]
   public async Task WaitForJsonMessageAsync_AwaitsMatchingCondition()
   {
      var server = MqttServerFactory.CreateBuilder()
         .UseTcp(new IPEndPoint(IPAddress.Loopback, 0))
         .WithDefaultClientIdGenerator()
         .Build();

      var startResult = await server.StartAsync();
      await Assert.That(startResult.Failed).IsFalse();

      try
      {
         var localAddress = (IPEndPoint)server.Listeners[0].LocalAddress;
         await using var publisher = MqttClientFactory.CreateTcp();
         await using var subscriber = MqttClientFactory.CreateTcp();

         var connectOptions = new ConnectOptionsBuilder(localAddress)
            .WithProtocolVersion(MqttProtocolVersion.V50)
            .WithCleanSession(true)
            .WithTimeout(TimeSpan.FromSeconds(5))
            .Build();

         await Assert.That((await publisher.ConnectAsync(connectOptions)).Failed).IsFalse();
         await Assert.That((await subscriber.ConnectAsync(connectOptions)).Failed).IsFalse();

         using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

         var pubTask = Task.Run(async () =>
         {
            await Task.Delay(100);
            var updates = new[]
            {
               new JobStatusUpdate("job-99", "Running", 10),
               new JobStatusUpdate("job-99", "Running", 50),
               new JobStatusUpdate("job-99", "Finished", 100),
            };

            foreach (var update in updates)
            {
               var json = JsonSerializer.Serialize(update);
               var pubOptions = PublishOptions.Create()
                  .WithTopic("jobs/job-99/status")
                  .WithPayload(json)
                  .WithQualityOfService(QualityOfServiceType.AtLeastOnce)
                  .Build();

               await publisher.PublishAsync(pubOptions, cts.Token);
               await Task.Delay(50);
            }
         });

         var completed = await subscriber.WaitForJsonMessageAsync<JobStatusUpdate>(
            "jobs/job-99/status",
            predicate: s => s.Status == "Finished",
            timeout: TimeSpan.FromSeconds(5),
            ct: cts.Token);

         await pubTask;

         await Assert.That(completed.JobId).IsEqualTo("job-99");
         await Assert.That(completed.Status).IsEqualTo("Finished");
         await Assert.That(completed.Progress).IsEqualTo(100);
      }
      finally
      {
         await server.StopAsync();
         await server.DisposeAsync();
      }
   }

   [Test]
   public async Task SubscribeStream_AutoResubscribesOnReconnect_WithoutBreakingStream()
   {
      var server = MqttServerFactory.CreateBuilder()
         .UseTcp(new IPEndPoint(IPAddress.Loopback, 0))
         .WithDefaultClientIdGenerator()
         .Build();

      var startResult = await server.StartAsync();
      await Assert.That(startResult.Failed).IsFalse();

      try
      {
         var localAddress = (IPEndPoint)server.Listeners[0].LocalAddress;
         await using var publisher = MqttClientFactory.CreateTcp();
         await using var subscriber = MqttClientFactory.CreateTcp();

         var connectOptions = new ConnectOptionsBuilder(localAddress)
            .WithProtocolVersion(MqttProtocolVersion.V50)
            .WithCleanSession(true)
            .WithTimeout(TimeSpan.FromSeconds(5))
            .Build();

         await Assert.That((await publisher.ConnectAsync(connectOptions)).Failed).IsFalse();
         await Assert.That((await subscriber.ConnectAsync(connectOptions)).Failed).IsFalse();

         var received = new List<string>();
         using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

         var pubTask = Task.Run(async () =>
         {
            await Task.Delay(100);

            // Message 1 (before reconnect)
            await publisher.PublishAsync(PublishOptions.Create()
               .WithTopic("reconnect/stream/test")
               .WithPayload("MSG-1")
               .WithQualityOfService(QualityOfServiceType.AtLeastOnce)
               .Build(), cts.Token);

            await Task.Delay(200);

            // Disconnect subscriber and reconnect
            await subscriber.DisconnectAsync(new DisconnectOptions(), cts.Token);
            await Task.Delay(100);

            var reconnResult = await subscriber.ConnectAsync(connectOptions, cts.Token);
            await Assert.That(reconnResult.Failed).IsFalse();

            await Task.Delay(200);

            // Message 2 (after reconnect - should be received because manager auto-resubscribed!)
            await publisher.PublishAsync(PublishOptions.Create()
               .WithTopic("reconnect/stream/test")
               .WithPayload("MSG-2")
               .WithQualityOfService(QualityOfServiceType.AtLeastOnce)
               .Build(), cts.Token);
         });

         await foreach (var msg in subscriber.SubscribeStream(
            "reconnect/stream/test",
            decoder: b => Encoding.UTF8.GetString(b.Span),
            ct: cts.Token))
         {
            received.Add(msg);
            if (received.Count == 2)
            {
               break;
            }
         }

         await pubTask;

         await Assert.That(received.Count).IsEqualTo(2);
         await Assert.That(received[0]).IsEqualTo("MSG-1");
         await Assert.That(received[1]).IsEqualTo("MSG-2");
      }
      finally
      {
         await server.StopAsync();
         await server.DisposeAsync();
      }
   }

   [Test]
   public async Task SubscribeTopicAsync_ReceivesMatchingMessagesAndUnsubscribesOnDispose()
   {
      var server = MqttServerFactory.CreateBuilder()
         .UseTcp(new IPEndPoint(IPAddress.Loopback, 0))
         .WithDefaultClientIdGenerator()
         .Build();

      var startResult = await server.StartAsync();
      await Assert.That(startResult.Failed).IsFalse();

      try
      {
         var localAddress = (IPEndPoint)server.Listeners[0].LocalAddress;
         await using var publisher = MqttClientFactory.CreateTcp();
         await using var subscriber = MqttClientFactory.CreateTcp();

         var connectOptions = new ConnectOptionsBuilder(localAddress)
            .WithProtocolVersion(MqttProtocolVersion.V50)
            .WithCleanSession(true)
            .WithTimeout(TimeSpan.FromSeconds(5))
            .Build();

         await Assert.That((await publisher.ConnectAsync(connectOptions)).Failed).IsFalse();
         await Assert.That((await subscriber.ConnectAsync(connectOptions)).Failed).IsFalse();

         var received = new List<string>();
         var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

         var registration = await subscriber.SubscribeTopicAsync(
            "orders/+/status",
            (msg, ctx, ct) =>
            {
               received.Add(msg);
               if (received.Count == 2)
               {
                  tcs.TrySetResult();
               }
               return ValueTask.CompletedTask;
            },
            decoder: b => Encoding.UTF8.GetString(b.Span));

         await publisher.PublishAsync(PublishOptions.Create()
            .WithTopic("orders/101/status")
            .WithPayload("ORDER-101-PAID")
            .WithQualityOfService(QualityOfServiceType.AtLeastOnce)
            .Build());

         await publisher.PublishAsync(PublishOptions.Create()
            .WithTopic("orders/102/status")
            .WithPayload("ORDER-102-SHIPPED")
            .WithQualityOfService(QualityOfServiceType.AtLeastOnce)
            .Build());

         await tcs.Task.WaitAsync(TimeSpan.FromSeconds(5));

         await Assert.That(received.Count).IsEqualTo(2);
         await Assert.That(received[0]).IsEqualTo("ORDER-101-PAID");
         await Assert.That(received[1]).IsEqualTo("ORDER-102-SHIPPED");

         // Dispose the subscription token -> cleans up handler & unsubscribes
         await registration.DisposeAsync();

         // Further publish should not be delivered
         await publisher.PublishAsync(PublishOptions.Create()
            .WithTopic("orders/103/status")
            .WithPayload("ORDER-103-CANCELLED")
            .WithQualityOfService(QualityOfServiceType.AtLeastOnce)
            .Build());

         await Task.Delay(200);
         await Assert.That(received.Count).IsEqualTo(2);
      }
      finally
      {
         await server.StopAsync();
         await server.DisposeAsync();
      }
   }

   [Test]
   public async Task SubscribeStream_MultipleStreamsOnSameTopic_ReferenceCountedCorrectly()
   {
      var server = MqttServerFactory.CreateBuilder()
         .UseTcp(new IPEndPoint(IPAddress.Loopback, 0))
         .WithDefaultClientIdGenerator()
         .Build();

      var startResult = await server.StartAsync();
      await Assert.That(startResult.Failed).IsFalse();

      try
      {
         var localAddress = (IPEndPoint)server.Listeners[0].LocalAddress;
         await using var publisher = MqttClientFactory.CreateTcp();
         await using var subscriber = MqttClientFactory.CreateTcp();

         var connectOptions = new ConnectOptionsBuilder(localAddress)
            .WithProtocolVersion(MqttProtocolVersion.V50)
            .WithCleanSession(true)
            .WithTimeout(TimeSpan.FromSeconds(5))
            .Build();

         await Assert.That((await publisher.ConnectAsync(connectOptions)).Failed).IsFalse();
         await Assert.That((await subscriber.ConnectAsync(connectOptions)).Failed).IsFalse();

         using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

         var streamAItems = new List<string>();
         var streamBItems = new List<string>();

         var streamA = subscriber.SubscribeStream(
            "shared/topic",
            decoder: b => Encoding.UTF8.GetString(b.Span),
            ct: cts.Token);

         var streamB = subscriber.SubscribeStream(
            "shared/topic",
            decoder: b => Encoding.UTF8.GetString(b.Span),
            ct: cts.Token);

         var taskA = Task.Run(async () =>
         {
            await foreach (var item in streamA)
            {
               streamAItems.Add(item);
               break; // Stream A exits and disposes after 1 item
            }
         });

         var taskB = Task.Run(async () =>
         {
            await foreach (var item in streamB)
            {
               streamBItems.Add(item);
               if (streamBItems.Count == 3)
               {
                  break; // Stream B reads all 3 items
               }
            }
         });

         await Task.Delay(100);

         // Publish message 1 (both should receive)
         await publisher.PublishAsync(PublishOptions.Create()
            .WithTopic("shared/topic")
            .WithPayload("MSG-1")
            .WithQualityOfService(QualityOfServiceType.AtLeastOnce)
            .Build(), cts.Token);

         await taskA; // Stream A should be completed now

         // Publish message 2 and 3 (only Stream B receives, broker subscription must still be active)
         await publisher.PublishAsync(PublishOptions.Create()
            .WithTopic("shared/topic")
            .WithPayload("MSG-2")
            .WithQualityOfService(QualityOfServiceType.AtLeastOnce)
            .Build(), cts.Token);

         await publisher.PublishAsync(PublishOptions.Create()
            .WithTopic("shared/topic")
            .WithPayload("MSG-3")
            .WithQualityOfService(QualityOfServiceType.AtLeastOnce)
            .Build(), cts.Token);

         await taskB;

         await Assert.That(streamAItems.Count).IsEqualTo(1);
         await Assert.That(streamAItems[0]).IsEqualTo("MSG-1");

         await Assert.That(streamBItems.Count).IsEqualTo(3);
         await Assert.That(streamBItems[0]).IsEqualTo("MSG-1");
         await Assert.That(streamBItems[1]).IsEqualTo("MSG-2");
         await Assert.That(streamBItems[2]).IsEqualTo("MSG-3");
      }
      finally
      {
         await server.StopAsync();
         await server.DisposeAsync();
      }
   }

   [Test]
   public async Task SubscribeStream_QosUpgradeOnSameTopic_BrokerSubscriptionUpgraded()
   {
      var server = MqttServerFactory.CreateBuilder()
         .UseTcp(new IPEndPoint(IPAddress.Loopback, 0))
         .WithDefaultClientIdGenerator()
         .Build();

      var startResult = await server.StartAsync();
      await Assert.That(startResult.Failed).IsFalse();

      try
      {
         var localAddress = (IPEndPoint)server.Listeners[0].LocalAddress;
         await using var publisher = MqttClientFactory.CreateTcp();
         await using var subscriber = MqttClientFactory.CreateTcp();

         var connectOptions = new ConnectOptionsBuilder(localAddress)
            .WithProtocolVersion(MqttProtocolVersion.V50)
            .WithCleanSession(true)
            .WithTimeout(TimeSpan.FromSeconds(5))
            .Build();

         await Assert.That((await publisher.ConnectAsync(connectOptions)).Failed).IsFalse();
         await Assert.That((await subscriber.ConnectAsync(connectOptions)).Failed).IsFalse();

         using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

         // Listener 1 registers QoS 0
         var streamQos0 = subscriber.SubscribeStream(
            "qos/upgrade",
            decoder: b => Encoding.UTF8.GetString(b.Span),
            qos: QualityOfServiceType.AtMostOnce,
            ct: cts.Token);

         await Task.Delay(100);

         // Listener 2 registers QoS 2 (triggers QoS upgrade on broker)
         var streamQos2 = subscriber.SubscribeStream(
            "qos/upgrade",
            decoder: b => Encoding.UTF8.GetString(b.Span),
            qos: QualityOfServiceType.ExactlyOnce,
            ct: cts.Token);

         await Task.Delay(100);

         var items0 = new List<string>();
         var items2 = new List<string>();

         var task0 = Task.Run(async () =>
         {
            await foreach (var item in streamQos0)
            {
               items0.Add(item);
               break;
            }
         });

         var task2 = Task.Run(async () =>
         {
            await foreach (var item in streamQos2)
            {
               items2.Add(item);
               break;
            }
         });

         await publisher.PublishAsync(PublishOptions.Create()
            .WithTopic("qos/upgrade")
            .WithPayload("UPGRADED-PAYLOAD")
            .WithQualityOfService(QualityOfServiceType.ExactlyOnce)
            .Build(), cts.Token);

         await Task.WhenAll(task0, task2);

         await Assert.That(items0.Count).IsEqualTo(1);
         await Assert.That(items0[0]).IsEqualTo("UPGRADED-PAYLOAD");
         await Assert.That(items2.Count).IsEqualTo(1);
         await Assert.That(items2[0]).IsEqualTo("UPGRADED-PAYLOAD");
      }
      finally
      {
         await server.StopAsync();
         await server.DisposeAsync();
      }
   }

   [Test]
   public async Task SubscribeStream_RegisteredPriorToConnect_SubscribesOnConnect()
   {
      var server = MqttServerFactory.CreateBuilder()
         .UseTcp(new IPEndPoint(IPAddress.Loopback, 0))
         .WithDefaultClientIdGenerator()
         .Build();

      var startResult = await server.StartAsync();
      await Assert.That(startResult.Failed).IsFalse();

      try
      {
         var localAddress = (IPEndPoint)server.Listeners[0].LocalAddress;
         await using var publisher = MqttClientFactory.CreateTcp();
         await using var subscriber = MqttClientFactory.CreateTcp();

         var connectOptions = new ConnectOptionsBuilder(localAddress)
            .WithProtocolVersion(MqttProtocolVersion.V50)
            .WithCleanSession(true)
            .WithTimeout(TimeSpan.FromSeconds(5))
            .Build();

         using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

         // Register stream BEFORE subscriber connects!
         var stream = subscriber.SubscribeStream(
            "preconnect/topic",
            decoder: b => Encoding.UTF8.GetString(b.Span),
            ct: cts.Token);

         var streamTask = Task.Run(async () =>
         {
            await foreach (var item in stream)
            {
               return item;
            }
            return null;
         });

         // Connect publisher and subscriber
         await Assert.That((await publisher.ConnectAsync(connectOptions)).Failed).IsFalse();
         await Assert.That((await subscriber.ConnectAsync(connectOptions)).Failed).IsFalse();

         await Task.Delay(150);

         await publisher.PublishAsync(PublishOptions.Create()
            .WithTopic("preconnect/topic")
            .WithPayload("PRECONNECTED-SUCCESS")
            .WithQualityOfService(QualityOfServiceType.AtLeastOnce)
            .Build(), cts.Token);

         var received = await streamTask.WaitAsync(TimeSpan.FromSeconds(5));
         await Assert.That(received).IsEqualTo("PRECONNECTED-SUCCESS");
      }
      finally
      {
         await server.StopAsync();
         await server.DisposeAsync();
      }
   }

   [Test]
   public async Task SubscribeJsonStream_DecoderException_IsIsolatedAndDoesNotCrashStream()
   {
      var server = MqttServerFactory.CreateBuilder()
         .UseTcp(new IPEndPoint(IPAddress.Loopback, 0))
         .WithDefaultClientIdGenerator()
         .Build();

      var startResult = await server.StartAsync();
      await Assert.That(startResult.Failed).IsFalse();

      try
      {
         var localAddress = (IPEndPoint)server.Listeners[0].LocalAddress;
         await using var publisher = MqttClientFactory.CreateTcp();
         await using var subscriber = MqttClientFactory.CreateTcp();

         var connectOptions = new ConnectOptionsBuilder(localAddress)
            .WithProtocolVersion(MqttProtocolVersion.V50)
            .WithCleanSession(true)
            .WithTimeout(TimeSpan.FromSeconds(5))
            .Build();

         await Assert.That((await publisher.ConnectAsync(connectOptions)).Failed).IsFalse();
         await Assert.That((await subscriber.ConnectAsync(connectOptions)).Failed).IsFalse();

         using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

         var received = new List<JobStatusUpdate>();

         var streamTask = Task.Run(async () =>
         {
            await foreach (var update in subscriber.SubscribeJsonStream<JobStatusUpdate>("resilience/jobs", ct: cts.Token))
            {
               received.Add(update);
               if (received.Count == 1)
               {
                  break;
               }
            }
         });

         await Task.Delay(100);

         // Publish 1: Corrupted / invalid JSON payload
         await publisher.PublishAsync(PublishOptions.Create()
            .WithTopic("resilience/jobs")
            .WithPayload("THIS IS CORRUPTED NOT JSON {{{")
            .WithQualityOfService(QualityOfServiceType.AtLeastOnce)
            .Build(), cts.Token);

         await Task.Delay(100);

         // Publish 2: Valid JSON payload
         var validJob = new JobStatusUpdate("resilient-1", "Done", 100);
         await publisher.PublishAsync(PublishOptions.Create()
            .WithTopic("resilience/jobs")
            .WithPayload(JsonSerializer.Serialize(validJob))
            .WithQualityOfService(QualityOfServiceType.AtLeastOnce)
            .Build(), cts.Token);

         await streamTask.WaitAsync(TimeSpan.FromSeconds(5));

         await Assert.That(received.Count).IsEqualTo(1);
         await Assert.That(received[0].JobId).IsEqualTo("resilient-1");
         await Assert.That(received[0].Status).IsEqualTo("Done");
      }
      finally
      {
         await server.StopAsync();
         await server.DisposeAsync();
      }
   }

   [Test]
   public async Task SubscribeStream_BackpressureDropOldest_DropsOldestWhenChannelFull()
   {
      var server = MqttServerFactory.CreateBuilder()
         .UseTcp(new IPEndPoint(IPAddress.Loopback, 0))
         .WithDefaultClientIdGenerator()
         .Build();

      var startResult = await server.StartAsync();
      await Assert.That(startResult.Failed).IsFalse();

      try
      {
         var localAddress = (IPEndPoint)server.Listeners[0].LocalAddress;
         await using var publisher = MqttClientFactory.CreateTcp();
         await using var subscriber = MqttClientFactory.CreateTcp();

         var connectOptions = new ConnectOptionsBuilder(localAddress)
            .WithProtocolVersion(MqttProtocolVersion.V50)
            .WithCleanSession(true)
            .WithTimeout(TimeSpan.FromSeconds(5))
            .Build();

         await Assert.That((await publisher.ConnectAsync(connectOptions)).Failed).IsFalse();
         await Assert.That((await subscriber.ConnectAsync(connectOptions)).Failed).IsFalse();

         using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

         // Bounded capacity = 2, DropOldest
         var options = new StreamSubscriptionOptions
         {
            BoundedCapacity = 2,
            FullMode = BoundedChannelFullMode.DropOldest
         };

         var stream = subscriber.SubscribeStream(
            "backpressure/test",
            decoder: b => Encoding.UTF8.GetString(b.Span),
            options: options,
            ct: cts.Token);

         await Task.Delay(100);

         // Publish 5 items quickly without consuming
         for (var i = 1; i <= 5; i++)
         {
            await publisher.PublishAsync(PublishOptions.Create()
               .WithTopic("backpressure/test")
               .WithPayload($"MSG-{i}")
               .WithQualityOfService(QualityOfServiceType.AtLeastOnce)
               .Build(), cts.Token);
         }

         await Task.Delay(200);

         // Now consume: channel size is 2, oldest (1, 2, 3) were dropped, so MSG-4 and MSG-5 should be present
         var received = new List<string>();
         await foreach (var item in stream)
         {
            received.Add(item);
            if (received.Count == 2)
            {
               break;
            }
         }

         await Assert.That(received.Count).IsEqualTo(2);
         await Assert.That(received[0]).IsEqualTo("MSG-4");
         await Assert.That(received[1]).IsEqualTo("MSG-5");
      }
      finally
      {
         await server.StopAsync();
         await server.DisposeAsync();
      }
   }

   [Test]
   public async Task SubscribeStream_CancellationToken_TerminatesStreamCleanly()
   {
      var server = MqttServerFactory.CreateBuilder()
         .UseTcp(new IPEndPoint(IPAddress.Loopback, 0))
         .WithDefaultClientIdGenerator()
         .Build();

      var startResult = await server.StartAsync();
      await Assert.That(startResult.Failed).IsFalse();

      try
      {
         var localAddress = (IPEndPoint)server.Listeners[0].LocalAddress;
         await using var publisher = MqttClientFactory.CreateTcp();
         await using var subscriber = MqttClientFactory.CreateTcp();

         var connectOptions = new ConnectOptionsBuilder(localAddress)
            .WithProtocolVersion(MqttProtocolVersion.V50)
            .WithCleanSession(true)
            .WithTimeout(TimeSpan.FromSeconds(5))
            .Build();

         await Assert.That((await publisher.ConnectAsync(connectOptions)).Failed).IsFalse();
         await Assert.That((await subscriber.ConnectAsync(connectOptions)).Failed).IsFalse();

         using var cts = new CancellationTokenSource();

         var stream = subscriber.SubscribeStream(
            "cancellation/test",
            decoder: b => Encoding.UTF8.GetString(b.Span),
            ct: cts.Token);

         var received = new List<string>();

         var streamTask = Task.Run(async () =>
         {
            await foreach (var item in stream)
            {
               received.Add(item);
            }
         });

         await Task.Delay(100);

         await publisher.PublishAsync(PublishOptions.Create()
            .WithTopic("cancellation/test")
            .WithPayload("HELLO")
            .WithQualityOfService(QualityOfServiceType.AtLeastOnce)
            .Build());

         await Task.Delay(100);

         // Cancel token -> stream channel should complete and loop exits
         cts.Cancel();

         await streamTask.WaitAsync(TimeSpan.FromSeconds(3));

         await Assert.That(received.Count).IsEqualTo(1);
         await Assert.That(received[0]).IsEqualTo("HELLO");
      }
      finally
      {
         await server.StopAsync();
         await server.DisposeAsync();
      }
   }

   [Test]
   public async Task WaitForMessageAsync_PredicateFilteringAndTimeout_BehavesCorrectly()
   {
      var server = MqttServerFactory.CreateBuilder()
         .UseTcp(new IPEndPoint(IPAddress.Loopback, 0))
         .WithDefaultClientIdGenerator()
         .Build();

      var startResult = await server.StartAsync();
      await Assert.That(startResult.Failed).IsFalse();

      try
      {
         var localAddress = (IPEndPoint)server.Listeners[0].LocalAddress;
         await using var publisher = MqttClientFactory.CreateTcp();
         await using var subscriber = MqttClientFactory.CreateTcp();

         var connectOptions = new ConnectOptionsBuilder(localAddress)
            .WithProtocolVersion(MqttProtocolVersion.V50)
            .WithCleanSession(true)
            .WithTimeout(TimeSpan.FromSeconds(5))
            .Build();

         await Assert.That((await publisher.ConnectAsync(connectOptions)).Failed).IsFalse();
         await Assert.That((await subscriber.ConnectAsync(connectOptions)).Failed).IsFalse();

         // Case 1: Timeout throws TimeoutException when condition not met
         await Assert.ThrowsAsync<TimeoutException>(async () =>
         {
            await subscriber.WaitForJsonMessageAsync<JobStatusUpdate>(
               "timeout/jobs",
               predicate: s => s.Status == "NonExistent",
               timeout: TimeSpan.FromMilliseconds(300));
         });

         // Case 2: Matching predicate among non-matching messages
         var waitTask = subscriber.WaitForJsonMessageAsync<JobStatusUpdate>(
            "filtered/jobs",
            predicate: s => s.Progress == 100,
            timeout: TimeSpan.FromSeconds(5));

         await Task.Delay(100);

         // Send non-matching
         await publisher.PublishAsync(PublishOptions.Create()
            .WithTopic("filtered/jobs")
            .WithPayload(JsonSerializer.Serialize(new JobStatusUpdate("job-1", "Step1", 25)))
            .WithQualityOfService(QualityOfServiceType.AtLeastOnce)
            .Build());

         await publisher.PublishAsync(PublishOptions.Create()
            .WithTopic("filtered/jobs")
            .WithPayload(JsonSerializer.Serialize(new JobStatusUpdate("job-1", "Step2", 50)))
            .WithQualityOfService(QualityOfServiceType.AtLeastOnce)
            .Build());

         // Send matching
         await publisher.PublishAsync(PublishOptions.Create()
            .WithTopic("filtered/jobs")
            .WithPayload(JsonSerializer.Serialize(new JobStatusUpdate("job-1", "Complete", 100)))
            .WithQualityOfService(QualityOfServiceType.AtLeastOnce)
            .Build());

         var result = await waitTask;
         await Assert.That(result.Status).IsEqualTo("Complete");
         await Assert.That(result.Progress).IsEqualTo(100);
      }
      finally
      {
         await server.StopAsync();
         await server.DisposeAsync();
      }
   }

   [Test]
   public async Task SubscribeStream_Wildcards_MatchesHierarchies()
   {
      var server = MqttServerFactory.CreateBuilder()
         .UseTcp(new IPEndPoint(IPAddress.Loopback, 0))
         .WithDefaultClientIdGenerator()
         .Build();

      var startResult = await server.StartAsync();
      await Assert.That(startResult.Failed).IsFalse();

      try
      {
         var localAddress = (IPEndPoint)server.Listeners[0].LocalAddress;
         await using var publisher = MqttClientFactory.CreateTcp();
         await using var subscriber = MqttClientFactory.CreateTcp();

         var connectOptions = new ConnectOptionsBuilder(localAddress)
            .WithProtocolVersion(MqttProtocolVersion.V50)
            .WithCleanSession(true)
            .WithTimeout(TimeSpan.FromSeconds(5))
            .Build();

         await Assert.That((await publisher.ConnectAsync(connectOptions)).Failed).IsFalse();
         await Assert.That((await subscriber.ConnectAsync(connectOptions)).Failed).IsFalse();

         using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

         var singleWildcardItems = new List<string>();
         var multiWildcardItems = new List<string>();

         var singleTask = Task.Run(async () =>
         {
            await foreach (var msg in subscriber.SubscribeStream("devices/+/telemetry", b => Encoding.UTF8.GetString(b.Span), ct: cts.Token))
            {
               singleWildcardItems.Add(msg);
               if (singleWildcardItems.Count == 2) break;
            }
         });

         var multiTask = Task.Run(async () =>
         {
            await foreach (var msg in subscriber.SubscribeStream("events/#", b => Encoding.UTF8.GetString(b.Span), ct: cts.Token))
            {
               multiWildcardItems.Add(msg);
               if (multiWildcardItems.Count == 2) break;
            }
         });

         await Task.Delay(100);

         // Devices topic (matches single-level +)
         await publisher.PublishAsync(PublishOptions.Create()
            .WithTopic("devices/kitchen/telemetry")
            .WithPayload("21C")
            .WithQualityOfService(QualityOfServiceType.AtLeastOnce)
            .Build(), cts.Token);

         await publisher.PublishAsync(PublishOptions.Create()
            .WithTopic("devices/living/telemetry")
            .WithPayload("23C")
            .WithQualityOfService(QualityOfServiceType.AtLeastOnce)
            .Build(), cts.Token);

         // Mismatched device topic (should NOT match devices/+/telemetry)
         await publisher.PublishAsync(PublishOptions.Create()
            .WithTopic("devices/kitchen/status")
            .WithPayload("ONLINE")
            .WithQualityOfService(QualityOfServiceType.AtLeastOnce)
            .Build(), cts.Token);

         // Events topic (matches multi-level #)
         await publisher.PublishAsync(PublishOptions.Create()
            .WithTopic("events/user/login")
            .WithPayload("LOGIN")
            .WithQualityOfService(QualityOfServiceType.AtLeastOnce)
            .Build(), cts.Token);

         await publisher.PublishAsync(PublishOptions.Create()
            .WithTopic("events/orders/eu/processed")
            .WithPayload("PROCESSED")
            .WithQualityOfService(QualityOfServiceType.AtLeastOnce)
            .Build(), cts.Token);

         await Task.WhenAll(singleTask, multiTask);

         await Assert.That(singleWildcardItems.Count).IsEqualTo(2);
         await Assert.That(singleWildcardItems[0]).IsEqualTo("21C");
         await Assert.That(singleWildcardItems[1]).IsEqualTo("23C");

         await Assert.That(multiWildcardItems.Count).IsEqualTo(2);
         await Assert.That(multiWildcardItems[0]).IsEqualTo("LOGIN");
         await Assert.That(multiWildcardItems[1]).IsEqualTo("PROCESSED");
      }
      finally
      {
         await server.StopAsync();
         await server.DisposeAsync();
      }
   }

   [Test]
   public async Task SubscribeStream_CancellationWithBufferedItems_DoesNotDrainStaleItems()
   {
      var server = MqttServerFactory.CreateBuilder()
         .UseTcp(new IPEndPoint(IPAddress.Loopback, 0))
         .WithDefaultClientIdGenerator()
         .Build();

      var startResult = await server.StartAsync();
      await Assert.That(startResult.Failed).IsFalse();

      try
      {
         var localAddress = (IPEndPoint)server.Listeners[0].LocalAddress;
         await using var publisher = MqttClientFactory.CreateTcp();
         await using var subscriber = MqttClientFactory.CreateTcp();

         var connectOptions = new ConnectOptionsBuilder(localAddress)
            .WithProtocolVersion(MqttProtocolVersion.V50)
            .WithCleanSession(true)
            .WithTimeout(TimeSpan.FromSeconds(5))
            .Build();

         await Assert.That((await publisher.ConnectAsync(connectOptions)).Failed).IsFalse();
         await Assert.That((await subscriber.ConnectAsync(connectOptions)).Failed).IsFalse();

         using var cts = new CancellationTokenSource();

         var stream = subscriber.SubscribeStream(
            "cancel/buffered",
            decoder: b => Encoding.UTF8.GetString(b.Span),
            ct: cts.Token);

         await Task.Delay(100);

         // Publish 5 items into the stream channel before consumer starts reading
         for (var i = 1; i <= 5; i++)
         {
            await publisher.PublishAsync(PublishOptions.Create()
               .WithTopic("cancel/buffered")
               .WithPayload($"ITEM-{i}")
               .WithQualityOfService(QualityOfServiceType.AtLeastOnce)
               .Build());
         }

         await Task.Delay(200);

         // Cancel before consumption begins
         cts.Cancel();

         var received = new List<string>();
         await foreach (var item in stream)
         {
            received.Add(item);
         }

         // Because stream token was cancelled, it must NOT drain the 5 buffered items
         await Assert.That(received.Count).IsEqualTo(0);
      }
      finally
      {
         await server.StopAsync();
         await server.DisposeAsync();
      }
   }

   [Test]
   public async Task SubscribeStream_WaitMode_BoundsPendingWritersWhenChannelFull()
   {
      var server = MqttServerFactory.CreateBuilder()
         .UseTcp(new IPEndPoint(IPAddress.Loopback, 0))
         .WithDefaultClientIdGenerator()
         .Build();

      var startResult = await server.StartAsync();
      await Assert.That(startResult.Failed).IsFalse();

      try
      {
         var localAddress = (IPEndPoint)server.Listeners[0].LocalAddress;
         await using var publisher = MqttClientFactory.CreateTcp();
         await using var subscriber = MqttClientFactory.CreateTcp();

         var connectOptions = new ConnectOptionsBuilder(localAddress)
            .WithProtocolVersion(MqttProtocolVersion.V50)
            .WithCleanSession(true)
            .WithTimeout(TimeSpan.FromSeconds(5))
            .Build();

         await Assert.That((await publisher.ConnectAsync(connectOptions)).Failed).IsFalse();
         await Assert.That((await subscriber.ConnectAsync(connectOptions)).Failed).IsFalse();

         using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

         // Bounded capacity = 2, Wait mode, MaxPendingWaitWrites = 2
         var options = new StreamSubscriptionOptions
         {
            BoundedCapacity = 2,
            FullMode = BoundedChannelFullMode.Wait,
            MaxPendingWaitWrites = 2
         };

         var stream = subscriber.SubscribeStream(
            "wait/bounded",
            decoder: b => Encoding.UTF8.GetString(b.Span),
            options: options,
            ct: cts.Token);

         await Task.Delay(100);

         // Send 10 messages without reading. Channel holds 2, 2 can wait, rest (6) are dropped
         for (var i = 1; i <= 10; i++)
         {
            await publisher.PublishAsync(PublishOptions.Create()
               .WithTopic("wait/bounded")
               .WithPayload($"MSG-{i}")
               .WithQualityOfService(QualityOfServiceType.AtMostOnce)
               .Build());
         }

         await Task.Delay(200);

         // Read available items from stream
         var received = new List<string>();
         await foreach (var item in stream)
         {
            received.Add(item);
            if (received.Count >= 4)
            {
               break;
            }
         }

         // Verify at most capacity (2) + max pending (2) = 4 items were delivered, preventing unbounded accumulation
         await Assert.That(received.Count).IsEqualTo(4);
      }
      finally
      {
         await server.StopAsync();
         await server.DisposeAsync();
      }
   }

   [Test]
   public async Task SubscribeStream_ConcurrentAddAndRemove_DoesNotOrphanActiveSinks()
   {
      var server = MqttServerFactory.CreateBuilder()
         .UseTcp(new IPEndPoint(IPAddress.Loopback, 0))
         .WithDefaultClientIdGenerator()
         .Build();

      var startResult = await server.StartAsync();
      await Assert.That(startResult.Failed).IsFalse();

      try
      {
         var localAddress = (IPEndPoint)server.Listeners[0].LocalAddress;
         await using var publisher = MqttClientFactory.CreateTcp();
         await using var subscriber = MqttClientFactory.CreateTcp();

         var connectOptions = new ConnectOptionsBuilder(localAddress)
            .WithProtocolVersion(MqttProtocolVersion.V50)
            .WithCleanSession(true)
            .WithTimeout(TimeSpan.FromSeconds(5))
            .Build();

         await Assert.That((await publisher.ConnectAsync(connectOptions)).Failed).IsFalse();
         await Assert.That((await subscriber.ConnectAsync(connectOptions)).Failed).IsFalse();

         using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

         // Register persistent stream on calling thread
         var persistentReceived = new List<string>();
         var persistentStream = subscriber.SubscribeStream("race/test", b => Encoding.UTF8.GetString(b.Span), ct: cts.Token);

         await Task.Delay(100);

         var persistentTask = Task.Run(async () =>
         {
            await foreach (var item in persistentStream)
            {
               persistentReceived.Add(item);
               if (persistentReceived.Count == 5)
               {
                  break;
               }
            }
         });

         // Concurrent task rapidly adding and removing temporary streams on the same topic
         var churnTask = Task.Run(async () =>
         {
            for (var i = 0; i < 10; i++)
            {
               var tempStream = subscriber.SubscribeStream("race/test", b => Encoding.UTF8.GetString(b.Span), ct: cts.Token);
               var enumerator = tempStream.GetAsyncEnumerator(cts.Token);
               await enumerator.DisposeAsync();
               await Task.Yield();
            }
         });

         // Publish 5 messages while churn is happening
         for (var i = 1; i <= 5; i++)
         {
            await publisher.PublishAsync(PublishOptions.Create()
               .WithTopic("race/test")
               .WithPayload($"DATA-{i}")
               .WithQualityOfService(QualityOfServiceType.AtLeastOnce)
               .Build());
            await Task.Delay(60);
         }

         await churnTask;
         await persistentTask;

         await Assert.That(persistentReceived.Count).IsEqualTo(5);
         await Assert.That(persistentReceived[0]).IsEqualTo("DATA-1");
         await Assert.That(persistentReceived[4]).IsEqualTo("DATA-5");
      }
      finally
      {
         await server.StopAsync();
         await server.DisposeAsync();
      }
   }
}
