using System.Net;
using System.Text;
using System.Text.Json;
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
}
