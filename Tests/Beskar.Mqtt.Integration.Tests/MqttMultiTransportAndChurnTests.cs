using System.Collections.Concurrent;
using System.Net;
using System.Text;
using Beskar.Mqtt.Client;
using Beskar.Mqtt.Common.Builders.Connecting;
using Beskar.Mqtt.Common.Builders.Disconnecting;
using Beskar.Mqtt.Common.Builders.Publishing;
using Beskar.Mqtt.Common.Builders.Subscribing;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Server;

namespace Beskar.Mqtt.Integration.Tests;

public class MqttMultiTransportAndChurnTests
{
   [Test]
   [Arguments(QualityOfServiceType.AtMostOnce, 256)]
   [Arguments(QualityOfServiceType.AtLeastOnce, 4096)]
   [Arguments(QualityOfServiceType.ExactlyOnce, 65536)]
   public async Task WebSocket_PublishAndSubscribe_ShouldDeliverAcrossAllQosAndPayloadSizes(
      QualityOfServiceType qos,
      int payloadSize)
   {
      await using var server = MqttServerFactory.CreateBuilder()
         .UseWs(new IPEndPoint(IPAddress.Loopback, 0))
         .WithDefaultClientIdGenerator()
         .Build();

      var startRes = await server.StartAsync();
      await Assert.That(startRes.Failed).IsFalse();

      try
      {
         var endpoint = (IPEndPoint)server.Listeners[0].LocalAddress;

         await using var sub = MqttClientFactory.CreateWs();
         await using var pub = MqttClientFactory.CreateWs();

         var expectedPayload = new byte[payloadSize];
         Random.Shared.NextBytes(expectedPayload);

         var tcs = new TaskCompletionSource<byte[]>(TaskCreationOptions.RunContinuationsAsynchronously);
         sub.AddMessageReceiveHandler((ctx, _) =>
         {
            tcs.TrySetResult(ctx.Message.Payload.ToArray());
            return ValueTask.CompletedTask;
         });

         var subConn = await sub.ConnectAsync(new ConnectOptionsBuilder(endpoint)
            .WithClientId($"ws-sub-{qos}-{payloadSize}")
            .Build());
         var pubConn = await pub.ConnectAsync(new ConnectOptionsBuilder(endpoint)
            .WithClientId($"ws-pub-{qos}-{payloadSize}")
            .Build());

         await Assert.That(subConn.Failed).IsFalse();
         await Assert.That(pubConn.Failed).IsFalse();

         await sub.SubscribeAsync(SubscribeOptions.Create()
            .WithTopicFilter("ws/test/payload", qos)
            .Build());

         var pubRes = await pub.PublishAsync(PublishOptions.Create()
            .WithTopic("ws/test/payload")
            .WithPayload(expectedPayload)
            .WithQualityOfService(qos)
            .Build());
         await Assert.That(pubRes.Failed).IsFalse();

         var received = await tcs.Task.WaitAsync(TimeSpan.FromSeconds(10));
         await Assert.That(received).IsEquivalentTo(expectedPayload);

         await sub.DisconnectAsync(new DisconnectOptions());
         await pub.DisconnectAsync(new DisconnectOptions());
      }
      finally
      {
         await server.StopAsync();
      }
   }

   [Test]
   public async Task WebSocket_LastWill_OnUngracefulDisconnect_ShouldDeliverWill()
   {
      await using var server = MqttServerFactory.CreateBuilder()
         .UseWs(new IPEndPoint(IPAddress.Loopback, 0))
         .WithDefaultClientIdGenerator()
         .Build();

      await server.StartAsync();

      try
      {
         var endpoint = (IPEndPoint)server.Listeners[0].LocalAddress;

         var willTcs = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
         await using var monitor = MqttClientFactory.CreateWs();
         monitor.AddMessageReceiveHandler((ctx, _) =>
         {
            willTcs.TrySetResult(Encoding.UTF8.GetString(ctx.Message.Payload.Span));
            return ValueTask.CompletedTask;
         });

         await monitor.ConnectAsync(new ConnectOptionsBuilder(endpoint).WithClientId("ws-monitor").Build());
         await monitor.SubscribeAsync(SubscribeOptions.Create()
            .WithTopicFilter("ws/nodes/worker", QualityOfServiceType.AtLeastOnce)
            .Build());

         var worker = MqttClientFactory.CreateWs();
         await worker.ConnectAsync(new ConnectOptionsBuilder(endpoint)
            .WithClientId("ws-worker")
            .WithWill("ws/nodes/worker", "ws-crashed"u8.ToArray(), QualityOfServiceType.AtLeastOnce)
            .Build());

         // Ungraceful WebSocket drop
         await worker.DisposeAsync();

         var willPayload = await willTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
         await Assert.That(willPayload).IsEqualTo("ws-crashed");

         await monitor.DisconnectAsync(new DisconnectOptions());
      }
      finally
      {
         await server.StopAsync();
      }
   }

   [Test]
   public async Task Churn_ConcurrentClientsConnectPublishDrop_ShouldNotLeakSessionsOrClients()
   {
      await using var server = MqttServerFactory.CreateBuilder()
         .UseTcp(new IPEndPoint(IPAddress.Loopback, 0))
         .WithDefaultClientIdGenerator()
         .Build();

      var startRes = await server.StartAsync();
      await Assert.That(startRes.Failed).IsFalse();

      try
      {
         var endpoint = (IPEndPoint)server.Listeners[0].LocalAddress;
         var deliveredCount = 0;

         await using var sink = MqttClientFactory.CreateTcp();
         sink.AddMessageReceiveHandler((_, _) =>
         {
            Interlocked.Increment(ref deliveredCount);
            return ValueTask.CompletedTask;
         });

         await sink.ConnectAsync(new ConnectOptionsBuilder(endpoint).WithClientId("churn-sink").Build());
         await sink.SubscribeAsync(SubscribeOptions.Create()
            .WithTopicFilter("churn/+", QualityOfServiceType.AtLeastOnce)
            .Build());

         const int workerCount = 24;
         var tasks = Enumerable.Range(0, workerCount).Select(async i =>
         {
            // Deliberately introduce some ClientId collisions (mod 16) and mixed graceful/abrupt teardowns
            var clientId = $"churn-worker-{i % 16}";
            var client = MqttClientFactory.CreateTcp();

            try
            {
               var conn = await client.ConnectAsync(new ConnectOptionsBuilder(endpoint)
                  .WithProtocolVersion(MqttProtocolVersion.V50)
                  .WithClientId(clientId)
                  .WithCleanSession(true)
                  .WithTimeout(TimeSpan.FromSeconds(5))
                  .Build());

               if (conn.Failed) return;

               await client.SubscribeAsync(SubscribeOptions.Create()
                  .WithTopicFilter($"churn/{i}", QualityOfServiceType.AtLeastOnce)
                  .Build());

               await client.PublishAsync(PublishOptions.Create()
                  .WithTopic($"churn/{i}")
                  .WithPayload($"payload-{i}")
                  .WithQualityOfService(i % 2 == 0 ? QualityOfServiceType.AtLeastOnce : QualityOfServiceType.ExactlyOnce)
                  .Build());

               if (i % 3 == 0)
               {
                  // Abrupt drop
                  await client.DisposeAsync();
               }
               else
               {
                  await client.DisconnectAsync(new DisconnectOptions());
                  await client.DisposeAsync();
               }
            }
            catch
            {
               await client.DisposeAsync();
            }
         });

         await Task.WhenAll(tasks);

         await sink.DisconnectAsync(new DisconnectOptions());
         await sink.DisposeAsync();

         // Wait for all server-side connection tasks to finish cleanup
         var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(5);
         var remainingClients = -1;
         while (DateTime.UtcNow < deadline)
         {
            using var clients = await server.ClientSessions.GetClients();
            remainingClients = clients.WrittenSpan.Length;
            if (remainingClients == 0) break;
            await Task.Delay(50);
         }

         await Assert.That(remainingClients).IsEqualTo(0);
         await Assert.That(Volatile.Read(ref deliveredCount)).IsGreaterThan(0);
      }
      finally
      {
         await server.StopAsync();
      }
   }
}
