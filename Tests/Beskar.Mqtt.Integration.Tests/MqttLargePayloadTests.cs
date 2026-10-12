using System.Net;
using Beskar.Mqtt.Client;
using Beskar.Mqtt.Common.Builders.Connecting;
using Beskar.Mqtt.Common.Builders.Publishing;
using Beskar.Mqtt.Common.Builders.Subscribing;
using Beskar.Mqtt.Common.Extensions;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Server;

namespace Beskar.Mqtt.Integration.Tests;

public class MqttLargePayloadTests
{
   [Test]
   public async Task Publish_PayloadGreaterThan4Kb_QoS0_V5_DeliveredSuccessfully()
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

         const int payloadSize = 8192; // 8KB > 4KB
         var expectedPayload = new byte[payloadSize];
         Random.Shared.NextBytes(expectedPayload);

         var tcs = new TaskCompletionSource<byte[]>(TaskCreationOptions.RunContinuationsAsynchronously);
         subscriber.AddMessageReceiveHandler((ctx, ct) =>
         {
            tcs.TrySetResult(ctx.Message.Payload.ToArray());
            return ValueTask.CompletedTask;
         });

         var subOptions = new SubscribeOptionsBuilder()
            .WithTopicFilter("large/qos0/v5", QualityOfServiceType.AtMostOnce)
            .Build();
         await Assert.That((await subscriber.SubscribeAsync(subOptions)).Failed).IsFalse();

         var pubOptions = new PublishOptionsBuilder()
            .WithTopic("large/qos0/v5")
            .WithQualityOfService(QualityOfServiceType.AtMostOnce)
            .WithPayload(expectedPayload)
            .Build();

         var pubResult = await publisher.PublishAsync(pubOptions);
         await Assert.That(pubResult.Failed).IsFalse();

         using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
         var received = await tcs.Task.WaitAsync(cts.Token);

         await Assert.That(received.Length).IsEqualTo(payloadSize);
         await Assert.That(received).IsEquivalentTo(expectedPayload);
      }
      finally
      {
         await server.StopAsync();
         await server.DisposeAsync();
      }
   }

   [Test]
   public async Task Publish_PayloadGreaterThan4Kb_QoS1_V5_DeliveredSuccessfully()
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

         const int payloadSize = 8192; // 8KB > 4KB
         var expectedPayload = new byte[payloadSize];
         Random.Shared.NextBytes(expectedPayload);

         var tcs = new TaskCompletionSource<byte[]>(TaskCreationOptions.RunContinuationsAsynchronously);
         subscriber.AddMessageReceiveHandler((ctx, ct) =>
         {
            tcs.TrySetResult(ctx.Message.Payload.ToArray());
            return ValueTask.CompletedTask;
         });

         var subOptions = new SubscribeOptionsBuilder()
            .WithTopicFilter("large/qos1/v5", QualityOfServiceType.AtLeastOnce)
            .Build();
         await Assert.That((await subscriber.SubscribeAsync(subOptions)).Failed).IsFalse();

         var pubOptions = new PublishOptionsBuilder()
            .WithTopic("large/qos1/v5")
            .WithQualityOfService(QualityOfServiceType.AtLeastOnce)
            .WithPayload(expectedPayload)
            .Build();

         var pubResult = await publisher.PublishAsync(pubOptions);
         await Assert.That(pubResult.Failed).IsFalse();

         using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
         var received = await tcs.Task.WaitAsync(cts.Token);

         await Assert.That(received.Length).IsEqualTo(payloadSize);
         await Assert.That(received).IsEquivalentTo(expectedPayload);
      }
      finally
      {
         await server.StopAsync();
         await server.DisposeAsync();
      }
   }

   [Test]
   public async Task Publish_PayloadGreaterThan4Kb_QoS2_V5_DeliveredSuccessfully()
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

         const int payloadSize = 8192; // 8KB > 4KB
         var expectedPayload = new byte[payloadSize];
         Random.Shared.NextBytes(expectedPayload);

         var tcs = new TaskCompletionSource<byte[]>(TaskCreationOptions.RunContinuationsAsynchronously);
         subscriber.AddMessageReceiveHandler((ctx, ct) =>
         {
            tcs.TrySetResult(ctx.Message.Payload.ToArray());
            return ValueTask.CompletedTask;
         });

         var subOptions = new SubscribeOptionsBuilder()
            .WithTopicFilter("large/qos2/v5", QualityOfServiceType.ExactlyOnce)
            .Build();
         await Assert.That((await subscriber.SubscribeAsync(subOptions)).Failed).IsFalse();

         var pubOptions = new PublishOptionsBuilder()
            .WithTopic("large/qos2/v5")
            .WithQualityOfService(QualityOfServiceType.ExactlyOnce)
            .WithPayload(expectedPayload)
            .Build();

         var pubResult = await publisher.PublishAsync(pubOptions);
         await Assert.That(pubResult.Failed).IsFalse();

         using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
         var received = await tcs.Task.WaitAsync(cts.Token);

         await Assert.That(received.Length).IsEqualTo(payloadSize);
         await Assert.That(received).IsEquivalentTo(expectedPayload);
      }
      finally
      {
         await server.StopAsync();
         await server.DisposeAsync();
      }
   }

   [Test]
   public async Task Publish_VeryLargePayload64Kb_QoS1_V5_DeliveredSuccessfully()
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
            .WithTimeout(TimeSpan.FromSeconds(10))
            .Build();

         await Assert.That((await publisher.ConnectAsync(connectOptions)).Failed).IsFalse();
         await Assert.That((await subscriber.ConnectAsync(connectOptions)).Failed).IsFalse();

         const int payloadSize = 65536; // 64KB > 4KB
         var expectedPayload = new byte[payloadSize];
         Random.Shared.NextBytes(expectedPayload);

         var tcs = new TaskCompletionSource<byte[]>(TaskCreationOptions.RunContinuationsAsynchronously);
         subscriber.AddMessageReceiveHandler((ctx, ct) =>
         {
            tcs.TrySetResult(ctx.Message.Payload.ToArray());
            return ValueTask.CompletedTask;
         });

         var subOptions = new SubscribeOptionsBuilder()
            .WithTopicFilter("large/64kb/v5", QualityOfServiceType.AtLeastOnce)
            .Build();
         await Assert.That((await subscriber.SubscribeAsync(subOptions)).Failed).IsFalse();

         var pubOptions = new PublishOptionsBuilder()
            .WithTopic("large/64kb/v5")
            .WithQualityOfService(QualityOfServiceType.AtLeastOnce)
            .WithPayload(expectedPayload)
            .Build();

         var pubResult = await publisher.PublishAsync(pubOptions);
         await Assert.That(pubResult.Failed).IsFalse();

         using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
         var received = await tcs.Task.WaitAsync(cts.Token);

         await Assert.That(received.Length).IsEqualTo(payloadSize);
         await Assert.That(received).IsEquivalentTo(expectedPayload);
      }
      finally
      {
         await server.StopAsync();
         await server.DisposeAsync();
      }
   }

   [Test]
   public async Task Publish_PayloadGreaterThan4Kb_QoS0_QoS1_QoS2_V311_DeliveredSuccessfully()
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
            .WithProtocolVersion(MqttProtocolVersion.V311)
            .WithCleanSession(true)
            .WithTimeout(TimeSpan.FromSeconds(5))
            .Build();

         await Assert.That((await publisher.ConnectAsync(connectOptions)).Failed).IsFalse();
         await Assert.That((await subscriber.ConnectAsync(connectOptions)).Failed).IsFalse();

         const int payloadSize = 8192; // 8KB > 4KB
         var expectedPayload = new byte[payloadSize];
         Random.Shared.NextBytes(expectedPayload);

         var receivedList = new List<byte[]>();
         var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

         subscriber.AddMessageReceiveHandler((ctx, ct) =>
         {
            lock (receivedList)
            {
               receivedList.Add(ctx.Message.Payload.ToArray());
               if (receivedList.Count == 3)
               {
                  tcs.TrySetResult();
               }
            }
            return ValueTask.CompletedTask;
         });

         var subOptions = new SubscribeOptionsBuilder()
            .WithTopicFilter("large/v311/#", QualityOfServiceType.ExactlyOnce)
            .Build();
         await Assert.That((await subscriber.SubscribeAsync(subOptions)).Failed).IsFalse();

         // Send QoS 0
         var pubOptions0 = new PublishOptionsBuilder()
            .WithTopic("large/v311/qos0")
            .WithQualityOfService(QualityOfServiceType.AtMostOnce)
            .WithPayload(expectedPayload)
            .Build();
         await Assert.That((await publisher.PublishAsync(pubOptions0)).Failed).IsFalse();

         // Send QoS 1
         var pubOptions1 = new PublishOptionsBuilder()
            .WithTopic("large/v311/qos1")
            .WithQualityOfService(QualityOfServiceType.AtLeastOnce)
            .WithPayload(expectedPayload)
            .Build();
         await Assert.That((await publisher.PublishAsync(pubOptions1)).Failed).IsFalse();

         // Send QoS 2
         var pubOptions2 = new PublishOptionsBuilder()
            .WithTopic("large/v311/qos2")
            .WithQualityOfService(QualityOfServiceType.ExactlyOnce)
            .WithPayload(expectedPayload)
            .Build();
         await Assert.That((await publisher.PublishAsync(pubOptions2)).Failed).IsFalse();

         using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
         await tcs.Task.WaitAsync(cts.Token);

         await Assert.That(receivedList.Count).IsEqualTo(3);
         foreach (var msg in receivedList)
         {
            await Assert.That(msg.Length).IsEqualTo(payloadSize);
            await Assert.That(msg).IsEquivalentTo(expectedPayload);
         }
      }
      finally
      {
         await server.StopAsync();
         await server.DisposeAsync();
      }
   }

   [Test]
   public async Task RequestResponse_PayloadGreaterThan4Kb_V5_ExchangedSuccessfully()
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

         const int payloadSize = 8192; // 8KB
         var requestPayload = new byte[payloadSize];
         var responsePayload = new byte[payloadSize];
         Random.Shared.NextBytes(requestPayload);
         Random.Shared.NextBytes(responsePayload);

         subscriber.AddMessageReceiveHandler(async (ctx, ct) =>
         {
            var reqData = ctx.Message.Payload.ToArray();
            if (reqData.SequenceEqual(requestPayload))
            {
               await ctx.RespondAsync(responsePayload, QualityOfServiceType.AtLeastOnce, ct);
            }
         });

         var subOptions = new SubscribeOptionsBuilder()
            .WithTopicFilter("large/rpc/req", QualityOfServiceType.AtLeastOnce)
            .Build();
         await Assert.That((await subscriber.SubscribeAsync(subOptions)).Failed).IsFalse();

         var responseResult = await publisher.RequestAsync("large/rpc/req", requestPayload, TimeSpan.FromSeconds(5));
         await Assert.That(responseResult.Failed).IsFalse();

         var response = responseResult.Success!;
         await Assert.That(response.Payload.Length).IsEqualTo(payloadSize);
         await Assert.That(response.Payload.ToArray()).IsEquivalentTo(responsePayload);
      }
      finally
      {
         await server.StopAsync();
         await server.DisposeAsync();
      }
   }

   [Test]
   public async Task SubscribeStream_PayloadGreaterThan4Kb_StreamsLargeItemsSuccessfully()
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

         const int payloadSize = 8192; // 8KB
         var payload1 = new byte[payloadSize];
         var payload2 = new byte[payloadSize];
         Random.Shared.NextBytes(payload1);
         Random.Shared.NextBytes(payload2);

         var receivedList = new List<byte[]>();
         using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

         var pubTask = Task.Run(async () =>
         {
            await Task.Delay(100);

            var pub1 = new PublishOptionsBuilder()
               .WithTopic("large/stream/topic")
               .WithPayload(payload1)
               .WithQualityOfService(QualityOfServiceType.AtLeastOnce)
               .Build();
            await publisher.PublishAsync(pub1, cts.Token);

            await Task.Delay(50);

            var pub2 = new PublishOptionsBuilder()
               .WithTopic("large/stream/topic")
               .WithPayload(payload2)
               .WithQualityOfService(QualityOfServiceType.AtLeastOnce)
               .Build();
            await publisher.PublishAsync(pub2, cts.Token);
         });

         await foreach (var item in subscriber.SubscribeStream(
            "large/stream/topic",
            decoder: bytes => bytes.ToArray(),
            ct: cts.Token))
         {
            receivedList.Add(item);
            if (receivedList.Count == 2)
            {
               break;
            }
         }

         await pubTask;

         await Assert.That(receivedList.Count).IsEqualTo(2);
         await Assert.That(receivedList[0]).IsEquivalentTo(payload1);
         await Assert.That(receivedList[1]).IsEquivalentTo(payload2);
      }
      finally
      {
         await server.StopAsync();
         await server.DisposeAsync();
      }
   }
}
