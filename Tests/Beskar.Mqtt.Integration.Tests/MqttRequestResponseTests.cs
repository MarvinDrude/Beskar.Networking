using System.Net;
using System.Text;
using Beskar.Mqtt.Client;
using Beskar.Mqtt.Common.Builders.Connecting;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Server;

namespace Beskar.Mqtt.Integration.Tests;

public class MqttRequestResponseTests
{
   [Test]
   public async Task RequestAsync_WithValidSubscriberResponse_ReturnsResponseContextSuccessfully()
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

         var publisher = MqttClientFactory.CreateTcp();
         var subscriber = MqttClientFactory.CreateTcp();

         var connectOptions = new ConnectOptionsBuilder(localAddress)
            .WithProtocolVersion(MqttProtocolVersion.V50)
            .WithCleanSession(true)
            .WithTimeout(TimeSpan.FromSeconds(5))
            .Build();

         var pubConnRes = await publisher.ConnectAsync(connectOptions);
         await Assert.That(pubConnRes.Failed).IsFalse();

         var subConnRes = await subscriber.ConnectAsync(connectOptions);
         await Assert.That(subConnRes.Failed).IsFalse();

         // Subscriber registers handler that responds using context.RespondAsync
         subscriber.AddMessageReceiveHandler(async (ctx, ct) =>
         {
            var reqStr = Encoding.UTF8.GetString(ctx.Message.Payload.Span);
            var replyStr = $"ACK:{reqStr}";
            await ctx.RespondAsync(replyStr, QualityOfServiceType.AtLeastOnce, ct);
         });

         // Subscriber subscribes to command topic
         var subOptions = Beskar.Mqtt.Common.Builders.Subscribing.SubscribeOptions.Create()
            .WithTopicFilter("test/rpc/request", QualityOfServiceType.AtLeastOnce)
            .Build();
         var subRes = await subscriber.SubscribeAsync(subOptions);
         await Assert.That(subRes.Failed).IsFalse();

         // Act - Publisher sends request
         var requestPayload = Encoding.UTF8.GetBytes("ORDER-999");
         var responseResult = await publisher.RequestAsync("test/rpc/request", requestPayload, TimeSpan.FromSeconds(5));

         // Assert
         await Assert.That(responseResult.Failed).IsFalse();

         var response = responseResult.Success!;
         var replyText = Encoding.UTF8.GetString(response.Payload.Span);
         await Assert.That(replyText).IsEqualTo("ACK:ORDER-999");
         await Assert.That(response.Elapsed.TotalMilliseconds).IsGreaterThan(0);

         await publisher.DisposeAsync();
         await subscriber.DisposeAsync();
      }
      finally
      {
         await server.StopAsync();
         await server.DisposeAsync();
      }
   }

   [Test]
   public async Task RequestAsync_WithDefaultResponseTopicAndCorrelation_AutoGeneratesBothAndSucceeds()
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

         var publisher = MqttClientFactory.CreateTcp();
         var subscriber = MqttClientFactory.CreateTcp();

         var pubConnectOptions = new ConnectOptionsBuilder(localAddress)
            .WithProtocolVersion(MqttProtocolVersion.V50)
            .WithClientId("pub-client-autogen")
            .WithCleanSession(true)
            .WithTimeout(TimeSpan.FromSeconds(5))
            .Build();

         var subConnectOptions = new ConnectOptionsBuilder(localAddress)
            .WithProtocolVersion(MqttProtocolVersion.V50)
            .WithClientId("sub-client-autogen")
            .WithCleanSession(true)
            .WithTimeout(TimeSpan.FromSeconds(5))
            .Build();

         await publisher.ConnectAsync(pubConnectOptions);
         await subscriber.ConnectAsync(subConnectOptions);

         subscriber.AddMessageReceiveHandler(async (ctx, ct) =>
         {
            // Verify ResponseTopic was auto-generated to clients/pub-client-autogen/response
            await Assert.That(ctx.Message.ResponseTopic).IsEqualTo("clients/pub-client-autogen/response");
            await Assert.That(ctx.Message.CorrelationData.HasValue).IsTrue();

            await ctx.RespondAsync("AUTO_REPLY_OK", QualityOfServiceType.AtLeastOnce, ct);
         });

         var subOptions = Beskar.Mqtt.Common.Builders.Subscribing.SubscribeOptions.Create()
            .WithTopicFilter("test/auto/topic", QualityOfServiceType.AtLeastOnce)
            .Build();
         await subscriber.SubscribeAsync(subOptions);

         var responseResult = await publisher.RequestAsync("test/auto/topic", "AUTO_REQ"u8.ToArray(), TimeSpan.FromSeconds(5));
         await Assert.That(responseResult.Failed).IsFalse();

         var replyText = Encoding.UTF8.GetString(responseResult.Success!.Payload.Span);
         await Assert.That(replyText).IsEqualTo("AUTO_REPLY_OK");

         await publisher.DisposeAsync();
         await subscriber.DisposeAsync();
      }
      finally
      {
         await server.StopAsync();
         await server.DisposeAsync();
      }
   }

   [Test]
   public async Task RequestAsync_ConcurrentRequests_CorrelatesAllResponsesCorrectly()
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

         var publisher = MqttClientFactory.CreateTcp();
         var subscriber = MqttClientFactory.CreateTcp();

         var connectOptions = new ConnectOptionsBuilder(localAddress)
            .WithProtocolVersion(MqttProtocolVersion.V50)
            .WithCleanSession(true)
            .WithTimeout(TimeSpan.FromSeconds(5))
            .Build();

         await publisher.ConnectAsync(connectOptions);
         await subscriber.ConnectAsync(connectOptions);

         subscriber.AddMessageReceiveHandler(async (ctx, ct) =>
         {
            var requestStr = Encoding.UTF8.GetString(ctx.Message.Payload.Span);
            var replyStr = $"REPLY_FOR_{requestStr}";
            await ctx.RespondAsync(replyStr, QualityOfServiceType.AtLeastOnce, ct);
         });

         var subOptions = Beskar.Mqtt.Common.Builders.Subscribing.SubscribeOptions.Create()
            .WithTopicFilter("test/concurrent/req", QualityOfServiceType.AtLeastOnce)
            .Build();
         await subscriber.SubscribeAsync(subOptions);

         const int requestCount = 20;
         var tasks = new Task[requestCount];

         for (var i = 0; i < requestCount; i++)
         {
            var reqId = i;
            tasks[i] = Task.Run(async () =>
            {
               var payloadStr = $"ID-{reqId}";
               var res = await publisher.RequestAsync("test/concurrent/req", Encoding.UTF8.GetBytes(payloadStr), TimeSpan.FromSeconds(5));

               await Assert.That(res.Failed).IsFalse();
               var replyText = Encoding.UTF8.GetString(res.Success!.Payload.Span);
               await Assert.That(replyText).IsEqualTo($"REPLY_FOR_ID-{reqId}");
            });
         }

         await Task.WhenAll(tasks);

         await publisher.DisposeAsync();
         await subscriber.DisposeAsync();
      }
      finally
      {
         await server.StopAsync();
         await server.DisposeAsync();
      }
   }

   [Test]
   public async Task RequestAsync_WhenClientDisconnectsWhileWaiting_CancelsPendingRequestsImmediately()
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

         var publisher = MqttClientFactory.CreateTcp();

         var connectOptions = new ConnectOptionsBuilder(localAddress)
            .WithProtocolVersion(MqttProtocolVersion.V50)
            .WithCleanSession(true)
            .WithTimeout(TimeSpan.FromSeconds(5))
            .Build();

         await publisher.ConnectAsync(connectOptions);

         // Launch request with long 10 second timeout, but no subscriber replying
         var requestTask = Task.Run(() => publisher.RequestAsync("test/disconnect/topic", "NO_REPLY"u8.ToArray(), TimeSpan.FromSeconds(10)));

         await Task.Delay(200);

         // Disconnect publisher client while request is waiting
         await publisher.DisconnectAsync(new Beskar.Mqtt.Common.Builders.Disconnecting.DisconnectOptions());

         var result = await requestTask;

         // Assert that request immediately returned failed result without waiting for 10s timeout
         await Assert.That(result.Failed).IsTrue();
         await Assert.That(result.Error.Detail).Contains("cancelled");

         await publisher.DisposeAsync();
      }
      finally
      {
         await server.StopAsync();
         await server.DisposeAsync();
      }
   }

   [Test]
   public async Task RespondAsync_WithoutResponseTopic_ReturnsError()
   {
      var server = MqttServerFactory.CreateBuilder()
         .UseTcp(new IPEndPoint(IPAddress.Loopback, 0))
         .WithDefaultClientIdGenerator()
         .Build();

      await server.StartAsync();

      try
      {
         var localAddress = (IPEndPoint)server.Listeners[0].LocalAddress;

         var publisher = MqttClientFactory.CreateTcp();
         var subscriber = MqttClientFactory.CreateTcp();

         var connectOptions = new ConnectOptionsBuilder(localAddress)
            .WithProtocolVersion(MqttProtocolVersion.V50)
            .WithCleanSession(true)
            .WithTimeout(TimeSpan.FromSeconds(5))
            .Build();

         await publisher.ConnectAsync(connectOptions);
         await subscriber.ConnectAsync(connectOptions);

         var tcs = new TaskCompletionSource<bool>();

         subscriber.AddMessageReceiveHandler(async (ctx, ct) =>
         {
            // Call RespondAsync on a message that does NOT have a ResponseTopic
            var respondResult = await ctx.RespondAsync("NO_RESPONSE_TOPIC_REPLY", QualityOfServiceType.AtLeastOnce, ct);

            await Assert.That(respondResult.Failed).IsTrue();
            await Assert.That(respondResult.Error.Detail).Contains("does not specify a ResponseTopic");

            tcs.TrySetResult(true);
         });

         var subOptions = Beskar.Mqtt.Common.Builders.Subscribing.SubscribeOptions.Create()
            .WithTopicFilter("test/no/responsetopic", QualityOfServiceType.AtLeastOnce)
            .Build();
         await subscriber.SubscribeAsync(subOptions);

         // Publish standard message WITHOUT ResponseTopic
         var pubOptions = Beskar.Mqtt.Common.Builders.Publishing.PublishOptions.Create()
            .WithTopic("test/no/responsetopic")
            .WithPayload("PLAIN_MESSAGE")
            .WithQualityOfService(QualityOfServiceType.AtLeastOnce)
            .Build();

         await publisher.PublishAsync(pubOptions);

         await tcs.Task.WaitAsync(TimeSpan.FromSeconds(5));

         await publisher.DisposeAsync();
         await subscriber.DisposeAsync();
      }
      finally
      {
         await server.StopAsync();
         await server.DisposeAsync();
      }
   }

   [Test]
   public async Task RequestAsync_WhenSubscriberFailsToReply_TimesOutWithError()
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

         var publisher = MqttClientFactory.CreateTcp();

         var connectOptions = new ConnectOptionsBuilder(localAddress)
            .WithProtocolVersion(MqttProtocolVersion.V50)
            .WithCleanSession(true)
            .WithTimeout(TimeSpan.FromSeconds(5))
            .Build();

         var pubConnRes = await publisher.ConnectAsync(connectOptions);
         await Assert.That(pubConnRes.Failed).IsFalse();

         // Act - Publisher sends request with short timeout and no subscriber replying
         var requestPayload = Encoding.UTF8.GetBytes("NO_REPLY_DATA");
         var responseResult = await publisher.RequestAsync("test/rpc/noreply", requestPayload, TimeSpan.FromMilliseconds(400));

         // Assert
         await Assert.That(responseResult.Failed).IsTrue();
         await Assert.That(responseResult.Error.Detail).Contains("timed out");

         await publisher.DisposeAsync();
      }
      finally
      {
         await server.StopAsync();
         await server.DisposeAsync();
      }
   }
}
