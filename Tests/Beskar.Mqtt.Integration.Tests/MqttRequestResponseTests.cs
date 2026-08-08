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
