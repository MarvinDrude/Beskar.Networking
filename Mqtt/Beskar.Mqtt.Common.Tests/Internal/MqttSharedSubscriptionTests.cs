using System.Net;
using Beskar.Mqtt.Client;
using Beskar.Mqtt.Common.Builders.Connecting;
using Beskar.Mqtt.Common.Builders.Disconnecting;
using Beskar.Mqtt.Common.Builders.Subscribing;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Server;

namespace Beskar.Mqtt.Common.Tests.Internal;

public class MqttSharedSubscriptionTests
{[Test]
   public async Task Subscribe_SharedSubscription_V5_ShouldReturnSharedSubscriptionsNotSupported()
   {
      await using var server = MqttServerFactory.CreateBuilder()
         .UseTcp(0)
         .WithDefaultClientIdGenerator()
         .Build();

      await server.StartAsync();

      var port = ((IPEndPoint)server.Listeners[0].LocalAddress).Port;

      var client = MqttClientFactory.CreateTcp();
      await client.ConnectAsync(new ConnectOptions
      {
         EndPoint = new IPEndPoint(IPAddress.Loopback, port)
      });

      var subscribeResult = await client.SubscribeAsync(new SubscribeOptionsBuilder()
         .WithTopicFilter("$share/group1/topic"u8, QualityOfServiceType.AtLeastOnce)
         .Build());

      await Assert.That(subscribeResult.Failed).IsFalse();
      await Assert.That(subscribeResult.Success!.Subscriptions.Count).IsEqualTo(1);
      await Assert.That(subscribeResult.Success!.Subscriptions[0].ReasonCode).IsEqualTo(SubscribeReasonCode.SharedSubscriptionsNotSupported);

      await client.DisconnectAsync(new DisconnectOptions());
   }

   [Test]
   public async Task Subscribe_SharedSubscription_V3_ShouldReturnUnspecifiedError()
   {
      await using var server = MqttServerFactory.CreateBuilder()
         .UseTcp(0)
         .WithDefaultClientIdGenerator()
         .Build();

      await server.StartAsync();

      var port = ((IPEndPoint)server.Listeners[0].LocalAddress).Port;

      var client = MqttClientFactory.CreateTcp();
      await client.ConnectAsync(new ConnectOptions
      {
         EndPoint = new IPEndPoint(IPAddress.Loopback, port),
         ProtocolVersion = MqttProtocolVersion.V311
      });

      var subscribeResult = await client.SubscribeAsync(new SubscribeOptionsBuilder()
         .WithTopicFilter("$share/group1/topic"u8, QualityOfServiceType.AtLeastOnce)
         .Build());

      await Assert.That(subscribeResult.Failed).IsFalse();
      await Assert.That(subscribeResult.Success!.Subscriptions.Count).IsEqualTo(1);
      
      // In MQTT v3, the failure code for SUBACK is 0x80 (Failure / UnspecifiedError)
      await Assert.That(subscribeResult.Success!.Subscriptions[0].ReasonCode).IsEqualTo(SubscribeReasonCode.UnspecifiedError);

      await client.DisconnectAsync(new DisconnectOptions());
   }
}
