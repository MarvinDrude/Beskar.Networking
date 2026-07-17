using System.Net;
using Beskar.Mqtt.Client;
using Beskar.Mqtt.Common.Builders.Connecting;
using Beskar.Mqtt.Common.Builders.Disconnecting;
using Beskar.Mqtt.Common.Builders.Subscribing;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Server;

namespace Beskar.Mqtt.Common.Tests.Internal;

public class MqttSharedSubscriptionTests
{
   private static int _nextPort = 14000;
   private static int GetFreePort()
   {
      return Interlocked.Increment(ref _nextPort);
   }

   [Test]
   public async Task Subscribe_SharedSubscription_V5_ShouldReturnSharedSubscriptionsNotSupported()
   {
      var port = GetFreePort();
      await using var server = MqttServerFactory.CreateBuilder()
         .UseTcp(port)
         .WithDefaultClientIdGenerator()
         .Build();

      await server.StartAsync();

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
      var port = GetFreePort();
      await using var server = MqttServerFactory.CreateBuilder()
         .UseTcp(port)
         .WithDefaultClientIdGenerator()
         .Build();

      await server.StartAsync();

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
