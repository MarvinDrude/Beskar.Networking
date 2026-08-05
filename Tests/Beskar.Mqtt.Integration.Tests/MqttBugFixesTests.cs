using System.Net;
using System.Text;
using Beskar.Mqtt.Client;
using Beskar.Mqtt.Common.Builders.Connecting;
using Beskar.Mqtt.Common.Builders.Disconnecting;
using Beskar.Mqtt.Common.Builders.Publishing;
using Beskar.Mqtt.Common.Builders.Subscribing;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Server;
using Beskar.Mqtt.Server.Options;

namespace Beskar.Mqtt.Integration.Tests;

public class MqttBugFixesTests
{
   [Test]
   public async Task Client_Disconnect_WithThrowingEventHandler_DoesNotThrowUnhandledException()
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

         var client = MqttClientFactory.CreateTcp();
         client.AddDisconnectedHandler((ctx, ct) =>
         {
            throw new InvalidOperationException("Simulated exception in disconnect handler");
         });

         var connectOptions = new ConnectOptionsBuilder(localAddress)
            .WithProtocolVersion(MqttProtocolVersion.V50)
            .WithClientId("test-disconnect-exception-client")
            .WithCleanSession(true)
            .WithTimeout(TimeSpan.FromSeconds(5))
            .Build();

         var connectResult = await client.ConnectAsync(connectOptions);
         await Assert.That(connectResult.Failed).IsFalse();

         // Act - Disconnect client.
         await client.DisconnectAsync(new DisconnectOptions
         {
            ReasonCode = DisconnectReasonCode.NormalDisconnection
         });

         await Task.Delay(100);

         await client.DisposeAsync();
      }
      finally
      {
         await server.StopAsync();
         await server.DisposeAsync();
      }
   }

   [Test]
   public async Task Server_QoS2_DoesNotLeakInFlightCountWhenSendFailsOrInterrupted()
   {
      var server = MqttServerFactory.CreateBuilder(new MqttServerOptions { ReceiveMaximum = 2 })
         .UseTcp(new IPEndPoint(IPAddress.Loopback, 0))
         .WithDefaultClientIdGenerator()
         .Build();

      var startResult = await server.StartAsync();
      await Assert.That(startResult.Failed).IsFalse();

      try
      {
         var localAddress = (IPEndPoint)server.Listeners[0].LocalAddress;

         var client = MqttClientFactory.CreateTcp();
         var connectOptions = new ConnectOptionsBuilder(localAddress)
            .WithProtocolVersion(MqttProtocolVersion.V50)
            .WithClientId("test-qos2-inflight-client")
            .WithCleanSession(true)
            .WithTimeout(TimeSpan.FromSeconds(5))
            .Build();

         var connectResult = await client.ConnectAsync(connectOptions);
         await Assert.That(connectResult.Failed).IsFalse();

         // Publish multiple QoS 2 messages
         for (var i = 0; i < 5; i++)
         {
            var publishOptions = new PublishOptionsBuilder()
               .WithTopic("test/qos2/topic")
               .WithPayload($"Payload {i}")
               .WithQualityOfService(QualityOfServiceType.ExactlyOnce)
               .Build();

            var pubResult = await client.PublishAsync(publishOptions);
            await Assert.That(pubResult.Failed).IsFalse();
         }

         await client.DisposeAsync();
      }
      finally
      {
         await server.StopAsync();
         await server.DisposeAsync();
      }
   }
}
