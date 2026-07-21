using System.Net;
using System.Text;
using Beskar.Mqtt.Server;
using MQTTnet;

namespace Beskar.Mqtt.Integration.Tests;

public class MqttServerCompatibilityTests
{
   [Test]
   public async Task Server_CanAcceptConnectionsAndRouteMessages_ForMqttnetClient()
   {
      // 1. Arrange - Setup and start Beskar MQTT Server on dynamic port
      var serverBuilder = MqttServerFactory.CreateBuilder()
         .UseTcp(new IPEndPoint(IPAddress.Loopback, 0))
         .WithDefaultClientIdGenerator();

      var server = serverBuilder.Build();
      var startResult = await server.StartAsync();
      await Assert.That(startResult.Failed).IsFalse();

      try
      {
         // Retrieve bound local endpoint port
         var listener = server.Listeners[0];
         var localAddress = (IPEndPoint)listener.LocalAddress;
         var boundPort = localAddress.Port;

         // 2. Act - Create and connect an MQTTnet Client to our server
         var mqttFactory = new MqttClientFactory();
         using var mqttClient = mqttFactory.CreateMqttClient();

         var receivedMessages = new List<(string Topic, string Payload)>();
         mqttClient.ApplicationMessageReceivedAsync += e =>
         {
            var payloadStr = Encoding.UTF8.GetString(e.ApplicationMessage.Payload);
            receivedMessages.Add((e.ApplicationMessage.Topic, payloadStr));
            return Task.CompletedTask;
         };

         var clientOptions = new MqttClientOptionsBuilder()
            .WithTcpServer("127.0.0.1", boundPort)
            .WithClientId("mqttnet-test-client")
            .WithCleanSession(true)
            .Build();

         var connectResult = await mqttClient.ConnectAsync(clientOptions);
         await Assert.That(connectResult.ResultCode).IsEqualTo(MqttClientConnectResultCode.Success);

         // Subscribe using MQTTnet client
         var subscribeOptions = mqttFactory.CreateSubscribeOptionsBuilder()
            .WithTopicFilter(f => f.WithTopic("test/server/compatibility"))
            .Build();

         await mqttClient.SubscribeAsync(subscribeOptions);

         // Publish message using MQTTnet client
         var message = new MqttApplicationMessageBuilder()
            .WithTopic("test/server/compatibility")
            .WithPayload("Hello from MQTTnet standard client!")
            .WithQualityOfServiceLevel(MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce)
            .Build();

         await mqttClient.PublishAsync(message);

         // Wait a moment for our server to receive, process, and route the message back
         var timeoutToken = new CancellationTokenSource(TimeSpan.FromSeconds(5)).Token;
         while (receivedMessages.Count == 0 && !timeoutToken.IsCancellationRequested)
         {
            await Task.Delay(50, timeoutToken);
         }

         // Assert
         await Assert.That(receivedMessages).Count().IsEqualTo(1);
         await Assert.That(receivedMessages[0].Topic).IsEqualTo("test/server/compatibility");
         await Assert.That(receivedMessages[0].Payload).IsEqualTo("Hello from MQTTnet standard client!");

         // Disconnect MQTTnet client
         await mqttClient.DisconnectAsync(new MqttClientDisconnectOptions(), cancellationToken: timeoutToken);
      }
      catch (Exception err)
      {
         Assert.Fail(err.Message);
      }
      finally
      {
         // Stop our server
         await server.StopAsync();
         await server.DisposeAsync();
      }
   }
}
