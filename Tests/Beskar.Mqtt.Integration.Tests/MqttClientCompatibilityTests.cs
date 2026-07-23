using System.Net;
using System.Text;
using Beskar.Mqtt.Client;
using Beskar.Mqtt.Common.Builders.Connecting;
using Beskar.Mqtt.Common.Builders.Publishing;
using Beskar.Mqtt.Common.Builders.Subscribing;
using Beskar.Mqtt.Protocol.Enums;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;

namespace Beskar.Mqtt.Integration.Tests;

public class MqttClientCompatibilityTests
{
   [Test]
   public async Task Client_CanConnectPublishAndSubscribe_AgainstMosquittoContainer()
   {
      // 1. Arrange - Setup Mosquitto configuration to allow anonymous connections
      var mosquittoConfig = "listener 1883 0.0.0.0\nallow_anonymous true\n";
      var configBytes = Encoding.UTF8.GetBytes(mosquittoConfig);

      // Spin up the container using generic ContainerBuilder
      IContainer container;
      try
      {
         container = new ContainerBuilder("eclipse-mosquitto:latest")
            .WithPortBinding(1883, true) // Bind to random public host port
            .WithResourceMapping(configBytes, "/mosquitto/config/mosquitto.conf")
            .Build();
      }
      catch (Exception ex) when (ex.GetType().Name.Contains("Docker") || ex.Message.Contains("Docker") || ex.Message.Contains("docker_engine"))
      {
         Console.WriteLine("Docker is not running or unavailable. Skipping Client_CanConnectPublishAndSubscribe_AgainstMosquittoContainer integration test.");
         return;
      }

      try
      {
         // Start container
         await container.StartAsync();

         // Get dynamic mapped port
         var mappedPort = container.GetMappedPublicPort(1883);
         var brokerEndPoint = new IPEndPoint(IPAddress.Loopback, mappedPort);

         // 2. Act - Create our Beskar MQTT Client
         var client = MqttClientFactory.CreateTcp();

         var receivedMessages = new List<(string Topic, string Payload)>();

         // Register message receive handler
         client.AddMessageReceiveHandler((context, ct) =>
         {
            var payloadStr = Encoding.UTF8.GetString(context.Message.Payload.Span);
            receivedMessages.Add((context.Message.Topic, payloadStr));
            return ValueTask.CompletedTask;
         });

         // Connect options
         var connectOptions = new ConnectOptionsBuilder(brokerEndPoint)
            .WithProtocolVersion(MqttProtocolVersion.V50)
            .WithClientId("beskar-test-client")
            .WithCleanSession(true)
            .WithTimeout(TimeSpan.FromSeconds(10))
            .Build();

         // Connect
         var connectResult = await client.ConnectAsync(connectOptions);
         await Assert.That(connectResult.Failed).IsFalse();
         await Assert.That(client.IsConnected).IsTrue();

         // Subscribe
         var subscribeOptions = new SubscribeOptionsBuilder()
            .WithTopicFilter("test/compatibility/topic", QualityOfServiceType.AtLeastOnce)
            .Build();

         var subscribeResult = await client.SubscribeAsync(subscribeOptions);
         await Assert.That(subscribeResult.Failed).IsFalse();

         // Publish
         var publishOptions = new PublishOptionsBuilder()
            .WithTopic("test/compatibility/topic")
            .WithPayload("Hello from Beskar MQTT Client!")
            .WithQualityOfService(QualityOfServiceType.AtLeastOnce)
            .Build();

         var publishResult = await client.PublishAsync(publishOptions);
         await Assert.That(publishResult.Failed).IsFalse();

         // Wait a moment to receive the message (up to 5 seconds)
         var timeoutToken = new CancellationTokenSource(TimeSpan.FromSeconds(5)).Token;
         while (receivedMessages.Count == 0 && !timeoutToken.IsCancellationRequested)
         {
            await Task.Delay(50, timeoutToken);
         }

         // Assert
         await Assert.That(receivedMessages).Count().IsEqualTo(1);
         await Assert.That(receivedMessages[0].Topic).IsEqualTo("test/compatibility/topic");
         await Assert.That(receivedMessages[0].Payload).IsEqualTo("Hello from Beskar MQTT Client!");

         // Disconnect
         await client.DisposeAsync();
      }
      catch (Exception err)
      {
         Assert.Fail(err.Message);
      }
      finally
      {
         // Clean up container
         await container.DisposeAsync();
      }
   }
}
