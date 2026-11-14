using System.Buffers;
using System.Collections.Concurrent;
using System.Net;
using System.Text;
using Beskar.Mqtt.Server;
using MQTTnet;
using MQTTnet.Formatter;
using MQTTnet.Packets;
using MQTTnet.Protocol;

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
            .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce)
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

   [Test]
   public async Task Server_MqttnetV5Clients_QoS2_UserProperties_AndLargePayload_ShouldWorkInteroperably()
   {
      await using var server = MqttServerFactory.CreateBuilder()
         .UseTcp(new IPEndPoint(IPAddress.Loopback, 0))
         .WithDefaultClientIdGenerator()
         .Build();

      var startResult = await server.StartAsync();
      await Assert.That(startResult.Failed).IsFalse();

      try
      {
         var boundPort = ((IPEndPoint)server.Listeners[0].LocalAddress).Port;
         var factory = new MqttClientFactory();

         using var pub = factory.CreateMqttClient();
         using var sub = factory.CreateMqttClient();

         var tcs = new TaskCompletionSource<MqttApplicationMessage>(TaskCreationOptions.RunContinuationsAsynchronously);
         sub.ApplicationMessageReceivedAsync += e =>
         {
            tcs.TrySetResult(e.ApplicationMessage);
            return Task.CompletedTask;
         };

         await pub.ConnectAsync(new MqttClientOptionsBuilder()
            .WithTcpServer("127.0.0.1", boundPort)
            .WithProtocolVersion(MqttProtocolVersion.V500)
            .WithClientId("mqttnet-v5-pub")
            .Build());

         await sub.ConnectAsync(new MqttClientOptionsBuilder()
            .WithTcpServer("127.0.0.1", boundPort)
            .WithProtocolVersion(MqttProtocolVersion.V500)
            .WithClientId("mqttnet-v5-sub")
            .Build());

         await sub.SubscribeAsync(factory.CreateSubscribeOptionsBuilder()
            .WithTopicFilter(f => f.WithTopic("interop/v5/qos2").WithQualityOfServiceLevel(MqttQualityOfServiceLevel.ExactlyOnce))
            .Build());

         var payload = new byte[32 * 1024];
         Random.Shared.NextBytes(payload);

         var msg = new MqttApplicationMessageBuilder()
            .WithTopic("interop/v5/qos2")
            .WithPayload(payload)
            .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.ExactlyOnce)
            .WithContentType("application/octet-stream")
            .WithResponseTopic("interop/v5/reply")
            .WithUserProperty("trace-id", "abc-123"u8.ToArray())
            .Build();

         var pubRes = await pub.PublishAsync(msg);
         await Assert.That(pubRes.ReasonCode).IsEqualTo(MqttClientPublishReasonCode.Success);

         var delivered = await tcs.Task.WaitAsync(TimeSpan.FromSeconds(10));
         await Assert.That(delivered.Payload.ToArray()).IsEquivalentTo(payload);
         await Assert.That(delivered.ContentType).IsEqualTo("application/octet-stream");
         await Assert.That(delivered.ResponseTopic).IsEqualTo("interop/v5/reply");
         await Assert.That(delivered.UserProperties.Any(p => p.Name == "trace-id" && p.ReadValueAsString() == "abc-123")).IsTrue();

         await pub.DisconnectAsync();
         await sub.DisconnectAsync();
      }
      finally
      {
         await server.StopAsync();
      }
   }

   [Test]
   public async Task Server_MqttnetV5Clients_RetainedAndEmptyWillCleanup_ShouldWorkInteroperably()
   {
      await using var server = MqttServerFactory.CreateBuilder()
         .UseTcp(new IPEndPoint(IPAddress.Loopback, 0))
         .WithDefaultClientIdGenerator()
         .Build();

      var startResult = await server.StartAsync();
      await Assert.That(startResult.Failed).IsFalse();

      try
      {
         var boundPort = ((IPEndPoint)server.Listeners[0].LocalAddress).Port;
         var factory = new MqttClientFactory();

         using var watcher = factory.CreateMqttClient();
         var announced = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
         var removed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

         watcher.ApplicationMessageReceivedAsync += e =>
         {
            if (e.ApplicationMessage.Payload.Length == 0)
            {
               removed.TrySetResult();
            }
            else
            {
               announced.TrySetResult();
            }

            return Task.CompletedTask;
         };

         await watcher.ConnectAsync(new MqttClientOptionsBuilder()
            .WithTcpServer("127.0.0.1", boundPort)
            .WithProtocolVersion(MqttProtocolVersion.V500)
            .WithClientId("mqttnet-watcher")
            .Build());

         await watcher.SubscribeAsync(factory.CreateSubscribeOptionsBuilder()
            .WithTopicFilter(f => f.WithTopic("interop/nodes/+").WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce))
            .Build());

         // Node connects via MQTTnet with a retained empty-payload Last Will
         var node = factory.CreateMqttClient();
         await node.ConnectAsync(new MqttClientOptionsBuilder()
            .WithTcpServer("127.0.0.1", boundPort)
            .WithProtocolVersion(MqttProtocolVersion.V500)
            .WithClientId("mqttnet-node-1")
            .WithWillTopic("interop/nodes/node-1")
            .WithWillPayload(Array.Empty<byte>())
            .WithWillQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce)
            .WithWillRetain(true)
            .Build());

         await node.PublishAsync(new MqttApplicationMessageBuilder()
            .WithTopic("interop/nodes/node-1")
            .WithPayload("online")
            .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce)
            .WithRetainFlag(true)
            .Build());

         await announced.Task.WaitAsync(TimeSpan.FromSeconds(5));

         // Simulate abrupt crash by disposing MQTTnet client without DisconnectAsync
         node.Dispose();
         await removed.Task.WaitAsync(TimeSpan.FromSeconds(5));

         // Verify late-joining MQTTnet client sees 0 retained nodes
         var lateEvents = new ConcurrentQueue<string>();
         using var late = factory.CreateMqttClient();
         late.ApplicationMessageReceivedAsync += e =>
         {
            lateEvents.Enqueue(e.ApplicationMessage.Topic);
            return Task.CompletedTask;
         };

         await late.ConnectAsync(new MqttClientOptionsBuilder()
            .WithTcpServer("127.0.0.1", boundPort)
            .WithProtocolVersion(MqttProtocolVersion.V500)
            .WithClientId("mqttnet-late")
            .Build());

         await late.SubscribeAsync(factory.CreateSubscribeOptionsBuilder()
            .WithTopicFilter(f => f.WithTopic("interop/nodes/+").WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce))
            .Build());

         await Task.Delay(250);
         await Assert.That(lateEvents.Count).IsEqualTo(0);

         await watcher.DisconnectAsync();
         await late.DisconnectAsync();
      }
      finally
      {
         await server.StopAsync();
      }
   }
}
