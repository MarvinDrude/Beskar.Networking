using System.Net;
using System.Text;
using Beskar.Mqtt.Client;
using Beskar.Mqtt.Common.Builders.Connecting;
using Beskar.Mqtt.Common.Builders.Disconnecting;
using Beskar.Mqtt.Common.Builders.Publishing;
using Beskar.Mqtt.Common.Builders.Subscribing;
using Beskar.Mqtt.Common.Builders.Unsubscribing;
using Beskar.Mqtt.Common.Handlers.Contexts;
using Beskar.Mqtt.Common.Options;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Server;
using Beskar.Utilities.Tracing;

TraceLogger.IsEnabled = true;
Console.WriteLine();
Console.WriteLine("==========================================================");
Console.WriteLine(" MQTT Quality of Service (QoS) Levels Example            ");
Console.WriteLine("==========================================================");

const int Port = 8004;
const string WildcardTopic = "qos/#";
const string TopicQos0 = "qos/level0";
const string TopicQos1 = "qos/level1";
const string TopicQos2 = "qos/level2";

// Build and configure the MQTT server
var mqttServer = MqttServerFactory.CreateBuilder()
   .WithDefaultClientIdGenerator()
   .UseTcp(Port)
   .Build();

TraceLogger.LogServerInfo("Server: Starting...");
var startResult = await mqttServer.StartAsync();
if (startResult.Failed)
{
   throw new InvalidOperationException($"Server failed to start: {startResult.Error.Detail}");
}
TraceLogger.LogServerInfo($"Server: Running on port {Port}.");

// Setup Clients
await using var subscriberClient = MqttClientFactory.CreateTcp();
await using var publisherClient = MqttClientFactory.CreateTcp();

// Handler on Subscriber: Prints the topic, QoS level, and payload of received messages
using var receiveHandlerToken = subscriberClient.AddMessageReceiveHandler((context, ct) =>
{
   var payload = Encoding.UTF8.GetString(context.Message.Payload.Span);
   TraceLogger.LogClientInfo(
      "[Subscriber] Received message on Topic '{0}' with QoS: {1} -> Payload: {2}",
      context.Message.Topic,
      context.Message.QualityOfService,
      payload);

   return ValueTask.CompletedTask;
});

// Connect Clients
var connectOptions = new ConnectOptions
{
   EndPoint = new IPEndPoint(IPAddress.Loopback, Port),
   ProtocolVersion = MqttProtocolVersion.V50
};

await subscriberClient.ConnectAsync(connectOptions);
await publisherClient.ConnectAsync(connectOptions);

// Subscribe with QoS 2 (to receive messages of any QoS level)
TraceLogger.LogInfo("\n--- Subscribing to Wildcard Topic 'qos/#' with QoS 2 ---");
var subscribeOptions = SubscribeOptions.Create()
   .WithTopicFilter(WildcardTopic, QualityOfServiceType.ExactlyOnce)
   .Build();

await subscriberClient.SubscribeAsync(subscribeOptions);

// Publish at QoS 0 (At Most Once - Fire and forget)
TraceLogger.LogInfo("\n--- Publishing Message at QoS 0 ---");
var pubOptions0 = PublishOptions.Create()
   .WithTopic(TopicQos0)
   .WithPayload("QoS 0 message: No delivery guarantee, minimal overhead.")
   .WithQualityOfService(QualityOfServiceType.AtMostOnce)
   .Build();

await publisherClient.PublishAsync(pubOptions0);
await Task.Delay(100);

// Publish at QoS 1 (At Least Once - Acknowledged delivery)
TraceLogger.LogInfo("\n--- Publishing Message at QoS 1 ---");
var pubOptions1 = PublishOptions.Create()
   .WithTopic(TopicQos1)
   .WithPayload("QoS 1 message: Guaranteed delivery, might be duplicated.")
   .WithQualityOfService(QualityOfServiceType.AtLeastOnce)
   .Build();

await publisherClient.PublishAsync(pubOptions1);
await Task.Delay(100);

// Publish at QoS 2 (Exactly Once - 4-step handshake delivery)
TraceLogger.LogInfo("\n--- Publishing Message at QoS 2 ---");
var pubOptions2 = PublishOptions.Create()
   .WithTopic(TopicQos2)
   .WithPayload("QoS 2 message: Guaranteed exact delivery, zero duplicates.")
   .WithQualityOfService(QualityOfServiceType.ExactlyOnce)
   .Build();

await publisherClient.PublishAsync(pubOptions2);

// Wait briefly for delivery of all messages
await Task.Delay(300);

// Clean up
TraceLogger.LogInfo("\n--- Unsubscribing and Disconnecting ---");
var unsubscribeOptions = UnsubscribeOptions.Create()
   .WithTopicFilter(WildcardTopic)
   .Build();

await subscriberClient.UnsubscribeAsync(unsubscribeOptions);
await subscriberClient.DisconnectAsync(new DisconnectOptions());
await publisherClient.DisconnectAsync(new DisconnectOptions());

await mqttServer.StopAsync();

Console.WriteLine("==========================================================");
Console.WriteLine(" QoS Demo Finished Successfully.                          ");
Console.WriteLine("==========================================================");
