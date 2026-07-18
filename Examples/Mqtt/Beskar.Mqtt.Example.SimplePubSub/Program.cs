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
Console.WriteLine(" MQTT Simple Subscribe, Publish & Disconnect Example      ");
Console.WriteLine("==========================================================");

const int Port = 8003;
const string Topic = "iot/devices/temperature";

// 1. Build and configure the MQTT server
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
TraceLogger.LogServerInfo($"Server: Running and listening on port {Port}.");

// 2. Setup Subscriber Client (Dashboard) and Publisher Client (Sensor)
TraceLogger.LogInfo("\n--- Initializing MQTT Clients ---");
await using var subscriberClient = MqttClientFactory.CreateTcp();
await using var publisherClient = MqttClientFactory.CreateTcp();

// Register message receive callback on the Subscriber Client
using var receiveHandlerToken = subscriberClient.AddMessageReceiveHandler((context, ct) =>
{
   var payload = Encoding.UTF8.GetString(context.Message.Payload.Span);
   TraceLogger.LogClientInfo("[Dashboard] Message received on topic '{0}': {1}", context.Message.Topic, payload);
   return ValueTask.CompletedTask;
});

// 3. Connect Clients to the Server
TraceLogger.LogInfo("\n--- Connecting Clients ---");
var connectOptions = new ConnectOptions
{
   EndPoint = new IPEndPoint(IPAddress.Loopback, Port),
   ProtocolVersion = MqttProtocolVersion.V50
};

TraceLogger.LogClientInfo("Dashboard Client: Connecting...");
var subConnectResult = await subscriberClient.ConnectAsync(connectOptions);
if (subConnectResult.Failed)
{
   throw new InvalidOperationException($"Subscriber failed to connect: {subConnectResult.Error.Detail}");
}

TraceLogger.LogClientInfo("Sensor Client: Connecting...");
var pubConnectResult = await publisherClient.ConnectAsync(connectOptions);
if (pubConnectResult.Failed)
{
   throw new InvalidOperationException($"Publisher failed to connect: {pubConnectResult.Error.Detail}");
}

// 4. Subscribe Dashboard Client to Topic
TraceLogger.LogInfo("\n--- Subscribing Dashboard ---");
var subscribeOptions = SubscribeOptions.Create()
   .WithTopicFilter(Topic, QualityOfServiceType.AtLeastOnce)
   .Build();

var subResult = await subscriberClient.SubscribeAsync(subscribeOptions);
if (subResult.Failed)
{
   throw new InvalidOperationException($"Subscriber failed to subscribe: {subResult.Error.Detail}");
}
TraceLogger.LogClientInfo($"Dashboard Client: Subscribed to '{Topic}'");

// 5. Publish Simulated Readings from Sensor Client
TraceLogger.LogInfo("\n--- Publishing Temperature Readings ---");
var readings = new[]
{
   "{ \"temp\": 18.5, \"status\": \"Normal\" }",
   "{ \"temp\": 24.2, \"status\": \"Warning\" }",
   "{ \"temp\": 31.0, \"status\": \"Critical\" }"
};

foreach (var payload in readings)
{
   var publishOptions = PublishOptions.Create()
      .WithTopic(Topic)
      .WithPayload(payload)
      .WithQualityOfService(QualityOfServiceType.AtLeastOnce)
      .Build();

   TraceLogger.LogClientInfo("Sensor Client: Publishing reading -> {0}", payload);
   var pubResult = await publisherClient.PublishAsync(publishOptions);
   if (pubResult.Failed)
   {
      TraceLogger.LogClientError($"Sensor Client: Publish failed: {pubResult.Error.Detail}");
   }

   // Small delay between publishes
   await Task.Delay(100);
}

// Wait briefly to ensure all messages are delivered
await Task.Delay(200);

// 6. Unsubscribe and Clean up
TraceLogger.LogInfo("\n--- Unsubscribing and Disconnecting ---");
var unsubscribeOptions = UnsubscribeOptions.Create()
   .WithTopicFilter(Topic)
   .Build();

var unsubResult = await subscriberClient.UnsubscribeAsync(unsubscribeOptions);
if (unsubResult.Failed)
{
   TraceLogger.LogClientError($"Dashboard Client: Unsubscribe failed: {unsubResult.Error.Detail}");
}

// Disconnect both clients
TraceLogger.LogClientInfo("Dashboard Client: Disconnecting...");
await subscriberClient.DisconnectAsync(new DisconnectOptions());

TraceLogger.LogClientInfo("Sensor Client: Disconnecting...");
await publisherClient.DisconnectAsync(new DisconnectOptions());

// 7. Stop Server
TraceLogger.LogServerInfo("Server: Stopping...");
await mqttServer.StopAsync();
TraceLogger.LogServerInfo("Server: Stopped.");

Console.WriteLine("==========================================================");
Console.WriteLine(" Pub-Sub Demo Finished Successfully.");
Console.WriteLine("==========================================================");
