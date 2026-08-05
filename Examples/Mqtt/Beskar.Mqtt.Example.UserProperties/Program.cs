using System.Net;
using System.Text;
using Beskar.Mqtt.Client;
using Beskar.Mqtt.Common.Builders.Connecting;
using Beskar.Mqtt.Common.Builders.Disconnecting;
using Beskar.Mqtt.Common.Builders.Publishing;
using Beskar.Mqtt.Common.Builders.Subscribing;
using Beskar.Mqtt.Common.Options;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Server;
using System.Buffers;

Console.WriteLine();
Console.WriteLine("==========================================================");
Console.WriteLine(" MQTT User Properties (Metadata) Example                   ");
Console.WriteLine("==========================================================");

const int Port = 8008;
const string TelemetryTopic = "sensor/telemetry";

// 1. Build and configure the MQTT server
TraceLogger.LogServerInfo("Starting MQTT Server...");
var mqttServer = MqttServerFactory.CreateBuilder()
   .WithDefaultClientIdGenerator()
   .UseTcp(Port)
   .Build();

var startResult = await mqttServer.StartAsync();
if (startResult.Failed)
{
   throw new InvalidOperationException($"Server failed to start: {startResult.Error.Detail}");
}
TraceLogger.LogServerInfo("Server listening on port {0}.", Port);

// 2. Setup Subscriber Client
await using var subscriberClient = MqttClientFactory.CreateTcp();

using var receiveToken = subscriberClient.AddMessageReceiveHandler((context, ct) =>
{
   var payload = Encoding.UTF8.GetString(context.Message.Payload.Span);
   TraceLogger.LogClientInfo("[Subscriber] Received payload: {0}", payload);

   // Extract and print custom user properties metadata
   if (context.Message.UserProperties.Count > 0)
   {
      TraceLogger.LogClientInfo("[Subscriber] Processing Metadata User Properties:");
      foreach (var prop in context.Message.UserProperties)
      {
         var valueStr = Encoding.UTF8.GetString(prop.Value.Span);
         TraceLogger.LogClientInfo("  - {0} : {1}", prop.Name, valueStr);
      }
   }
   else
   {
      TraceLogger.LogClientWarning("[Subscriber] Message received without user properties.");
   }
   return ValueTask.CompletedTask;
});

var connectOptions = new ConnectOptions
{
   EndPoint = new IPEndPoint(IPAddress.Loopback, Port),
   ProtocolVersion = MqttProtocolVersion.V50
};

TraceLogger.LogClientInfo("[Subscriber] Connecting...");
await subscriberClient.ConnectAsync(connectOptions);

var subOptions = SubscribeOptions.Create()
   .WithTopicFilter(TelemetryTopic, QualityOfServiceType.AtLeastOnce)
   .Build();

await subscriberClient.SubscribeAsync(subOptions);
TraceLogger.LogClientInfo("[Subscriber] Subscribed to '{0}'.", TelemetryTopic);

// 3. Setup Publisher Client
await using var publisherClient = MqttClientFactory.CreateTcp();
TraceLogger.LogClientInfo("[Publisher] Connecting...");
await publisherClient.ConnectAsync(connectOptions);

// 4. Publish message with custom tracing and source details via User Properties
var telemetryPub = PublishOptions.Create()
   .WithTopic(TelemetryTopic)
   .WithPayload("{ \"temp\": 22.4, \"humidity\": 58.1 }")
   .WithQualityOfService(QualityOfServiceType.AtLeastOnce)
   // Attach trace headers (W3C traceparent example) and client versions
   .WithUserProperty("traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
   .WithUserProperty("client-version", "v1.2.0")
   .WithUserProperty("device-id", "sensor-node-west-4")
   .Build();

TraceLogger.LogClientInfo("[Publisher] Publishing reading with custom traceparent metadata headers...");
await publisherClient.PublishAsync(telemetryPub);

// Wait to receive message
await Task.Delay(1000);

// 5. Cleanup
await subscriberClient.DisconnectAsync(new DisconnectOptions());
await publisherClient.DisconnectAsync(new DisconnectOptions());
await mqttServer.StopAsync();

Console.WriteLine();
Console.WriteLine("==========================================================");
Console.WriteLine(" MQTT User Properties Example Finished Successfully       ");
Console.WriteLine("==========================================================");


// =====================================================================
// LOCAL CONSOLE ONLY LOGGER WRAPPER
// =====================================================================

public static class TraceLogger
{
   public static void LogServerInfo(string format, params object?[] arg) => Console.WriteLine("[Server] " + format, arg);
   public static void LogClientInfo(string format, params object?[] arg) => Console.WriteLine("[Client] " + format, arg);
   public static void LogClientWarning(string format, params object?[] arg) => Console.WriteLine("[Client Warning] " + format, arg);
}
