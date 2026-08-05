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
using Beskar.Mqtt.Server.Options;
using System.Buffers;

Console.WriteLine();
Console.WriteLine("==========================================================");
Console.WriteLine(" MQTT Topic Aliases (Bandwidth Optimization) Example      ");
Console.WriteLine("==========================================================");

const int Port = 8009;
const string LongTopic = "vehicles/diagnostics/sensors/engine/temperature";

// 1. Build and configure the MQTT server
TraceLogger.LogServerInfo("Starting MQTT Server...");
var serverOptions = new MqttServerOptions
{
   TopicAliasMaximum = 32 // Set the maximum number of topic aliases the server allows
};

var mqttServer = MqttServerFactory.CreateBuilder(serverOptions)
   .WithDefaultClientIdGenerator()
   .UseTcp(Port)
   .Build();

var startResult = await mqttServer.StartAsync();
if (startResult.Failed)
{
   throw new InvalidOperationException($"Server failed to start: {startResult.Error.Detail}");
}
TraceLogger.LogServerInfo("Server listening on port {0} (TopicAliasMaximum = {1}).", Port, serverOptions.TopicAliasMaximum);

// 2. Setup Subscriber Client
await using var subscriberClient = MqttClientFactory.CreateTcp();

using var receiveToken = subscriberClient.AddMessageReceiveHandler((context, ct) =>
{
   var payload = Encoding.UTF8.GetString(context.Message.Payload.Span);
   // Note: The subscriber will ALWAYS receive the message with the full topic path resolved by the broker,
   // regardless of whether the publisher used an alias to send it!
   TraceLogger.LogClientInfo("[Subscriber] Received payload on topic '{0}': {1}", context.Message.Topic, payload);
   return ValueTask.CompletedTask;
});

var connectOptions = new ConnectOptions
{
   EndPoint = new IPEndPoint(IPAddress.Loopback, Port),
   ProtocolVersion = MqttProtocolVersion.V50,
   TopicAliasMaximum = 32 // Request/declare client-side topic alias maximum
};

TraceLogger.LogClientInfo("[Subscriber] Connecting...");
await subscriberClient.ConnectAsync(connectOptions);

var subOptions = SubscribeOptions.Create()
   .WithTopicFilter(LongTopic, QualityOfServiceType.AtLeastOnce)
   .Build();

await subscriberClient.SubscribeAsync(subOptions);
TraceLogger.LogClientInfo("[Subscriber] Subscribed to '{0}'.", LongTopic);

// 3. Setup Publisher Client
await using var publisherClient = MqttClientFactory.CreateTcp();
TraceLogger.LogClientInfo("[Publisher] Connecting...");
await publisherClient.ConnectAsync(connectOptions);

// 4. Publish 1: Set topic string AND specify topic alias to map it on the server
var pub1 = PublishOptions.Create()
   .WithTopic(LongTopic)
   .WithTopicAlias(1) // Map topic to alias 1
   .WithPayload("22.5 C (Initial Map)")
   .WithQualityOfService(QualityOfServiceType.AtLeastOnce)
   .Build();

TraceLogger.LogClientInfo("[Publisher] Publishing first message specifying topic path AND TopicAlias = 1...");
await publisherClient.PublishAsync(pub1);
await Task.Delay(200);

// 5. Publish 2: Omit topic string and specify TopicAlias = 1 (Optimized packet size)
var pub2 = PublishOptions.Create()
   .WithTopicAlias(1) // Refer to mapped alias 1 (Topic name left blank)
   .WithPayload("23.1 C (Optimized)")
   .WithQualityOfService(QualityOfServiceType.AtLeastOnce)
   .Build();

TraceLogger.LogClientInfo("[Publisher] Publishing second message omitting topic path (only setting TopicAlias = 1)...");
await publisherClient.PublishAsync(pub2);
await Task.Delay(200);

// 6. Publish 3: Omit topic string and specify TopicAlias = 1 again
var pub3 = PublishOptions.Create()
   .WithTopicAlias(1)
   .WithPayload("23.8 C (Optimized)")
   .WithQualityOfService(QualityOfServiceType.AtLeastOnce)
   .Build();

TraceLogger.LogClientInfo("[Publisher] Publishing third message omitting topic path (only setting TopicAlias = 1)...");
await publisherClient.PublishAsync(pub3);
await Task.Delay(500);

// 7. Cleanup
await subscriberClient.DisconnectAsync(new DisconnectOptions());
await publisherClient.DisconnectAsync(new DisconnectOptions());
await mqttServer.StopAsync();

Console.WriteLine();
Console.WriteLine("==========================================================");
Console.WriteLine(" MQTT Topic Aliases Example Finished Successfully         ");
Console.WriteLine("==========================================================");


// =====================================================================
// LOCAL CONSOLE ONLY LOGGER WRAPPER
// =====================================================================

public static class TraceLogger
{
   public static void LogServerInfo(string format, params object?[] arg) => Console.WriteLine("[Server] " + format, arg);
   public static void LogClientInfo(string format, params object?[] arg) => Console.WriteLine("[Client] " + format, arg);
}
