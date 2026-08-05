using System.Net;
using System.Text;
using Beskar.Mqtt.Client;
using Beskar.Mqtt.Common.Builders.Connecting;
using Beskar.Mqtt.Common.Builders.Disconnecting;
using Beskar.Mqtt.Common.Builders.Subscribing;
using Beskar.Mqtt.Common.Options;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Server;
using System.Buffers;

Console.WriteLine();
Console.WriteLine("==========================================================");
Console.WriteLine(" MQTT Last Will and Testament (LWT) Example               ");
Console.WriteLine("==========================================================");

const int Port = 8007;
const string PresenceTopic = "device/status/presence";
const string WillPayloadText = "offline_unexpected";

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

// 2. Setup Monitor Client (Subscriber)
await using var monitorClient = MqttClientFactory.CreateTcp();

// Watch for any messages published to the presence topic
using var receiveToken = monitorClient.AddMessageReceiveHandler((context, ct) =>
{
   var payload = Encoding.UTF8.GetString(context.Message.Payload.Span);
   TraceLogger.LogClientInfo("[Monitor] Received presence update on topic '{0}': {1}", context.Message.Topic, payload);
   return ValueTask.CompletedTask;
});

var connectOptions = new ConnectOptions
{
   EndPoint = new IPEndPoint(IPAddress.Loopback, Port),
   ProtocolVersion = MqttProtocolVersion.V50
};

TraceLogger.LogClientInfo("[Monitor] Connecting to broker...");
await monitorClient.ConnectAsync(connectOptions);

var subOptions = SubscribeOptions.Create()
   .WithTopicFilter(PresenceTopic, QualityOfServiceType.AtLeastOnce)
   .Build();

await monitorClient.SubscribeAsync(subOptions);
TraceLogger.LogClientInfo("[Monitor] Subscribed to '{0}' to monitor device presence.", PresenceTopic);

// 3. Test Graceful Disconnect (No Will Message should be triggered)
TraceLogger.LogInfo("\n--- Scenario A: Graceful Disconnect ---");

await using (var deviceClient = MqttClientFactory.CreateTcp())
{
   var deviceConnectOptions = new ConnectOptions
   {
      EndPoint = new IPEndPoint(IPAddress.Loopback, Port),
      ProtocolVersion = MqttProtocolVersion.V50,
      ClientIdUtf8Bytes = Encoding.UTF8.GetBytes("Sensor-Device-A"),
      HasWill = true,
      WillTopicUtf8Bytes = Encoding.UTF8.GetBytes(PresenceTopic),
      WillPayload = new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes(WillPayloadText)),
      WillQualityOfService = QualityOfServiceType.AtLeastOnce,
      WillRetain = false
   };

   TraceLogger.LogClientInfo("[Device A] Connecting with configured Last Will...");
   await deviceClient.ConnectAsync(deviceConnectOptions);
   await Task.Delay(100);

   TraceLogger.LogClientInfo("[Device A] Disconnecting gracefully (sending DISCONNECT packet)...");
   await deviceClient.DisconnectAsync(new DisconnectOptions());
}

TraceLogger.LogInfo("Verification: Waiting 1.5 seconds. Monitor should receive NO message because Device A disconnected gracefully.");
await Task.Delay(1500);

// 4. Test Ungraceful Disconnect (Will Message should be triggered)
TraceLogger.LogInfo("\n--- Scenario B: Ungraceful Disconnect ---");

// We open a separate block so we can dispose the device client directly without calling DisconnectAsync
await using (var deviceClient = MqttClientFactory.CreateTcp())
{
   var deviceConnectOptions = new ConnectOptions
   {
      EndPoint = new IPEndPoint(IPAddress.Loopback, Port),
      ProtocolVersion = MqttProtocolVersion.V50,
      ClientIdUtf8Bytes = Encoding.UTF8.GetBytes("Sensor-Device-B"),
      HasWill = true,
      WillTopicUtf8Bytes = Encoding.UTF8.GetBytes(PresenceTopic),
      WillPayload = new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes(WillPayloadText)),
      WillQualityOfService = QualityOfServiceType.AtLeastOnce,
      WillRetain = false
   };

   TraceLogger.LogClientInfo("[Device B] Connecting with configured Last Will...");
   await deviceClient.ConnectAsync(deviceConnectOptions);
   await Task.Delay(100);

   TraceLogger.LogClientInfo("[Device B] Dropping connection abruptly (disposing without sending DISCONNECT)...");
   // Disposing directly (which internally shuts down transport without sending a DISCONNECT packet)
   await deviceClient.DisposeAsync();
}

TraceLogger.LogInfo("Verification: Waiting for broker to detect connection drop and publish Will...");
await Task.Delay(1500);

// 5. Cleanup
await monitorClient.DisconnectAsync(new DisconnectOptions());
await mqttServer.StopAsync();

Console.WriteLine();
Console.WriteLine("==========================================================");
Console.WriteLine(" MQTT Last Will Example Finished Successfully             ");
Console.WriteLine("==========================================================");


// =====================================================================
// LOCAL CONSOLE ONLY LOGGER WRAPPER
// =====================================================================

public static class TraceLogger
{
   public static void LogInfo(string format, params object?[] arg) => Console.WriteLine(format, arg);
   public static void LogServerInfo(string format, params object?[] arg) => Console.WriteLine("[Server] " + format, arg);
   public static void LogClientInfo(string format, params object?[] arg) => Console.WriteLine("[Client] " + format, arg);
}
