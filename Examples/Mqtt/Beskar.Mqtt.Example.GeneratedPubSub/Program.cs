using System;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Beskar.Mqtt.Client;
using Beskar.Mqtt.Common.Builders.Connecting;
using Beskar.Mqtt.Common.Builders.Disconnecting;
using Beskar.Mqtt.Common.Builders.Publishing;
using Beskar.Mqtt.Common.Builders.Subscribing;
using Beskar.Mqtt.Common.Generators;
using Beskar.Mqtt.Common.Options;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Server;
using Beskar.Utilities.Tracing;

namespace Beskar.Mqtt.Example.GeneratedPubSub;

// Define compile-time generated MQTT topics using the source generator
public static partial class Topics
{
   [GeneratedMqttTopic("devices/{deviceId}/status/{isOk}")]
   public static partial bool TryParseStatus(
      ReadOnlySpan<char> topic,
      out int deviceId,
      out bool isOk);

   [GeneratedMqttTopic("devices/{deviceId}/status/{isOk}")]
   public static partial bool TryFormatStatus(
      Span<char> destination,
      int deviceId,
      bool isOk,
      out int charsWritten);

   [GeneratedMqttTopic("devices/{deviceId}/telemetry/{sensorType}/{value}")]
   public static partial bool TryParseTelemetry(
      ReadOnlySpan<char> topic,
      out int deviceId,
      out string sensorType,
      out double value);

   [GeneratedMqttTopic("devices/{deviceId}/telemetry/{sensorType}/{value}")]
   public static partial bool TryFormatTelemetry(
      Span<char> destination,
      int deviceId,
      string sensorType,
      double value,
      out int charsWritten);
}

public static class Program
{
   public static async Task Main()
   {
      TraceLogger.IsEnabled = true;
      Console.WriteLine();
      Console.WriteLine("==========================================================");
      Console.WriteLine(" MQTT Generated Topic Pub-Sub Example                     ");
      Console.WriteLine("==========================================================");

      const int Port = 8004;

      // Build and start the MQTT server
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

      // Setup Subscriber Client (Dashboard) and Publisher Client (Sensors)
      TraceLogger.LogInfo("\n--- Initializing MQTT Clients ---");
      await using var subscriberClient = MqttClientFactory.CreateTcp();
      await using var publisherClient = MqttClientFactory.CreateTcp();

      // Register message receive callback on the Subscriber Client
      using var receiveHandlerToken = subscriberClient.AddMessageReceiveHandler((context, ct) =>
      {
         var payload = Encoding.UTF8.GetString(context.Message.Payload.Span);
         var topicSpan = context.Message.Topic.AsSpan();

         // This is more niche since you would put this probably more into the payload
         // Match status topic using generated parser
         if (Topics.TryParseStatus(topicSpan, out var deviceId, out var isOk))
         {
            TraceLogger.LogClientInfo(
               $"[Dashboard] Received STATUS -> DeviceId: {deviceId}, IsOk: {isOk}, Payload: {payload}");
         }
         // Match telemetry topic using generated parser
         else if (Topics.TryParseTelemetry(topicSpan, out var telDeviceId, out var sensorType, out var value))
         {
            TraceLogger.LogClientInfo(
               $"[Dashboard] Received TELEMETRY -> DeviceId: {telDeviceId}, Sensor: {sensorType}, Value: {value:F2}, Payload: {payload}");
         }
         else
         {
            TraceLogger.LogClientInfo($"[Dashboard] Received Unknown Topic '{context.Message.Topic}': {payload}");
         }

         return ValueTask.CompletedTask;
      });

      // 4. Connect Clients to the Server
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

      // 5. Subscribe Dashboard Client using wildcards
      TraceLogger.LogInfo("\n--- Subscribing Dashboard ---");
      var subscribeOptions = SubscribeOptions.Create()
         .WithTopicFilter("devices/+/status/+"u8, QualityOfServiceType.AtLeastOnce)
         .WithTopicFilter("devices/+/telemetry/+/+"u8, QualityOfServiceType.AtLeastOnce)
         .Build();

      var subResult = await subscriberClient.SubscribeAsync(subscribeOptions);
      if (subResult.Failed)
      {
         throw new InvalidOperationException($"Subscriber failed to subscribe: {subResult.Error.Detail}");
      }
      TraceLogger.LogClientInfo("Dashboard Client: Subscribed to wildcard topics.");

      // 6. Format and Publish topics using the generated helper methods
      TraceLogger.LogInfo("\n--- Formatting and Publishing ---");

      //  Publish Status using string helper FormatStatus(int, bool)
      var statusTopic = Topics.FormatStatus(42, true);
      TraceLogger.LogClientInfo($"Sensor Client: Formatted string topic -> '{statusTopic}'");

      var statusPubOptions = PublishOptions.Create()
         // we can also use the byte[] helper FormatStatusToBytes(int, bool) because WithTopic accepts utf8 spans too
         .WithTopic(statusTopic)
         .WithPayload("Normal Operations")
         .WithQualityOfService(QualityOfServiceType.AtLeastOnce)
         .Build();
      await publisherClient.PublishAsync(statusPubOptions);

      await Task.Delay(100);

      // Publish Telemetry using byte[] helper FormatTelemetryToBytes(int, string, double)
      var telemetryTopicBytes = Topics.FormatTelemetryToBytes(42, "temperature", 23.45);
      TraceLogger.LogClientInfo($"Sensor Client: Formatted byte[] topic -> '{Encoding.UTF8.GetString(telemetryTopicBytes)}'");

      var telemetryPubOptions = PublishOptions.Create()
         .WithTopic(telemetryTopicBytes) // utf8 bytes directly, no garbage string
         .WithPayload("Reading: 23.45 C")
         .WithQualityOfService(QualityOfServiceType.AtLeastOnce)
         .Build();
      await publisherClient.PublishAsync(telemetryPubOptions);

      // Wait briefly for delivery
      await Task.Delay(200);

      // Clean up and Shutdown
      TraceLogger.LogInfo("\n--- Shutting Down ---");
      await subscriberClient.DisconnectAsync(new DisconnectOptions());
      await publisherClient.DisconnectAsync(new DisconnectOptions());
      await mqttServer.StopAsync();

      Console.WriteLine("==========================================================");
      Console.WriteLine(" Pub-Sub Demo Finished Successfully.");
      Console.WriteLine("==========================================================");
   }
}
