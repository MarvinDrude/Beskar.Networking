using System.Net;
using Beskar.Mqtt.Client;
using Beskar.Mqtt.Common.Builders.Connecting;
using Beskar.Mqtt.Common.Builders.Disconnecting;
using Beskar.Mqtt.Common.Builders.Publishing;
using Beskar.Mqtt.Common.Interfaces;
using Beskar.Mqtt.Common.Options;
using Beskar.Mqtt.Common.Serialization;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Server;
using MessagePack;

namespace Beskar.Mqtt.Example.MessagePackStreaming;

/// <summary>
/// Domain model serialized using high-performance binary MessagePack.
/// </summary>
[MessagePackObject]
public record SensorTelemetry(
   [property: Key(0)] string DeviceId,
   [property: Key(1)] double Temperature,
   [property: Key(2)] double Humidity,
   [property: Key(3)] DateTime Timestamp,
   [property: Key(4)] bool IsAlert);

/// <summary>
/// Custom per-subscription payload decoder for MessagePack binary data.
/// Implements <see cref="IMqttPayloadDecoder{T}"/>.
/// </summary>
/// <typeparam name="T">Target type to deserialize into.</typeparam>
public sealed class MessagePackPayloadDecoder<T>(MessagePackSerializerOptions? options = null) : IMqttPayloadDecoder<T>
{
   private readonly MessagePackSerializerOptions? _options = options;

   public T Decode(ReadOnlyMemory<byte> payload)
   {
      return MessagePackSerializer.Deserialize<T>(payload, _options);
   }
}

/// <summary>
/// Helper extension methods to provide seamless MessagePack streaming syntax on <see cref="IMqttClient"/>.
/// </summary>
public static class MessagePackMqttExtensions
{
   public static IAsyncEnumerable<T> SubscribeMessagePackStream<T>(
      this IMqttClient client,
      string topicFilter,
      MessagePackSerializerOptions? options = null,
      QualityOfServiceType qos = QualityOfServiceType.AtLeastOnce,
      StreamSubscriptionOptions? streamOptions = null,
      CancellationToken ct = default)
   {
      ArgumentNullException.ThrowIfNull(client);
      return client.SubscribeStream(topicFilter, new MessagePackPayloadDecoder<T>(options), qos, streamOptions, ct);
   }

   public static Task<T> WaitForMessagePackMessageAsync<T>(
      this IMqttClient client,
      string topicFilter,
      Func<T, bool>? predicate = null,
      MessagePackSerializerOptions? options = null,
      TimeSpan timeout = default,
      CancellationToken ct = default)
   {
      ArgumentNullException.ThrowIfNull(client);
      return client.WaitForMessageAsync(topicFilter, new MessagePackPayloadDecoder<T>(options), predicate, timeout, ct);
   }
}

public static class Program
{
   public static async Task Main()
   {
      Console.WriteLine("==========================================================");
      Console.WriteLine(" Beskar MQTT - MessagePack Custom Decoder Streaming       ");
      Console.WriteLine("==========================================================");

      const int Port = 8012;

      // 1. Start embedded MQTT Server
      Console.WriteLine("\n[1] Starting embedded MQTT server on port {0}...", Port);
      var mqttServer = MqttServerFactory.CreateBuilder()
         .UseTcp(Port)
         .WithDefaultClientIdGenerator()
         .Build();

      var startResult = await mqttServer.StartAsync();
      if (startResult.Failed)
      {
         throw new InvalidOperationException($"Server failed to start: {startResult.Error.Detail}");
      }
      Console.WriteLine("    Server is running!");

      // 2. Connect SensorPublisher and AnalyticsDashboard
      Console.WriteLine("\n[2] Connecting Sensor Publisher and Analytics Dashboard...");
      await using var publisher = MqttClientFactory.CreateTcp();
      await using var dashboard = MqttClientFactory.CreateTcp();

      var connectOptions = new ConnectOptions
      {
         EndPoint = new IPEndPoint(IPAddress.Loopback, Port),
         ProtocolVersion = MqttProtocolVersion.V50
      };

      await publisher.ConnectAsync(connectOptions);
      await dashboard.ConnectAsync(connectOptions);
      Console.WriteLine("    Connected to broker!");

      // 3. Scenario A: Wildcard streaming with custom MessagePack decoder
      Console.WriteLine("\n[3] Scenario A: Streaming binary MessagePack with wildcard filter 'sensors/+/telemetry'...");

      using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));

      // Simulate periodic binary sensor telemetry publishing
      var publisherTask = Task.Run(async () =>
      {
         await Task.Delay(150, cts.Token);

         var readings = new[]
         {
            new SensorTelemetry("sensor-alpha", 22.4, 45.2, DateTime.UtcNow, false),
            new SensorTelemetry("sensor-beta", 24.1, 48.0, DateTime.UtcNow, false),
            new SensorTelemetry("sensor-alpha", 23.0, 44.9, DateTime.UtcNow, false),
            new SensorTelemetry("sensor-beta", 25.3, 50.1, DateTime.UtcNow, false),
            new SensorTelemetry("sensor-alpha", 31.8, 62.5, DateTime.UtcNow, true) // Alert condition
         };

         foreach (var reading in readings)
         {
            var binaryPayload = MessagePackSerializer.Serialize(reading);
            var topic = $"sensors/{reading.DeviceId}/telemetry";

            var pubOptions = PublishOptions.Create()
               .WithTopic(topic)
               .WithPayload(binaryPayload)
               .WithQualityOfService(QualityOfServiceType.AtLeastOnce)
               .Build();

            await publisher.PublishAsync(pubOptions, cts.Token);
            Console.WriteLine("    [Publisher] Published {0} bytes MessagePack to '{1}'", binaryPayload.Length, topic);
            await Task.Delay(100, cts.Token);
         }
      }, cts.Token);

      // Consume stream via custom MessagePack decoder
      var receivedCount = 0;
      await foreach (var telemetry in dashboard.SubscribeMessagePackStream<SensorTelemetry>("sensors/+/telemetry", ct: cts.Token))
      {
         receivedCount++;
         Console.WriteLine("    [Dashboard] Stream received: Device={0,-12} Temp={1,4:F1}°C Hum={2,4:F1}% Alert={3}",
            telemetry.DeviceId, telemetry.Temperature, telemetry.Humidity, telemetry.IsAlert);

         if (telemetry.IsAlert)
         {
            Console.WriteLine("    [Dashboard] High temperature alert received! Breaking from stream...");
            break; // Auto-unsubscribes
         }
      }

      await publisherTask;
      Console.WriteLine("    Received {0} MessagePack frames. Stream cleanly disposed.", receivedCount);

      // 4. Scenario B: Awaiting an alert condition with WaitForMessagePackMessageAsync
      Console.WriteLine("\n[4] Scenario B: Awaiting next alert frame using `WaitForMessagePackMessageAsync`...");

      var alertTask = Task.Run(async () =>
      {
         await Task.Delay(200, cts.Token);

         var normal = new SensorTelemetry("sensor-gamma", 20.0, 40.0, DateTime.UtcNow, false);
         await publisher.PublishAsync(PublishOptions.Create()
            .WithTopic("sensors/sensor-gamma/telemetry")
            .WithPayload(MessagePackSerializer.Serialize(normal))
            .Build(), cts.Token);

         await Task.Delay(100, cts.Token);

         var alert = new SensorTelemetry("sensor-gamma", 85.5, 90.0, DateTime.UtcNow, true);
         await publisher.PublishAsync(PublishOptions.Create()
            .WithTopic("sensors/sensor-gamma/telemetry")
            .WithPayload(MessagePackSerializer.Serialize(alert))
            .Build(), cts.Token);
      }, cts.Token);

      var alertResult = await dashboard.WaitForMessagePackMessageAsync<SensorTelemetry>(
         "sensors/+/telemetry",
         predicate: t => t.IsAlert,
         timeout: TimeSpan.FromSeconds(5),
         ct: cts.Token);

      await alertTask;

      Console.WriteLine("    [Dashboard] Alert intercepted: Device '{0}' temperature reached {1:F1}°C!",
         alertResult.DeviceId, alertResult.Temperature);

      // 5. Cleanup
      Console.WriteLine("\n[5] Shutting down...");
      await publisher.DisconnectAsync(new DisconnectOptions());
      await dashboard.DisconnectAsync(new DisconnectOptions());
      await mqttServer.StopAsync();
      await mqttServer.DisposeAsync();

      Console.WriteLine("==========================================================");
      Console.WriteLine(" MessagePack Custom Decoder Example Completed!           ");
      Console.WriteLine("==========================================================");
   }
}
