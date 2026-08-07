using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using System.Net;
using System.Text;
using Beskar.Mqtt.Client;
using Beskar.Mqtt.Common.Builders.Connecting;
using Beskar.Mqtt.Common.Builders.Disconnecting;
using Beskar.Mqtt.Common.Builders.Publishing;
using Beskar.Mqtt.Common.Builders.Subscribing;
using Beskar.Mqtt.Common.Telemetry;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Server;
using Beskar.Networking.Abstractions.Telemetry;
using Beskar.Networking.Resilient.Common.Telemetry;
using OpenTelemetry;
using OpenTelemetry.Metrics;

namespace Beskar.Example.Telemetry;

internal static class Program
{
   private static readonly Lock ConsoleLock = new();
   private static readonly ConcurrentDictionary<string, long> MetricCounters = new();
   private static readonly ConcurrentDictionary<string, long> MetricGauges = new();

   public static async Task Main(string[] args)
   {
      lock (ConsoleLock)
      {
         Console.Title = "Beskar.Networking Live Telemetry Dashboard";
         Console.ForegroundColor = ConsoleColor.Magenta;
         Console.WriteLine(@"
 ┌────────────────────────────────────────────────────────────────────────┐
 │        Beskar.Networking  ──  Live OpenTelemetry Dashboard             │
 └────────────────────────────────────────────────────────────────────────┘");
         Console.ResetColor();
      }

      // =========================================================================================
      // Note for OpenTelemetry SDK Export (Prometheus / OTLP / Grafana):
      //
      //   using var meterProvider = Sdk.CreateMeterProviderBuilder()
      //      .AddMeter(TransportMetrics.MeterName)
      //      .AddMeter(ResilientMetrics.MeterName)
      //      .AddMeter(MqttMetrics.MeterName)
      //      .AddOtlpExporter() // Export directly to Grafana Tempo / OpenTelemetry Collector
      //      .Build();
      // =========================================================================================

      // =========================================================================================
      // High-Performance Custom Live Console Telemetry Viewer
      // =========================================================================================
      using var meterListener = new MeterListener();

      meterListener.InstrumentPublished = (instrument, listener) =>
      {
         if (instrument.Meter.Name is TransportMetrics.MeterName or ResilientMetrics.MeterName or MqttMetrics.MeterName)
         {
            listener.EnableMeasurementEvents(instrument);
         }
      };

      meterListener.SetMeasurementEventCallback<long>((instrument, measurement, tags, state) =>
      {
         var metricKey = $"{instrument.Name}";
         
         if (instrument is UpDownCounter<long>)
         {
            MetricGauges.AddOrUpdate(metricKey, measurement, (_, prev) => prev + measurement);
         }
         else
         {
            MetricCounters.AddOrUpdate(metricKey, measurement, (_, prev) => prev + measurement);
         }

         var tagStr = tags.Length > 0 ? $" [{string.Join(", ", tags.ToArray().Select(t => $"{t.Key}={t.Value}"))}]" : string.Empty;
         
         var categoryBadge = instrument.Meter.Name switch
         {
            TransportMetrics.MeterName => ("⚡ TRANS", ConsoleColor.Yellow),
            MqttMetrics.MeterName => ("📡 MQTT ", ConsoleColor.Cyan),
            ResilientMetrics.MeterName => ("🔄 RESI ", ConsoleColor.Magenta),
            _ => ("📊 METR ", ConsoleColor.White)
         };

         lock (ConsoleLock)
         {
            Console.ForegroundColor = ConsoleColor.DarkGray;
            Console.Write($"[{DateTime.Now:HH:mm:ss.fff}] ");
            Console.ForegroundColor = categoryBadge.Item2;
            Console.Write($"{categoryBadge.Item1} ");
            Console.ForegroundColor = ConsoleColor.White;
            Console.Write($"{instrument.Name,-38}");
            Console.ForegroundColor = measurement >= 0 ? ConsoleColor.Green : ConsoleColor.Red;
            Console.Write($" {(measurement >= 0 ? "+" : "")}{measurement,-6}");
            Console.ForegroundColor = ConsoleColor.DarkGray;
            Console.WriteLine($"{tagStr}");
            Console.ResetColor();
         }
      });

      meterListener.SetMeasurementEventCallback<double>((instrument, measurement, tags, state) =>
      {
         lock (ConsoleLock)
         {
            Console.ForegroundColor = ConsoleColor.DarkGray;
            Console.Write($"[{DateTime.Now:HH:mm:ss.fff}] ");
            Console.ForegroundColor = ConsoleColor.Magenta;
            Console.Write($"⏱ HISTO ");
            Console.ForegroundColor = ConsoleColor.White;
            Console.Write($"{instrument.Name,-38}");
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine($" {measurement:F2} ms");
            Console.ResetColor();
         }
      });

      meterListener.Start();

      Console.ForegroundColor = ConsoleColor.Green;
      Console.WriteLine("✔ Telemetry MeterListener started! Subscribed meters:");
      Console.WriteLine($"   • Transport : {TransportMetrics.MeterName}");
      Console.WriteLine($"   • Resilient : {ResilientMetrics.MeterName}");
      Console.WriteLine($"   • MQTT      : {MqttMetrics.MeterName}\n");
      Console.ResetColor();

      // =========================================================================================
      // Step 2: Spin up MQTT Broker Server
      // =========================================================================================
      Console.WriteLine("--> Starting MQTT Broker Server on TCP 127.0.0.1:0...");
      var server = MqttServerFactory.CreateBuilder()
         .UseTcp(new IPEndPoint(IPAddress.Loopback, 0))
         .WithDefaultClientIdGenerator()
         .Build();

      await server.StartAsync();
      var boundEndPoint = (IPEndPoint)server.Listeners[0].LocalAddress;
      Console.ForegroundColor = ConsoleColor.Green;
      Console.WriteLine($"✔ MQTT Server bound and listening on {boundEndPoint}\n");
      Console.ResetColor();

      // =========================================================================================
      // Step 3: Connect MQTT Client & Perform Subscriptions / Publishes in Loop
      // =========================================================================================
      Console.WriteLine("--> Connecting MQTT Client over TCP transport...");
      var client = MqttClientFactory.CreateTcp();

      var connectOptions = new ConnectOptionsBuilder(boundEndPoint)
         .WithProtocolVersion(MqttProtocolVersion.V50)
         .WithClientId("telemetry-example-client")
         .WithCleanSession(true)
         .Build();

      var connectResult = await client.ConnectAsync(connectOptions);
      if (connectResult.Failed)
      {
         Console.WriteLine($"Failed to connect: {connectResult.Error}");
         return;
      }

      Console.WriteLine("--> Subscribing to 'sensors/temperature/#'...");
      var subOptions = new SubscribeOptionsBuilder()
         .WithTopicFilter("sensors/temperature/#", QualityOfServiceType.AtLeastOnce)
         .Build();
      await client.SubscribeAsync(subOptions);

      Console.WriteLine("\n--> Publishing telemetry payloads in a loop to watch live metric growth...\n");

      for (var i = 1; i <= 5; i++)
      {
         await Task.Delay(300);

         var pubOptions = new PublishOptionsBuilder()
            .WithTopic($"sensors/temperature/room-{i}")
            .WithPayload(Encoding.UTF8.GetBytes($"{{\"room\": {i}, \"temp\": {21.5 + i}}}"))
            .WithQualityOfService(QualityOfServiceType.AtLeastOnce)
            .WithRetain(i == 1) // First message is retained
            .Build();

         await client.PublishAsync(pubOptions);
      }

      await Task.Delay(500);

      // =========================================================================================
      // Step 4: Display Final Live Metrics Summary Table
      // =========================================================================================
      lock (ConsoleLock)
      {
         Console.ForegroundColor = ConsoleColor.Yellow;
         Console.WriteLine(@"
 ┌────────────────────────────────────────────────────────────────────────┐
 │                    LIVE METRICS SUMMARY DASHBOARD                      │
 └────────────────────────────────────────────────────────────────────────┘");
         Console.ResetColor();

         Console.ForegroundColor = ConsoleColor.White;
         Console.WriteLine(" 📊 LIVE GAUGES (Current Active State):");
         Console.WriteLine(" ──────────────────────────────────────────────────────────────────");
         foreach (var kvp in MetricGauges.OrderBy(k => k.Key))
         {
            Console.ForegroundColor = ConsoleColor.Cyan;
            Console.Write($"   • {kvp.Key,-45} : ");
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine($"{kvp.Value}");
         }

         Console.ForegroundColor = ConsoleColor.White;
         Console.WriteLine("\n 📈 CUMULATIVE COUNTERS (Total Volume):");
         Console.WriteLine(" ──────────────────────────────────────────────────────────────────");
         foreach (var kvp in MetricCounters.OrderBy(k => k.Key))
         {
            Console.ForegroundColor = ConsoleColor.Cyan;
            Console.Write($"   • {kvp.Key,-45} : ");
            Console.ForegroundColor = ConsoleColor.Yellow;
            Console.WriteLine($"{kvp.Value}");
         }
         Console.ResetColor();
         Console.WriteLine(" ──────────────────────────────────────────────────────────────────\n");
      }

      // Clean disconnect & shutdown
      await client.DisconnectAsync(new DisconnectOptions { ReasonCode = DisconnectReasonCode.NormalDisconnection });
      await client.DisposeAsync();
      await server.StopAsync();
      await server.DisposeAsync();

      Console.ForegroundColor = ConsoleColor.Green;
      Console.WriteLine("\n✔ Telemetry Example Completed Successfully!\n");
      Console.ResetColor();
   }
}
