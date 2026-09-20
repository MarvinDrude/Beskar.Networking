using System.Net;
using System.Text.Json;
using Beskar.Mqtt.Client;
using Beskar.Mqtt.Common.Builders.Connecting;
using Beskar.Mqtt.Common.Builders.Disconnecting;
using Beskar.Mqtt.Common.Builders.Publishing;
using Beskar.Mqtt.Common.Extensions;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Server;

namespace Beskar.Mqtt.Example.JsonStreaming;

/// <summary>
/// Domain model for job status and progress updates transmitted as JSON over MQTT.
/// </summary>
public record JobProgressUpdate(
   string JobId,
   string Stage,
   int ProgressPercent,
   bool IsCompleted,
   string? Message);

public static class Program
{
   public static async Task Main()
   {
      Console.WriteLine("==========================================================");
      Console.WriteLine(" Beskar MQTT - JSON Async Streaming Example              ");
      Console.WriteLine("==========================================================");

      const int Port = 8011;

      // 1. Start an embedded MQTT server
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

      // 2. Create and connect Publisher (Worker) and Subscriber (Dashboard/Watcher)
      Console.WriteLine("\n[2] Connecting Worker and Dashboard clients...");
      await using var worker = MqttClientFactory.CreateTcp();
      await using var dashboard = MqttClientFactory.CreateTcp();

      var connectOptions = new ConnectOptions
      {
         EndPoint = new IPEndPoint(IPAddress.Loopback, Port),
         ProtocolVersion = MqttProtocolVersion.V50
      };

      await worker.ConnectAsync(connectOptions);
      await dashboard.ConnectAsync(connectOptions);
      Console.WriteLine("    Both clients connected!");

      // 3. Scenario A: Pull-based streaming using `await foreach`
      const string JobId = "JOB-4096";
      var topic = $"jobs/{JobId}/progress";

      Console.WriteLine("\n[3] Scenario A: Subscribing to stream using C# `await foreach`...");
      Console.WriteLine("    Topic: '{0}'", topic);

      using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));

      // Simulate asynchronous background job execution on the worker
      var workerTask = Task.Run(async () =>
      {
         await Task.Delay(200, cts.Token);

         var stages = new (string Stage, int Progress, string Msg)[]
         {
            ("Initializing environment", 10, "Allocating compute resources"),
            ("Downloading input datasets", 35, "Received 1.4 GB payload"),
            ("Executing model inference", 70, "Batch 3/4 completed"),
            ("Rendering artifacts", 90, "Writing output image"),
            ("Finished", 100, "Execution succeeded in 1.8s")
         };

         foreach (var (stage, progress, msg) in stages)
         {
            var isFinished = progress == 100;
            var payload = new JobProgressUpdate(JobId, stage, progress, isFinished, msg);
            var json = JsonSerializer.Serialize(payload);

            var pubOptions = PublishOptions.Create()
               .WithTopic(topic)
               .WithPayload(json)
               .WithQualityOfService(QualityOfServiceType.AtLeastOnce)
               .Build();

            await worker.PublishAsync(pubOptions, cts.Token);
            Console.WriteLine("    [Worker] Published: {0}% - {1}", progress, stage);
            await Task.Delay(150, cts.Token);
         }
      }, cts.Token);

      // Consume the typed JSON stream using modern C# async iteration
      await foreach (var update in dashboard.SubscribeJsonStream<JobProgressUpdate>(topic, ct: cts.Token))
      {
         Console.WriteLine("    [Dashboard] Stream received: [{0}%] Stage: {1} | Note: {2}",
            update.ProgressPercent, update.Stage, update.Message);

         if (update.IsCompleted)
         {
            Console.WriteLine("    [Dashboard] Job finished! Exiting await foreach loop...");
            break; // Exiting loop automatically unsubscribes and cleans up channel resources!
         }
      }

      await workerTask;
      Console.WriteLine("    Stream iteration cleanly terminated and auto-unsubscribed.");

      // 4. Scenario B: One-shot await for terminal condition
      const string BatchJobId = "JOB-8192";
      var batchTopic = $"jobs/{BatchJobId}/progress";

      Console.WriteLine("\n[4] Scenario B: Awaiting completion directly with `WaitForJsonMessageAsync`...");
      Console.WriteLine("    Waiting for '{0}' to report IsCompleted == true...", batchTopic);

      var batchWorkerTask = Task.Run(async () =>
      {
         await Task.Delay(200, cts.Token);

         // Intermediate progress updates
         for (var p = 25; p <= 75; p += 25)
         {
            var intermediate = new JobProgressUpdate(BatchJobId, "Processing chunk", p, false, null);
            await worker.PublishAsync(PublishOptions.Create()
               .WithTopic(batchTopic)
               .WithPayload(JsonSerializer.Serialize(intermediate))
               .Build(), cts.Token);
            await Task.Delay(100, cts.Token);
         }

         // Terminal update
         var done = new JobProgressUpdate(BatchJobId, "Complete", 100, true, "All tasks completed successfully");
         await worker.PublishAsync(PublishOptions.Create()
            .WithTopic(batchTopic)
            .WithPayload(JsonSerializer.Serialize(done))
            .Build(), cts.Token);
      }, cts.Token);

      var finalResult = await dashboard.WaitForJsonMessageAsync<JobProgressUpdate>(
         batchTopic,
         predicate: update => update.IsCompleted,
         timeout: TimeSpan.FromSeconds(5),
         ct: cts.Token);

      await batchWorkerTask;

      Console.WriteLine("    [Dashboard] Completed event received: Job: {0} | Final Stage: {1} | Message: {2}",
         finalResult.JobId, finalResult.Stage, finalResult.Message);

      // 5. Cleanup
      Console.WriteLine("\n[5] Shutting down clients and server...");
      await worker.DisconnectAsync(new DisconnectOptions());
      await dashboard.DisconnectAsync(new DisconnectOptions());
      await mqttServer.StopAsync();
      await mqttServer.DisposeAsync();

      Console.WriteLine("==========================================================");
      Console.WriteLine(" JSON Streaming Example Completed Successfully!           ");
      Console.WriteLine("==========================================================");
   }
}
