using System.Net;
using Beskar.Jobs.Client;
using Beskar.Jobs.Common.Attributes;
using Beskar.Jobs.Common.Enums;
using Beskar.Jobs.Common.Interfaces;
using Beskar.Jobs.Scheduler;
using Beskar.Jobs.Worker;
using Beskar.Mqtt.Server;

Console.WriteLine("==========================================================");
Console.WriteLine("  Beskar.Jobs — QuickStart Example (In-Process Cluster)");
Console.WriteLine("==========================================================");

var endPoint = new IPEndPoint(IPAddress.Loopback, 18881);

// 1. Start the MQTT v5.0 Broker
var broker = MqttServerFactory.CreateBuilder()
   .WithDefaultClientIdGenerator()
   .UseTcp(endPoint.Port)
   .Build();
await broker.StartAsync();

// 2. Start the Job Scheduler
await using var scheduler = JobSchedulerBuilder.Create()
   .WithTickInterval(TimeSpan.FromMilliseconds(25))
   .UseTcp(endPoint)
   .Build();
await scheduler.StartAsync();

// 3. Start a Job Worker with a source-generated handler
await using var worker = JobWorkerBuilder.Create()
   .WithWorkerId("worker-quickstart-01")
   .WithMaxConcurrency(4)
   .RegisterGeneratedHandlerWithResult<ImageResizeJobHandler, ImageResizeArgs, ImageResizeResult>()
   .UseTcp(endPoint)
   .Build();
await worker.StartAsync();

// 4. Connect a Job Client, enqueue a job, and stream real-time progress
await using var client = JobClientBuilder.Create()
   .UseTcp(endPoint)
   .Build();
await client.ConnectAsync();

var jobArgs = new ImageResizeArgs("hero-banner.png", 1920, 1080);
Console.WriteLine($"\n[Client] Enqueuing '{ImageResizeJobHandler.JobType}' for {jobArgs.FileName}...");

var enqueueResult = await client.EnqueueGeneratedWithResultAsync<ImageResizeJobHandler, ImageResizeArgs, ImageResizeResult>(jobArgs);
var response = enqueueResult.Success!;

Console.WriteLine($"[Client] Accepted JobId: {response.JobId} (Initial Status: {response.InitialStatus})");

await foreach (var update in client.WatchJobStatusAsync(response.JobId))
{
   Console.WriteLine($"[Status Stream] {update.Status,-10} | {update.ProgressPercent,3}% | {update.Message}");

   if (update is { Status: JobStatus.Succeeded, ResultPayload: not null })
   {
      var result = client.Options.Serializer.Deserialize<ImageResizeResult>(update.ResultPayload);
      Console.WriteLine($"\n[Client] Completed! Output: {result?.OutputPath} ({result?.OutputBytes:N0} bytes)");
   }
}

await broker.StopAsync();
Console.WriteLine("\nDone.");

public sealed record ImageResizeArgs(string FileName, int Width, int Height);
public sealed record ImageResizeResult(string OutputPath, long OutputBytes);

[GeneratedJobHandler("media.image.resize", Queue = "media")]
public sealed partial class ImageResizeJobHandler : IJobHandlerWithResult<ImageResizeArgs, ImageResizeResult>
{
   public async ValueTask<ImageResizeResult> ExecuteAsync(
      IJobExecutionContext<ImageResizeArgs> context,
      CancellationToken ct = default)
   {
      await context.ReportProgressAsync(15, $"Decoding {context.Arguments.FileName}...", ct);
      await Task.Delay(100, ct);

      await context.ReportProgressAsync(60, $"Resizing to {context.Arguments.Width}x{context.Arguments.Height}...", ct);
      await Task.Delay(120, ct);

      await context.ReportProgressAsync(95, "Encoding WebP output...", ct);
      await Task.Delay(80, ct);

      var outputPath = $"/cdn/resized/{context.Arguments.Width}x{context.Arguments.Height}_{context.Arguments.FileName}.webp";
      return new ImageResizeResult(outputPath, 148_920);
   }
}
