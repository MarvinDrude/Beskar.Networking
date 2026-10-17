using System.Net;
using Beskar.Jobs.Client;
using Beskar.Jobs.Common.Enums;
using Beskar.Jobs.Common.Models;
using Beskar.Jobs.Scheduler;
using Beskar.Jobs.Worker;
using Beskar.Mqtt.Server;
using Beskar.Networking.Abstractions.Backoffs;

Console.WriteLine("=======================================================================");
Console.WriteLine("  Beskar.Jobs — Retries, Cooperative Cancellation & LWT Failover Demo");
Console.WriteLine("=======================================================================");

var endPoint = new IPEndPoint(IPAddress.Loopback, 18883);

var broker = MqttServerFactory.CreateBuilder()
   .WithDefaultClientIdGenerator()
   .UseTcp(endPoint.Port)
   .Build();
await broker.StartAsync();

await using var scheduler = JobSchedulerBuilder.Create()
   .WithTickInterval(TimeSpan.FromMilliseconds(25))
   .WithRetryPolicy(new RetryPolicy
   {
      MaxAttempts = 3,
      Backoff = new ConstantBackoffPolicy(TimeSpan.FromMilliseconds(150))
   })
   .UseTcp(endPoint)
   .Build();

scheduler.Events.OnJobRetryScheduled.Add((ctx, _) =>
{
   Console.WriteLine($"[Scheduler] Scheduling retry for job {ctx.Job.JobId} after attempt #{ctx.FailedAttempt} failed ({ctx.Reason})");
   return ValueTask.CompletedTask;
});

scheduler.Events.OnJobCancelled.Add((ctx, _) =>
{
   Console.WriteLine($"[Scheduler] Confirmed job {ctx.Job.JobId} cancelled.");
   return ValueTask.CompletedTask;
});

await scheduler.StartAsync();

await using var worker = JobWorkerBuilder.Create()
   .WithWorkerId("worker-resilient-01")
   .SubscribeQueue("default")
   .WithMaxConcurrency(4)
   .RegisterHandlerWithResult<FlakyWebhookArgs, string>("webhook.deliver", async (ctx, ct) =>
   {
      Console.WriteLine($"[Worker] Executing '{ctx.JobType}' attempt {ctx.Attempt}/{ctx.MaxAttempts} for {ctx.Arguments.Url}...");
      await Task.Delay(60, ct);

      if (ctx.Attempt < 3)
      {
         throw new HttpRequestException($"Simulated HTTP 503 Service Unavailable on attempt #{ctx.Attempt}");
      }

      return $"HTTP 200 OK delivered on attempt #{ctx.Attempt}";
   })
   .RegisterHandler<LongSimulationArgs>("simulation.long", async (ctx, ct) =>
   {
      Console.WriteLine($"[Worker] Starting long simulation {ctx.JobId}...");
      for (var step = 1; step <= 20; step++)
      {
         ct.ThrowIfCancellationRequested();
         await ctx.ReportProgressAsync(step * 5, $"Running step {step}/20...", ct);
         await Task.Delay(100, ct);
      }
   })
   .UseTcp(endPoint)
   .Build();

await worker.StartAsync();

await using var client = JobClientBuilder.Create()
   .UseTcp(endPoint)
   .Build();
await client.ConnectAsync();

// --- Part 1: Automatic Retry with Backoff ---
Console.WriteLine("\n--- 1. Enqueuing Flaky Webhook Job (Fails 2x, Succeeds on Attempt 3) ---");
var retryJob = (await client.EnqueueAsync(
   "webhook.deliver",
   new FlakyWebhookArgs("https://api.partner.example/v1/events"),
   maxAttempts: 3)).Success!;

await foreach (var update in client.WatchJobStatusAsync(retryJob.JobId))
{
   Console.WriteLine($"[Client Stream] Status: {update.Status,-10} | Attempt: {update.Attempt} | {update.Message ?? update.ErrorMessage}");
   if (update is { Status: JobStatus.Succeeded, ResultPayload: not null })
   {
      var msg = client.Options.Serializer.Deserialize<string>(update.ResultPayload);
      Console.WriteLine($"[Client] Final Result: {msg}");
   }
}

// --- Part 2: Mid-Flight Cooperative Cancellation ---
Console.WriteLine("\n--- 2. Enqueuing Long-Running Job and Cancelling Mid-Flight ---");
var cancelJob = (await client.EnqueueAsync(
   "simulation.long",
   new LongSimulationArgs("monte-carlo-10k"))).Success!;

var cancelSent = false;
await foreach (var update in client.WatchJobStatusAsync(cancelJob.JobId))
{
   Console.WriteLine($"[Client Stream] Status: {update.Status,-10} | Progress: {update.ProgressPercent,3}% | {update.Message}");

   if (!cancelSent && update is { Status: JobStatus.Running, ProgressPercent: >= 15 })
   {
      cancelSent = true;
      Console.WriteLine($"[Client] Requesting cancellation for {cancelJob.JobId}...");
      await client.CancelJobAsync(cancelJob.JobId, "Aborted by user");
   }
}

await broker.StopAsync();
Console.WriteLine("\nDone.");

public sealed record FlakyWebhookArgs(string Url);
public sealed record LongSimulationArgs(string SimulationName);
