using System.Net;
using Beskar.Jobs.Client;
using Beskar.Jobs.Common.Enums;
using Beskar.Jobs.Scheduler;
using Beskar.Jobs.Worker;
using Beskar.Mqtt.Server;

Console.WriteLine("====================================================================");
Console.WriteLine("  Beskar.Jobs — Cron Schedules & DAG Continuation Workflows Example");
Console.WriteLine("====================================================================");

var endPoint = new IPEndPoint(IPAddress.Loopback, 18882);

var broker = MqttServerFactory.CreateBuilder()
   .WithDefaultClientIdGenerator()
   .UseTcp(endPoint.Port)
   .Build();
await broker.StartAsync();

await using var scheduler = JobSchedulerBuilder.Create()
   .WithTickInterval(TimeSpan.FromMilliseconds(25))
   .UseTcp(endPoint)
   .Build();

scheduler.Events.OnRecurringJobTriggered.Add((ctx, _) =>
{
   Console.WriteLine($"[Scheduler] Cron '{ctx.RecurringJob.RecurringJobId}' ({ctx.RecurringJob.CronExpression}) -> spawned job {ctx.SpawnedJob.JobId}");
   return ValueTask.CompletedTask;
});

await scheduler.StartAsync();

var pipelineFinished = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

await using var worker = JobWorkerBuilder.Create()
   .WithWorkerId("worker-etl-01")
   .SubscribeQueue("etl", "ops")
   .WithMaxConcurrency(4)
   .RegisterHandler<MetricSnapshotArgs>("ops.metrics.snapshot", (ctx, _) =>
   {
      Console.WriteLine($"[Worker] Executed recurring snapshot for '{ctx.Arguments.ServiceName}' (JobId: {ctx.JobId})");
      return ValueTask.CompletedTask;
   })
   .RegisterHandler<EtlStepArgs>("etl.step.extract", async (ctx, ct) =>
   {
      Console.WriteLine($"[Worker] Step 1 (Extract): Downloading dataset '{ctx.Arguments.Dataset}'...");
      await ctx.ReportProgressAsync(50, "Downloading parquet partitions...", ct);
      await Task.Delay(180, ct);
      Console.WriteLine($"[Worker] Step 1 (Extract): Completed '{ctx.Arguments.Dataset}'.");
   })
   .RegisterHandler<EtlStepArgs>("etl.step.transform", async (ctx, ct) =>
   {
      Console.WriteLine($"[Worker] Step 2 (Transform - Child of Extract): Normalizing '{ctx.Arguments.Dataset}'...");
      await Task.Delay(150, ct);
      Console.WriteLine($"[Worker] Step 2 (Transform): Completed '{ctx.Arguments.Dataset}'.");
   })
   .RegisterHandler<EtlStepArgs>("etl.step.publish", async (ctx, ct) =>
   {
      Console.WriteLine($"[Worker] Step 3 (Publish - Child of Transform): Publishing '{ctx.Arguments.Dataset}' to warehouse...");
      await Task.Delay(120, ct);
      Console.WriteLine($"[Worker] Step 3 (Publish): Pipeline finished for '{ctx.Arguments.Dataset}'!");
      pipelineFinished.TrySetResult();
   })
   .UseTcp(endPoint)
   .Build();

await worker.StartAsync();

await using var client = JobClientBuilder.Create()
   .UseTcp(endPoint)
   .Build();
await client.ConnectAsync();

// --- Part 1: Multi-Stage DAG Continuation Pipeline (Extract -> Transform -> Publish) ---
Console.WriteLine("\n--- 1. Submitting 3-Stage ETL Continuation Pipeline ---");
var datasetArgs = new EtlStepArgs("sales_2026_q4.parquet");

var extractJob = (await client.EnqueueAsync("etl.step.extract", datasetArgs, queue: "etl")).Success!;
var transformJob = (await client.EnqueueContinuationAsync(extractJob.JobId, "etl.step.transform", datasetArgs, queue: "etl")).Success!;
var publishJob = (await client.EnqueueContinuationAsync(transformJob.JobId, "etl.step.publish", datasetArgs, queue: "etl")).Success!;

Console.WriteLine($"[Client] Chained DAG: Extract ({extractJob.JobId}) -> Transform ({transformJob.JobId}) -> Publish ({publishJob.JobId})");

await pipelineFinished.Task.WaitAsync(TimeSpan.FromSeconds(10));

// --- Part 2: Recurring Interval / Cron Schedule ---
Console.WriteLine("\n--- 2. Registering Recurring Schedule (@every 350ms) ---");
var scheduleResp = (await client.ScheduleRecurringAsync(
   recurringJobId: "service-metrics-collector",
   cronExpression: "@every 350ms",
   jobType: "ops.metrics.snapshot",
   args: new MetricSnapshotArgs("payment-gateway"),
   queue: "ops",
   overlapPolicy: OverlapPolicy.SkipIfRunning)).Success!;

Console.WriteLine($"[Client] Recurring job accepted: {scheduleResp.Accepted} (NextRunAtUtc: {scheduleResp.NextRunAtUtc:HH:mm:ss.fff})");

await Task.Delay(1200);

Console.WriteLine("[Client] Removing recurring job 'service-metrics-collector' from storage...");
await scheduler.Storage.RemoveRecurringJobAsync("service-metrics-collector");

await broker.StopAsync();
Console.WriteLine("\nDone.");

public sealed record EtlStepArgs(string Dataset);
public sealed record MetricSnapshotArgs(string ServiceName);
