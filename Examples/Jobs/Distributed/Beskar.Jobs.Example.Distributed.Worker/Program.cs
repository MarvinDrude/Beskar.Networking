using Beskar.Jobs.Example.Distributed.Common;
using Beskar.Jobs.Example.Distributed.Worker;
using Beskar.Jobs.Worker;

var workerId = args.Length > 0 && !string.IsNullOrWhiteSpace(args[0])
   ? args[0]
   : $"worker-{Environment.ProcessId}";

Console.WriteLine("=====================================================================");
Console.WriteLine($"  Beskar.Jobs Distributed Cluster — Worker Node '{workerId}'");
Console.WriteLine($"  Broker: {DistributedClusterConfig.BrokerEndPoint}");
Console.WriteLine("=====================================================================");

await using var worker = JobWorkerBuilder.Create()
   .WithWorkerId(workerId)
   .WithMaxConcurrency(4)
   .WithHeartbeatInterval(TimeSpan.FromSeconds(3))
   .RegisterGeneratedHandlerWithResult<VideoTranscodeJobHandler, VideoTranscodeArgs, VideoTranscodeResult>()
   .RegisterGeneratedHandlerWithResult<ThumbnailGenerateJobHandler, ThumbnailGenerateArgs, ThumbnailGenerateResult>()
   .RegisterGeneratedHandler<ClusterHealthAuditJobHandler, ClusterHealthAuditArgs>()
   .UseTcp(DistributedClusterConfig.BrokerEndPoint)
   .Build();

worker.Events.OnJobStarted.Add((ctx, _) =>
{
   Console.WriteLine($"[Worker {DateTime.Now:HH:mm:ss}] Started   | JobId: {ctx.JobId} | Type: {ctx.JobType} | Queue: {ctx.Queue} (Attempt #{ctx.Attempt})");
   return ValueTask.CompletedTask;
});

worker.Events.OnJobCompleted.Add((ctx, _) =>
{
   Console.WriteLine($"[Worker {DateTime.Now:HH:mm:ss}] Completed | JobId: {ctx.JobId} | Type: {ctx.JobType} in {ctx.DurationMs:F1}ms");
   return ValueTask.CompletedTask;
});

worker.Events.OnJobFailed.Add((ctx, _) =>
{
   Console.WriteLine($"[Worker {DateTime.Now:HH:mm:ss}] Failed    | JobId: {ctx.JobId} | Error: {ctx.ErrorMessage}");
   return ValueTask.CompletedTask;
});

worker.Events.OnJobCancelled.Add((ctx, _) =>
{
   Console.WriteLine($"[Worker {DateTime.Now:HH:mm:ss}] Cancelled | JobId: {ctx.JobId}");
   return ValueTask.CompletedTask;
});

await worker.StartAsync();
Console.WriteLine($"[Worker '{workerId}'] Online and subscribed to queues ['{DistributedClusterConfig.MediaQueue}', '{DistributedClusterConfig.OpsQueue}'].");
Console.WriteLine("Tip: Launch additional instances of this Worker project in other terminals to distribute load!");
Console.WriteLine("Press Ctrl+C to stop gracefully (or kill the terminal to trigger MQTT LWT crash recovery).");

using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) =>
{
   e.Cancel = true;
   cts.Cancel();
};

try
{
   await Task.Delay(Timeout.Infinite, cts.Token);
}
catch (OperationCanceledException)
{
   // Shutting down
}

Console.WriteLine($"[Worker '{workerId}'] Disconnecting gracefully...");
await worker.StopAsync();
