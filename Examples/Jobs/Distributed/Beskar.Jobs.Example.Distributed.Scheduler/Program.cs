using Beskar.Jobs.Example.Distributed.Common;
using Beskar.Jobs.Scheduler;

Console.WriteLine("=====================================================================");
Console.WriteLine($"  Beskar.Jobs Distributed Cluster — Scheduler Node (-> {DistributedClusterConfig.BrokerEndPoint})");
Console.WriteLine("=====================================================================");

await using var scheduler = JobSchedulerBuilder.Create()
   .WithSchedulerId($"scheduler-{Environment.ProcessId}")
   .WithTickInterval(TimeSpan.FromMilliseconds(50))
   .UseTcp(DistributedClusterConfig.BrokerEndPoint)
   .Build();

scheduler.Events.OnJobEnqueued.Add((ctx, _) =>
{
   Console.WriteLine($"[Scheduler {DateTime.Now:HH:mm:ss}] Enqueued   | JobId: {ctx.Job.JobId} | Type: {ctx.Job.JobType} | Queue: {ctx.Job.Queue} | Status: {ctx.Job.Status}");
   return ValueTask.CompletedTask;
});

scheduler.Events.OnJobDispatched.Add((ctx, _) =>
{
   Console.WriteLine($"[Scheduler {DateTime.Now:HH:mm:ss}] Dispatched | JobId: {ctx.Job.JobId} -> Worker: {ctx.WorkerId} (Attempt #{ctx.Job.Attempt})");
   return ValueTask.CompletedTask;
});

scheduler.Events.OnJobSucceeded.Add((ctx, _) =>
{
   Console.WriteLine($"[Scheduler {DateTime.Now:HH:mm:ss}] Succeeded  | JobId: {ctx.Job.JobId} | Worker: {ctx.Job.AssignedWorkerId}");
   return ValueTask.CompletedTask;
});

scheduler.Events.OnJobRetryScheduled.Add((ctx, _) =>
{
   Console.WriteLine($"[Scheduler {DateTime.Now:HH:mm:ss}] Retry      | JobId: {ctx.Job.JobId} | FailedAttempt: #{ctx.FailedAttempt} | Delay: {ctx.RetryDelay.TotalMilliseconds:F0}ms | Reason: {ctx.Reason}");
   return ValueTask.CompletedTask;
});

scheduler.Events.OnJobDeadLettered.Add((ctx, _) =>
{
   Console.WriteLine($"[Scheduler {DateTime.Now:HH:mm:ss}] FAILED     | JobId: {ctx.Job.JobId} | Error: {ctx.ErrorMessage}");
   return ValueTask.CompletedTask;
});

scheduler.Events.OnJobCancelled.Add((ctx, _) =>
{
   Console.WriteLine($"[Scheduler {DateTime.Now:HH:mm:ss}] Cancelled  | JobId: {ctx.Job.JobId}");
   return ValueTask.CompletedTask;
});

scheduler.Events.OnRecurringJobTriggered.Add((ctx, _) =>
{
   Console.WriteLine($"[Scheduler {DateTime.Now:HH:mm:ss}] Cron Fired | Schedule: '{ctx.RecurringJob.RecurringJobId}' -> Spawned JobId: {ctx.SpawnedJob.JobId}");
   return ValueTask.CompletedTask;
});

scheduler.Events.OnWorkerCrashed.Add((ctx, _) =>
{
   Console.WriteLine($"[Scheduler {DateTime.Now:HH:mm:ss}] LWT CRASH  | Worker '{ctx.WorkerId}' went offline! Recovering {ctx.OrphanedJobs.Count} in-flight job(s)...");
   return ValueTask.CompletedTask;
});

await scheduler.StartAsync();
Console.WriteLine("[Scheduler] Connected to Broker and orchestrating queues. Press Ctrl+C to stop.");

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

Console.WriteLine("[Scheduler] Stopping...");
await scheduler.StopAsync();
