using Beskar.Jobs.Client;
using Beskar.Jobs.Common.Enums;
using Beskar.Jobs.Example.Distributed.Common;

Console.WriteLine("=====================================================================");
Console.WriteLine($"  Beskar.Jobs Distributed Cluster — Client Producer (-> {DistributedClusterConfig.BrokerEndPoint})");
Console.WriteLine("=====================================================================");

await using var client = JobClientBuilder.Create()
   .WithClientId($"job-client-{Environment.ProcessId}")
   .UseTcp(DistributedClusterConfig.BrokerEndPoint)
   .Build();

var connectResult = await client.ConnectAsync();
if (connectResult.Failed)
{
   Console.WriteLine($"[Client] Failed to connect to Broker at {DistributedClusterConfig.BrokerEndPoint}: {connectResult.Error.Detail}");
   return;
}

Console.WriteLine("[Client] Connected to MQTT Broker.");

// 1. Register a recurring cluster health audit job (@every 10s)
var cronResult = await client.ScheduleRecurringAsync(
   recurringJobId: "cluster-health-audit-10s",
   cronExpression: "@every 10s",
   jobType: DistributedClusterConfig.ClusterHealthAuditJobType,
   args: new ClusterHealthAuditArgs("eu-central-1", IncludeStorageCheck: true),
   queue: DistributedClusterConfig.OpsQueue,
   overlapPolicy: OverlapPolicy.SkipIfRunning);

if (!cronResult.Failed)
{
   Console.WriteLine($"[Client] Registered recurring job 'cluster-health-audit-10s' (NextRunAtUtc: {cronResult.Success!.NextRunAtUtc:HH:mm:ss})");
}

// 2. Run an initial Video Transcode + Child Thumbnail Continuation workflow
await RunTranscodeAndThumbnailWorkflowAsync(client, "trailer_4k_master.mov");

if (Console.IsInputRedirected)
{
   return;
}

while (true)
{
   Console.WriteLine();
   Console.WriteLine("Choose an action:");
   Console.WriteLine("  [1] Enqueue Video Transcode + Thumbnail Continuation Workflow");
   Console.WriteLine("  [2] Enqueue Parallel Burst (5 Video Transcode Jobs across Workers)");
   Console.WriteLine("  [3] Enqueue Job & Cancel Mid-Flight");
   Console.WriteLine("  [Q] Quit");
   Console.Write("> ");

   var key = Console.ReadLine()?.Trim().ToUpperInvariant();
   if (key is "Q" or null)
   {
      break;
   }

   switch (key)
   {
      case "1":
         await RunTranscodeAndThumbnailWorkflowAsync(client, $"episode_{Random.Shared.Next(100, 999)}.mp4");
         break;
      case "2":
         await RunParallelBurstAsync(client, 5);
         break;
      case "3":
         await RunCancelDemoAsync(client);
         break;
   }
}

static async Task RunTranscodeAndThumbnailWorkflowAsync(JobClient client, string assetName)
{
   Console.WriteLine($"\n[Client] Submitting parent VideoTranscode + child Thumbnail workflow for '{assetName}'...");

   var parentResp = (await client.EnqueueAsync(
      DistributedClusterConfig.VideoTranscodeJobType,
      new VideoTranscodeArgs(assetName, "av1", 1080),
      queue: DistributedClusterConfig.MediaQueue)).Success!;

   var childResp = (await client.EnqueueContinuationAsync(
      parentResp.JobId,
      DistributedClusterConfig.ThumbnailGenerateJobType,
      new ThumbnailGenerateArgs(assetName, FrameCount: 4),
      queue: DistributedClusterConfig.MediaQueue)).Success!;

   Console.WriteLine($"[Client] Parent JobId: {parentResp.JobId} -> Child Continuation JobId: {childResp.JobId}");

   await foreach (var update in client.WatchJobStatusAsync(parentResp.JobId))
   {
      Console.WriteLine($"  [Parent {parentResp.JobId}] {update.Status,-10} | {update.ProgressPercent,3}% | {update.Message}");
      if (update is { Status: JobStatus.Succeeded, ResultPayload: not null })
      {
         var res = client.Options.Serializer.Deserialize<VideoTranscodeResult>(update.ResultPayload);
         Console.WriteLine($"  [Parent Result] URI: {res?.OutputUri} (Worker: {res?.ProcessedByWorkerId}, {res?.DurationSeconds}s)");
      }
   }

   await foreach (var update in client.WatchJobStatusAsync(childResp.JobId))
   {
      Console.WriteLine($"  [Child  {childResp.JobId}] {update.Status,-10} | {update.ProgressPercent,3}% | {update.Message}");
      if (update is { Status: JobStatus.Succeeded, ResultPayload: not null })
      {
         var res = client.Options.Serializer.Deserialize<ThumbnailGenerateResult>(update.ResultPayload);
         Console.WriteLine($"  [Child Result]  Generated {res?.SpriteUrls.Length} thumbnails on Worker '{res?.ProcessedByWorkerId}'");
      }
   }
}

static async Task RunParallelBurstAsync(JobClient client, int count)
{
   Console.WriteLine($"\n[Client] Enqueuing {count} parallel transcode jobs...");
   var watchTasks = new List<Task>(count);

   for (var i = 1; i <= count; i++)
   {
      var asset = $"burst_clip_{i:D2}.mp4";
      var resp = (await client.EnqueueAsync(
         DistributedClusterConfig.VideoTranscodeJobType,
         new VideoTranscodeArgs(asset, "h265", 720),
         queue: DistributedClusterConfig.MediaQueue)).Success!;

      watchTasks.Add(Task.Run(async () =>
      {
         await foreach (var update in client.WatchJobStatusAsync(resp.JobId))
         {
            if (update is { Status: JobStatus.Succeeded, ResultPayload: not null })
            {
               var res = client.Options.Serializer.Deserialize<VideoTranscodeResult>(update.ResultPayload);
               Console.WriteLine($"  [Burst Done] {asset} -> {res?.OutputUri} (Worker: {res?.ProcessedByWorkerId})");
            }
         }
      }));
   }

   await Task.WhenAll(watchTasks);
}

static async Task RunCancelDemoAsync(JobClient client)
{
   var resp = (await client.EnqueueAsync(
      DistributedClusterConfig.ThumbnailGenerateJobType,
      new ThumbnailGenerateArgs("cancel_demo_movie.mkv", FrameCount: 10),
      queue: DistributedClusterConfig.MediaQueue)).Success!;

   Console.WriteLine($"\n[Client] Enqueued {resp.JobId}; will cancel once progress >= 20%...");

   var cancelRequested = false;
   await foreach (var update in client.WatchJobStatusAsync(resp.JobId))
   {
      Console.WriteLine($"  [{resp.JobId}] {update.Status,-10} | {update.ProgressPercent,3}% | {update.Message}");
      if (!cancelRequested && update is { Status: JobStatus.Running, ProgressPercent: >= 20 })
      {
         cancelRequested = true;
         await client.CancelJobAsync(resp.JobId, "Cancelled interactively by Distributed.Client");
      }
   }
}
