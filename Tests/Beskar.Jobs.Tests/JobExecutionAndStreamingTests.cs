using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using Beskar.Jobs.Client;
using Beskar.Jobs.Common.Attributes;
using Beskar.Jobs.Common.Enums;
using Beskar.Jobs.Common.Interfaces;
using Beskar.Jobs.Common.Models;
using Beskar.Jobs.Scheduler;
using Beskar.Jobs.Worker;
using Beskar.Mqtt.Server;

namespace Beskar.Jobs.Tests;

public sealed record MultiplyJobArgs(int A, int B);
public sealed record MultiplyJobResult(int Product, string WorkerId);

[GeneratedJobHandler("math.generated.sum", Queue = "math")]
public sealed partial class GeneratedSumJobHandler : IJobHandler<MultiplyJobArgs>
{
   public static int LastComputedSum;

   public ValueTask ExecuteAsync(IJobExecutionContext<MultiplyJobArgs> context, CancellationToken ct = default)
   {
      Interlocked.Exchange(ref LastComputedSum, context.Arguments.A + context.Arguments.B);
      return ValueTask.CompletedTask;
   }
}

[NotInParallel]
public class JobExecutionAndStreamingTests
{
   private static int GetFreeTcpPort()
   {
      using var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
      socket.Bind(new IPEndPoint(IPAddress.Loopback, 0));
      return ((IPEndPoint)socket.LocalEndPoint!).Port;
   }

   [Test]
   public async Task EnqueueAndStreamProgress_DeliversProgressAndTypedResult()
   {
      var port = GetFreeTcpPort();
      var endPoint = new IPEndPoint(IPAddress.Loopback, port);

      var broker = MqttServerFactory.CreateBuilder()
         .WithDefaultClientIdGenerator()
         .UseTcp(port)
         .Build();

      await broker.StartAsync();

      await using var scheduler = JobSchedulerBuilder.Create()
         .WithTickInterval(TimeSpan.FromMilliseconds(20))
         .UseTcp(endPoint)
         .Build();

      await using var worker = JobWorkerBuilder.Create()
         .WithWorkerId("worker-multiply-1")
         .SubscribeQueue("compute")
         .RegisterHandlerWithResult<MultiplyJobArgs, MultiplyJobResult>(
            "math.multiply",
            async (ctx, ct) =>
            {
               await ctx.ReportProgressAsync(25, "Validated inputs", ct);
               await Task.Delay(30, ct);
               await ctx.ReportProgressAsync(75, "Multiplying values", ct);
               await Task.Delay(30, ct);
               return new MultiplyJobResult(ctx.Arguments.A * ctx.Arguments.B, ctx.WorkerId);
            })
         .UseTcp(endPoint)
         .Build();

      await using var client = JobClientBuilder.Create()
         .UseTcp(endPoint)
         .Build();

      await scheduler.StartAsync();
      await worker.StartAsync();
      await client.ConnectAsync();

      var enqueueResult = await client.EnqueueAsync(
         "math.multiply",
         new MultiplyJobArgs(6, 7),
         queue: "compute");

      await Assert.That(enqueueResult.Failed).IsFalse();
      var jobId = enqueueResult.Success!.JobId;

      var observedUpdates = new List<JobStatusUpdate>();
      await foreach (var update in client.WatchJobStatusAsync(jobId))
      {
         observedUpdates.Add(update);
      }

      await Assert.That(observedUpdates.Count).IsGreaterThanOrEqualTo(2);
      var terminal = observedUpdates[^1];
      await Assert.That(terminal.Status).IsEqualTo(JobStatus.Succeeded);
      await Assert.That(terminal.ProgressPercent).IsEqualTo(100);
      await Assert.That(observedUpdates.Any(u => u.ProgressPercent is 25 or 75)).IsTrue();

      var typedResult = await client.WaitForJobResultAsync<MultiplyJobResult>(jobId, TimeSpan.FromSeconds(5));
      await Assert.That(typedResult.Failed).IsFalse();
      await Assert.That(typedResult.Success).IsNotNull();
      await Assert.That(typedResult.Success!.Product).IsEqualTo(42);
      await Assert.That(typedResult.Success.WorkerId).IsEqualTo("worker-multiply-1");

      await broker.StopAsync();
   }

   [Test]
   public async Task SourceGeneratedJobHandler_RegistersAndExecutesViaGeneratedMetadata()
   {
      var port = GetFreeTcpPort();
      var endPoint = new IPEndPoint(IPAddress.Loopback, port);

      var broker = MqttServerFactory.CreateBuilder()
         .WithDefaultClientIdGenerator()
         .UseTcp(port)
         .Build();

      await broker.StartAsync();

      await using var scheduler = JobSchedulerBuilder.Create()
         .WithTickInterval(TimeSpan.FromMilliseconds(20))
         .UseTcp(endPoint)
         .Build();

      await using var worker = JobWorkerBuilder.Create()
         .WithWorkerId("worker-gen-1")
         .RegisterGeneratedHandler<GeneratedSumJobHandler, MultiplyJobArgs>()
         .UseTcp(endPoint)
         .Build();

      await using var client = JobClientBuilder.Create()
         .UseTcp(endPoint)
         .Build();

      await scheduler.StartAsync();
      await worker.StartAsync();
      await client.ConnectAsync();

      Interlocked.Exchange(ref GeneratedSumJobHandler.LastComputedSum, 0);

      var enqueueResult = await client.EnqueueGeneratedAsync<GeneratedSumJobHandler, MultiplyJobArgs>(
         new MultiplyJobArgs(19, 23));

      await Assert.That(enqueueResult.Failed).IsFalse();

      var completion = await client.WaitForJobCompletionAsync(
         enqueueResult.Success!.JobId,
         TimeSpan.FromSeconds(5));

      await Assert.That(completion.Failed).IsFalse();
      await Assert.That(completion.Success!.Status).IsEqualTo(JobStatus.Succeeded);
      await Assert.That(Volatile.Read(ref GeneratedSumJobHandler.LastComputedSum)).IsEqualTo(42);

      await broker.StopAsync();
   }

   [Test]
   public async Task MultiQueueAndMetadata_RoutesToMatchingQueueAndPreservesMetadata()
   {
      var port = GetFreeTcpPort();
      var endPoint = new IPEndPoint(IPAddress.Loopback, port);

      var broker = MqttServerFactory.CreateBuilder()
         .WithDefaultClientIdGenerator()
         .UseTcp(port)
         .Build();
      await broker.StartAsync();

      await using var scheduler = JobSchedulerBuilder.Create()
         .WithTickInterval(TimeSpan.FromMilliseconds(20))
         .UseTcp(endPoint)
         .Build();

      var capturedTraceId = string.Empty;

      await using var gpuWorker = JobWorkerBuilder.Create()
         .WithWorkerId("worker-gpu")
         .SubscribeQueue("gpu-queue")
         .RegisterHandlerWithResult<string, string>("ai.infer", (ctx, _) =>
         {
            ctx.Metadata.TryGetValue("trace-id", out var traceId);
            capturedTraceId = traceId ?? string.Empty;
            return ValueTask.FromResult($"{ctx.WorkerId}:{ctx.Queue}:{ctx.Arguments}");
         })
         .UseTcp(endPoint)
         .Build();

      await using var cpuWorker = JobWorkerBuilder.Create()
         .WithWorkerId("worker-cpu")
         .SubscribeQueue("cpu-queue")
         .RegisterHandlerWithResult<string, string>("ai.infer", (ctx, _) =>
            ValueTask.FromResult($"{ctx.WorkerId}:{ctx.Queue}:{ctx.Arguments}"))
         .UseTcp(endPoint)
         .Build();

      await using var client = JobClientBuilder.Create()
         .UseTcp(endPoint)
         .Build();

      await scheduler.StartAsync();
      await gpuWorker.StartAsync();
      await cpuWorker.StartAsync();
      await client.ConnectAsync();

      var metadata = new Dictionary<string, string> { ["trace-id"] = "span-98765" };
      var gpuEnq = await client.EnqueueAsync("ai.infer", "model-v1", queue: "gpu-queue", metadata: metadata);
      var cpuEnq = await client.EnqueueAsync("ai.infer", "model-v2", queue: "cpu-queue");

      var gpuRes = await client.WaitForJobResultAsync<string>(gpuEnq.Success!.JobId, TimeSpan.FromSeconds(5));
      var cpuRes = await client.WaitForJobResultAsync<string>(cpuEnq.Success!.JobId, TimeSpan.FromSeconds(5));

      await Assert.That(gpuRes.Failed).IsFalse();
      await Assert.That(gpuRes.Success).IsEqualTo("worker-gpu:gpu-queue:model-v1");
      await Assert.That(capturedTraceId).IsEqualTo("span-98765");

      await Assert.That(cpuRes.Failed).IsFalse();
      await Assert.That(cpuRes.Success).IsEqualTo("worker-cpu:cpu-queue:model-v2");

      await broker.StopAsync();
   }

   [Test]
   public async Task MultiWorkerLoadBalancing_DistributesConcurrentBatchAcrossWorkers()
   {
      var port = GetFreeTcpPort();
      var endPoint = new IPEndPoint(IPAddress.Loopback, port);

      var broker = MqttServerFactory.CreateBuilder()
         .WithDefaultClientIdGenerator()
         .UseTcp(port)
         .Build();
      await broker.StartAsync();

      await using var scheduler = JobSchedulerBuilder.Create()
         .WithTickInterval(TimeSpan.FromMilliseconds(15))
         .UseTcp(endPoint)
         .Build();

      var executedByWorker = new ConcurrentDictionary<string, int>(StringComparer.Ordinal);

      JobWorker CreateWorker(string id) => JobWorkerBuilder.Create()
         .WithWorkerId(id)
         .WithMaxConcurrency(4)
         .SubscribeQueue("batch")
         .RegisterHandlerWithResult<int, int>("batch.item", async (ctx, ct) =>
         {
            await Task.Delay(25, ct);
            executedByWorker.AddOrUpdate(ctx.WorkerId, 1, (_, c) => c + 1);
            return ctx.Arguments * 2;
         })
         .UseTcp(endPoint)
         .Build();

      await using var w1 = CreateWorker("worker-pool-1");
      await using var w2 = CreateWorker("worker-pool-2");
      await using var w3 = CreateWorker("worker-pool-3");

      await using var client = JobClientBuilder.Create()
         .UseTcp(endPoint)
         .Build();

      await scheduler.StartAsync();
      await w1.StartAsync();
      await w2.StartAsync();
      await w3.StartAsync();
      await client.ConnectAsync();

      const int JobCount = 18;
      var jobIds = new string[JobCount];
      for (var i = 0; i < JobCount; i++)
      {
         var enq = await client.EnqueueAsync("batch.item", i + 1, queue: "batch");
         await Assert.That(enq.Failed).IsFalse();
         jobIds[i] = enq.Success!.JobId;
      }

      var results = await Task.WhenAll(
         jobIds.Select(id => client.WaitForJobResultAsync<int>(id, TimeSpan.FromSeconds(10))));

      for (var i = 0; i < JobCount; i++)
      {
         await Assert.That(results[i].Failed).IsFalse();
         await Assert.That(results[i].Success).IsEqualTo((i + 1) * 2);
      }

      // Multiple workers in the pool should have participated
      await Assert.That(executedByWorker.Values.Sum()).IsEqualTo(JobCount);
      await Assert.That(executedByWorker.Count).IsGreaterThanOrEqualTo(2);

      await broker.StopAsync();
   }

   [Test]
   public async Task DelayedJob_RemainsScheduledUntilDueTimeThenExecutes()
   {
      var port = GetFreeTcpPort();
      var endPoint = new IPEndPoint(IPAddress.Loopback, port);

      var broker = MqttServerFactory.CreateBuilder()
         .WithDefaultClientIdGenerator()
         .UseTcp(port)
         .Build();
      await broker.StartAsync();

      await using var scheduler = JobSchedulerBuilder.Create()
         .WithTickInterval(TimeSpan.FromMilliseconds(20))
         .UseTcp(endPoint)
         .Build();

      await using var worker = JobWorkerBuilder.Create()
         .WithWorkerId("worker-delayed")
         .RegisterHandlerWithResult<string, string>("delayed.echo", (ctx, _) =>
            ValueTask.FromResult($"echo:{ctx.Arguments}"))
         .UseTcp(endPoint)
         .Build();

      await using var client = JobClientBuilder.Create()
         .UseTcp(endPoint)
         .Build();

      await scheduler.StartAsync();
      await worker.StartAsync();
      await client.ConnectAsync();

      var enq = await client.EnqueueAsync("delayed.echo", "hello-future", delay: TimeSpan.FromMilliseconds(150));
      await Assert.That(enq.Failed).IsFalse();
      await Assert.That(enq.Success!.InitialStatus).IsEqualTo(JobStatus.Scheduled);

      var result = await client.WaitForJobResultAsync<string>(enq.Success.JobId, TimeSpan.FromSeconds(5));
      await Assert.That(result.Failed).IsFalse();
      await Assert.That(result.Success).IsEqualTo("echo:hello-future");

      await broker.StopAsync();
   }

   [Test]
   public async Task MultiQueueDispatch_UnservicedQueueExceedingBatchSize_DoesNotStarveActiveQueue()
   {
      var port = GetFreeTcpPort();
      var endPoint = new IPEndPoint(IPAddress.Loopback, port);

      var broker = MqttServerFactory.CreateBuilder()
         .WithDefaultClientIdGenerator()
         .UseTcp(port)
         .Build();
      await broker.StartAsync();

      await using var scheduler = JobSchedulerBuilder.Create()
         .WithTickInterval(TimeSpan.FromMilliseconds(20))
         .WithDispatchBatchSize(4)
         .UseTcp(endPoint)
         .Build();

      await using var worker = JobWorkerBuilder.Create()
         .WithWorkerId("worker-active-only")
         .SubscribeQueue("active-queue")
         .RegisterHandlerWithResult<string, string>("active.echo", (ctx, _) =>
            ValueTask.FromResult($"serviced:{ctx.Arguments}"))
         .UseTcp(endPoint)
         .Build();

      await using var client = JobClientBuilder.Create()
         .UseTcp(endPoint)
         .Build();

      await scheduler.StartAsync();
      await worker.StartAsync();
      await client.ConnectAsync();

      // Enqueue more jobs than DispatchBatchSize (4) onto a queue with no online workers
      for (var i = 0; i < 6; i++)
      {
         var unserviced = await client.EnqueueAsync("unserviced.work", $"item-{i}", queue: "unserviced-queue");
         await Assert.That(unserviced.Failed).IsFalse();
      }

      // Now enqueue a job onto 'active-queue' which has an online idle worker
      var activeJob = await client.EnqueueAsync("active.echo", "ready-now", queue: "active-queue");
      await Assert.That(activeJob.Failed).IsFalse();

      var activeResult = await client.WaitForJobResultAsync<string>(
         activeJob.Success!.JobId,
         TimeSpan.FromSeconds(2));

      await Assert.That(activeResult.Failed).IsFalse();
      await Assert.That(activeResult.Success).IsEqualTo("serviced:ready-now");

      await broker.StopAsync();
   }

   [Test]
   public async Task WorkerCompletionEvent_SingleConcurrencySlot_ReleasesActiveSlotBeforeFiringCompletedAndAcceptingNextJob()
   {
      var port = GetFreeTcpPort();
      var endPoint = new IPEndPoint(IPAddress.Loopback, port);

      var broker = MqttServerFactory.CreateBuilder()
         .WithDefaultClientIdGenerator()
         .UseTcp(port)
         .Build();
      await broker.StartAsync();

      await using var scheduler = JobSchedulerBuilder.Create()
         .WithTickInterval(TimeSpan.FromMilliseconds(500))
         .UseTcp(endPoint)
         .Build();

      JobWorker? workerRef = null;
      var activeCountInsideCompletedEvent = -1;

      await using var worker = JobWorkerBuilder.Create()
         .WithWorkerId("worker-single-slot")
         .WithMaxConcurrency(1)
         .WithHeartbeatInterval(TimeSpan.Zero)
         .RegisterHandlerWithResult<string, string>("chain.step", (ctx, _) =>
            ValueTask.FromResult($"done:{ctx.Arguments}"))
         .UseTcp(endPoint)
         .Build();

      workerRef = worker;

      worker.Events.OnJobCompleted.Add(async (_, ct) =>
      {
         Interlocked.CompareExchange(ref activeCountInsideCompletedEvent, workerRef.ActiveJobCount, -1);
         await Task.Delay(80, ct);
      });

      await using var client = JobClientBuilder.Create()
         .UseTcp(endPoint)
         .Build();

      await scheduler.StartAsync();
      await worker.StartAsync();
      await client.ConnectAsync();

      var step1 = await client.EnqueueAsync("chain.step", "first");
      var step2 = await client.EnqueueContinuationAsync(step1.Success!.JobId, "chain.step", "second");

      // With TickInterval = 500ms and HeartbeatInterval = 0, step2 must be accepted on the immediate
      // wakeup triggered by step1's completion rather than being rejected due to stale ActiveJobCount.
      var step2Result = await client.WaitForJobResultAsync<string>(
         step2.Success!.JobId,
         TimeSpan.FromMilliseconds(350));

      await Assert.That(Volatile.Read(ref activeCountInsideCompletedEvent)).IsEqualTo(0);
      await Assert.That(step2Result.Failed).IsFalse();
      await Assert.That(step2Result.Success).IsEqualTo("done:second");

      await broker.StopAsync();
   }

   [Test]
   public async Task StartAsync_AfterInitialConnectionFailure_CanRetryAndConnectSuccessfully()
   {
      var port = GetFreeTcpPort();
      var endPoint = new IPEndPoint(IPAddress.Loopback, port);

      await using var scheduler = JobSchedulerBuilder.Create()
         .WithTickInterval(TimeSpan.FromMilliseconds(20))
         .UseTcp(endPoint)
         .Build();

      await using var worker = JobWorkerBuilder.Create()
         .WithWorkerId("worker-reconnect-start")
         .RegisterHandlerWithResult<int, int>("math.double", (ctx, _) =>
            ValueTask.FromResult(ctx.Arguments * 2))
         .UseTcp(endPoint)
         .Build();

      // Broker is not started yet -> initial StartAsync calls fail
      var initialSchedulerStart = await scheduler.StartAsync();
      var initialWorkerStart = await worker.StartAsync();

      await Assert.That(initialSchedulerStart.Failed).IsTrue();
      await Assert.That(initialWorkerStart.Failed).IsTrue();

      // Now start the broker on the endpoint and retry StartAsync
      var broker = MqttServerFactory.CreateBuilder()
         .WithDefaultClientIdGenerator()
         .UseTcp(port)
         .Build();
      await broker.StartAsync();

      var retrySchedulerStart = await scheduler.StartAsync();
      var retryWorkerStart = await worker.StartAsync();

      await Assert.That(retrySchedulerStart.Failed).IsFalse();
      await Assert.That(retryWorkerStart.Failed).IsFalse();
      await Assert.That(scheduler.MqttClient.IsConnected).IsTrue();
      await Assert.That(worker.MqttClient.IsConnected).IsTrue();

      await using var client = JobClientBuilder.Create()
         .UseTcp(endPoint)
         .Build();
      await client.ConnectAsync();

      var enq = await client.EnqueueAsync("math.double", 21);
      await Assert.That(enq.Failed).IsFalse();

      var res = await client.WaitForJobResultAsync<int>(enq.Success!.JobId, TimeSpan.FromSeconds(3));
      await Assert.That(res.Failed).IsFalse();
      await Assert.That(res.Success).IsEqualTo(42);

      await broker.StopAsync();
   }
}
