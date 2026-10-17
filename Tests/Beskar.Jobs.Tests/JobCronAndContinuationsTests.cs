using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using Beskar.Jobs.Client;
using Beskar.Jobs.Common.Enums;
using Beskar.Jobs.Common.Models;
using Beskar.Jobs.Scheduler;
using Beskar.Jobs.Worker;
using Beskar.Mqtt.Server;

namespace Beskar.Jobs.Tests;

[NotInParallel]
public class JobCronAndContinuationsTests
{
   private static int GetFreeTcpPort()
   {
      using var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
      socket.Bind(new IPEndPoint(IPAddress.Loopback, 0));
      return ((IPEndPoint)socket.LocalEndPoint!).Port;
   }

   [Test]
   public async Task RecurringJob_FiresRepeatedlyOnIntervalSchedule()
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

      var executionCount = 0;
      var reachedThreeRuns = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

      await using var worker = JobWorkerBuilder.Create()
         .WithWorkerId("worker-cron")
         .RegisterHandler<string>("cron.heartbeat", (_, _) =>
         {
            if (Interlocked.Increment(ref executionCount) >= 3)
            {
               reachedThreeRuns.TrySetResult();
            }

            return ValueTask.CompletedTask;
         })
         .UseTcp(endPoint)
         .Build();

      await using var client = JobClientBuilder.Create()
         .UseTcp(endPoint)
         .Build();

      await scheduler.StartAsync();
      await worker.StartAsync();
      await client.ConnectAsync();

      var scheduleRes = await client.ScheduleRecurringAsync(
         recurringJobId: "fast-pulse",
         cronExpression: "@every 60ms",
         jobType: "cron.heartbeat",
         args: "ping");

      await Assert.That(scheduleRes.Failed).IsFalse();

      await reachedThreeRuns.Task.WaitAsync(TimeSpan.FromSeconds(5));
      await Assert.That(Volatile.Read(ref executionCount)).IsGreaterThanOrEqualTo(3);

      await broker.StopAsync();
   }

   [Test]
   public async Task RecurringJob_SkipIfRunning_PreventsConcurrentOverlappingRuns()
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

      var currentActive = 0;
      var maxObservedConcurrency = 0;
      var completedRuns = 0;
      var twoRunsDone = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

      await using var worker = JobWorkerBuilder.Create()
         .WithWorkerId("worker-overlap")
         .WithMaxConcurrency(8)
         .RegisterHandler<string>("slow.cron", async (_, ct) =>
         {
            var activeNow = Interlocked.Increment(ref currentActive);
            while (true)
            {
               var prevMax = Volatile.Read(ref maxObservedConcurrency);
               if (activeNow <= prevMax ||
                   Interlocked.CompareExchange(ref maxObservedConcurrency, activeNow, prevMax) == prevMax)
               {
                  break;
               }
            }

            await Task.Delay(140, ct);
            Interlocked.Decrement(ref currentActive);

            if (Interlocked.Increment(ref completedRuns) >= 2)
            {
               twoRunsDone.TrySetResult();
            }
         })
         .UseTcp(endPoint)
         .Build();

      await using var client = JobClientBuilder.Create()
         .UseTcp(endPoint)
         .Build();

      await scheduler.StartAsync();
      await worker.StartAsync();
      await client.ConnectAsync();

      var scheduleRes = await client.ScheduleRecurringAsync(
         recurringJobId: "singleton-cron",
         cronExpression: "@every 40ms",
         jobType: "slow.cron",
         args: "work",
         overlapPolicy: OverlapPolicy.SkipIfRunning);

      await Assert.That(scheduleRes.Failed).IsFalse();

      await twoRunsDone.Task.WaitAsync(TimeSpan.FromSeconds(5));
      await Assert.That(Volatile.Read(ref maxObservedConcurrency)).IsEqualTo(1);

      await broker.StopAsync();
   }

   [Test]
   public async Task MisfirePolicy_FireAllMissed_EnqueuesMissedOccurrencesUpToCap()
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

      var runs = 0;
      var threeCaughtUp = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

      await using var worker = JobWorkerBuilder.Create()
         .WithWorkerId("worker-misfire")
         .RegisterHandler<string>("catchup.job", (_, _) =>
         {
            if (Interlocked.Increment(ref runs) >= 3)
            {
               threeCaughtUp.TrySetResult();
            }

            return ValueTask.CompletedTask;
         })
         .UseTcp(endPoint)
         .Build();

      // Seed a recurring job whose NextRunAtUtc is 350ms in the past with a 100ms interval -> ~3-4 missed runs
      await scheduler.Storage.UpsertRecurringJobAsync(new RecurringJobRecord
      {
         RecurringJobId = "missed-schedule",
         CronExpression = "@every 100ms",
         JobType = "catchup.job",
         MisfirePolicy = MisfirePolicy.FireAllMissed,
         OverlapPolicy = OverlapPolicy.AllowConcurrent,
         NextRunAtUtc = DateTimeOffset.UtcNow.AddMilliseconds(-350)
      });

      await worker.StartAsync();
      await scheduler.StartAsync();

      await threeCaughtUp.Task.WaitAsync(TimeSpan.FromSeconds(5));
      await Assert.That(Volatile.Read(ref runs)).IsGreaterThanOrEqualTo(3);

      await broker.StopAsync();
   }

   [Test]
   public async Task ContinuationJobs_ThreeStepPipelineAndFanOut_ExecuteInStrictDependencyOrder()
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

      var executionOrder = new ConcurrentQueue<string>();

      await using var worker = JobWorkerBuilder.Create()
         .WithWorkerId("worker-dag")
         .WithMaxConcurrency(1)
         .RegisterHandler<string>("dag.step", async (ctx, ct) =>
         {
            if (ctx.Arguments == "step-1")
            {
               await Task.Delay(50, ct);
            }
            else if (ctx.Arguments == "step-2")
            {
               await Task.Delay(30, ct);
            }

            executionOrder.Enqueue(ctx.Arguments);
         })
         .UseTcp(endPoint)
         .Build();

      await using var client = JobClientBuilder.Create()
         .UseTcp(endPoint)
         .Build();

      await scheduler.StartAsync();
      await worker.StartAsync();
      await client.ConnectAsync();

      var step1 = await client.EnqueueAsync("dag.step", "step-1");
      await Assert.That(step1.Failed).IsFalse();

      var step2 = await client.EnqueueContinuationAsync(
         parentJobId: step1.Success!.JobId,
         jobType: "dag.step",
         args: "step-2");
      await Assert.That(step2.Failed).IsFalse();

      var step3 = await client.EnqueueContinuationAsync(
         parentJobId: step2.Success!.JobId,
         jobType: "dag.step",
         args: "step-3");
      await Assert.That(step3.Failed).IsFalse();

      var step3Completion = await client.WaitForJobCompletionAsync(step3.Success!.JobId, TimeSpan.FromSeconds(5));
      await Assert.That(step3Completion.Failed).IsFalse();
      await Assert.That(step3Completion.Success!.Status).IsEqualTo(JobStatus.Succeeded);

      var items = executionOrder.ToArray();
      await Assert.That(items.Length).IsEqualTo(3);
      await Assert.That(items[0]).IsEqualTo("step-1");
      await Assert.That(items[1]).IsEqualTo("step-2");
      await Assert.That(items[2]).IsEqualTo("step-3");

      await broker.StopAsync();
   }

   [Test]
   public async Task ContinuationJobs_WhenParentDeadLetters_CascadesCancellationToChildAndGrandchild()
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
         .WithWorkerId("worker-dag-fail")
         .RegisterHandler<string>("dag.fail.step", (ctx, _) =>
         {
            if (ctx.Arguments == "step-1")
            {
               throw new InvalidOperationException("Parent stage failed permanently");
            }

            return ValueTask.CompletedTask;
         })
         .UseTcp(endPoint)
         .Build();

      await using var client = JobClientBuilder.Create()
         .UseTcp(endPoint)
         .Build();

      await scheduler.StartAsync();
      await worker.StartAsync();
      await client.ConnectAsync();

      var step1 = await client.EnqueueAsync("dag.fail.step", "step-1", maxAttempts: 1);
      await Assert.That(step1.Failed).IsFalse();

      var step2 = await client.EnqueueContinuationAsync(
         parentJobId: step1.Success!.JobId,
         jobType: "dag.fail.step",
         args: "step-2");
      await Assert.That(step2.Failed).IsFalse();

      var step3 = await client.EnqueueContinuationAsync(
         parentJobId: step2.Success!.JobId,
         jobType: "dag.fail.step",
         args: "step-3");
      await Assert.That(step3.Failed).IsFalse();

      var step1Completion = await client.WaitForJobCompletionAsync(step1.Success.JobId, TimeSpan.FromSeconds(2));
      await Assert.That(step1Completion.Failed).IsFalse();
      await Assert.That(step1Completion.Success!.Status).IsEqualTo(JobStatus.DeadLettered);

      var step2Completion = await client.WaitForJobCompletionAsync(step2.Success.JobId, TimeSpan.FromSeconds(2));
      await Assert.That(step2Completion.Failed).IsFalse();
      await Assert.That(step2Completion.Success!.Status).IsEqualTo(JobStatus.Cancelled);

      var step3Completion = await client.WaitForJobCompletionAsync(step3.Success.JobId, TimeSpan.FromSeconds(2));
      await Assert.That(step3Completion.Failed).IsFalse();
      await Assert.That(step3Completion.Success!.Status).IsEqualTo(JobStatus.Cancelled);

      await broker.StopAsync();
   }

   [Test]
   public async Task RecurringJob_QueueNextOverlapPolicy_ContinuesExecutingSubsequentRunsAfterFailedInstanceAndSetsRecurringJobId()
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

      string? observedRecurringJobId = null;
      scheduler.Events.OnRecurringJobTriggered.Add((ctx, _) =>
      {
         Interlocked.CompareExchange(ref observedRecurringJobId, ctx.SpawnedJob.RecurringJobId, null);
         return ValueTask.CompletedTask;
      });

      var invocationCount = 0;
      var successfulRuns = 0;
      var twoSuccessesDone = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

      await using var worker = JobWorkerBuilder.Create()
         .WithWorkerId("worker-queue-next")
         .WithMaxConcurrency(2)
         .RegisterHandler<string>("cron.queuenext", async (_, ct) =>
         {
            var runNumber = Interlocked.Increment(ref invocationCount);
            if (runNumber == 1)
            {
               // Hold long enough for the next Cron tick to queue behind run #1 via OverlapPolicy.QueueNext, then fail
               await Task.Delay(120, ct);
               throw new InvalidOperationException("First recurring instance failed");
            }

            if (Interlocked.Increment(ref successfulRuns) >= 2)
            {
               twoSuccessesDone.TrySetResult();
            }
         })
         .UseTcp(endPoint)
         .Build();

      await using var client = JobClientBuilder.Create()
         .UseTcp(endPoint)
         .Build();

      await scheduler.StartAsync();
      await worker.StartAsync();
      await client.ConnectAsync();

      var scheduleRes = await client.ScheduleRecurringAsync(
         recurringJobId: "resilient-queue-next",
         cronExpression: "@every 50ms",
         jobType: "cron.queuenext",
         args: "tick",
         overlapPolicy: OverlapPolicy.QueueNext,
         maxAttempts: 1);

      await Assert.That(scheduleRes.Failed).IsFalse();

      await twoSuccessesDone.Task.WaitAsync(TimeSpan.FromSeconds(2));
      await Assert.That(Volatile.Read(ref successfulRuns)).IsGreaterThanOrEqualTo(2);
      await Assert.That(Volatile.Read(ref observedRecurringJobId)).IsEqualTo("resilient-queue-next");

      await broker.StopAsync();
   }
}
