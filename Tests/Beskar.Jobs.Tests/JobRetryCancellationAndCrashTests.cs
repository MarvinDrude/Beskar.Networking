using System.Net;
using System.Net.Sockets;
using Beskar.Jobs.Client;
using Beskar.Jobs.Common.Enums;
using Beskar.Jobs.Common.Models;
using Beskar.Jobs.Scheduler;
using Beskar.Jobs.Worker;
using Beskar.Mqtt.Server;
using Beskar.Networking.Abstractions.Backoffs;

namespace Beskar.Jobs.Tests;

[NotInParallel]
public class JobRetryCancellationAndCrashTests
{
   private static int GetFreeTcpPort()
   {
      using var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
      socket.Bind(new IPEndPoint(IPAddress.Loopback, 0));
      return ((IPEndPoint)socket.LocalEndPoint!).Port;
   }

   [Test]
   public async Task JobRetry_FlakyHandler_RetriesWithBackoffUntilSuccess()
   {
      var port = GetFreeTcpPort();
      var endPoint = new IPEndPoint(IPAddress.Loopback, port);

      var broker = MqttServerFactory.CreateBuilder()
         .WithDefaultClientIdGenerator()
         .UseTcp(port)
         .Build();
      await broker.StartAsync();

      var retryPolicy = new RetryPolicy
      {
         MaxAttempts = 3,
         Backoff = new ConstantBackoffPolicy(TimeSpan.FromMilliseconds(40))
      };

      var retryEventsCount = 0;

      await using var scheduler = JobSchedulerBuilder.Create()
         .WithTickInterval(TimeSpan.FromMilliseconds(20))
         .WithRetryPolicy(retryPolicy)
         .UseTcp(endPoint)
         .Build();

      scheduler.Events.OnJobRetryScheduled.Add((_, _) =>
      {
         Interlocked.Increment(ref retryEventsCount);
         return ValueTask.CompletedTask;
      });

      var attemptsSeen = 0;

      await using var worker = JobWorkerBuilder.Create()
         .WithWorkerId("worker-retry")
         .RegisterHandler<string>("flaky.job", (_, _) =>
         {
            var currentAttempt = Interlocked.Increment(ref attemptsSeen);
            if (currentAttempt < 3)
            {
               throw new InvalidOperationException($"Simulated transient failure on attempt {currentAttempt}");
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

      var enq = await client.EnqueueAsync("flaky.job", "payload", maxAttempts: 3);
      await Assert.That(enq.Failed).IsFalse();

      var completion = await client.WaitForJobCompletionAsync(enq.Success!.JobId, TimeSpan.FromSeconds(5));
      await Assert.That(completion.Failed).IsFalse();
      await Assert.That(completion.Success!.Status).IsEqualTo(JobStatus.Succeeded);
      await Assert.That(completion.Success.Attempt).IsEqualTo(3);
      await Assert.That(Volatile.Read(ref attemptsSeen)).IsEqualTo(3);
      await Assert.That(Volatile.Read(ref retryEventsCount)).IsEqualTo(2);

      await broker.StopAsync();
   }

   [Test]
   public async Task JobRetry_ExhaustsMaxAttempts_TransitionsToDeadLettered()
   {
      var port = GetFreeTcpPort();
      var endPoint = new IPEndPoint(IPAddress.Loopback, port);

      var broker = MqttServerFactory.CreateBuilder()
         .WithDefaultClientIdGenerator()
         .UseTcp(port)
         .Build();
      await broker.StartAsync();

      var retryPolicy = new RetryPolicy
      {
         MaxAttempts = 2,
         Backoff = new ConstantBackoffPolicy(TimeSpan.FromMilliseconds(30))
      };

      var deadLetteredFired = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);

      await using var scheduler = JobSchedulerBuilder.Create()
         .WithTickInterval(TimeSpan.FromMilliseconds(20))
         .WithRetryPolicy(retryPolicy)
         .UseTcp(endPoint)
         .Build();

      scheduler.Events.OnJobDeadLettered.Add((ctx, _) =>
      {
         deadLetteredFired.TrySetResult(ctx.Job.JobId);
         return ValueTask.CompletedTask;
      });

      await using var worker = JobWorkerBuilder.Create()
         .WithWorkerId("worker-dlq")
         .RegisterHandler<string>("always.fail", (_, _) =>
            throw new InvalidOperationException("Permanent failure"))
         .UseTcp(endPoint)
         .Build();

      await using var client = JobClientBuilder.Create()
         .UseTcp(endPoint)
         .Build();

      await scheduler.StartAsync();
      await worker.StartAsync();
      await client.ConnectAsync();

      var enq = await client.EnqueueAsync("always.fail", "bad-data", maxAttempts: 2);
      await Assert.That(enq.Failed).IsFalse();

      var completion = await client.WaitForJobCompletionAsync(enq.Success!.JobId, TimeSpan.FromSeconds(5));
      await Assert.That(completion.Failed).IsFalse();
      await Assert.That(completion.Success!.Status).IsEqualTo(JobStatus.DeadLettered);
      await Assert.That(completion.Success.Attempt).IsEqualTo(2);

      var dlqJobId = await deadLetteredFired.Task.WaitAsync(TimeSpan.FromSeconds(2));
      await Assert.That(dlqJobId).IsEqualTo(enq.Success.JobId);

      await broker.StopAsync();
   }

   [Test]
   public async Task CooperativeCancellation_CancelsRunningJobOnWorker()
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

      var workerStartedJob = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

      await using var worker = JobWorkerBuilder.Create()
         .WithWorkerId("worker-cancel")
         .RegisterHandler<string>("long.running", async (_, ct) =>
         {
            workerStartedJob.TrySetResult();
            await Task.Delay(TimeSpan.FromSeconds(30), ct);
         })
         .UseTcp(endPoint)
         .Build();

      await using var client = JobClientBuilder.Create()
         .UseTcp(endPoint)
         .Build();

      await scheduler.StartAsync();
      await worker.StartAsync();
      await client.ConnectAsync();

      var enq = await client.EnqueueAsync("long.running", "wait");
      await Assert.That(enq.Failed).IsFalse();

      await workerStartedJob.Task.WaitAsync(TimeSpan.FromSeconds(5));

      var cancelRes = await client.CancelJobAsync(enq.Success!.JobId, "User requested stop");
      await Assert.That(cancelRes.Failed).IsFalse();

      var completion = await client.WaitForJobCompletionAsync(enq.Success.JobId, TimeSpan.FromSeconds(5));
      await Assert.That(completion.Failed).IsFalse();
      await Assert.That(completion.Success!.Status).IsEqualTo(JobStatus.Cancelled);

      await broker.StopAsync();
   }

   [Test]
   public async Task CancelDelayedJob_BeforeExecution_CancelsImmediatelyInScheduler()
   {
      var port = GetFreeTcpPort();
      var endPoint = new IPEndPoint(IPAddress.Loopback, port);

      var broker = MqttServerFactory.CreateBuilder()
         .WithDefaultClientIdGenerator()
         .UseTcp(port)
         .Build();
      await broker.StartAsync();

      var cancelledInScheduler = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);

      await using var scheduler = JobSchedulerBuilder.Create()
         .WithTickInterval(TimeSpan.FromMilliseconds(20))
         .UseTcp(endPoint)
         .Build();

      scheduler.Events.OnJobCancelled.Add((ctx, _) =>
      {
         cancelledInScheduler.TrySetResult(ctx.Job.JobId);
         return ValueTask.CompletedTask;
      });

      await using var client = JobClientBuilder.Create()
         .UseTcp(endPoint)
         .Build();

      await scheduler.StartAsync();
      await client.ConnectAsync();

      var enq = await client.EnqueueAsync("future.job", "data", delay: TimeSpan.FromMinutes(10));
      await Assert.That(enq.Failed).IsFalse();
      await Assert.That(enq.Success!.InitialStatus).IsEqualTo(JobStatus.Scheduled);

      await client.CancelJobAsync(enq.Success.JobId, "No longer needed");

      var cancelledId = await cancelledInScheduler.Task.WaitAsync(TimeSpan.FromSeconds(3));
      await Assert.That(cancelledId).IsEqualTo(enq.Success.JobId);

      var completion = await client.WaitForJobCompletionAsync(enq.Success.JobId, TimeSpan.FromSeconds(3));
      await Assert.That(completion.Failed).IsFalse();
      await Assert.That(completion.Success!.Status).IsEqualTo(JobStatus.Cancelled);

      await broker.StopAsync();
   }

   [Test]
   public async Task JobTimeout_ExceedingConfiguredTimeout_FailsAndDeadLettersWithTimeoutMessage()
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
         .WithWorkerId("worker-timeout")
         .RegisterHandler<string>("slow.job", async (_, ct) =>
         {
            await Task.Delay(TimeSpan.FromSeconds(10), ct);
         })
         .UseTcp(endPoint)
         .Build();

      await using var client = JobClientBuilder.Create()
         .UseTcp(endPoint)
         .Build();

      await scheduler.StartAsync();
      await worker.StartAsync();
      await client.ConnectAsync();

      var enq = await client.EnqueueAsync(
         "slow.job",
         "slow-payload",
         maxAttempts: 1,
         timeout: TimeSpan.FromMilliseconds(80));
      await Assert.That(enq.Failed).IsFalse();

      var completion = await client.WaitForJobCompletionAsync(enq.Success!.JobId, TimeSpan.FromSeconds(5));
      await Assert.That(completion.Failed).IsFalse();
      await Assert.That(completion.Success!.Status).IsEqualTo(JobStatus.DeadLettered);
      await Assert.That(completion.Success.ErrorMessage).Contains("timed out");

      await broker.StopAsync();
   }

   [Test]
   public async Task WorkerCrashRecovery_LastWillTriggersImmediateFailoverToHealthyWorker()
   {
      var port = GetFreeTcpPort();
      var endPoint = new IPEndPoint(IPAddress.Loopback, port);

      var broker = MqttServerFactory.CreateBuilder()
         .WithDefaultClientIdGenerator()
         .UseTcp(port)
         .Build();
      await broker.StartAsync();

      var retryPolicy = new RetryPolicy
      {
         MaxAttempts = 3,
         Backoff = new ConstantBackoffPolicy(TimeSpan.FromMilliseconds(25))
      };

      var crashedWorkerTcs = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);

      await using var scheduler = JobSchedulerBuilder.Create()
         .WithTickInterval(TimeSpan.FromMilliseconds(20))
         .WithRetryPolicy(retryPolicy)
         .UseTcp(endPoint)
         .Build();

      scheduler.Events.OnWorkerCrashed.Add((ctx, _) =>
      {
         crashedWorkerTcs.TrySetResult(ctx.WorkerId);
         return ValueTask.CompletedTask;
      });

      var worker1EnteredJob = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

      await using var worker1 = JobWorkerBuilder.Create()
         .WithWorkerId("worker-crash-victim")
         .WithHeartbeatInterval(TimeSpan.Zero)
         .RegisterHandlerWithResult<string, string>("critical.task", async (ctx, ct) =>
         {
            worker1EnteredJob.TrySetResult();
            await Task.Delay(TimeSpan.FromSeconds(30), ct);
            return ctx.WorkerId;
         })
         .UseTcp(endPoint)
         .Build();

      await using var client = JobClientBuilder.Create()
         .UseTcp(endPoint)
         .Build();

      await scheduler.StartAsync();
      await worker1.StartAsync();
      await client.ConnectAsync();

      var enq = await client.EnqueueAsync("critical.task", "important-payload", maxAttempts: 3);
      await Assert.That(enq.Failed).IsFalse();

      // Wait until worker-1 is actively running the job
      await worker1EnteredJob.Task.WaitAsync(TimeSpan.FromSeconds(5));

      // Start backup healthy worker-2 before crashing worker-1
      await using var worker2 = JobWorkerBuilder.Create()
         .WithWorkerId("worker-healthy-backup")
         .RegisterHandlerWithResult<string, string>("critical.task", (ctx, _) =>
            ValueTask.FromResult($"recovered-by-{ctx.WorkerId}-attempt-{ctx.Attempt}"))
         .UseTcp(endPoint)
         .Build();

      await worker2.StartAsync();

      // Simulate abrupt crash on worker-1 (no MQTT DISCONNECT -> Broker fires LWT immediately)
      await worker1.SimulateCrashAsync();

      var crashedWorkerId = await crashedWorkerTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
      await Assert.That(crashedWorkerId).IsEqualTo("worker-crash-victim");

      var result = await client.WaitForJobResultAsync<string>(enq.Success!.JobId, TimeSpan.FromSeconds(5));
      await Assert.That(result.Failed).IsFalse();
      await Assert.That(result.Success).IsEqualTo("recovered-by-worker-healthy-backup-attempt-2");

      await broker.StopAsync();
   }
}
