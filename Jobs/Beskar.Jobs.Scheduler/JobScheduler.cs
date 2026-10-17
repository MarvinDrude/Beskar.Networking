using System.Text;
using Beskar.Jobs.Common.Enums;
using Beskar.Jobs.Common.Interfaces;
using Beskar.Jobs.Common.Models;
using Beskar.Jobs.Common.Scheduling;
using Beskar.Jobs.Common.Telemetry;
using Beskar.Jobs.Common.Topics;
using Beskar.Jobs.Scheduler.Events;
using Beskar.Jobs.Scheduler.Internal;
using Beskar.Jobs.Scheduler.Options;
using Beskar.Memory.Results;
using Beskar.Memory.Results.Errors;
using Beskar.Mqtt.Common.Builders.Connecting;
using Beskar.Mqtt.Common.Builders.Disconnecting;
using Beskar.Mqtt.Common.Builders.Publishing;
using Beskar.Mqtt.Common.Builders.Subscribing;
using Beskar.Mqtt.Common.Handlers.Contexts;
using Beskar.Mqtt.Common.Interfaces;
using Beskar.Mqtt.Protocol.Enums;

namespace Beskar.Jobs.Scheduler;

/// <summary>
/// High-precision job scheduler, Cron evaluator, worker load-balancer, and LWT crash-recovery coordinator.
/// </summary>
public sealed class JobScheduler : IAsyncDisposable
{
   private readonly WorkerRegistry _workerRegistry = new();
   private readonly SemaphoreSlim _tickSignal = new(0, 1);
   private readonly SemaphoreSlim _dispatchLock = new(1, 1);
   private readonly bool _ownsMqttClient;

   private IDisposable? _messageHandlerToken;
   private CancellationTokenSource? _schedulerCts;
   private Task? _schedulerLoopTask;
   private long _statusSequence;
   private int _started;
   private int _disposed;

   public IMqttClient MqttClient { get; }
   public IJobStorage Storage { get; }
   public JobSchedulerOptions Options { get; }
   public JobSchedulerEvents Events { get; } = new();

   /// <summary>
   /// Current count of online workers registered with this scheduler.
   /// </summary>
   public int OnlineWorkerCount => _workerRegistry.OnlineWorkerCount;

   public JobScheduler(
      IMqttClient mqttClient,
      IJobStorage storage,
      JobSchedulerOptions? options = null,
      bool ownsMqttClient = true)
   {
      ArgumentNullException.ThrowIfNull(mqttClient);
      ArgumentNullException.ThrowIfNull(storage);

      MqttClient = mqttClient;
      Storage = storage;
      Options = options ?? new JobSchedulerOptions();

      _ownsMqttClient = ownsMqttClient;
   }

   /// <summary>
   /// Connects to the MQTT broker, subscribes to command, worker heartbeat, and job lifecycle topics,
   /// and starts the background scheduling and Cron evaluation loop.
   /// </summary>
   public async ValueTask<VoidResult<StringError>> StartAsync(CancellationToken ct = default)
   {
      if (Interlocked.Exchange(ref _started, 1) != 0)
      {
         return new VoidResult<StringError>();
      }

      var startedSuccessfully = false;
      try
      {
         if (!MqttClient.IsConnected && Options.EndPoint is null)
         {
            return new StringError("JobSchedulerOptions.EndPoint must be set when starting an unconnected IMqttClient.");
         }

         _schedulerCts = new CancellationTokenSource();
         _messageHandlerToken = MqttClient.AddMessageReceiveHandler(HandleIncomingMessageAsync);

         if (!MqttClient.IsConnected)
         {
            var connectOptions = new ConnectOptions
            {
               EndPoint = Options.EndPoint!,
               ProtocolVersion = MqttProtocolVersion.V50,
               ClientIdUtf8Bytes = Encoding.UTF8.GetBytes(Options.SchedulerId),
               CleanSession = true
            };

            var connectResult = await MqttClient.ConnectAsync(connectOptions, ct);
            if (connectResult.Failed)
            {
               return connectResult.Error;
            }
         }

         var subOptions = SubscribeOptions.Create()
            .WithTopicFilter(JobTopics.EnqueueCommandTopic, QualityOfServiceType.AtLeastOnce)
            .WithTopicFilter(JobTopics.RecurringCommandTopic, QualityOfServiceType.AtLeastOnce)
            .WithTopicFilter(JobTopics.CancelCommandWildcard, QualityOfServiceType.AtLeastOnce)
            .WithTopicFilter(JobTopics.EventsWildcard, QualityOfServiceType.AtLeastOnce)
            .WithTopicFilter(JobTopics.WorkerHeartbeatWildcard, QualityOfServiceType.AtLeastOnce)
            .Build();

         var subResult = await MqttClient.SubscribeAsync(subOptions, ct);
         if (subResult.Failed)
         {
            if (_ownsMqttClient && MqttClient.IsConnected)
            {
               await MqttClient.DisconnectAsync(new DisconnectOptions(), ct);
            }

            return subResult.Error;
         }

         _schedulerLoopTask = Task.Run(() => RunSchedulerLoopAsync(_schedulerCts.Token), CancellationToken.None);
         SignalWakeup();

         startedSuccessfully = true;
         return new VoidResult<StringError>();
      }
      finally
      {
         if (!startedSuccessfully)
         {
            _messageHandlerToken?.Dispose();
            _messageHandlerToken = null;

            _schedulerCts?.Dispose();
            _schedulerCts = null;

            Interlocked.Exchange(ref _started, 0);
         }
      }
   }

   /// <summary>
   /// Stops the background scheduling loop and disconnects from the MQTT broker if owned.
   /// </summary>
   public async ValueTask StopAsync(CancellationToken ct = default)
   {
      if (Interlocked.Exchange(ref _started, 0) == 0)
      {
         return;
      }

      if (_schedulerCts is not null)
      {
         await _schedulerCts.CancelAsync();
      }

      SignalWakeup();

      if (_schedulerLoopTask is not null)
      {
         try
         {
            await _schedulerLoopTask;
         }
         catch (OperationCanceledException)
         {
         }
      }

      _messageHandlerToken?.Dispose();
      _messageHandlerToken = null;

      if (_ownsMqttClient && MqttClient.IsConnected)
      {
         await MqttClient.DisconnectAsync(new DisconnectOptions(), ct);
      }

      _schedulerCts?.Dispose();
      _schedulerCts = null;
   }

   /// <summary>
   /// Directly enqueues a job into the scheduler without an MQTT round-trip.
   /// </summary>
   public async ValueTask<EnqueueJobResponse> EnqueueAsync(
      EnqueueJobRequest request,
      CancellationToken ct = default)
   {
      ArgumentNullException.ThrowIfNull(request);

      if (string.IsNullOrWhiteSpace(request.JobType))
      {
         return new EnqueueJobResponse
         {
            Accepted = false,
            JobId = request.JobId ?? string.Empty,
            ErrorMessage = "JobType cannot be empty."
         };
      }

      var now = DateTimeOffset.UtcNow;
      var jobId = string.IsNullOrWhiteSpace(request.JobId) ? $"job-{Guid.NewGuid():N}" : request.JobId;
      var scheduledAt = request.ScheduledAtUtc ?? now;

      var initialStatus = JobStatus.Enqueued;
      if (!string.IsNullOrWhiteSpace(request.ParentJobId))
      {
         var parent = await Storage.GetJobAsync(request.ParentJobId, ct);
         if (parent is null || !parent.Status.IsTerminal())
         {
            initialStatus = JobStatus.Scheduled;
         }
         else if (parent.Status != JobStatus.Succeeded && string.IsNullOrEmpty(request.RecurringJobId))
         {
            initialStatus = JobStatus.Scheduled;
         }
      }
      else if (scheduledAt > now)
      {
         initialStatus = JobStatus.Scheduled;
      }

      var record = new JobInstanceRecord
      {
         JobId = jobId,
         JobType = request.JobType,
         Queue = string.IsNullOrWhiteSpace(request.Queue) ? "default" : request.Queue,
         Payload = request.Payload ?? [],
         Status = initialStatus,
         CreatedAtUtc = now,
         ScheduledAtUtc = scheduledAt,
         MaxAttempts = Math.Max(1, request.MaxAttempts),
         Timeout = request.Timeout,
         ParentJobId = request.ParentJobId,
         RecurringJobId = request.RecurringJobId,
         Metadata = request.Metadata
      };

      await Storage.UpsertJobAsync(record, ct);
      JobMetrics.RecordJobEnqueued(record.Queue, record.JobType);

      await PublishJobStatusAsync(record, message: initialStatus == JobStatus.Scheduled ? "Job scheduled" : "Job enqueued", ct: ct);

      if (Events.OnJobEnqueued.Count > 0)
      {
         await Events.OnJobEnqueued.ExecuteAsync(new JobSchedulerJobContext(record), cancellationToken: ct);
      }

      // Guard against a parent finishing concurrently while this continuation was being inserted
      if (!string.IsNullOrWhiteSpace(request.ParentJobId) && initialStatus == JobStatus.Scheduled)
      {
         var parentAfterInsert = await Storage.GetJobAsync(request.ParentJobId, ct);
         if (parentAfterInsert is not null && parentAfterInsert.Status.IsTerminal())
         {
            if (parentAfterInsert.Status == JobStatus.Succeeded)
            {
               await TriggerContinuationJobsAsync(request.ParentJobId, ct);
               SignalWakeup();
            }
            else
            {
               await HandleFailedOrCancelledParentContinuationsAsync(request.ParentJobId, parentAfterInsert.Status, ct);
               SignalWakeup();
            }
         }
      }
      else if (initialStatus == JobStatus.Enqueued)
      {
         SignalWakeup();
      }

      var latestRecord = await Storage.GetJobAsync(record.JobId, ct) ?? record;

      return new EnqueueJobResponse
      {
         Accepted = true,
         JobId = latestRecord.JobId,
         InitialStatus = latestRecord.Status,
         ScheduledAtUtc = latestRecord.ScheduledAtUtc
      };
   }

   /// <summary>
   /// Directly registers or updates a recurring Cron or interval job schedule.
   /// </summary>
   public async ValueTask<ScheduleRecurringJobResponse> ScheduleRecurringAsync(
      ScheduleRecurringJobRequest request,
      CancellationToken ct = default)
   {
      ArgumentNullException.ThrowIfNull(request);

      if (!CronSchedule.TryParse(request.CronExpression, out var cron))
      {
         return new ScheduleRecurringJobResponse
         {
            Accepted = false,
            RecurringJobId = request.RecurringJobId,
            ErrorMessage = $"Invalid cron expression: '{request.CronExpression}'."
         };
      }

      var now = DateTimeOffset.UtcNow;
      var nextRun = cron.GetNextOccurrence(now, inclusive: false);

      var record = new RecurringJobRecord
      {
         RecurringJobId = request.RecurringJobId,
         CronExpression = request.CronExpression,
         JobType = request.JobType,
         Queue = string.IsNullOrWhiteSpace(request.Queue) ? "default" : request.Queue,
         Payload = request.Payload ?? [],
         MisfirePolicy = request.MisfirePolicy,
         OverlapPolicy = request.OverlapPolicy,
         MaxAttempts = Math.Max(1, request.MaxAttempts),
         Timeout = request.Timeout,
         CreatedAtUtc = now,
         NextRunAtUtc = nextRun
      };

      await Storage.UpsertRecurringJobAsync(record, ct);
      SignalWakeup();

      return new ScheduleRecurringJobResponse
      {
         Accepted = true,
         RecurringJobId = record.RecurringJobId,
         NextRunAtUtc = nextRun
      };
   }

   private void SignalWakeup()
   {
      try
      {
         if (_tickSignal.CurrentCount == 0)
         {
            _tickSignal.Release();
         }
      }
      catch (ObjectDisposedException)
      {
      }
      catch (SemaphoreFullException)
      {
      }
   }

   private async ValueTask HandleIncomingMessageAsync(MessageReceiveContext context, CancellationToken ct)
   {
      var topic = context.Message.Topic;
      var topicSpan = topic.AsSpan();

      if (string.Equals(topic, JobTopics.EnqueueCommandTopic, StringComparison.Ordinal))
      {
         var req = Options.Serializer.Deserialize<EnqueueJobRequest>(context.Message.Payload.Span);
         if (req is not null)
         {
            var resp = await EnqueueAsync(req, ct);
            await context.RespondAsync(Options.Serializer.Serialize(resp), QualityOfServiceType.AtLeastOnce, ct);
         }
         return;
      }

      if (string.Equals(topic, JobTopics.RecurringCommandTopic, StringComparison.Ordinal))
      {
         var req = Options.Serializer.Deserialize<ScheduleRecurringJobRequest>(context.Message.Payload.Span);
         if (req is not null)
         {
            var resp = await ScheduleRecurringAsync(req, ct);
            await context.RespondAsync(Options.Serializer.Serialize(resp), QualityOfServiceType.AtLeastOnce, ct);
         }
         return;
      }

      if (JobTopics.TryParseCancelCommand(topicSpan, out var cancelJobId))
      {
         var cmd = Options.Serializer.Deserialize<JobCancelCommand>(context.Message.Payload.Span);
         await HandleCancelJobCommandAsync(cancelJobId, cmd?.Reason, ct);
         return;
      }

      if (JobTopics.TryParseWorkerEvent(topicSpan, out _, out _))
      {
         var workerEvent = Options.Serializer.Deserialize<JobWorkerEvent>(context.Message.Payload.Span);
         if (workerEvent is not null)
         {
            await HandleWorkerEventAsync(workerEvent, ct);
         }
         return;
      }

      if (JobTopics.TryParseWorkerHeartbeat(topicSpan, out _))
      {
         var heartbeat = Options.Serializer.Deserialize<WorkerHeartbeatPayload>(context.Message.Payload.Span);
         if (heartbeat is not null)
         {
            await HandleWorkerHeartbeatAsync(heartbeat, ct);
         }
      }
   }

   private async ValueTask HandleCancelJobCommandAsync(string jobId, string? reason, CancellationToken ct)
   {
      var job = await Storage.GetJobAsync(jobId, ct);
      if (job is null || job.Status.IsTerminal())
      {
         return;
      }

      // If the job is not actively running on a worker (e.g. Scheduled, Enqueued, AwaitingRetry, Dispatching), transition immediately
      if (job.Status is JobStatus.Scheduled or JobStatus.Enqueued or JobStatus.AwaitingRetry or JobStatus.Dispatching)
      {
         var updated = await Storage.TryUpdateJobAsync(
            jobId,
            r =>
            {
               if (r.Status is JobStatus.Scheduled or JobStatus.Enqueued or JobStatus.AwaitingRetry or JobStatus.Dispatching)
               {
                  r.Status = JobStatus.Cancelled;
                  r.LockedUntilUtc = null;
                  r.CompletedAtUtc = DateTimeOffset.UtcNow;
                  r.ErrorMessage = reason ?? "Job cancelled before execution.";
               }
            },
            allowIfTerminal: false,
            ct);

         if (updated is { Status: JobStatus.Cancelled })
         {
            JobMetrics.RecordJobCompleted(JobStatus.Cancelled);
            await PublishJobStatusAsync(updated, message: updated.ErrorMessage, ct: ct);

            if (Events.OnJobCancelled.Count > 0)
            {
               await Events.OnJobCancelled.ExecuteAsync(new JobSchedulerJobContext(updated), cancellationToken: ct);
            }

            await HandleFailedOrCancelledParentContinuationsAsync(updated.JobId, JobStatus.Cancelled, ct);
            SignalWakeup();
         }
      }
   }

   private async ValueTask HandleWorkerEventAsync(JobWorkerEvent evt, CancellationToken ct)
   {
      switch (evt.EventType)
      {
         case "started":
         {
            await Storage.TryUpdateJobAsync(
               evt.JobId,
               r =>
               {
                  if (r.Status is JobStatus.Dispatching or JobStatus.Enqueued or JobStatus.Running)
                  {
                     r.Status = JobStatus.Running;
                     r.StartedAtUtc ??= evt.TimestampUtc;
                     r.AssignedWorkerId = evt.WorkerId;
                     r.LockedUntilUtc = null;
                     r.ProgressPercent = 0;
                  }
               },
               allowIfTerminal: false,
               ct);
            break;
         }

         case "progress":
         {
            await Storage.TryUpdateJobAsync(
               evt.JobId,
               r =>
               {
                  if (r.Status == JobStatus.Running)
                  {
                     r.ProgressPercent = evt.ProgressPercent;
                     r.ProgressMessage = evt.Message;
                  }
               },
               allowIfTerminal: false,
               ct);
            break;
         }

         case "completed":
         {
            _workerRegistry.DecrementWorkerActiveCount(evt.WorkerId);

            var updated = await Storage.TryUpdateJobAsync(
               evt.JobId,
               r =>
               {
                  r.Status = JobStatus.Succeeded;
                  r.Attempt = Math.Max(r.Attempt, evt.Attempt);
                  r.AssignedWorkerId = evt.WorkerId;
                  r.StartedAtUtc ??= evt.TimestampUtc;
                  r.LockedUntilUtc = null;
                  r.ProgressPercent = 100;
                  r.ProgressMessage = evt.Message ?? "Completed";
                  r.ResultPayload = evt.ResultPayload;
                  r.CompletedAtUtc = evt.TimestampUtc;
               },
               allowIfTerminal: false,
               ct);

            if (updated is not null)
            {
               JobMetrics.RecordJobCompleted(JobStatus.Succeeded);
               await PublishJobStatusAsync(updated, message: updated.ProgressMessage, ct: ct);

               if (Events.OnJobSucceeded.Count > 0)
               {
                  await Events.OnJobSucceeded.ExecuteAsync(new JobSchedulerJobContext(updated), cancellationToken: ct);
               }

               await TriggerContinuationJobsAsync(updated.JobId, ct);
               SignalWakeup();
            }
            break;
         }

         case "cancelled":
         {
            _workerRegistry.DecrementWorkerActiveCount(evt.WorkerId);

            var updated = await Storage.TryUpdateJobAsync(
               evt.JobId,
               r =>
               {
                  r.Status = JobStatus.Cancelled;
                  r.Attempt = Math.Max(r.Attempt, evt.Attempt);
                  r.AssignedWorkerId = evt.WorkerId;
                  r.StartedAtUtc ??= evt.TimestampUtc;
                  r.LockedUntilUtc = null;
                  r.CompletedAtUtc = evt.TimestampUtc;
                  r.ErrorMessage = evt.Message ?? "Job cancelled";
               },
               allowIfTerminal: false,
               ct);

            if (updated is not null)
            {
               JobMetrics.RecordJobCompleted(JobStatus.Cancelled);
               await PublishJobStatusAsync(updated, message: updated.ErrorMessage, ct: ct);

               if (Events.OnJobCancelled.Count > 0)
               {
                  await Events.OnJobCancelled.ExecuteAsync(new JobSchedulerJobContext(updated), cancellationToken: ct);
               }

               await HandleFailedOrCancelledParentContinuationsAsync(updated.JobId, JobStatus.Cancelled, ct);
               SignalWakeup();
            }
            break;
         }

         case "failed":
         {
            _workerRegistry.DecrementWorkerActiveCount(evt.WorkerId);
            await HandleJobAttemptFailureAsync(evt.JobId, evt.ErrorMessage ?? "Job handler failed", ct, evt.Attempt, evt.WorkerId);
            SignalWakeup();
            break;
         }
      }
   }

   private async ValueTask HandleWorkerHeartbeatAsync(WorkerHeartbeatPayload heartbeat, CancellationToken ct)
   {
      var transitionedToOffline = _workerRegistry.UpdateFromHeartbeat(heartbeat, out var transitionedToOnline);

      if (heartbeat.IsOnline)
      {
         if (transitionedToOnline || heartbeat.ActiveJobs < heartbeat.MaxConcurrency)
         {
            SignalWakeup();
         }
         return;
      }

      // Worker reported offline (or broker fired Last Will and Testament on unexpected disconnect!)
      var orphanedJobs = await Storage.GetActiveJobsByWorkerAsync(heartbeat.WorkerId, ct);
      if (!transitionedToOffline && orphanedJobs.Count == 0)
      {
         return;
      }

      if (orphanedJobs.Count > 0)
      {
         JobMetrics.RecordWorkerCrash(heartbeat.WorkerId);

         if (Events.OnWorkerCrashed.Count > 0)
         {
            await Events.OnWorkerCrashed.ExecuteAsync(
               new JobSchedulerWorkerCrashedContext(heartbeat.WorkerId, orphanedJobs),
               cancellationToken: ct);
         }

         foreach (var orphan in orphanedJobs)
         {
            await HandleJobAttemptFailureAsync(
               orphan.JobId,
               $"Worker '{heartbeat.WorkerId}' disconnected unexpectedly (LWT crash recovery).",
               ct,
               orphan.Attempt,
               heartbeat.WorkerId);
         }

         SignalWakeup();
      }
   }

   private async ValueTask HandleJobAttemptFailureAsync(
      string jobId,
      string errorReason,
      CancellationToken ct,
      int? failedAttempt = null,
      string? workerId = null)
   {
      var current = await Storage.GetJobAsync(jobId, ct);
      if (current is null || current.Status.IsTerminal())
      {
         return;
      }

      var now = DateTimeOffset.UtcNow;
      var effectiveAttempt = Math.Max(current.Attempt, failedAttempt.GetValueOrDefault());
      if (effectiveAttempt <= 0)
      {
         effectiveAttempt = 1;
      }

      if (effectiveAttempt < current.MaxAttempts)
      {
         var retryDelay = Options.RetryPolicy.GetRetryDelay(effectiveAttempt);
         var nextRun = now.Add(retryDelay);

         var retrying = await Storage.TryUpdateJobAsync(
            jobId,
            r =>
            {
               r.Status = JobStatus.AwaitingRetry;
               r.Attempt = Math.Max(r.Attempt, effectiveAttempt);
               if (workerId is not null)
               {
                  r.AssignedWorkerId = workerId;
               }
               r.ScheduledAtUtc = nextRun;
               r.LockedUntilUtc = null;
               r.ErrorMessage = errorReason;
            },
            allowIfTerminal: false,
            ct);

         if (retrying is not null)
         {
            JobMetrics.RecordJobRetry(retrying.JobType, effectiveAttempt);
            await PublishJobStatusAsync(
               retrying,
               message: $"Retry scheduled after {retryDelay.TotalMilliseconds:F0}ms ({errorReason})",
               ct: ct);

            if (Events.OnJobRetryScheduled.Count > 0)
            {
               await Events.OnJobRetryScheduled.ExecuteAsync(
                  new JobSchedulerRetryContext(retrying, effectiveAttempt, retryDelay, errorReason),
                  cancellationToken: ct);
            }
         }
      }
      else
      {
         var deadLettered = await Storage.TryUpdateJobAsync(
            jobId,
            r =>
            {
               r.Status = JobStatus.DeadLettered;
               r.Attempt = Math.Max(r.Attempt, effectiveAttempt);
               if (workerId is not null)
               {
                  r.AssignedWorkerId = workerId;
               }
               r.CompletedAtUtc = now;
               r.LockedUntilUtc = null;
               r.ErrorMessage = errorReason;
            },
            allowIfTerminal: false,
            ct);

         if (deadLettered is not null)
         {
            JobMetrics.RecordJobCompleted(JobStatus.DeadLettered);
            await PublishJobStatusAsync(deadLettered, message: $"Exhausted {deadLettered.MaxAttempts} attempts: {errorReason}", ct: ct);

            if (Events.OnJobDeadLettered.Count > 0)
            {
               await Events.OnJobDeadLettered.ExecuteAsync(
                  new JobSchedulerFailedContext(deadLettered, errorReason),
                  cancellationToken: ct);
            }

            await HandleFailedOrCancelledParentContinuationsAsync(deadLettered.JobId, JobStatus.DeadLettered, ct);
         }
      }
   }

   private async ValueTask TriggerContinuationJobsAsync(string completedParentJobId, CancellationToken ct)
   {
      var continuations = await Storage.GetContinuationJobsAsync(completedParentJobId, ct);
      if (continuations.Count == 0)
      {
         return;
      }

      var now = DateTimeOffset.UtcNow;
      foreach (var child in continuations)
      {
         var activated = await Storage.TryTransitionStateAsync(
            child.JobId,
            JobStatus.Scheduled,
            JobStatus.Enqueued,
            expectedVersion: child.Version,
            mutate: r =>
            {
               if (r.ScheduledAtUtc < now)
               {
                  r.ScheduledAtUtc = now;
               }
            },
            ct: ct);

         if (activated is not null)
         {
            await PublishJobStatusAsync(activated, message: $"Continuation unlocked by parent '{completedParentJobId}'", ct: ct);
         }
      }
   }

   private async ValueTask HandleFailedOrCancelledParentContinuationsAsync(
      string parentJobId,
      JobStatus parentTerminalStatus,
      CancellationToken ct)
   {
      var continuations = await Storage.GetContinuationJobsAsync(parentJobId, ct);
      if (continuations.Count == 0)
      {
         return;
      }

      var now = DateTimeOffset.UtcNow;
      foreach (var child in continuations)
      {
         if (!string.IsNullOrEmpty(child.RecurringJobId))
         {
            // Recurring jobs chained via OverlapPolicy.QueueNext should still execute their next scheduled occurrence
            // after the previous run terminates, even if that previous run failed or was cancelled.
            var activated = await Storage.TryTransitionStateAsync(
               child.JobId,
               JobStatus.Scheduled,
               JobStatus.Enqueued,
               expectedVersion: child.Version,
               mutate: r =>
               {
                  if (r.ScheduledAtUtc < now)
                  {
                     r.ScheduledAtUtc = now;
                  }
               },
               ct: ct);

            if (activated is not null)
            {
               await PublishJobStatusAsync(
                  activated,
                  message: $"Queued recurring job unlocked after previous run '{parentJobId}' ended with '{parentTerminalStatus}'",
                  ct: ct);
            }

            continue;
         }

         var cancelledChild = await Storage.TryTransitionStateAsync(
            child.JobId,
            JobStatus.Scheduled,
            JobStatus.Cancelled,
            expectedVersion: child.Version,
            mutate: r =>
            {
               r.CompletedAtUtc = now;
               r.LockedUntilUtc = null;
               r.ErrorMessage = $"Cancelled because parent job '{parentJobId}' terminated with status '{parentTerminalStatus}'.";
            },
            ct: ct);

         if (cancelledChild is not null)
         {
            JobMetrics.RecordJobCompleted(JobStatus.Cancelled);
            await PublishJobStatusAsync(cancelledChild, message: cancelledChild.ErrorMessage, ct: ct);

            if (Events.OnJobCancelled.Count > 0)
            {
               await Events.OnJobCancelled.ExecuteAsync(new JobSchedulerJobContext(cancelledChild), cancellationToken: ct);
            }

            await HandleFailedOrCancelledParentContinuationsAsync(cancelledChild.JobId, JobStatus.Cancelled, ct);
         }
      }
   }

   private async Task RunSchedulerLoopAsync(CancellationToken ct)
   {
      while (!ct.IsCancellationRequested)
      {
         try
         {
            await _tickSignal.WaitAsync(Options.TickInterval, ct);
         }
         catch (OperationCanceledException) when (ct.IsCancellationRequested)
         {
            break;
         }

         if (ct.IsCancellationRequested)
         {
            break;
         }

         try
         {
            await EvaluateDueRecurringJobsAsync(ct);
            await DispatchDueJobsAsync(ct);
         }
         catch (OperationCanceledException) when (ct.IsCancellationRequested)
         {
            break;
         }
         catch
         {
         }
      }
   }

   private async ValueTask EvaluateDueRecurringJobsAsync(CancellationToken ct)
   {
      var now = DateTimeOffset.UtcNow;
      var dueRecurring = await Storage.GetDueRecurringJobsAsync(now, ct);
      if (dueRecurring.Count == 0)
      {
         return;
      }

      foreach (var rec in dueRecurring)
      {
         if (!CronSchedule.TryParse(rec.CronExpression, out var schedule))
         {
            continue;
         }

         // Check OverlapPolicy against the most recently spawned job instance
         string? parentContinuationId = null;
         if (!string.IsNullOrEmpty(rec.LastJobId) && rec.OverlapPolicy != OverlapPolicy.AllowConcurrent)
         {
            var lastJob = await Storage.GetJobAsync(rec.LastJobId, ct);
            if (lastJob is not null && !lastJob.Status.IsTerminal())
            {
               if (rec.OverlapPolicy == OverlapPolicy.SkipIfRunning)
               {
                  rec.NextRunAtUtc = schedule.GetNextOccurrence(now, inclusive: false);
                  await Storage.UpsertRecurringJobAsync(rec, ct);
                  continue;
               }

               if (rec.OverlapPolicy == OverlapPolicy.QueueNext)
               {
                  parentContinuationId = lastJob.JobId;
               }
            }
         }

         var occurrencesToFire = 1;
         if (rec.MisfirePolicy == MisfirePolicy.Skip &&
             rec.NextRunAtUtc.HasValue &&
             now - rec.NextRunAtUtc.Value > TimeSpan.FromSeconds(5))
         {
            occurrencesToFire = 0;
         }
         else if (rec.MisfirePolicy == MisfirePolicy.FireAllMissed && rec.NextRunAtUtc.HasValue)
         {
            var cursor = rec.NextRunAtUtc.Value;
            var count = 0;
            while (cursor <= now && count < Options.MaxMisfireCatchUpCount)
            {
               count++;
               var next = schedule.GetNextOccurrence(cursor, inclusive: false);
               if (!next.HasValue || next.Value <= cursor) break;
               cursor = next.Value;
            }
            occurrencesToFire = Math.Max(1, count);
         }

         for (var i = 0; i < occurrencesToFire; i++)
         {
            var enqueueReq = new EnqueueJobRequest
            {
               JobId = $"cron-{rec.RecurringJobId}-{Guid.NewGuid():N}",
               JobType = rec.JobType,
               Queue = rec.Queue,
               Payload = rec.Payload,
               ScheduledAtUtc = now,
               MaxAttempts = rec.MaxAttempts,
               Timeout = rec.Timeout,
               ParentJobId = parentContinuationId,
               RecurringJobId = rec.RecurringJobId
            };

            var enqueueResp = await EnqueueAsync(enqueueReq, ct);
            rec.LastJobId = enqueueResp.JobId;
            rec.LastRunAtUtc = now;
            parentContinuationId = rec.OverlapPolicy == OverlapPolicy.QueueNext ? enqueueResp.JobId : null;

            JobMetrics.RecordCronTriggered(rec.RecurringJobId);

            if (Events.OnRecurringJobTriggered.Count > 0)
            {
               var spawned = await Storage.GetJobAsync(enqueueResp.JobId, ct);
               if (spawned is not null)
               {
                  await Events.OnRecurringJobTriggered.ExecuteAsync(
                     new JobSchedulerRecurringTriggeredContext(rec, spawned),
                     cancellationToken: ct);
               }
            }
         }

         rec.NextRunAtUtc = schedule.GetNextOccurrence(now, inclusive: false);
         await Storage.UpsertRecurringJobAsync(rec, ct);
      }
   }

   private async ValueTask DispatchDueJobsAsync(CancellationToken ct)
   {
      if (_workerRegistry.TotalAvailableSlots == 0)
      {
         return;
      }

      if (!await _dispatchLock.WaitAsync(0, ct))
      {
         return;
      }

      var deferredJobs = new List<JobInstanceRecord>();

      try
      {
         while (_workerRegistry.TotalAvailableSlots > 0)
         {
            var now = DateTimeOffset.UtcNow;
            var claimedJobs = await Storage.ClaimDueJobsAsync(
               now,
               Options.DispatchBatchSize,
               Options.ClaimLockDuration,
               ct);

            if (claimedJobs.Count == 0)
            {
               break;
            }

            foreach (var job in claimedJobs)
            {
               var candidateWorkers = _workerRegistry.SelectCandidateWorkers(job.Queue, job.JobType);
               if (candidateWorkers.Count == 0)
               {
                  // Keep claimed until this dispatch pass finishes scanning all due batches,
                  // then release back to Enqueued in finally.
                  deferredJobs.Add(job);
                  continue;
               }

               var dispatched = false;
               var nextAttempt = job.Attempt + 1;

               foreach (var workerId in candidateWorkers)
               {
                  _workerRegistry.IncrementWorkerActiveCount(workerId);

                  var dispatchReq = new JobDispatchRequest
                  {
                     JobId = job.JobId,
                     JobType = job.JobType,
                     Queue = job.Queue,
                     Payload = job.Payload,
                     Attempt = nextAttempt,
                     MaxAttempts = job.MaxAttempts,
                     Timeout = job.Timeout,
                     Metadata = job.Metadata,
                     DispatchedAtUtc = DateTimeOffset.UtcNow
                  };

                  var dispatchTopic = JobTopics.FormatDispatch(job.Queue, workerId);
                  var reqBytes = Options.Serializer.Serialize(dispatchReq);

                  var rpcResult = await MqttClient.RequestAsync(
                     dispatchTopic,
                     reqBytes,
                     Options.DispatchAckTimeout,
                     ct);

                  if (rpcResult.Failed)
                  {
                     _workerRegistry.DecrementWorkerActiveCount(workerId);
                     continue;
                  }

                  var dispatchResp = Options.Serializer.Deserialize<JobDispatchResponse>(rpcResult.Success.Payload.Span);
                  if (dispatchResp is not { Accepted: true })
                  {
                     _workerRegistry.DecrementWorkerActiveCount(workerId);
                     continue;
                  }

                  dispatched = true;

                  var dispatchTimestamp = DateTimeOffset.UtcNow;
                  var queueLagMs = Math.Max(0, (dispatchTimestamp - job.ScheduledAtUtc).TotalMilliseconds);
                  JobMetrics.RecordJobDispatched(job.Queue, job.JobType, queueLagMs);

                  var runningRecord = await Storage.TryUpdateJobAsync(
                     job.JobId,
                     r =>
                     {
                        if (r.Status == JobStatus.Dispatching)
                        {
                           r.Status = JobStatus.Running;
                           r.LockedUntilUtc = null;
                        }

                        r.Attempt = Math.Max(r.Attempt, nextAttempt);
                        r.AssignedWorkerId ??= workerId;
                        r.StartedAtUtc ??= dispatchTimestamp;
                     },
                     allowIfTerminal: false,
                     ct);

                  if (Events.OnJobDispatched.Count > 0)
                  {
                     var dispatchedRecord = runningRecord ?? await Storage.GetJobAsync(job.JobId, ct);
                     if (dispatchedRecord is not null)
                     {
                        await Events.OnJobDispatched.ExecuteAsync(
                           new JobSchedulerDispatchedContext(dispatchedRecord, workerId),
                           cancellationToken: ct);
                     }
                  }

                  break;
               }

               if (!dispatched)
               {
                  deferredJobs.Add(job);
               }
            }
         }
      }
      finally
      {
         foreach (var deferred in deferredJobs)
         {
            try
            {
               await Storage.TryTransitionStateAsync(
                  deferred.JobId,
                  JobStatus.Dispatching,
                  JobStatus.Enqueued,
                  expectedVersion: deferred.Version,
                  mutate: r => r.LockedUntilUtc = null,
                  ct: CancellationToken.None);
            }
            catch
            {
               // Best-effort release; claim lock expiration will recover if storage throws
            }
         }

         _dispatchLock.Release();
      }
   }

   private async ValueTask PublishJobStatusAsync(
      JobInstanceRecord job,
      string? message = null,
      CancellationToken ct = default)
   {
      if (!MqttClient.IsConnected)
      {
         return;
      }

      var update = new JobStatusUpdate
      {
         JobId = job.JobId,
         JobType = job.JobType,
         Queue = job.Queue,
         Status = job.Status,
         Attempt = job.Attempt,
         MaxAttempts = job.MaxAttempts,
         ProgressPercent = job.ProgressPercent,
         Message = message ?? job.ProgressMessage,
         WorkerId = job.AssignedWorkerId,
         ResultPayload = job.ResultPayload,
         ErrorMessage = job.ErrorMessage,
         TimestampUtc = DateTimeOffset.UtcNow,
         SequenceNumber = Interlocked.Increment(ref _statusSequence)
      };

      var topicBytes = JobTopics.FormatStatusToBytes(job.JobId);
      var payloadBytes = Options.Serializer.Serialize(update);

      var pubOptions = PublishOptions.Create()
         .WithTopic(topicBytes)
         .WithPayload(payloadBytes)
         .WithQualityOfService(QualityOfServiceType.AtLeastOnce)
         .WithRetain(true)
         .Build();

      await MqttClient.PublishAsync(pubOptions, ct);
   }

   public async ValueTask DisposeAsync()
   {
      if (Interlocked.Exchange(ref _disposed, 1) != 0)
      {
         return;
      }

      await StopAsync();

      _tickSignal.Dispose();
      _dispatchLock.Dispose();

      if (_ownsMqttClient)
      {
         await MqttClient.DisposeAsync();
      }
   }
}
