using System.Buffers;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text;
using Beskar.Jobs.Common.Enums;
using Beskar.Jobs.Common.Models;
using Beskar.Jobs.Common.Telemetry;
using Beskar.Jobs.Common.Topics;
using Beskar.Jobs.Worker.Events;
using Beskar.Jobs.Worker.Options;
using Beskar.Memory.Results;
using Beskar.Memory.Results.Errors;
using Beskar.Mqtt.Common.Builders.Connecting;
using Beskar.Mqtt.Common.Builders.Disconnecting;
using Beskar.Mqtt.Common.Builders.Publishing;
using Beskar.Mqtt.Common.Builders.Subscribing;
using Beskar.Mqtt.Common.Handlers.Contexts;
using Beskar.Mqtt.Common.Interfaces;
using Beskar.Mqtt.Protocol.Enums;

namespace Beskar.Jobs.Worker;

/// <summary>
/// Distributed MQTT job execution worker supporting concurrency slots, cooperative cancellation,
/// real-time progress streaming, and MQTT Last-Will (LWT) crash notifications.
/// </summary>
public sealed class JobWorker : IAsyncDisposable
{
   private readonly Dictionary<string, JobHandlerExecutor> _handlers;
   private readonly ConcurrentDictionary<string, CancellationTokenSource> _runningJobTokens = new(StringComparer.Ordinal);
   private readonly ConcurrentDictionary<string, Task> _runningJobTasks = new(StringComparer.Ordinal);
   private readonly bool _ownsMqttClient;

   private IDisposable? _messageHandlerToken;
   private CancellationTokenSource? _workerCts;
   private Task? _heartbeatLoopTask;
   private int _activeJobs;
   private long _statusSequence;
   private int _started;
   private int _disposed;

   public IMqttClient MqttClient { get; }
   public JobWorkerOptions Options { get; }
   public JobWorkerEvents Events { get; } = new();

   /// <summary>
   /// Current count of jobs actively executing on this worker.
   /// </summary>
   public int ActiveJobCount => Volatile.Read(ref _activeJobs);

   internal JobWorker(
      IMqttClient mqttClient,
      JobWorkerOptions options,
      Dictionary<string, JobHandlerExecutor> handlers,
      bool ownsMqttClient)
   {
      MqttClient = mqttClient;
      Options = options;

      _handlers = handlers;
      _ownsMqttClient = ownsMqttClient;
   }

   /// <summary>
   /// Connects to the MQTT broker with Last Will configured, subscribes to dispatch and cancellation topics,
   /// and publishes an online capacity heartbeat.
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
            return new StringError("JobWorkerOptions.EndPoint must be set when starting an unconnected IMqttClient.");
         }

         _workerCts = new CancellationTokenSource();
         _messageHandlerToken = MqttClient.AddMessageReceiveHandler(HandleIncomingMessageAsync);

         if (!MqttClient.IsConnected)
         {
            var offlineWill = new WorkerHeartbeatPayload
            {
               WorkerId = Options.WorkerId,
               IsOnline = false,
               Queues = [.. Options.Queues],
               SupportedJobTypes = [.. _handlers.Keys],
               ActiveJobs = 0,
               MaxConcurrency = Options.MaxConcurrency,
               TimestampUtc = DateTimeOffset.UtcNow
            };

            var willTopicBytes = JobTopics.FormatWorkerHeartbeatToBytes(Options.WorkerId);
            var willPayloadBytes = Options.Serializer.Serialize(offlineWill);

            var connectOptions = new ConnectOptions
            {
               EndPoint = Options.EndPoint!,
               ProtocolVersion = MqttProtocolVersion.V50,
               ClientIdUtf8Bytes = Encoding.UTF8.GetBytes(Options.WorkerId),
               CleanSession = true,
               KeepAlivePeriod = Options.KeepAlivePeriodSeconds,
               HasWill = true,
               WillTopicUtf8Bytes = willTopicBytes,
               WillPayload = new ReadOnlySequence<byte>(willPayloadBytes),
               WillQualityOfService = QualityOfServiceType.AtLeastOnce,
               WillRetain = true
            };

            var connectResult = await MqttClient.ConnectAsync(connectOptions, ct);
            if (connectResult.Failed)
            {
               return connectResult.Error;
            }
         }

         var subBuilder = SubscribeOptions.Create()
            .WithTopicFilter(JobTopics.CancelCommandWildcard, QualityOfServiceType.AtLeastOnce);

         if (Options.Queues.Count == 0)
         {
            Options.Queues.Add("default");
         }

         foreach (var queue in Options.Queues.Distinct(StringComparer.Ordinal))
         {
            var dispatchTopic = JobTopics.FormatDispatch(queue, Options.WorkerId);
            subBuilder.WithTopicFilter(dispatchTopic, QualityOfServiceType.AtLeastOnce);
         }

         var subResult = await MqttClient.SubscribeAsync(subBuilder.Build(), ct);
         if (subResult.Failed)
         {
            if (_ownsMqttClient && MqttClient.IsConnected)
            {
               await MqttClient.DisconnectAsync(new DisconnectOptions(), ct);
            }

            return subResult.Error;
         }

         await PublishHeartbeatAsync(isOnline: true, ct);

         if (Options.HeartbeatInterval > TimeSpan.Zero)
         {
            _heartbeatLoopTask = Task.Run(() => RunHeartbeatLoopAsync(_workerCts.Token), CancellationToken.None);
         }

         startedSuccessfully = true;
         return new VoidResult<StringError>();
      }
      finally
      {
         if (!startedSuccessfully)
         {
            _messageHandlerToken?.Dispose();
            _messageHandlerToken = null;

            _workerCts?.Dispose();
            _workerCts = null;

            Interlocked.Exchange(ref _started, 0);
         }
      }
   }

   /// <summary>
   /// Gracefully stops the worker, publishes an offline heartbeat, and disconnects from the broker.
   /// </summary>
   public async ValueTask StopAsync(CancellationToken ct = default)
   {
      if (Interlocked.Exchange(ref _started, 0) == 0)
      {
         return;
      }

      if (_workerCts is not null)
      {
         await _workerCts.CancelAsync();
      }

      if (_heartbeatLoopTask is not null)
      {
         try
         {
            await _heartbeatLoopTask;
         }
         catch (OperationCanceledException)
         {
         }
      }

      foreach (var kvp in _runningJobTokens)
      {
         try
         {
            await kvp.Value.CancelAsync();
         }
         catch
         {
            // ignored
         }
      }

      var activeTasks = _runningJobTasks.Values.ToArray();
      if (activeTasks.Length > 0)
      {
         try
         {
            await Task.WhenAll(activeTasks).WaitAsync(TimeSpan.FromSeconds(5), ct);
         }
         catch
         {
            // ignored
         }
      }

      _messageHandlerToken?.Dispose();
      _messageHandlerToken = null;

      if (MqttClient.IsConnected)
      {
         try
         {
            await PublishHeartbeatAsync(isOnline: false, ct);
         }
         catch
         {
            // ignored
         }

         if (_ownsMqttClient)
         {
            await MqttClient.DisconnectAsync(new DisconnectOptions(), ct);
         }
      }

      _workerCts?.Dispose();
      _workerCts = null;
   }

   /// <summary>
   /// Simulates an abrupt worker crash by cancelling in-flight work and dropping the underlying MQTT transport
   /// without sending a graceful DISCONNECT packet, causing the MQTT broker to immediately fire the worker's Last Will (LWT).
   /// </summary>
   public async ValueTask SimulateCrashAsync()
   {
      Interlocked.Exchange(ref _started, 0);
      Interlocked.Exchange(ref _disposed, 1);

      if (_workerCts is not null)
      {
         await _workerCts.CancelAsync();
      }

      foreach (var kvp in _runningJobTokens)
      {
         try
         {
            await kvp.Value.CancelAsync();
         }
         catch
         {
            // ignored
         }
      }

      _messageHandlerToken?.Dispose();
      _messageHandlerToken = null;

      // Disposing MqttClient directly without calling DisconnectAsync drops the socket abruptly and triggers LWT on the broker
      await MqttClient.DisposeAsync();
   }

   private async ValueTask HandleIncomingMessageAsync(MessageReceiveContext context, CancellationToken ct)
   {
      var topicSpan = context.Message.Topic.AsSpan();

      if (JobTopics.TryParseDispatch(topicSpan, out _, out var targetWorkerId))
      {
         if (!string.Equals(targetWorkerId, Options.WorkerId, StringComparison.Ordinal))
         {
            return;
         }

         await HandleDispatchRequestAsync(context, ct);
         return;
      }

      if (JobTopics.TryParseCancelCommand(topicSpan, out var cancelJobId))
      {
         if (_runningJobTokens.TryGetValue(cancelJobId, out var jobCts))
         {
            try
            {
               await jobCts.CancelAsync();
            }
            catch
            {
               // ignored
            }
         }
      }
   }

   private async ValueTask HandleDispatchRequestAsync(MessageReceiveContext context, CancellationToken ct)
   {
      var request = Options.Serializer.Deserialize<JobDispatchRequest>(context.Message.Payload.Span);
      if (request is null)
      {
         return;
      }

      if (!_handlers.TryGetValue(request.JobType, out var executor))
      {
         var reject = new JobDispatchResponse
         {
            Accepted = false,
            JobId = request.JobId,
            WorkerId = Options.WorkerId,
            RejectReason = $"Worker '{Options.WorkerId}' does not have a registered handler for job type '{request.JobType}'."
         };

         await context.RespondAsync(Options.Serializer.Serialize(reject), QualityOfServiceType.AtLeastOnce, ct);
         return;
      }

      // Atomically reserve a concurrency slot
      while (true)
      {
         var current = Volatile.Read(ref _activeJobs);
         if (current >= Options.MaxConcurrency)
         {
            var busyReject = new JobDispatchResponse
            {
               Accepted = false,
               JobId = request.JobId,
               WorkerId = Options.WorkerId,
               RejectReason = $"Worker '{Options.WorkerId}' is at maximum concurrency ({Options.MaxConcurrency})."
            };

            await context.RespondAsync(Options.Serializer.Serialize(busyReject), QualityOfServiceType.AtLeastOnce, ct);
            return;
         }

         if (Interlocked.CompareExchange(ref _activeJobs, current + 1, current) == current)
         {
            break;
         }
      }

      JobMetrics.RecordActiveJobsChange(1);

      var acceptResponse = new JobDispatchResponse
      {
         Accepted = true,
         JobId = request.JobId,
         WorkerId = Options.WorkerId
      };

      await context.RespondAsync(Options.Serializer.Serialize(acceptResponse), QualityOfServiceType.AtLeastOnce, ct);

      var executionTask = Task.Run(() => ExecuteClaimedJobAsync(request, executor), CancellationToken.None);
      _runningJobTasks[request.JobId] = executionTask;
   }

   private async Task ExecuteClaimedJobAsync(JobDispatchRequest request, JobHandlerExecutor executor)
   {
      var sw = Stopwatch.StartNew();
      using var cancelCts = _workerCts is not null
         ? CancellationTokenSource.CreateLinkedTokenSource(_workerCts.Token)
         : new CancellationTokenSource();

      using var timeoutCts = request.Timeout is { TotalMilliseconds: > 0 } timeout
         ? new CancellationTokenSource(timeout)
         : null;

      using var combinedCts = timeoutCts is not null
         ? CancellationTokenSource.CreateLinkedTokenSource(cancelCts.Token, timeoutCts.Token)
         : null;

      var executionToken = combinedCts?.Token ?? cancelCts.Token;
      _runningJobTokens[request.JobId] = cancelCts;
      var slotReleased = false;

      void ReleaseActiveSlot()
      {
         if (slotReleased)
         {
            return;
         }

         slotReleased = true;
         _runningJobTokens.TryRemove(request.JobId, out _);

         Interlocked.Decrement(ref _activeJobs);
         JobMetrics.RecordActiveJobsChange(-1);
      }

      try
      {
         // ReSharper disable once MethodSupportsCancellation
         await PublishWorkerEventAsync(
            request.JobId,
            "started",
            request.Attempt,
            progressPercent: 0,
            message: "Job execution started");

         // ReSharper disable once MethodSupportsCancellation
         await PublishStatusUpdateAsync(
            request,
            JobStatus.Running,
            progressPercent: 0,
            message: "Job execution started");

         if (Events.OnJobStarted.Count > 0)
         {
            await Events.OnJobStarted.ExecuteAsync(
               new JobWorkerStartedContext(Options.WorkerId, request.JobId, request.JobType, request.Queue, request.Attempt),
               cancellationToken: CancellationToken.None);
         }

         var resultBytes = await executor(
            request,
            (percent, msg, progressCt) => ReportJobProgressAsync(request, percent, msg, progressCt),
            executionToken);

         sw.Stop();
         var durationMs = sw.Elapsed.TotalMilliseconds;

         ReleaseActiveSlot();
         JobMetrics.RecordExecutionDuration(request.JobType, succeeded: true, durationMs);

         // ReSharper disable once MethodSupportsCancellation
         await PublishWorkerEventAsync(
            request.JobId,
            "completed",
            request.Attempt,
            progressPercent: 100,
            message: "Job completed successfully",
            resultPayload: resultBytes,
            durationMs: durationMs);

         if (Events.OnJobCompleted.Count > 0)
         {
            await Events.OnJobCompleted.ExecuteAsync(
               new JobWorkerCompletedContext(
                  Options.WorkerId,
                  request.JobId,
                  request.JobType,
                  request.Queue,
                  request.Attempt,
                  durationMs),
               cancellationToken: CancellationToken.None);
         }
      }
      catch (OperationCanceledException) when (cancelCts.IsCancellationRequested && (_workerCts is null || !_workerCts.IsCancellationRequested))
      {
         sw.Stop();
         var durationMs = sw.Elapsed.TotalMilliseconds;

         ReleaseActiveSlot();
         JobMetrics.RecordExecutionDuration(request.JobType, succeeded: false, durationMs);

         // ReSharper disable once MethodSupportsCancellation
         await PublishWorkerEventAsync(
            request.JobId,
            "cancelled",
            request.Attempt,
            progressPercent: 0,
            message: "Job cancelled cooperatively",
            durationMs: durationMs);

         if (Events.OnJobCancelled.Count > 0)
         {
            await Events.OnJobCancelled.ExecuteAsync(
               new JobWorkerCancelledContext(Options.WorkerId, request.JobId, request.JobType, request.Queue, request.Attempt),
               cancellationToken: CancellationToken.None);
         }
      }
      catch (Exception ex) when (_workerCts is null || !_workerCts.IsCancellationRequested)
      {
         sw.Stop();
         var durationMs = sw.Elapsed.TotalMilliseconds;

         ReleaseActiveSlot();
         JobMetrics.RecordExecutionDuration(request.JobType, succeeded: false, durationMs);

         var errorMsg = timeoutCts is { IsCancellationRequested: true }
            ? $"Job timed out after {request.Timeout?.TotalMilliseconds:F0}ms."
            : ex.Message;

         // ReSharper disable once MethodSupportsCancellation
         await PublishWorkerEventAsync(
            request.JobId,
            "failed",
            request.Attempt,
            progressPercent: 0,
            errorMessage: errorMsg,
            durationMs: durationMs);

         if (Events.OnJobFailed.Count > 0)
         {
            await Events.OnJobFailed.ExecuteAsync(
               new JobWorkerFailedContext(
                  Options.WorkerId,
                  request.JobId,
                  request.JobType,
                  request.Queue,
                  request.Attempt,
                  errorMsg,
                  durationMs),
               cancellationToken: CancellationToken.None);
         }
      }
      finally
      {
         ReleaseActiveSlot();
         _runningJobTasks.TryRemove(request.JobId, out _);

         if (Volatile.Read(ref _started) == 1 && MqttClient.IsConnected)
         {
            try
            {
               await PublishHeartbeatAsync(isOnline: true, CancellationToken.None);
            }
            catch
            {
               // ignored
            }
         }
      }
   }

   private async ValueTask ReportJobProgressAsync(
      JobDispatchRequest request,
      int progressPercent,
      string? message,
      CancellationToken ct)
   {
      await PublishStatusUpdateAsync(request, JobStatus.Running, progressPercent, message, ct: ct);
      await PublishWorkerEventAsync(request.JobId, "progress", request.Attempt, progressPercent, message, ct: ct);
   }

   private async ValueTask PublishStatusUpdateAsync(
      JobDispatchRequest request,
      JobStatus status,
      int progressPercent,
      string? message = null,
      byte[]? resultPayload = null,
      string? errorMessage = null,
      CancellationToken ct = default)
   {
      if (!MqttClient.IsConnected)
      {
         return;
      }

      var update = new JobStatusUpdate
      {
         JobId = request.JobId,
         JobType = request.JobType,
         Queue = request.Queue,
         Status = status,
         Attempt = request.Attempt,
         MaxAttempts = request.MaxAttempts,
         ProgressPercent = progressPercent,
         Message = message,
         WorkerId = Options.WorkerId,
         ResultPayload = resultPayload,
         ErrorMessage = errorMessage,
         TimestampUtc = DateTimeOffset.UtcNow,
         SequenceNumber = Interlocked.Increment(ref _statusSequence)
      };

      var topicBytes = JobTopics.FormatStatusToBytes(request.JobId);
      var payloadBytes = Options.Serializer.Serialize(update);

      var pubOptions = PublishOptions.Create()
         .WithTopic(topicBytes)
         .WithPayload(payloadBytes)
         .WithQualityOfService(QualityOfServiceType.AtLeastOnce)
         .WithRetain()
         .Build();

      await MqttClient.PublishAsync(pubOptions, ct);
   }

   private async ValueTask PublishWorkerEventAsync(
      string jobId,
      string eventType,
      int attempt,
      int progressPercent = 0,
      string? message = null,
      byte[]? resultPayload = null,
      string? errorMessage = null,
      double durationMs = 0,
      CancellationToken ct = default)
   {
      if (!MqttClient.IsConnected)
      {
         return;
      }

      var workerEvent = new JobWorkerEvent
      {
         JobId = jobId,
         WorkerId = Options.WorkerId,
         EventType = eventType,
         Attempt = attempt,
         ProgressPercent = progressPercent,
         Message = message,
         ResultPayload = resultPayload,
         ErrorMessage = errorMessage,
         DurationMs = durationMs,
         TimestampUtc = DateTimeOffset.UtcNow
      };

      var topicBytes = JobTopics.FormatWorkerEventToBytes(jobId, eventType);
      var payloadBytes = Options.Serializer.Serialize(workerEvent);

      var pubOptions = PublishOptions.Create()
         .WithTopic(topicBytes)
         .WithPayload(payloadBytes)
         .WithQualityOfService(QualityOfServiceType.AtLeastOnce)
         .Build();

      await MqttClient.PublishAsync(pubOptions, ct);
   }

   private async ValueTask PublishHeartbeatAsync(bool isOnline, CancellationToken ct)
   {
      if (!MqttClient.IsConnected)
      {
         return;
      }

      var heartbeat = new WorkerHeartbeatPayload
      {
         WorkerId = Options.WorkerId,
         IsOnline = isOnline,
         Queues = Options.Queues.ToArray(),
         SupportedJobTypes = _handlers.Keys.ToArray(),
         ActiveJobs = Volatile.Read(ref _activeJobs),
         MaxConcurrency = Options.MaxConcurrency,
         TimestampUtc = DateTimeOffset.UtcNow
      };

      var topicBytes = JobTopics.FormatWorkerHeartbeatToBytes(Options.WorkerId);
      var payloadBytes = Options.Serializer.Serialize(heartbeat);

      var pubOptions = PublishOptions.Create()
         .WithTopic(topicBytes)
         .WithPayload(payloadBytes)
         .WithQualityOfService(QualityOfServiceType.AtLeastOnce)
         .WithRetain()
         .Build();

      await MqttClient.PublishAsync(pubOptions, ct);
   }

   private async Task RunHeartbeatLoopAsync(CancellationToken ct)
   {
      using var timer = new PeriodicTimer(Options.HeartbeatInterval);
      while (await timer.WaitForNextTickAsync(ct))
      {
         if (MqttClient.IsConnected)
         {
            try
            {
               await PublishHeartbeatAsync(isOnline: true, ct);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
               break;
            }
            catch
            {
               // ignored
            }
         }
      }
   }

   public async ValueTask DisposeAsync()
   {
      if (Interlocked.Exchange(ref _disposed, 1) != 0)
      {
         return;
      }

      await StopAsync();

      if (_ownsMqttClient)
      {
         await MqttClient.DisposeAsync();
      }
   }
}


internal delegate ValueTask<byte[]?> JobHandlerExecutor(
   JobDispatchRequest dispatchRequest,
   Func<int, string?, CancellationToken, ValueTask> progressReporter,
   CancellationToken ct);
