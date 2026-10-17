using System.Diagnostics.Metrics;
using Beskar.Jobs.Common.Enums;

namespace Beskar.Jobs.Common.Telemetry;

/// <summary>
/// Contains <see cref="System.Diagnostics.Metrics"/> telemetry meters and instruments for
/// schedulers, workers, and clients.
/// </summary>
public static class JobMetrics
{
   /// <summary>
   /// The meter name for Beskar.Jobs metrics.
   /// </summary>
   public const string MeterName = "Beskar.Jobs";

   /// <summary>
   /// Meter instance for Beskar.Jobs telemetry.
   /// </summary>
   public static readonly Meter Meter = new(MeterName, "1.0.0");

   /// <summary>
   /// Total number of jobs enqueued or scheduled.
   /// </summary>
   public static readonly Counter<long> JobsEnqueued = Meter.CreateCounter<long>(
      "beskar.jobs.enqueued",
      "{job}",
      "Total number of jobs enqueued or scheduled.");

   /// <summary>
   /// Total number of jobs dispatched and claimed by workers.
   /// </summary>
   public static readonly Counter<long> JobsDispatched = Meter.CreateCounter<long>(
      "beskar.jobs.dispatched",
      "{job}",
      "Total number of jobs dispatched and claimed by workers.");

   /// <summary>
   /// Current number of actively running jobs across workers.
   /// </summary>
   public static readonly UpDownCounter<long> JobsActive = Meter.CreateUpDownCounter<long>(
      "beskar.jobs.active",
      "{job}",
      "Current number of actively executing jobs.");

   /// <summary>
   /// Total number of jobs reaching a terminal state (tagged by status: succeeded, failed, cancelled, dead_lettered).
   /// </summary>
   public static readonly Counter<long> JobsCompleted = Meter.CreateCounter<long>(
      "beskar.jobs.completed",
      "{job}",
      "Total number of jobs reaching a terminal state.");

   /// <summary>
   /// Total number of job retry attempts scheduled after failure or worker crash.
   /// </summary>
   public static readonly Counter<long> JobRetries = Meter.CreateCounter<long>(
      "beskar.jobs.retries",
      "{retry}",
      "Total number of job retry attempts scheduled.");

   /// <summary>
   /// Total number of recurring Cron job triggers fired.
   /// </summary>
   public static readonly Counter<long> CronTriggers = Meter.CreateCounter<long>(
      "beskar.jobs.cron.triggered",
      "{trigger}",
      "Total number of recurring Cron job occurrences triggered.");

   /// <summary>
   /// Total number of unexpected worker crashes detected via MQTT Last Will (LWT).
   /// </summary>
   public static readonly Counter<long> WorkerCrashes = Meter.CreateCounter<long>(
      "beskar.jobs.worker.crashes",
      "{crash}",
      "Total number of unexpected worker disconnects/crashes detected.");

   /// <summary>
   /// Current count of online workers registered with the scheduler.
   /// </summary>
   public static readonly UpDownCounter<long> WorkersOnline = Meter.CreateUpDownCounter<long>(
      "beskar.jobs.workers.online",
      "{worker}",
      "Current count of online workers.");

   /// <summary>
   /// Execution duration of job handlers in milliseconds.
   /// </summary>
   public static readonly Histogram<double> ExecutionDuration = Meter.CreateHistogram<double>(
      "beskar.jobs.execution.duration",
      "ms",
      "Duration of job execution on workers in milliseconds.");

   /// <summary>
   /// Queue waiting lag from ScheduledAtUtc to actual worker dispatch in milliseconds.
   /// </summary>
   public static readonly Histogram<double> QueueLag = Meter.CreateHistogram<double>(
      "beskar.jobs.queue.lag",
      "ms",
      "Queue waiting lag from scheduled due time to worker dispatch in milliseconds.");

   private static readonly KeyValuePair<string, object?>[] TagSucceeded = [new("status", "succeeded")];
   private static readonly KeyValuePair<string, object?>[] TagFailed = [new("status", "failed")];
   private static readonly KeyValuePair<string, object?>[] TagCancelled = [new("status", "cancelled")];
   private static readonly KeyValuePair<string, object?>[] TagDeadLettered = [new("status", "dead_lettered")];

   public static void RecordJobEnqueued(string queue, string jobType)
   {
      if (JobsEnqueued.Enabled)
      {
         JobsEnqueued.Add(
            1,
            new KeyValuePair<string, object?>("queue", queue),
            new KeyValuePair<string, object?>("job.type", jobType));
      }
   }

   public static void RecordJobDispatched(string queue, string jobType, double queueLagMs)
   {
      if (JobsDispatched.Enabled)
      {
         JobsDispatched.Add(
            1,
            new KeyValuePair<string, object?>("queue", queue),
            new KeyValuePair<string, object?>("job.type", jobType));
      }

      if (QueueLag.Enabled && queueLagMs >= 0)
      {
         QueueLag.Record(
            queueLagMs,
            new KeyValuePair<string, object?>("queue", queue),
            new KeyValuePair<string, object?>("job.type", jobType));
      }
   }

   public static void RecordActiveJobsChange(int delta)
   {
      if (JobsActive.Enabled && delta != 0)
      {
         JobsActive.Add(delta);
      }
   }

   public static void RecordJobCompleted(JobStatus terminalStatus)
   {
      if (!JobsCompleted.Enabled)
      {
         return;
      }

      var tags = terminalStatus switch
      {
         JobStatus.Succeeded => TagSucceeded,
         JobStatus.Cancelled => TagCancelled,
         JobStatus.DeadLettered => TagDeadLettered,
         _ => TagFailed
      };

      JobsCompleted.Add(1, tags);
   }

   public static void RecordJobRetry(string jobType, int attempt)
   {
      if (JobRetries.Enabled)
      {
         JobRetries.Add(
            1,
            new KeyValuePair<string, object?>("job.type", jobType),
            new KeyValuePair<string, object?>("attempt", attempt));
      }
   }

   public static void RecordCronTriggered(string recurringJobId)
   {
      if (CronTriggers.Enabled)
      {
         CronTriggers.Add(1, new KeyValuePair<string, object?>("recurring.id", recurringJobId));
      }
   }

   public static void RecordWorkerCrash(string workerId)
   {
      if (WorkerCrashes.Enabled)
      {
         WorkerCrashes.Add(1, new KeyValuePair<string, object?>("worker.id", workerId));
      }
   }

   public static void RecordWorkersOnlineChange(int delta)
   {
      if (WorkersOnline.Enabled && delta != 0)
      {
         WorkersOnline.Add(delta);
      }
   }

   public static void RecordExecutionDuration(string jobType, bool succeeded, double durationMs)
   {
      if (ExecutionDuration.Enabled)
      {
         ExecutionDuration.Record(
            durationMs,
            new KeyValuePair<string, object?>("job.type", jobType),
            new KeyValuePair<string, object?>("status", succeeded ? "succeeded" : "failed"));
      }
   }
}
