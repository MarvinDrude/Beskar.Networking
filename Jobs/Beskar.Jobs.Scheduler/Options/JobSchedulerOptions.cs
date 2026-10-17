using System.Net;
using Beskar.Jobs.Common.Models;
using Beskar.Jobs.Common.Serialization;

namespace Beskar.Jobs.Scheduler.Options;

/// <summary>
/// Configuration options for <see cref="JobScheduler"/>.
/// </summary>
public sealed class JobSchedulerOptions
{
   /// <summary>
   /// Unique identifier for this scheduler coordinator instance.
   /// </summary>
   public string SchedulerId { get; set; } = $"scheduler-{Guid.NewGuid():N}";

   /// <summary>
   /// Target MQTT broker endpoint when connecting an owned <c>IMqttClient</c>.
   /// </summary>
   public IPEndPoint? EndPoint { get; set; }

   /// <summary>
   /// Background polling/evaluation interval for due Cron schedules and delayed jobs. Defaults to 50ms.
   /// Immediate jobs and worker heartbeats also wake the scheduler loop immediately without waiting for a tick.
   /// </summary>
   public TimeSpan TickInterval { get; set; } = TimeSpan.FromMilliseconds(50);

   /// <summary>
   /// Maximum number of due jobs claimed in a single scheduling batch. Defaults to 64.
   /// </summary>
   public int DispatchBatchSize { get; set; } = 64;

   /// <summary>
   /// Timeout when waiting for a worker to acknowledge a job dispatch RPC. Defaults to 5 seconds.
   /// </summary>
   public TimeSpan DispatchAckTimeout { get; set; } = TimeSpan.FromSeconds(5);

   /// <summary>
   /// Lease duration held on a job while in <c>Dispatching</c> state before it becomes eligible for reclaim.
   /// </summary>
   public TimeSpan ClaimLockDuration { get; set; } = TimeSpan.FromSeconds(15);

   /// <summary>
   /// Default retry and backoff policy applied when a job fails or its worker crashes.
   /// </summary>
   public RetryPolicy RetryPolicy { get; set; } = RetryPolicy.Default;

   /// <summary>
   /// Safety cap on the maximum number of missed occurrences fired at once when using <c>MisfirePolicy.FireAllMissed</c>.
   /// </summary>
   public int MaxMisfireCatchUpCount { get; set; } = 10;

   /// <summary>
   /// Serializer used for job arguments, results, and control payloads.
   /// </summary>
   public IJobPayloadSerializer Serializer { get; set; } = JsonJobPayloadSerializer.Default;
}
