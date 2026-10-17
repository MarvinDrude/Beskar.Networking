using Beskar.Jobs.Common.Enums;

namespace Beskar.Jobs.Common.Models;

/// <summary>
/// Represents a recurring Cron or interval job schedule definition.
/// </summary>
public sealed class RecurringJobRecord
{
   /// <summary>
   /// Unique identifier for the recurring schedule (e.g. "nightly-cleanup").
   /// </summary>
   public required string RecurringJobId { get; init; }

   /// <summary>
   /// 5-field, 6-field, or macro Cron expression (e.g. "0 */5 * * * *" or "@every 10s").
   /// </summary>
   public required string CronExpression { get; set; }

   /// <summary>
   /// Logical job handler type to invoke on each occurrence.
   /// </summary>
   public required string JobType { get; set; }

   /// <summary>
   /// Target queue name for spawned job instances.
   /// </summary>
   public string Queue { get; set; } = "default";

   /// <summary>
   /// Serialized argument payload passed to each spawned job instance.
   /// </summary>
   public byte[] Payload { get; set; } = [];

   /// <summary>
   /// Behavior when one or more scheduled occurrences were missed.
   /// </summary>
   public MisfirePolicy MisfirePolicy { get; set; } = MisfirePolicy.FireOnceNow;

   /// <summary>
   /// Behavior when the previous instance of this recurring job is still running.
   /// </summary>
   public OverlapPolicy OverlapPolicy { get; set; } = OverlapPolicy.SkipIfRunning;

   /// <summary>
   /// Maximum execution attempts per spawned job instance.
   /// </summary>
   public int MaxAttempts { get; set; } = 3;

   /// <summary>
   /// Optional execution timeout per spawned job instance.
   /// </summary>
   public TimeSpan? Timeout { get; set; }

   /// <summary>
   /// Whether this recurring job schedule is currently paused.
   /// </summary>
   public bool IsPaused { get; set; }

   /// <summary>
   /// UTC timestamp when the schedule was created.
   /// </summary>
   public DateTimeOffset CreatedAtUtc { get; init; } = DateTimeOffset.UtcNow;

   /// <summary>
   /// UTC timestamp when the last job instance was triggered.
   /// </summary>
   public DateTimeOffset? LastRunAtUtc { get; set; }

   /// <summary>
   /// UTC timestamp of the next scheduled execution.
   /// </summary>
   public DateTimeOffset? NextRunAtUtc { get; set; }

   /// <summary>
   /// Job ID of the most recently spawned job instance.
   /// </summary>
   public string? LastJobId { get; set; }

   /// <summary>
   /// Creates a deep copy of this recurring job record.
   /// </summary>
   public RecurringJobRecord Clone() => new()
   {
      RecurringJobId = RecurringJobId,
      CronExpression = CronExpression,
      JobType = JobType,
      Queue = Queue,
      Payload = [.. Payload.AsSpan()],
      MisfirePolicy = MisfirePolicy,
      OverlapPolicy = OverlapPolicy,
      MaxAttempts = MaxAttempts,
      Timeout = Timeout,
      IsPaused = IsPaused,
      CreatedAtUtc = CreatedAtUtc,
      LastRunAtUtc = LastRunAtUtc,
      NextRunAtUtc = NextRunAtUtc,
      LastJobId = LastJobId
   };
}
