using Beskar.Jobs.Common.Enums;

namespace Beskar.Jobs.Common.Models;

/// <summary>
/// Represents the persistent state and execution metadata of a single job instance.
/// </summary>
public sealed class JobInstanceRecord
{
   /// <summary>
   /// Unique job identifier.
   /// </summary>
   public required string JobId { get; init; }

   /// <summary>
   /// Logical job handler type name (e.g. "video.transcode").
   /// </summary>
   public required string JobType { get; init; }

   /// <summary>
   /// Target execution queue name. Defaults to "default".
   /// </summary>
   public string Queue { get; init; } = "default";

   /// <summary>
   /// Serialized argument payload bytes.
   /// </summary>
   public byte[] Payload { get; init; } = [];

   /// <summary>
   /// Current lifecycle status of the job.
   /// </summary>
   public JobStatus Status { get; set; } = JobStatus.Enqueued;

   /// <summary>
   /// Optimistic concurrency version counter incremented on every state mutation.
   /// </summary>
   public long Version { get; set; } = 1;

   /// <summary>
   /// UTC timestamp when the job was originally created.
   /// </summary>
   public DateTimeOffset CreatedAtUtc { get; init; } = DateTimeOffset.UtcNow;

   /// <summary>
   /// UTC timestamp when the job is eligible to be claimed and dispatched.
   /// </summary>
   public DateTimeOffset ScheduledAtUtc { get; set; } = DateTimeOffset.UtcNow;

   /// <summary>
   /// UTC timestamp when a worker started executing the job.
   /// </summary>
   public DateTimeOffset? StartedAtUtc { get; set; }

   /// <summary>
   /// UTC timestamp when the job reached a terminal state.
   /// </summary>
   public DateTimeOffset? CompletedAtUtc { get; set; }

   /// <summary>
   /// UTC timestamp when the scheduler's dispatch claim lock expires if not acknowledged.
   /// </summary>
   public DateTimeOffset? LockedUntilUtc { get; set; }

   /// <summary>
   /// Current execution attempt counter (1-based when running).
   /// </summary>
   public int Attempt { get; set; }

   /// <summary>
   /// Maximum allowed execution attempts before moving to DeadLettered.
   /// </summary>
   public int MaxAttempts { get; init; } = 3;

   /// <summary>
   /// Optional per-attempt execution timeout.
   /// </summary>
   public TimeSpan? Timeout { get; init; }

   /// <summary>
   /// Identifier of the worker currently or most recently executing this job.
   /// </summary>
   public string? AssignedWorkerId { get; set; }

   /// <summary>
   /// Latest reported completion percentage (0..100).
   /// </summary>
   public int ProgressPercent { get; set; }

   /// <summary>
   /// Latest human-readable progress message reported by the worker.
   /// </summary>
   public string? ProgressMessage { get; set; }

   /// <summary>
   /// Optional serialized result payload produced by the job handler upon completion.
   /// </summary>
   public byte[]? ResultPayload { get; set; }

   /// <summary>
   /// Last error message recorded if the job failed or was retried.
   /// </summary>
   public string? ErrorMessage { get; set; }

   /// <summary>
   /// Optional recurring schedule ID if this instance was spawned by a Cron schedule.
   /// </summary>
   public string? RecurringJobId { get; init; }

   /// <summary>
   /// Optional parent job ID if this job is a continuation that waits for the parent to succeed.
   /// </summary>
   public string? ParentJobId { get; init; }

   /// <summary>
   /// Custom user metadata headers attached to the job.
   /// </summary>
   public Dictionary<string, string>? Metadata { get; init; }

   /// <summary>
   /// Creates a deep snapshot copy of this record for safe concurrent storage reads/writes.
   /// </summary>
   public JobInstanceRecord Clone() => new()
   {
      JobId = JobId,
      JobType = JobType,
      Queue = Queue,
      Payload = [.. Payload.AsSpan()],
      Status = Status,
      Version = Version,
      CreatedAtUtc = CreatedAtUtc,
      ScheduledAtUtc = ScheduledAtUtc,
      StartedAtUtc = StartedAtUtc,
      CompletedAtUtc = CompletedAtUtc,
      LockedUntilUtc = LockedUntilUtc,
      Attempt = Attempt,
      MaxAttempts = MaxAttempts,
      Timeout = Timeout,
      AssignedWorkerId = AssignedWorkerId,
      ProgressPercent = ProgressPercent,
      ProgressMessage = ProgressMessage,
      ResultPayload = ResultPayload?.AsSpan().ToArray(),
      ErrorMessage = ErrorMessage,
      RecurringJobId = RecurringJobId,
      ParentJobId = ParentJobId,
      Metadata = Metadata is not null ? new Dictionary<string, string>(Metadata, StringComparer.Ordinal) : null
   };
}
