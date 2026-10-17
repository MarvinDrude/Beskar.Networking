using Beskar.Jobs.Common.Enums;

namespace Beskar.Jobs.Common.Models;

/// <summary>
/// Real-time status and progress notification published to <c>jobs/status/{jobId}</c>.
/// </summary>
public sealed record JobStatusUpdate
{
   /// <summary>
   /// Unique job identifier.
   /// </summary>
   public required string JobId { get; init; }

   /// <summary>
   /// Logical job handler type.
   /// </summary>
   public required string JobType { get; init; }

   /// <summary>
   /// Target queue name.
   /// </summary>
   public string Queue { get; init; } = "default";

   /// <summary>
   /// Current status of the job.
   /// </summary>
   public required JobStatus Status { get; init; }

   /// <summary>
   /// Execution attempt number (1-based).
   /// </summary>
   public int Attempt { get; init; }

   /// <summary>
   /// Maximum configured attempts.
   /// </summary>
   public int MaxAttempts { get; init; }

   /// <summary>
   /// Completion progress percentage (0..100).
   /// </summary>
   public int ProgressPercent { get; init; }

   /// <summary>
   /// Optional progress or status message.
   /// </summary>
   public string? Message { get; init; }

   /// <summary>
   /// Worker node ID assigned to the job, if any.
   /// </summary>
   public string? WorkerId { get; init; }

   /// <summary>
   /// Optional serialized result payload when <see cref="Status"/> is <see cref="JobStatus.Succeeded"/>.
   /// </summary>
   public byte[]? ResultPayload { get; init; }

   /// <summary>
   /// Optional error message if failed, awaiting retry, or dead-lettered.
   /// </summary>
   public string? ErrorMessage { get; init; }

   /// <summary>
   /// UTC timestamp when this status update was emitted.
   /// </summary>
   public DateTimeOffset TimestampUtc { get; init; } = DateTimeOffset.UtcNow;

   /// <summary>
   /// Monotonically increasing sequence/version number for ordering status updates.
   /// </summary>
   public long SequenceNumber { get; init; }

   /// <summary>
   /// True if this status is terminal (<c>Succeeded</c>, <c>Failed</c>, <c>Cancelled</c>, or <c>DeadLettered</c>).
   /// </summary>
   public bool IsTerminal => Status.IsTerminal();
}
