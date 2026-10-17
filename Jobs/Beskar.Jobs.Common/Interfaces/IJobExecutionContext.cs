namespace Beskar.Jobs.Common.Interfaces;

/// <summary>
/// Provides execution metadata, deserialized arguments, and live progress reporting to a running job handler.
/// </summary>
public interface IJobExecutionContext<out TArgs>
{
   /// <summary>
   /// Unique job instance identifier.
   /// </summary>
   public string JobId { get; }

   /// <summary>
   /// Logical job handler type name.
   /// </summary>
   public string JobType { get; }

   /// <summary>
   /// Queue on which the job was dispatched.
   /// </summary>
   public string Queue { get; }

   /// <summary>
   /// Current execution attempt (1-based).
   /// </summary>
   public int Attempt { get; }

   /// <summary>
   /// Maximum configured execution attempts for this job.
   /// </summary>
   public int MaxAttempts { get; }

   /// <summary>
   /// Identifier of the worker executing this job.
   /// </summary>
   public string WorkerId { get; }

   /// <summary>
   /// Deserialized job arguments payload.
   /// </summary>
   public TArgs Arguments { get; }

   /// <summary>
   /// Optional custom user metadata headers attached to the job.
   /// </summary>
   public IReadOnlyDictionary<string, string> Metadata { get; }

   /// <summary>
   /// Publishes a real-time progress update (0..100%) to both the scheduler and any subscribers watching <c>jobs/status/{jobId}</c>.
   /// </summary>
   public ValueTask ReportProgressAsync(int progressPercent, string? message = null, CancellationToken ct = default);
}
