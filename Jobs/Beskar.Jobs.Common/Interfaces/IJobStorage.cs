using Beskar.Jobs.Common.Enums;
using Beskar.Jobs.Common.Models;

namespace Beskar.Jobs.Common.Interfaces;

/// <summary>
/// Pluggable persistence abstraction for storing job instances, state transitions, and recurring Cron schedules.
/// Designed to support both in-memory/file storage and external SQL/NoSQL databases with optimistic concurrency.
/// </summary>
public interface IJobStorage
{
   /// <summary>
   /// Inserts or updates a job instance record.
   /// </summary>
   public ValueTask UpsertJobAsync(JobInstanceRecord job, CancellationToken ct = default);

   /// <summary>
   /// Retrieves a snapshot of a job instance by its identifier, or <c>null</c> if not found.
   /// </summary>
   public ValueTask<JobInstanceRecord?> GetJobAsync(string jobId, CancellationToken ct = default);

   /// <summary>
   /// Atomically transitions a job from <paramref name="expectedStatus"/> (and optional <paramref name="expectedVersion"/>)
   /// to <paramref name="newStatus"/>, applying an optional mutation callback and incrementing <see cref="JobInstanceRecord.Version"/>.
   /// </summary>
   public ValueTask<JobInstanceRecord?> TryTransitionStateAsync(
      string jobId,
      JobStatus expectedStatus,
      JobStatus newStatus,
      long? expectedVersion = null,
      Action<JobInstanceRecord>? mutate = null,
      CancellationToken ct = default);

   /// <summary>
   /// Atomically mutates an existing job record regardless of current status (unless terminal when <paramref name="allowIfTerminal"/> is false)
   /// and increments its <see cref="JobInstanceRecord.Version"/>.
   /// </summary>
   public ValueTask<JobInstanceRecord?> TryUpdateJobAsync(
      string jobId,
      Action<JobInstanceRecord> mutate,
      bool allowIfTerminal = false,
      CancellationToken ct = default);

   /// <summary>
   /// Atomically claims up to <paramref name="maxBatchSize"/> due jobs (whose status is <see cref="JobStatus.Scheduled"/>,
   /// <see cref="JobStatus.Enqueued"/>, or <see cref="JobStatus.AwaitingRetry"/> with <c>ScheduledAtUtc &lt;= nowUtc</c>,
   /// and no unresolved <c>ParentJobId</c>) by transitioning them to <see cref="JobStatus.Dispatching"/> with a lease lock.
   /// </summary>
   public ValueTask<IReadOnlyList<JobInstanceRecord>> ClaimDueJobsAsync(
      DateTimeOffset nowUtc,
      int maxBatchSize,
      TimeSpan lockDuration,
      CancellationToken ct = default);

   /// <summary>
   /// Retrieves all jobs currently in <see cref="JobStatus.Dispatching"/> or <see cref="JobStatus.Running"/> assigned to the specified worker.
   /// Used for immediate LWT crash recovery.
   /// </summary>
   public ValueTask<IReadOnlyList<JobInstanceRecord>> GetActiveJobsByWorkerAsync(
      string workerId,
      CancellationToken ct = default);

   /// <summary>
   /// Retrieves child continuation jobs that are waiting on <paramref name="parentJobId"/> to complete.
   /// </summary>
   public ValueTask<IReadOnlyList<JobInstanceRecord>> GetContinuationJobsAsync(
      string parentJobId,
      CancellationToken ct = default);

   /// <summary>
   /// Inserts or updates a recurring Cron job definition.
   /// </summary>
   public ValueTask UpsertRecurringJobAsync(RecurringJobRecord recurringJob, CancellationToken ct = default);

   /// <summary>
   /// Gets a recurring job definition by its identifier.
   /// </summary>
   public ValueTask<RecurringJobRecord?> GetRecurringJobAsync(string recurringJobId, CancellationToken ct = default);

   /// <summary>
   /// Retrieves all non-paused recurring jobs whose <see cref="RecurringJobRecord.NextRunAtUtc"/> is <c>&lt;= nowUtc</c>.
   /// </summary>
   public ValueTask<IReadOnlyList<RecurringJobRecord>> GetDueRecurringJobsAsync(
      DateTimeOffset nowUtc,
      CancellationToken ct = default);

   /// <summary>
   /// Removes a recurring job definition by its identifier.
   /// </summary>
   public ValueTask<bool> RemoveRecurringJobAsync(string recurringJobId, CancellationToken ct = default);

   /// <summary>
   /// Removes terminal jobs completed before <paramref name="completedBeforeUtc"/> and returns the number of pruned records.
   /// </summary>
   public ValueTask<int> PruneCompletedJobsAsync(DateTimeOffset completedBeforeUtc, CancellationToken ct = default);
}
