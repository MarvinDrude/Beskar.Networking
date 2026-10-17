using System.Collections.Concurrent;
using Beskar.Jobs.Common.Enums;
using Beskar.Jobs.Common.Interfaces;
using Beskar.Jobs.Common.Models;

namespace Beskar.Jobs.Common.Storage;

/// <summary>
/// Thread-safe, low-allocation in-memory implementation of <see cref="IJobStorage"/> supporting
/// optimistic concurrency version checks, lease locking, recurring Cron schedules, and parent-child continuations.
/// </summary>
public sealed class InMemoryJobStorage : IJobStorage
{
   private readonly ConcurrentDictionary<string, JobInstanceRecord> _jobs = new(StringComparer.Ordinal);
   private readonly ConcurrentDictionary<string, RecurringJobRecord> _recurringJobs = new(StringComparer.Ordinal);
   private readonly Lock _syncLock = new();

   /// <summary>
   /// Total number of job instances currently stored.
   /// </summary>
   public int JobCount => _jobs.Count;

   /// <summary>
   /// Total number of recurring job definitions currently stored.
   /// </summary>
   public int RecurringJobCount => _recurringJobs.Count;

   public ValueTask UpsertJobAsync(JobInstanceRecord job, CancellationToken ct = default)
   {
      ArgumentNullException.ThrowIfNull(job);

      lock (_syncLock)
      {
         var clone = job.Clone();
         if (_jobs.TryGetValue(job.JobId, out var existing))
         {
            clone.Version = Math.Max(existing.Version + 1, clone.Version);
         }

         _jobs[job.JobId] = clone;
      }

      return ValueTask.CompletedTask;
   }

   public ValueTask<JobInstanceRecord?> GetJobAsync(string jobId, CancellationToken ct = default)
   {
      lock (_syncLock)
      {
         return ValueTask.FromResult(_jobs.TryGetValue(jobId, out var record) ? record.Clone() : null);
      }
   }

   public ValueTask<JobInstanceRecord?> TryTransitionStateAsync(
      string jobId,
      JobStatus expectedStatus,
      JobStatus newStatus,
      long? expectedVersion = null,
      Action<JobInstanceRecord>? mutate = null,
      CancellationToken ct = default)
   {
      lock (_syncLock)
      {
         if (!_jobs.TryGetValue(jobId, out var record))
         {
            return ValueTask.FromResult<JobInstanceRecord?>(null);
         }

         if (record.Status != expectedStatus)
         {
            return ValueTask.FromResult<JobInstanceRecord?>(null);
         }

         if (expectedVersion.HasValue && record.Version != expectedVersion.Value)
         {
            return ValueTask.FromResult<JobInstanceRecord?>(null);
         }

         record.Status = newStatus;
         record.Version++;
         mutate?.Invoke(record);

         return ValueTask.FromResult<JobInstanceRecord?>(record.Clone());
      }
   }

   public ValueTask<JobInstanceRecord?> TryUpdateJobAsync(
      string jobId,
      Action<JobInstanceRecord> mutate,
      bool allowIfTerminal = false,
      CancellationToken ct = default)
   {
      ArgumentNullException.ThrowIfNull(mutate);

      lock (_syncLock)
      {
         if (!_jobs.TryGetValue(jobId, out var record))
         {
            return ValueTask.FromResult<JobInstanceRecord?>(null);
         }

         if (!allowIfTerminal && record.Status.IsTerminal())
         {
            return ValueTask.FromResult<JobInstanceRecord?>(null);
         }

         mutate(record);
         record.Version++;

         return ValueTask.FromResult<JobInstanceRecord?>(record.Clone());
      }
   }

   public ValueTask<IReadOnlyList<JobInstanceRecord>> ClaimDueJobsAsync(
      DateTimeOffset nowUtc,
      int maxBatchSize,
      TimeSpan lockDuration,
      CancellationToken ct = default)
   {
      if (maxBatchSize <= 0)
      {
         return ValueTask.FromResult<IReadOnlyList<JobInstanceRecord>>(Array.Empty<JobInstanceRecord>());
      }

      lock (_syncLock)
      {
         var candidates = new List<JobInstanceRecord>();

         foreach (var kvp in _jobs)
         {
            var job = kvp.Value;

            // If a job has a ParentJobId and is still Scheduled, wait until parent succeeds (which transitions child to Enqueued)
            if (!string.IsNullOrEmpty(job.ParentJobId)
                && job.Status == JobStatus.Scheduled)
            {
               continue;
            }

            var isEligibleDue = job.Status is JobStatus.Scheduled or JobStatus.Enqueued or JobStatus.AwaitingRetry
                                && job.ScheduledAtUtc <= nowUtc;

            var isExpiredDispatchLock = job is { Status: JobStatus.Dispatching, LockedUntilUtc: not null }
                                        && job.LockedUntilUtc.Value <= nowUtc;

            if (isEligibleDue || isExpiredDispatchLock)
            {
               candidates.Add(job);
            }
         }

         if (candidates.Count == 0)
         {
            return ValueTask.FromResult<IReadOnlyList<JobInstanceRecord>>(Array.Empty<JobInstanceRecord>());
         }

         candidates.Sort(static (a, b) => a.ScheduledAtUtc.CompareTo(b.ScheduledAtUtc));

         var claimCount = Math.Min(maxBatchSize, candidates.Count);
         var result = new List<JobInstanceRecord>(claimCount);
         var lockExpiry = nowUtc.Add(lockDuration);

         for (var i = 0; i < claimCount; i++)
         {
            var job = candidates[i];
            job.Status = JobStatus.Dispatching;
            job.LockedUntilUtc = lockExpiry;
            job.Version++;
            result.Add(job.Clone());
         }

         return ValueTask.FromResult<IReadOnlyList<JobInstanceRecord>>(result);
      }
   }

   public ValueTask<IReadOnlyList<JobInstanceRecord>> GetActiveJobsByWorkerAsync(
      string workerId,
      CancellationToken ct = default)
   {
      if (string.IsNullOrWhiteSpace(workerId))
      {
         return ValueTask.FromResult<IReadOnlyList<JobInstanceRecord>>(Array.Empty<JobInstanceRecord>());
      }

      lock (_syncLock)
      {
         var active = new List<JobInstanceRecord>();
         foreach (var kvp in _jobs)
         {
            var job = kvp.Value;
            if ((job.Status is JobStatus.Running or JobStatus.Dispatching) &&
                string.Equals(job.AssignedWorkerId, workerId, StringComparison.Ordinal))
            {
               active.Add(job.Clone());
            }
         }

         return ValueTask.FromResult<IReadOnlyList<JobInstanceRecord>>(active);
      }
   }

   public ValueTask<IReadOnlyList<JobInstanceRecord>> GetContinuationJobsAsync(
      string parentJobId,
      CancellationToken ct = default)
   {
      if (string.IsNullOrWhiteSpace(parentJobId))
      {
         return ValueTask.FromResult<IReadOnlyList<JobInstanceRecord>>(Array.Empty<JobInstanceRecord>());
      }

      lock (_syncLock)
      {
         var continuations = new List<JobInstanceRecord>();
         foreach (var kvp in _jobs)
         {
            var job = kvp.Value;
            if (job.Status == JobStatus.Scheduled &&
                string.Equals(job.ParentJobId, parentJobId, StringComparison.Ordinal))
            {
               continuations.Add(job.Clone());
            }
         }

         return ValueTask.FromResult<IReadOnlyList<JobInstanceRecord>>(continuations);
      }
   }

   public ValueTask UpsertRecurringJobAsync(RecurringJobRecord recurringJob, CancellationToken ct = default)
   {
      ArgumentNullException.ThrowIfNull(recurringJob);

      lock (_syncLock)
      {
         _recurringJobs[recurringJob.RecurringJobId] = recurringJob.Clone();
      }

      return ValueTask.CompletedTask;
   }

   public ValueTask<RecurringJobRecord?> GetRecurringJobAsync(string recurringJobId, CancellationToken ct = default)
   {
      lock (_syncLock)
      {
         return ValueTask.FromResult(
            _recurringJobs.TryGetValue(recurringJobId, out var rec) ? rec.Clone() : null);
      }
   }

   public ValueTask<IReadOnlyList<RecurringJobRecord>> GetDueRecurringJobsAsync(
      DateTimeOffset nowUtc,
      CancellationToken ct = default)
   {
      lock (_syncLock)
      {
         var due = new List<RecurringJobRecord>();
         foreach (var kvp in _recurringJobs)
         {
            var rec = kvp.Value;

            if (!rec.IsPaused && rec.NextRunAtUtc.HasValue && rec.NextRunAtUtc.Value <= nowUtc)
            {
               due.Add(rec.Clone());
            }
         }

         return ValueTask.FromResult<IReadOnlyList<RecurringJobRecord>>(due);
      }
   }

   public ValueTask<bool> RemoveRecurringJobAsync(string recurringJobId, CancellationToken ct = default)
   {
      lock (_syncLock)
      {
         return ValueTask.FromResult(_recurringJobs.TryRemove(recurringJobId, out _));
      }
   }

   public ValueTask<int> PruneCompletedJobsAsync(DateTimeOffset completedBeforeUtc, CancellationToken ct = default)
   {
      var pruned = 0;
      lock (_syncLock)
      {
         foreach (var (key, job) in _jobs)
         {
            if (job.Status.IsTerminal() &&
                job.CompletedAtUtc.HasValue &&
                job.CompletedAtUtc.Value < completedBeforeUtc)
            {
               if (_jobs.TryRemove(key, out _))
               {
                  pruned++;
               }
            }
         }
      }

      return ValueTask.FromResult(pruned);
   }
}
