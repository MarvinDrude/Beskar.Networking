# Job Storage & Custom Database Persistence

`Beskar.Jobs` decouples orchestration from persistence via the [`IJobStorage`](https://github.com/MarvinDrude/Beskar.Networking/blob/master/Jobs/Beskar.Jobs.Common/Interfaces/IJobStorage.cs) interface.

Out of the box, `JobScheduler` uses [`InMemoryJobStorage`](https://github.com/MarvinDrude/Beskar.Networking/blob/master/Jobs/Beskar.Jobs.Common/Storage/InMemoryJobStorage.cs) for zero-setup, high-throughput operation. When you need durable state across scheduler restarts, you can plug in any relational or NoSQL database (PostgreSQL, SQLite, SQL Server, Redis) by implementing `IJobStorage` and passing `.UseStorage(customStorage)` to `JobSchedulerBuilder`.

---

## 1. The `IJobStorage` Contract

```csharp
using Beskar.Jobs.Common.Enums;
using Beskar.Jobs.Common.Models;

namespace Beskar.Jobs.Common.Interfaces;

public interface IJobStorage
{
   // 1. Insert or update a job instance record
   public ValueTask UpsertJobAsync(JobInstanceRecord job, CancellationToken ct = default);

   // 2. Retrieve a snapshot of a job by its unique ID
   public ValueTask<JobInstanceRecord?> GetJobAsync(string jobId, CancellationToken ct = default);

   // 3. Atomically transition a job from expectedStatus (and optional expectedVersion) to newStatus
   public ValueTask<JobInstanceRecord?> TryTransitionStateAsync(
      string jobId,
      JobStatus expectedStatus,
      JobStatus newStatus,
      long? expectedVersion = null,
      Action<JobInstanceRecord>? mutate = null,
      CancellationToken ct = default);

   // 4. Atomically mutate an existing job record and increment its Version
   public ValueTask<JobInstanceRecord?> TryUpdateJobAsync(
      string jobId,
      Action<JobInstanceRecord> mutate,
      bool allowIfTerminal = false,
      CancellationToken ct = default);

   // 5. Atomically claim up to maxBatchSize due jobs (Scheduled, Enqueued, or AwaitingRetry with
   //    ScheduledAtUtc <= nowUtc and no unresolved ParentJobId) by transitioning them to Dispatching
   //    with LockedUntilUtc = nowUtc + lockDuration, ordered by ScheduledAtUtc ASC
   public ValueTask<IReadOnlyList<JobInstanceRecord>> ClaimDueJobsAsync(
      DateTimeOffset nowUtc,
      int maxBatchSize,
      TimeSpan lockDuration,
      CancellationToken ct = default);

   // 6. Return all active (Dispatching or Running) jobs assigned to a crashed workerId
   public ValueTask<IReadOnlyList<JobInstanceRecord>> GetActiveJobsByWorkerAsync(
      string workerId,
      CancellationToken ct = default);

   // 7. Return all child jobs waiting on parentJobId (Status == Scheduled && ParentJobId == parentJobId)
   public ValueTask<IReadOnlyList<JobInstanceRecord>> GetContinuationJobsAsync(
      string parentJobId,
      CancellationToken ct = default);

   // 8. Insert or update a recurring Cron job schedule
   public ValueTask UpsertRecurringJobAsync(RecurringJobRecord recurringJob, CancellationToken ct = default);

   // 9. Lookup a recurring schedule by ID
   public ValueTask<RecurringJobRecord?> GetRecurringJobAsync(string recurringJobId, CancellationToken ct = default);

   // 10. Return all non-paused recurring schedules whose NextRunAtUtc <= nowUtc
   public ValueTask<IReadOnlyList<RecurringJobRecord>> GetDueRecurringJobsAsync(
      DateTimeOffset nowUtc,
      CancellationToken ct = default);

   // 11. Delete a recurring schedule
   public ValueTask<bool> RemoveRecurringJobAsync(string recurringJobId, CancellationToken ct = default);

   // 12. Prune terminal jobs (Succeeded, Failed, Cancelled, DeadLettered) completed at or before completedBeforeUtc
   public ValueTask<int> PruneCompletedJobsAsync(
      DateTimeOffset completedBeforeUtc,
      CancellationToken ct = default);
}
```

---

## 2. Historical Job Retention & Pruning (`PruneCompletedJobsAsync`)

To prevent unbounded storage growth, call `IJobStorage.PruneCompletedJobsAsync` periodically (for example, from a background maintenance job or recurring Cron schedule):

```csharp
var storage = new InMemoryJobStorage();

await using var scheduler = JobSchedulerBuilder.Create()
   .UseStorage(storage)
   .UseTcp(endPoint)
   .Build();

// Remove terminal jobs (Succeeded, Failed, Cancelled, DeadLettered) completed more than 24 hours ago
int prunedCount = await storage.PruneCompletedJobsAsync(DateTimeOffset.UtcNow.AddHours(-24));
```

> [!NOTE]
> `PruneCompletedJobsAsync` only removes jobs where `Status` is terminal (`Succeeded`, `Failed`, `Cancelled`, or `DeadLettered`) and `CompletedAtUtc <= completedBeforeUtc`. Active or scheduled jobs are never pruned.

---

## 3. Implementing a Custom Database Storage Provider

Below is a reference blueprint showing how to map `IJobStorage` to a relational database (such as PostgreSQL or SQLite via Dapper/EF Core):

### Recommended SQL Schema & Indexes

```sql
CREATE TABLE beskar_jobs (
    job_id              TEXT PRIMARY KEY,
    job_type            TEXT NOT NULL,
    queue               TEXT NOT NULL,
    status              SMALLINT NOT NULL,
    payload             BYTEA NOT NULL,
    result_payload      BYTEA NULL,
    attempt             INT NOT NULL,
    max_attempts        INT NOT NULL,
    created_at_utc      TIMESTAMPTZ NOT NULL,
    scheduled_at_utc    TIMESTAMPTZ NOT NULL,
    started_at_utc      TIMESTAMPTZ NULL,
    completed_at_utc    TIMESTAMPTZ NULL,
    locked_until_utc    TIMESTAMPTZ NULL,
    timeout_ms          BIGINT NULL,
    assigned_worker_id  TEXT NULL,
    parent_job_id       TEXT NULL,
    recurring_job_id    TEXT NULL,
    progress_percent    INT NOT NULL DEFAULT 0,
    progress_message    TEXT NULL,
    error_message       TEXT NULL,
    metadata_json       TEXT NULL,
    version             BIGINT NOT NULL DEFAULT 1
);

-- Fast index for ClaimDueJobsAsync(nowUtc, maxBatchSize, lockDuration)
CREATE INDEX ix_beskar_jobs_due
    ON beskar_jobs (scheduled_at_utc ASC)
    WHERE status IN (0, 1, 2, 4);

-- Fast index for GetActiveJobsByWorkerAsync(workerId) on LWT crash recovery
CREATE INDEX ix_beskar_jobs_worker
    ON beskar_jobs (assigned_worker_id)
    WHERE status IN (2, 3);

-- Fast index for GetContinuationJobsAsync(parentJobId)
CREATE INDEX ix_beskar_jobs_continuations
    ON beskar_jobs (parent_job_id)
    WHERE status = 0 AND parent_job_id IS NOT NULL;
```

### Plugging Custom Storage into `JobSchedulerBuilder`

```csharp
await using var scheduler = JobSchedulerBuilder.Create()
   .UseStorage(new PostgresJobStorage(connectionString))
   .WithTickInterval(TimeSpan.FromMilliseconds(50))
   .UseTcp(endPoint)
   .Build();

await scheduler.StartAsync();
```
