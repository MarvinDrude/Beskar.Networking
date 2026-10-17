# Job Storage & Custom Database Persistence

`Beskar.Jobs` decouples orchestration from persistence via the [`IJobStorage`](https://github.com/MarvinDrude/Beskar.Networking/blob/master/Jobs/Beskar.Jobs.Common/Interfaces/IJobStorage.cs) interface.

Out of the box, `JobScheduler` uses [`InMemoryJobStorage`](https://github.com/MarvinDrude/Beskar.Networking/blob/master/Jobs/Beskar.Jobs.Common/Storage/InMemoryJobStorage.cs) for zero-setup, high-throughput operation. When you need durable state across scheduler restarts, you can plug in any relational or NoSQL database (PostgreSQL, SQLite, SQL Server, Redis) by implementing `IJobStorage` and passing `.WithStorage(customStorage)` to `JobSchedulerBuilder`.

---

## 1. The `IJobStorage` Contract

```csharp
using Beskar.Jobs.Common.Models;

namespace Beskar.Jobs.Common.Interfaces;

public interface IJobStorage
{
   // 1. Insert or update a job instance record
   public ValueTask SaveJobAsync(JobInstanceRecord job, CancellationToken ct = default);

   // 2. Lookup a job by its unique ID
   public ValueTask<JobInstanceRecord?> GetJobAsync(string jobId, CancellationToken ct = default);

   // 3. Return up to maxCount jobs in Enqueued, Scheduled, or AwaitingRetry state
   //    whose ScheduledAtUtc <= nowUtc and ParentJobId is null, ordered by ScheduledAtUtc ASC
   public ValueTask<IReadOnlyList<JobInstanceRecord>> GetDueJobsAsync(
      DateTimeOffset nowUtc,
      int maxCount,
      CancellationToken ct = default);

   // 4. Return all active (Dispatching or Running) jobs assigned to a crashed workerId
   public ValueTask<IReadOnlyList<JobInstanceRecord>> GetJobsByWorkerAsync(
      string workerId,
      CancellationToken ct = default);

   // 5. Return all child jobs waiting on parentJobId (Status == Scheduled && ParentJobId == parentJobId)
   public ValueTask<IReadOnlyList<JobInstanceRecord>> GetReadyContinuationsAsync(
      string parentJobId,
      CancellationToken ct = default);

   // 6. Insert or update a recurring Cron job schedule
   public ValueTask SaveRecurringJobAsync(RecurringJobRecord recurringJob, CancellationToken ct = default);

   // 7. Lookup a recurring schedule by ID
   public ValueTask<RecurringJobRecord?> GetRecurringJobAsync(string recurringJobId, CancellationToken ct = default);

   // 8. Delete a recurring schedule
   public ValueTask<bool> RemoveRecurringJobAsync(string recurringJobId, CancellationToken ct = default);

   // 9. Return all recurring schedules whose NextRunAtUtc <= nowUtc
   public ValueTask<IReadOnlyList<RecurringJobRecord>> GetDueRecurringJobsAsync(
      DateTimeOffset nowUtc,
      CancellationToken ct = default);

   // 10. Prune terminal jobs (Succeeded, Failed, Cancelled, DeadLettered) older than completedBeforeUtc
   public ValueTask<int> PruneHistoricalJobsAsync(
      DateTimeOffset completedBeforeUtc,
      CancellationToken ct = default);
}
```

---

## 2. Historical Job Retention & Automatic Pruning

To prevent unbounded storage growth, `JobScheduler` automatically runs `IJobStorage.PruneHistoricalJobsAsync` on a periodic timer:

```csharp
var scheduler = JobSchedulerBuilder.Create()
   .WithStorage(new InMemoryJobStorage())
   .WithOptions(opts =>
   {
      // Retain terminal jobs (Succeeded, Failed, Cancelled) for 24 hours before pruning
      opts.HistoricalJobRetention = TimeSpan.FromHours(24);
   })
   .UseTcp(endPoint)
   .Build();
```

> [!NOTE]
> `PruneHistoricalJobsAsync` only removes jobs where `Status` is terminal (`Succeeded`, `Failed`, `Cancelled`, or `DeadLettered`) and `CompletedAtUtc <= completedBeforeUtc`. Active or scheduled jobs are never pruned.

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
    timeout_ms          BIGINT NULL,
    assigned_worker_id  TEXT NULL,
    parent_job_id       TEXT NULL,
    recurring_job_id    TEXT NULL,
    progress_percent    INT NOT NULL DEFAULT 0,
    status_message      TEXT NULL,
    last_error          TEXT NULL,
    metadata_json       TEXT NULL
);

-- Fast index for GetDueJobsAsync(nowUtc, maxCount)
CREATE INDEX ix_beskar_jobs_due
    ON beskar_jobs (scheduled_at_utc ASC)
    WHERE status IN (0, 1, 4) AND parent_job_id IS NULL;

-- Fast index for GetJobsByWorkerAsync(workerId) on LWT crash recovery
CREATE INDEX ix_beskar_jobs_worker
    ON beskar_jobs (assigned_worker_id)
    WHERE status IN (2, 3);

-- Fast index for GetReadyContinuationsAsync(parentJobId)
CREATE INDEX ix_beskar_jobs_continuations
    ON beskar_jobs (parent_job_id)
    WHERE status = 0 AND parent_job_id IS NOT NULL;
```

### Plugging Custom Storage into `JobSchedulerBuilder`

```csharp
await using var scheduler = JobSchedulerBuilder.Create()
   .WithStorage(new PostgresJobStorage(connectionString))
   .WithTickInterval(TimeSpan.FromMilliseconds(50))
   .UseTcp(endPoint)
   .Build();

await scheduler.StartAsync();
```
