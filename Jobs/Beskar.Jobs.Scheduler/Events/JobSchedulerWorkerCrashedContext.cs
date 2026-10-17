using Beskar.Jobs.Common.Models;

namespace Beskar.Jobs.Scheduler.Events;

public sealed record JobSchedulerWorkerCrashedContext(
   string WorkerId,
   IReadOnlyList<JobInstanceRecord> OrphanedJobs);
