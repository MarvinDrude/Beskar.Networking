using Beskar.Jobs.Common.Models;

namespace Beskar.Jobs.Scheduler.Events;

public sealed record JobSchedulerDispatchedContext(JobInstanceRecord Job, string WorkerId);
