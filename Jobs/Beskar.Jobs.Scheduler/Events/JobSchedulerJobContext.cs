using Beskar.Jobs.Common.Models;

namespace Beskar.Jobs.Scheduler.Events;

public sealed record JobSchedulerJobContext(JobInstanceRecord Job);
