using Beskar.Jobs.Common.Models;

namespace Beskar.Jobs.Scheduler.Events;

public sealed record JobSchedulerRetryContext(
   JobInstanceRecord Job,
   int FailedAttempt,
   TimeSpan RetryDelay,
   string Reason);
