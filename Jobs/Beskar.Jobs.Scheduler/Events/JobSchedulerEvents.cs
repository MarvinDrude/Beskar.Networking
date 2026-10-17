using Beskar.Memory.Threading;

namespace Beskar.Jobs.Scheduler.Events;

/// <summary>
/// Event pipelines exposed by <see cref="JobScheduler"/>.
/// </summary>
public sealed class JobSchedulerEvents
{
   public readonly HandlerPipeline<JobSchedulerJobContext> OnJobEnqueued = new();
   public readonly HandlerPipeline<JobSchedulerDispatchedContext> OnJobDispatched = new();
   public readonly HandlerPipeline<JobSchedulerJobContext> OnJobSucceeded = new();
   public readonly HandlerPipeline<JobSchedulerRetryContext> OnJobRetryScheduled = new();
   public readonly HandlerPipeline<JobSchedulerFailedContext> OnJobDeadLettered = new();
   public readonly HandlerPipeline<JobSchedulerJobContext> OnJobCancelled = new();
   public readonly HandlerPipeline<JobSchedulerRecurringTriggeredContext> OnRecurringJobTriggered = new();
   public readonly HandlerPipeline<JobSchedulerWorkerCrashedContext> OnWorkerCrashed = new();
}
