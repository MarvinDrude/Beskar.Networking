using Beskar.Memory.Threading;

namespace Beskar.Jobs.Worker.Events;

/// <summary>
/// Event hooks exposed by <see cref="JobWorker"/>.
/// </summary>
public sealed class JobWorkerEvents
{
   public readonly HandlerPipeline<JobWorkerStartedContext> OnJobStarted = new();
   public readonly HandlerPipeline<JobWorkerCompletedContext> OnJobCompleted = new();
   public readonly HandlerPipeline<JobWorkerFailedContext> OnJobFailed = new();
   public readonly HandlerPipeline<JobWorkerCancelledContext> OnJobCancelled = new();
}
