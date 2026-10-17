namespace Beskar.Jobs.Common.Enums;

/// <summary>
/// Represents the lifecycle state of a job instance.
/// </summary>
public enum JobStatus : byte
{
   /// <summary>
   /// Job is scheduled to run at a future timestamp or waiting for a parent continuation job.
   /// </summary>
   Scheduled = 0,

   /// <summary>
   /// Job is due and waiting in its target queue to be claimed and dispatched to an available worker.
   /// </summary>
   Enqueued = 1,

   /// <summary>
   /// Scheduler is actively dispatching the job to a candidate worker and awaiting claim acknowledgment.
   /// </summary>
   Dispatching = 2,

   /// <summary>
   /// A worker has claimed the job and is actively executing its handler.
   /// </summary>
   Running = 3,

   /// <summary>
   /// Job execution failed or its worker crashed, and it is scheduled for a retry attempt after backoff.
   /// </summary>
   AwaitingRetry = 4,

   /// <summary>
   /// Job finished execution successfully (terminal state).
   /// </summary>
   Succeeded = 5,

   /// <summary>
   /// Job failed permanently without moving to dead-letter or before retry evaluation (terminal state).
   /// </summary>
   Failed = 6,

   /// <summary>
   /// Job was cooperatively or administratively cancelled (terminal state).
   /// </summary>
   Cancelled = 7,

   /// <summary>
   /// Job exhausted all configured retry attempts and was moved to the dead-letter state (terminal state).
   /// </summary>
   DeadLettered = 8
}

/// <summary>
/// Extension methods for <see cref="JobStatus"/>.
/// </summary>
public static class JobStatusExtensions
{
   extension(JobStatus status)
   {
      /// <summary>
      /// Returns true if the status represents a terminal state where no further transitions occur.
      /// </summary>
      public bool IsTerminal() =>
         status is JobStatus.Succeeded or JobStatus.Failed or JobStatus.Cancelled or JobStatus.DeadLettered;
   }
}
