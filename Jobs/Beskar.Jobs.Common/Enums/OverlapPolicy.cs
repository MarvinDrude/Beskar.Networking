namespace Beskar.Jobs.Common.Enums;

/// <summary>
/// Defines how recurring Cron triggers behave when a previous instance of the same recurring job is still active.
/// </summary>
public enum OverlapPolicy : byte
{
   /// <summary>
   /// Allows multiple instances of the same recurring job to run concurrently.
   /// </summary>
   AllowConcurrent = 0,

   /// <summary>
   /// Skips triggering a new instance if the previous run of this recurring job is still Enqueued, Dispatching, or Running.
   /// </summary>
   SkipIfRunning = 1,

   /// <summary>
   /// Enqueues the new instance as a continuation of the currently active instance so it runs immediately after completion.
   /// </summary>
   QueueNext = 2
}
