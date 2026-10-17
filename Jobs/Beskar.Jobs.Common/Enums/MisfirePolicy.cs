namespace Beskar.Jobs.Common.Enums;

/// <summary>
/// Defines how the scheduler handles missed recurring Cron occurrences (e.g. after downtime).
/// </summary>
public enum MisfirePolicy : byte
{
   /// <summary>
   /// Skips any missed occurrences and schedules the job for the next future occurrence.
   /// </summary>
   Skip = 0,

   /// <summary>
   /// Fires a single catch-up job immediately if one or more occurrences were missed, then advances to the next future occurrence.
   /// </summary>
   FireOnceNow = 1,

   /// <summary>
   /// Enqueues an instance for every missed occurrence since the last scheduled run (up to configured safety cap).
   /// </summary>
   FireAllMissed = 2
}
