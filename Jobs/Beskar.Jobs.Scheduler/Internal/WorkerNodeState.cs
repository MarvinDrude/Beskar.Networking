namespace Beskar.Jobs.Scheduler.Internal;

internal sealed class WorkerNodeState
{
   public required string WorkerId { get; init; }
   public bool IsOnline { get; set; }

   public HashSet<string> Queues { get; set; } = new(StringComparer.Ordinal);
   public HashSet<string> SupportedJobTypes { get; set; } = new(StringComparer.Ordinal);

   public int ActiveJobs { get; set; }
   public int MaxConcurrency { get; set; } = 1;
   public DateTimeOffset LastHeartbeatUtc { get; set; }

   public int AvailableSlots => Math.Max(0, MaxConcurrency - ActiveJobs);
   public double LoadFactor => MaxConcurrency > 0 ? (double)ActiveJobs / MaxConcurrency : 1.0;
}
