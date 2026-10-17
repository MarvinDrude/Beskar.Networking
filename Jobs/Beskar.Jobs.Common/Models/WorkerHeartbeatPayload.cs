namespace Beskar.Jobs.Common.Models;

/// <summary>
/// Worker capacity and online/LWT status published to <c>jobs/workers/{workerId}/heartbeat</c>.
/// </summary>
public sealed record WorkerHeartbeatPayload
{
   public required string WorkerId { get; init; }
   public required bool IsOnline { get; init; }

   public string[] Queues { get; init; } = ["default"];
   public string[] SupportedJobTypes { get; init; } = [];

   public int ActiveJobs { get; init; }
   public int MaxConcurrency { get; init; } = 1;

   public DateTimeOffset TimestampUtc { get; init; } = DateTimeOffset.UtcNow;
}
