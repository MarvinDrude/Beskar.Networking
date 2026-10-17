namespace Beskar.Jobs.Common.Models;

/// <summary>
/// RPC request sent by <c>JobClient</c> to <c>jobs/commands/enqueue</c>.
/// </summary>
public sealed record EnqueueJobRequest
{
   public string? JobId { get; init; }
   public required string JobType { get; init; }

   public string Queue { get; init; } = "default";
   public byte[] Payload { get; init; } = [];

   public DateTimeOffset? ScheduledAtUtc { get; init; }

   public int MaxAttempts { get; init; } = 3;
   public TimeSpan? Timeout { get; init; }
   public string? ParentJobId { get; init; }

   public Dictionary<string, string>? Metadata { get; init; }
}
