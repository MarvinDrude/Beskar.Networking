namespace Beskar.Jobs.Common.Models;

/// <summary>
/// RPC dispatch request sent by <c>JobScheduler</c> to <c>jobs/dispatch/{queue}/{workerId}</c>.
/// </summary>
public sealed record JobDispatchRequest
{
   public required string JobId { get; init; }
   public required string JobType { get; init; }

   public required string Queue { get; init; }
   public required byte[] Payload { get; init; }

   public required int Attempt { get; init; }
   public required int MaxAttempts { get; init; }
   public TimeSpan? Timeout { get; init; }

   public Dictionary<string, string>? Metadata { get; init; }
   public DateTimeOffset DispatchedAtUtc { get; init; } = DateTimeOffset.UtcNow;
}
