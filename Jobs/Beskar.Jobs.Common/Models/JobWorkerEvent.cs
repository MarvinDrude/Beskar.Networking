namespace Beskar.Jobs.Common.Models;

/// <summary>
/// Worker lifecycle event published to <c>jobs/events/{jobId}/{eventType}</c>.
/// </summary>
public sealed record JobWorkerEvent
{
   public required string JobId { get; init; }
   public required string WorkerId { get; init; }
   public required string EventType { get; init; }

   public required int Attempt { get; init; }
   public int ProgressPercent { get; init; }

   public string? Message { get; init; }
   public byte[]? ResultPayload { get; init; }
   public string? ErrorMessage { get; init; }

   public double DurationMs { get; init; }
   public DateTimeOffset TimestampUtc { get; init; } = DateTimeOffset.UtcNow;
}
