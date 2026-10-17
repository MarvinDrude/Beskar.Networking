namespace Beskar.Jobs.Common.Models;

/// <summary>
/// Command published to <c>jobs/commands/cancel/{jobId}</c>.
/// </summary>
public sealed record JobCancelCommand
{
   public required string JobId { get; init; }

   public string? Reason { get; init; }

   public DateTimeOffset TimestampUtc { get; init; } = DateTimeOffset.UtcNow;
}
