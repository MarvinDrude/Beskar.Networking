namespace Beskar.Jobs.Common.Models;

/// <summary>
/// RPC response returned by <c>JobScheduler</c> for a <see cref="ScheduleRecurringJobRequest"/>.
/// </summary>
public sealed record ScheduleRecurringJobResponse
{
   public required bool Accepted { get; init; }

   public required string RecurringJobId { get; init; }
   public DateTimeOffset? NextRunAtUtc { get; init; }

   public string? ErrorMessage { get; init; }
}
