using Beskar.Jobs.Common.Enums;

namespace Beskar.Jobs.Common.Models;

/// <summary>
/// RPC request sent by <c>JobClient</c> to <c>jobs/commands/recurring</c>.
/// </summary>
public sealed record ScheduleRecurringJobRequest
{
   public required string RecurringJobId { get; init; }
   public required string CronExpression { get; init; }
   public required string JobType { get; init; }

   public string Queue { get; init; } = "default";
   public byte[] Payload { get; init; } = [];

   public MisfirePolicy MisfirePolicy { get; init; } = MisfirePolicy.FireOnceNow;
   public OverlapPolicy OverlapPolicy { get; init; } = OverlapPolicy.SkipIfRunning;
   public int MaxAttempts { get; init; } = 3;

   public TimeSpan? Timeout { get; init; }
}
