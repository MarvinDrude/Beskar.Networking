using Beskar.Jobs.Common.Enums;

namespace Beskar.Jobs.Common.Models;

/// <summary>
/// RPC response returned by <c>JobScheduler</c> for an <see cref="EnqueueJobRequest"/>.
/// </summary>
public sealed record EnqueueJobResponse
{
   public required bool Accepted { get; init; }
   public required string JobId { get; init; }

   public JobStatus InitialStatus { get; init; }
   public DateTimeOffset ScheduledAtUtc { get; init; }

   public string? ErrorMessage { get; init; }
}
