namespace Beskar.Jobs.Common.Models;

/// <summary>
/// RPC claim response returned by <c>JobWorker</c> to <c>JobScheduler</c>.
/// </summary>
public sealed record JobDispatchResponse
{
   public required bool Accepted { get; init; }

   public required string JobId { get; init; }
   public required string WorkerId { get; init; }

   public string? RejectReason { get; init; }
}
