using Beskar.Memory.Threading;

namespace Beskar.Networking.Raft.Events;

/// <summary>
/// Event hooks exposed by <see cref="RaftNode"/>.
/// </summary>
public sealed class RaftNodeEvents
{
   /// <summary>
   /// Pipeline fired when the node transitions role (e.g. Follower -> Candidate -> Leader).
   /// </summary>
   public readonly HandlerPipeline<RaftRoleChangedContext> OnRoleChanged = new();

   /// <summary>
   /// Pipeline fired when a new cluster leader is detected or elected.
   /// </summary>
   public readonly HandlerPipeline<RaftLeaderChangedContext> OnLeaderChanged = new();

   /// <summary>
   /// Pipeline fired when a log entry is successfully committed to quorum and applied to state machine.
   /// </summary>
   public readonly HandlerPipeline<RaftEntryCommittedContext> OnEntryCommitted = new();
}
