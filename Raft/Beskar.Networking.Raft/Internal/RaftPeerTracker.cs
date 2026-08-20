namespace Beskar.Networking.Raft.Internal;

/// <summary>
/// Tracks per-follower replication state on the leader.
/// </summary>
internal sealed class RaftPeerTracker(string peerId, ulong initialNextIndex)
{
   public string PeerId { get; } = peerId;

   /// <summary>
   /// For each server, index of the next log entry to send to that server (initialized to leader last log index + 1).
   /// </summary>
   public ulong NextIndex { get; set; } = initialNextIndex;

   /// <summary>
   /// For each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically).
   /// </summary>
   public ulong MatchIndex { get; set; } = 0;
}
