namespace Beskar.Networking.Raft.Internal;

/// <summary>
/// Tracks per-follower replication state on the leader.
/// </summary>
internal sealed class RaftPeerTracker(string peerId, ulong initialNextIndex)
{
   private int _inFlight;
   private int _pendingRequest;

   public string PeerId { get; } = peerId;

   /// <summary>
   /// For each server, index of the next log entry to send to that server (initialized to leader last log index + 1).
   /// </summary>
   public ulong NextIndex { get; set; } = initialNextIndex;

   /// <summary>
   /// For each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically).
   /// </summary>
   public ulong MatchIndex { get; set; } = 0;

   public void RequestReplication() => Volatile.Write(ref _pendingRequest, 1);

   public bool ConsumeReplicationRequest() => Interlocked.Exchange(ref _pendingRequest, 0) == 1;

   public bool TryBeginReplication() => Interlocked.CompareExchange(ref _inFlight, 1, 0) == 0;

   public void EndReplication() => Volatile.Write(ref _inFlight, 0);
}
