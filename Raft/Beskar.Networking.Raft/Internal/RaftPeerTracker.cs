namespace Beskar.Networking.Raft.Internal;

/// <summary>
/// Tracks per-follower replication state on the leader.
/// </summary>
internal sealed class RaftPeerTracker(string peerId, ulong initialNextIndex)
{
   private int _inFlight;
   private int _pendingRequest;

   private ulong _nextIndex = initialNextIndex;
   private ulong _matchIndex;

   private long _lastContactTicks;

   public string PeerId { get; } = peerId;

   /// <summary>
   /// For each server, index of the next log entry to send to that server (initialized to leader last log index + 1).
   /// </summary>
   public ulong NextIndex
   {
      get => Volatile.Read(ref _nextIndex);
      set => Volatile.Write(ref _nextIndex, value);
   }

   /// <summary>
   /// For each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically).
   /// </summary>
   public ulong MatchIndex
   {
      get => Volatile.Read(ref _matchIndex);
      set => Volatile.Write(ref _matchIndex, value);
   }

   public long LastContactTicks => Volatile.Read(ref _lastContactTicks);

   public bool HasPendingRequest => Volatile.Read(ref _pendingRequest) == 1;

   public void RecordContact(long ticks)
   {
      while (true)
      {
         var current = Volatile.Read(ref _lastContactTicks);
         if (ticks <= current || Interlocked.CompareExchange(ref _lastContactTicks, ticks, current) == current)
         {
            break;
         }
      }
   }

   public void RequestReplication() => Volatile.Write(ref _pendingRequest, 1);

   public bool ConsumeReplicationRequest() => Interlocked.Exchange(ref _pendingRequest, 0) == 1;

   public bool TryBeginReplication() => Interlocked.CompareExchange(ref _inFlight, 1, 0) == 0;

   public void EndReplication() => Volatile.Write(ref _inFlight, 0);
}
