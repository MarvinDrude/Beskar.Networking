namespace Beskar.Networking.Raft.Protocol.Messages;

/// <summary>
/// Response to an <see cref="AppendEntriesRequest"/> RPC.
/// </summary>
public sealed class AppendEntriesResponse
{
   /// <summary>
   /// The responder's current term, for leader to update itself if behind.
   /// </summary>
   public ulong Term { get; set; }

   /// <summary>
   /// True if follower contained entry matching prevLogIndex and prevLogTerm.
   /// </summary>
   public bool Success { get; set; }

   /// <summary>
   /// The highest log index known to be replicated on the follower if successful,
   /// or the follower's last log index to help the leader quickly find matchIndex on failure.
   /// </summary>
   public ulong MatchIndex { get; set; }
}
