using Beskar.Networking.Raft.Protocol.Models;

namespace Beskar.Networking.Raft.Protocol.Messages;

/// <summary>
/// Invoked by leader to replicate log entries and also used as heartbeat.
/// </summary>
public sealed class AppendEntriesRequest
{
   /// <summary>
   /// The leader's term.
   /// </summary>
   public ulong Term { get; set; }

   /// <summary>
   /// Identifier of the leader so follower can redirect clients.
   /// </summary>
   public string LeaderId { get; set; } = string.Empty;

   /// <summary>
   /// Index of log entry immediately preceding new ones.
   /// </summary>
   public ulong PrevLogIndex { get; set; }

   /// <summary>
   /// Term of <see cref="PrevLogIndex"/> entry.
   /// </summary>
   public ulong PrevLogTerm { get; set; }

   /// <summary>
   /// Leader's commit index.
   /// </summary>
   public ulong LeaderCommitIndex { get; set; }

   /// <summary>
   /// Log entries to store (empty for heartbeat; may send more than one for efficiency).
   /// </summary>
   public IReadOnlyList<RaftLogEntry> Entries { get; set; } = [];
}
