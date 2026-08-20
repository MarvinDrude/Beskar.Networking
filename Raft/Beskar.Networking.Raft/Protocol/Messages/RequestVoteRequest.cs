namespace Beskar.Networking.Raft.Protocol.Messages;

/// <summary>
/// Invoked by candidates to gather votes from cluster peers.
/// </summary>
public sealed class RequestVoteRequest
{
   /// <summary>
   /// The candidate's term.
   /// </summary>
   public ulong Term { get; set; }

   /// <summary>
   /// The identifier of the candidate requesting the vote.
   /// </summary>
   public string CandidateId { get; set; } = string.Empty;

   /// <summary>
   /// Index of candidate's last log entry.
   /// </summary>
   public ulong LastLogIndex { get; set; }

   /// <summary>
   /// Term of candidate's last log entry.
   /// </summary>
   public ulong LastLogTerm { get; set; }

   /// <summary>
   /// Whether this request is a non-disruptive Pre-Vote check before term increment.
   /// </summary>
   public bool IsPreVote { get; set; }
}
