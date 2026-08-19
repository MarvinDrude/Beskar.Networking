namespace Beskar.Networking.Raft.Protocol.Messages;

/// <summary>
/// Response to a <see cref="RequestVoteRequest"/> RPC.
/// </summary>
public sealed class RequestVoteResponse
{
   /// <summary>
   /// The responder's current term, for candidate to update itself if behind.
   /// </summary>
   public ulong Term { get; set; }

   /// <summary>
   /// True means candidate received vote.
   /// </summary>
   public bool VoteGranted { get; set; }
}
