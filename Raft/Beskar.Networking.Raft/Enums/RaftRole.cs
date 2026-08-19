namespace Beskar.Networking.Raft.Enums;

/// <summary>
/// Defines the role of a Raft node in the cluster consensus state machine.
/// </summary>
public enum RaftRole
{
   /// <summary>
   /// The node is passive, responding to incoming RPCs from candidates and leaders.
   /// </summary>
   Follower = 0,

   /// <summary>
   /// The node has timed out without hearing from a leader and is soliciting votes.
   /// </summary>
   Candidate = 1,

   /// <summary>
   /// The node has won the election and is actively serving client proposals and sending heartbeats.
   /// </summary>
   Leader = 2,

   /// <summary>
   /// The node has been stopped.
   /// </summary>
   Stopped = 3
}
