namespace Beskar.Networking.Raft.Protocol.Enums;

/// <summary>
/// Identifies the type of Raft RPC message transmitted across the cluster transport.
/// </summary>
public enum RaftMessageType : byte
{
   /// <summary>
   /// RequestVote RPC sent by candidates to gather votes.
   /// </summary>
   RequestVote = 1,

   /// <summary>
   /// Response to a RequestVote RPC.
   /// </summary>
   RequestVoteResponse = 2,

   /// <summary>
   /// AppendEntries RPC sent by leaders to replicate log entries and as heartbeats.
   /// </summary>
   AppendEntries = 3,

   /// <summary>
   /// Response to an AppendEntries RPC.
   /// </summary>
   AppendEntriesResponse = 4,

   /// <summary>
   /// InstallSnapshot RPC sent by leaders to send chunks of a snapshot to followers.
   /// </summary>
   InstallSnapshot = 5,

   /// <summary>
   /// Response to an InstallSnapshot RPC.
   /// </summary>
   InstallSnapshotResponse = 6,

   /// <summary>
   /// Client proposal or query sent to the cluster.
   /// </summary>
   ClientCommand = 7,

   /// <summary>
   /// Response to a client proposal or query.
   /// </summary>
   ClientResponse = 8
}
