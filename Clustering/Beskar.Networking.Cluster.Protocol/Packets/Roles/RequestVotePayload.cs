using Beskar.Memory.Serialization.Attributes;
using Beskar.Networking.Cluster.Protocol.Interfaces;

namespace Beskar.Networking.Cluster.Protocol.Packets.Roles;

/// <summary>
/// Sent by a candidate node to request a vote.
/// </summary>
[BeskarObject]
public struct RequestVotePayload
   : IClusterPacketPayload
{
   /// <summary>
   /// The unique identifier of the candidate node.
   /// </summary>
   [BeskarOrder(0)]
   public Guid CandidateNodeId { get; init; }

   /// <summary>
   /// The last log entry that the candidate has received.
   /// </summary>
   [BeskarOrder(1)]
   public long LastLogIndex { get; init; }

   /// <summary>
   /// The epoch of the last log entry that the candidate has received.
   /// </summary>
   [BeskarOrder(2)]
   public long LastLogEpoch { get; init; }
}
