using System;
using Beskar.Memory.Serialization.Attributes;
using Beskar.Networking.Cluster.Protocol.Interfaces;

namespace Beskar.Networking.Cluster.Protocol.Packets.Shard;

/// <summary>
/// Returned by a peer node to vote for or reject a candidate.
/// </summary>
[BeskarObject]
public struct RequestVoteResponsePayload
   : IClusterPacketPayload
{
   /// <summary>
   /// The unique identifier of the voting node.
   /// </summary>
   [BeskarOrder(0)]
   public Guid VoterNodeId { get; init; }

   /// <summary>
   /// The voter's current epoch (term).
   /// </summary>
   [BeskarOrder(1)]
   public long Term { get; init; }

   /// <summary>
   /// True if the vote is granted; otherwise, false.
   /// </summary>
   [BeskarOrder(2)]
   public bool VoteGranted { get; init; }
}
