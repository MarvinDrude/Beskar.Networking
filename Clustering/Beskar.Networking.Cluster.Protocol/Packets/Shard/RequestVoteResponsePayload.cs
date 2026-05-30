using System;
using Beskar.Memory.Code.PacketGenerator.Attributes;
using Beskar.Memory.Code.PacketGenerator.Interfaces;
using Beskar.Memory.Serialization.Attributes;
using Beskar.Networking.Cluster.Protocol.Interfaces;
using Beskar.Networking.Cluster.Protocol.Registries;

namespace Beskar.Networking.Cluster.Protocol.Packets.Shard;

/// <summary>
/// Returned by a peer node to vote for or reject a candidate.
/// </summary>
[BeskarObject]
[Packet(typeof(ClusterMessageRegistry), Wrapper = typeof(ClusterPacket<>))]
public struct RequestVoteResponsePayload
   : IClusterPacketPayload, IPacket
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
