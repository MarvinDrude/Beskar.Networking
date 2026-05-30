using Beskar.Memory.Code.PacketGenerator.Attributes;
using Beskar.Memory.Code.PacketGenerator.Interfaces;
using Beskar.Memory.Serialization.Attributes;
using Beskar.Networking.Cluster.Protocol.Interfaces;
using Beskar.Networking.Cluster.Protocol.Registries;

namespace Beskar.Networking.Cluster.Protocol.Packets.Roles;

/// <summary>
/// Sent by a candidate node to request a vote.
/// </summary>
[BeskarObject]
[Packet(typeof(ClusterMessageRegistry), Wrapper = typeof(ClusterPacket<>))]
public struct RequestVotePayload
   : IClusterPacketPayload, IPacket
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
