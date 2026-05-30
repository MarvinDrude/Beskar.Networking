using System;
using Beskar.Memory.Code.PacketGenerator.Attributes;
using Beskar.Memory.Code.PacketGenerator.Interfaces;
using Beskar.Memory.Serialization.Attributes;
using Beskar.Networking.Cluster.Protocol.Enums;
using Beskar.Networking.Cluster.Protocol.Interfaces;
using Beskar.Networking.Cluster.Protocol.Models;
using Beskar.Networking.Cluster.Protocol.Registries;

namespace Beskar.Networking.Cluster.Protocol.Packets.Node;

/// <summary>
/// Returned by an active cluster node to approve/reject
/// the join request and sync global cluster state.
/// </summary>
[BeskarObject]
[Packet(typeof(ClusterMessageRegistry), Wrapper = typeof(ClusterPacket<>))]
public struct JoinClusterResponsePayload
   : IClusterPacketPayload, IPacket
{
   /// <summary>
   /// Whether the join request was approved.
   /// </summary>
   [BeskarOrder(0)]
   public bool IsSuccess { get; init; }

   /// <summary>
   /// The reason for rejection.
   /// </summary>
   [BeskarOrder(1)]
   public ClusterJoinRejectReason RejectReasonCode { get; init; }

   /// <summary>
   /// The unique identifier of the responding node.
   /// </summary>
   [BeskarOrder(2)]
   public Guid RespondingNodeId { get; init; }

   /// <summary>
   /// The list of known cluster members.
   /// </summary>
   [BeskarOrder(3)]
   public required ShortClusterMemberInfo[] KnownMembers { get; init; }
}
