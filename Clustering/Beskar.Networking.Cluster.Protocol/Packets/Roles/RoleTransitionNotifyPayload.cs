using System;
using Beskar.Memory.Code.PacketGenerator.Attributes;
using Beskar.Memory.Code.PacketGenerator.Interfaces;
using Beskar.Memory.Serialization.Attributes;
using Beskar.Networking.Cluster.Protocol.Enums;
using Beskar.Networking.Cluster.Protocol.Interfaces;
using Beskar.Networking.Cluster.Protocol.Registries;

namespace Beskar.Networking.Cluster.Protocol.Packets.Roles;

/// <summary>
/// Sent by a node to notify peers that its operational role in a shard has transitioned.
/// </summary>
[BeskarObject]
[Packet(typeof(ClusterMessageRegistry), Wrapper = typeof(ClusterPacket<>))]
public struct RoleTransitionNotifyPayload
   : IClusterPacketPayload, IPacket
{
   /// <summary>
   /// The unique identifier of the node undergoing transition.
   /// </summary>
   [BeskarOrder(0)]
   public Guid TargetNodeId { get; init; }

   /// <summary>
   /// The new active role that the node is transitioning to.
   /// </summary>
   [BeskarOrder(1)]
   public ClusterNodeRole NewRole { get; init; }

   /// <summary>
   /// The incarnation sequence number to resolve concurrent gossip state updates (higher wins).
   /// </summary>
   [BeskarOrder(2)]
   public long Incarnation { get; init; }

   /// <summary>
   /// The timestamp when the transition was initiated.
   /// </summary>
   [BeskarOrder(3)]
   public long Timestamp { get; init; }
}
