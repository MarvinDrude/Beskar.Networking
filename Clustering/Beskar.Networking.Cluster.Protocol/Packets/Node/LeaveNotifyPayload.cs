using System;
using Beskar.Memory.Code.PacketGenerator.Attributes;
using Beskar.Memory.Code.PacketGenerator.Interfaces;
using Beskar.Memory.Serialization.Attributes;
using Beskar.Networking.Cluster.Protocol.Enums;
using Beskar.Networking.Cluster.Protocol.Interfaces;
using Beskar.Networking.Cluster.Protocol.Registries;

namespace Beskar.Networking.Cluster.Protocol.Packets.Node;

/// <summary>
/// Sent by a cluster node to notify other nodes that it is leaving.
/// </summary>
[BeskarObject]
[Packet(typeof(ClusterMessageRegistry), Wrapper = typeof(ClusterPacket<>))]
public struct LeaveNotifyPayload
   : IClusterPacketPayload, IPacket
{
   /// <summary>
   /// The unique identifier of the leaving node.
   /// </summary>
   [BeskarOrder(0)]
   public Guid LeavingNodeId { get; init; }

   /// <summary>
   /// The timestamp of the leave notification.
   /// </summary>
   [BeskarOrder(1)]
   public long Timestamp { get; init; }

   /// <summary>
   /// The reason for the node leaving.
   /// </summary>
   [BeskarOrder(2)]
   public ClusterNodeShutdownReason ShutdownReason { get; init; }
}
