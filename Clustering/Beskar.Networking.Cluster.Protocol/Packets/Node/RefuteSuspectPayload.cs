using System;
using Beskar.Memory.Code.PacketGenerator.Attributes;
using Beskar.Memory.Code.PacketGenerator.Interfaces;
using Beskar.Memory.Serialization.Attributes;
using Beskar.Networking.Cluster.Protocol.Interfaces;
using Beskar.Networking.Cluster.Protocol.Registries;

namespace Beskar.Networking.Cluster.Protocol.Packets.Node;

/// <summary>
/// Sent by a cluster node to refute a suspected node.
/// </summary>
[BeskarObject]
[Packet(typeof(ClusterMessageRegistry), Wrapper = typeof(ClusterPacket<>))]
public struct RefuteSuspectPayload
   : IClusterPacketPayload, IPacket
{
   /// <summary>
   /// The unique identifier of the target node.
   /// </summary>
   [BeskarOrder(0)]
   public Guid TargetNodeId { get; init; }

   /// <summary>
   /// The new incarnation of the target node.
   /// </summary>
   [BeskarOrder(1)]
   public long NewIncarnation { get; init; }
}
