using System;
using Beskar.Memory.Code.PacketGenerator.Attributes;
using Beskar.Memory.Code.PacketGenerator.Interfaces;
using Beskar.Memory.Serialization.Attributes;
using Beskar.Networking.Cluster.Protocol.Interfaces;
using Beskar.Networking.Cluster.Protocol.Registries;

namespace Beskar.Networking.Cluster.Protocol.Packets.Node;

/// <summary>
/// Sent by a cluster node to check if a peer is still physically alive.
/// </summary>
[BeskarObject]
[Packet(typeof(ClusterMessageRegistry), Wrapper = typeof(ClusterPacket<>))]
public struct HeartbeatPingPayload
   : IClusterPacketPayload, IPacket
{
   /// <summary>
   /// The unique identifier of the sender node.
   /// </summary>
   [BeskarOrder(0)]
   public Guid SenderNodeId { get; init; }

   /// <summary>
   /// The sequence number of the heartbeat.
   /// </summary>
   [BeskarOrder(1)]
   public long SequenceNumber { get; init; }

   /// <summary>
   /// The timestamp of the heartbeat.
   /// </summary>
   [BeskarOrder(2)]
   public long SenderTimestamp { get; init; }
}
