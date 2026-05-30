using System;
using Beskar.Memory.Code.PacketGenerator.Attributes;
using Beskar.Memory.Code.PacketGenerator.Interfaces;
using Beskar.Memory.Serialization.Attributes;
using Beskar.Networking.Cluster.Protocol.Interfaces;
using Beskar.Networking.Cluster.Protocol.Registries;

namespace Beskar.Networking.Cluster.Protocol.Packets.Node;

/// <summary>
/// Returned by a cluster node in response to a heartbeat ping.
/// </summary>
[BeskarObject]
[Packet(typeof(ClusterMessageRegistry), Wrapper = typeof(ClusterPacket<>))]
public struct HeartbeatPongPayload
   : IClusterPacketPayload, IPacket
{
   /// <summary>
   /// The unique identifier of the responding node.
   /// </summary>
   [BeskarOrder(0)]
   public Guid ResponderNodeId { get; init; }

   /// <summary>
   /// The sequence number of the heartbeat.
   /// </summary>
   [BeskarOrder(1)]
   public long SequenceNumber { get; init; }

   /// <summary>
   /// The initial timestamp of the heartbeat coming in.
   /// </summary>
   [BeskarOrder(2)]
   public long PingTimestamp { get; init; }

   /// <summary>
   /// The timestamp of the heartbeat responding.
   /// </summary>
   [BeskarOrder(3)]
   public long ResponderTimestamp { get; init; }
}
