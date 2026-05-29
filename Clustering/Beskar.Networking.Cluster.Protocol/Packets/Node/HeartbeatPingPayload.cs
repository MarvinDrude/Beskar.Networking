using System;
using Beskar.Memory.Serialization.Attributes;
using Beskar.Networking.Cluster.Protocol.Interfaces;

namespace Beskar.Networking.Cluster.Protocol.Packets.Node;

/// <summary>
/// Sent by a cluster node to check if a peer is still physically alive.
/// </summary>
[BeskarObject]
public struct HeartbeatPingPayload
   : IClusterPacketPayload
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
