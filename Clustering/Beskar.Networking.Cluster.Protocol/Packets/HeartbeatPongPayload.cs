using Beskar.Memory.Serialization.Attributes;
using Beskar.Networking.Cluster.Protocol.Interfaces;

namespace Beskar.Networking.Cluster.Protocol.Packets;

[BeskarObject]
public struct HeartbeatPongPayload
   : IClusterPacketPayload
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
   /// (Sender does not need to keep track of this)
   /// </summary>
   [BeskarOrder(2)]
   public long PingTimestamp { get; init; }

   /// <summary>
   /// The timestamp of the heartbeat responding.
   /// (Separates network delay from node processing overhead)
   /// </summary>
   [BeskarOrder(3)]
   public long ResponderTimestamp { get; init; }
}
