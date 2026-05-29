using Beskar.Memory.Serialization.Attributes;
using Beskar.Networking.Cluster.Protocol.Interfaces;

namespace Beskar.Networking.Cluster.Protocol.Packets;

/// <summary>
/// Sent by a cluster node to refute a suspected node.
/// </summary>
[BeskarObject]
public struct RefuteSuspectPayload
   : IClusterPacketPayload
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
