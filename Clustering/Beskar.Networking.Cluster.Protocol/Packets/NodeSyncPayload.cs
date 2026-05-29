using Beskar.Memory.Serialization.Attributes;
using Beskar.Networking.Cluster.Protocol.Interfaces;
using Beskar.Networking.Cluster.Protocol.Models;

namespace Beskar.Networking.Cluster.Protocol.Packets;

[BeskarObject]
public struct NodeSyncPayload
   : IClusterPacketPayload
{
   [BeskarOrder(0)]
   public Guid SourceNodeId { get; init; }

   [BeskarOrder(1)]
   public required NodeStateDelta[] StateDeltas { get; init; }
}
