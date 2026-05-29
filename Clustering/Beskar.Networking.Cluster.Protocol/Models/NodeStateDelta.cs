using Beskar.Memory.Serialization.Attributes;
using Beskar.Networking.Cluster.Protocol.Enums;

namespace Beskar.Networking.Cluster.Protocol.Models;

[BeskarObject]
public struct NodeStateDelta
{
   [BeskarOrder(0)]
   public Guid TargetNodeId { get; init; }

   [BeskarOrder(1)]
   public string TargetAddress { get; init; }

   [BeskarOrder(2)]
   public int TargetPort { get; init; }

   [BeskarOrder(3)]
   public ClusterNodeStatus NewStatus { get; init; }

   [BeskarOrder(4)]
   public long Incarnation { get; init; }
}
