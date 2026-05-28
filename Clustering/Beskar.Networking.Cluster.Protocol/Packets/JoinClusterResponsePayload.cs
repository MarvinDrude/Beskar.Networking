using Beskar.Memory.Serialization.Attributes;
using Beskar.Networking.Cluster.Protocol.Enums;
using Beskar.Networking.Cluster.Protocol.Interfaces;

namespace Beskar.Networking.Cluster.Protocol.Packets;

/// <summary>
/// Returned by an active cluster node to approve/reject
/// the join request and sync global cluster state.
/// </summary>
[BeskarObject]
public struct JoinClusterResponsePayload
   : IClusterPacketPayload
{
   /// <summary>
   /// Whether the join request was approved.
   /// </summary>
   [BeskarOrder(0)]
   public bool IsSuccess { get; init; }

   /// <summary>
   /// The reason for rejection.
   /// </summary>
   [BeskarOrder(1)]
   public ClusterJoinRejectReason RejectReasonCode { get; init; }

   
}
