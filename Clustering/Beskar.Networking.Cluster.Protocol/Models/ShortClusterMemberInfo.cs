using Beskar.Memory.Serialization.Attributes;
using Beskar.Networking.Cluster.Protocol.Enums;

namespace Beskar.Networking.Cluster.Protocol.Models;

/// <summary>
/// Represents the short information of a cluster member.
/// </summary>
[BeskarObject]
public struct ShortClusterMemberInfo
{
   /// <summary>
   /// The unique identifier of the cluster member.
   /// </summary>
   [BeskarOrder(0)]
   public Guid NodeId { get; init; }

   /// <summary>
   /// The address of the cluster member.
   /// </summary>
   [BeskarOrder(1)]
   public required string Address { get; init; }

   /// <summary>
   /// The port of the cluster member.
   /// </summary>
   [BeskarOrder(2)]
   public int Port { get; init; }

   /// <summary>
   /// The status of the cluster member.
   /// </summary>
   [BeskarOrder(3)]
   public ClusterNodeStatus Status { get; init; }

   /// <summary>
   /// The incarnation of the cluster member.
   /// </summary>
   [BeskarOrder(4)]
   public long Incarnation { get; init; }

   /// <summary>
   /// The capabilities of the cluster member.
   /// </summary>
   [BeskarOrder(5)]
   public required string[] Capabilities { get; init; }
}
