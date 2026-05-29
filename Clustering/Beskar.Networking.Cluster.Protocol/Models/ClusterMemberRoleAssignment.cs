using System;
using Beskar.Memory.Serialization.Attributes;
using Beskar.Networking.Cluster.Protocol.Enums;

namespace Beskar.Networking.Cluster.Protocol.Models;

/// <summary>
/// Represents a specific role assignment for a cluster member within a shard.
/// </summary>
[BeskarObject]
public struct ClusterMemberRoleAssignment
{
   /// <summary>
   /// The unique identifier of the physical node.
   /// </summary>
   [BeskarOrder(0)]
   public Guid NodeId { get; init; }

   /// <summary>
   /// The current active role of this node in the shard's consensus group.
   /// </summary>
   [BeskarOrder(1)]
   public ClusterNodeRole Role { get; init; }

   /// <summary>
   /// The incarnation of this role state. Used to resolve conflicts via gossip (higher wins).
   /// </summary>
   [BeskarOrder(2)]
   public long Incarnation { get; init; }
}
