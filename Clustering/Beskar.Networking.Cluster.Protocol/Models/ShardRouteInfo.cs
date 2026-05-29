using System;
using Beskar.Memory.Serialization.Attributes;

namespace Beskar.Networking.Cluster.Protocol.Models;

/// <summary>
/// Represents the routing and ownership mapping for a specific shard.
/// </summary>
[BeskarObject]
public struct ShardRouteInfo
{
   /// <summary>
   /// The unique identifier of the shard.
   /// </summary>
   [BeskarOrder(0)]
   public Guid ShardId { get; init; }

   /// <summary>
   /// The unique identifier of the physical node that is currently the Leader for this shard.
   /// </summary>
   [BeskarOrder(1)]
   public Guid LeaderNodeId { get; init; }

   /// <summary>
   /// The list of unique identifiers of the physical nodes hosting Replicas for this shard.
   /// </summary>
   [BeskarOrder(2)]
   public required Guid[] ReplicaNodeIds { get; init; }
}
