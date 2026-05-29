using Beskar.Memory.Serialization.Attributes;
using Beskar.Networking.Cluster.Protocol.Interfaces;
using Beskar.Networking.Cluster.Protocol.Models;

namespace Beskar.Networking.Cluster.Protocol.Packets.Shard;

/// <summary>
/// Sent to synchronize the shard layout and active leaders across nodes in the cluster.
/// </summary>
[BeskarObject]
public struct ShardRoutingTableSyncPayload
   : IClusterPacketPayload
{
   /// <summary>
   /// The complete partition mapping routes for all active shards in the cluster.
   /// </summary>
   [BeskarOrder(0)]
   public required ShardRouteInfo[] Shards { get; init; }
}
