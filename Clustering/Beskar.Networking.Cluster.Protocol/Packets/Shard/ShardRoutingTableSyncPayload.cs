using Beskar.Memory.Code.PacketGenerator.Attributes;
using Beskar.Memory.Code.PacketGenerator.Interfaces;
using Beskar.Memory.Serialization.Attributes;
using Beskar.Networking.Cluster.Protocol.Interfaces;
using Beskar.Networking.Cluster.Protocol.Models;
using Beskar.Networking.Cluster.Protocol.Registries;

namespace Beskar.Networking.Cluster.Protocol.Packets.Shard;

/// <summary>
/// Sent to synchronize the shard layout and active leaders across nodes in the cluster.
/// </summary>
[BeskarObject]
[Packet(typeof(ClusterMessageRegistry), Wrapper = typeof(ClusterPacket<>))]
public readonly struct ShardRoutingTableSyncPayload
   : IClusterPacketPayload, IPacket
{
   /// <summary>
   /// The complete partition mapping routes for all active shards in the cluster.
   /// </summary>
   [BeskarOrder(0)]
   public required ShardRouteInfo[] Shards { get; init; }
}
