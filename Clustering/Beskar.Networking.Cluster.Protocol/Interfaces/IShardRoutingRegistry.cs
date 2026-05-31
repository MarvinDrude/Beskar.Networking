using Beskar.Memory.Owners;
using Beskar.Networking.Cluster.Protocol.Models;

namespace Beskar.Networking.Cluster.Protocol.Interfaces;

public interface IShardRoutingRegistry
{
   public void UpdateRoute(scoped in ShardRouteInfo route);

   public void SyncRoutes(ShardRouteInfo[] routes);

   public Guid[] GetReplicaNodes(Guid shardId);

   public MemoryOwner<Guid> RentReplicaNodes(Guid shardId);

   public bool TryGetLeader(Guid shardId, out Guid leaderNodeId);
}
