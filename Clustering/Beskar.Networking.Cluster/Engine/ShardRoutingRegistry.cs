using System.Collections.Concurrent;
using Beskar.Memory.Owners;
using Beskar.Memory.Writers;
using Beskar.Networking.Cluster.Protocol.Interfaces;
using Beskar.Networking.Cluster.Protocol.Models;

namespace Beskar.Networking.Cluster.Engine;

public sealed class ShardRoutingRegistry : IShardRoutingRegistry
{
   private readonly ConcurrentDictionary<Guid, ShardRouteInfo> _routingTable = [];

   public void UpdateRoute(scoped in ShardRouteInfo route)
   {
      _routingTable[route.ShardId] = route;
   }

   public void SyncRoutes(ShardRouteInfo[] routes)
   {
      foreach (var route in routes)
      {
         _routingTable[route.ShardId] = route;
      }
   }

   public Guid[] GetReplicaNodes(Guid shardId)
   {
      if (!_routingTable.TryGetValue(shardId, out var route))
      {
         return [];
      }

      Guid[] arr = [route.LeaderNodeId, ..route.ReplicaNodeIds];
      return arr;
   }

   public MemoryOwner<Guid> RentReplicaNodes(Guid shardId)
   {
      if (!_routingTable.TryGetValue(shardId, out var route))
      {
         return MemoryOwner<Guid>.Empty;
      }

      var owner = new MemoryOwner<Guid>(route.ReplicaNodeIds.Length + 1);
      owner.Span[0] = route.LeaderNodeId;
      route.ReplicaNodeIds.CopyTo(owner.Span[1..]);

      return owner;
   }

   public bool TryGetLeader(Guid shardId, out Guid leaderNodeId)
   {
      if (_routingTable.TryGetValue(shardId, out var route))
      {
         leaderNodeId = route.LeaderNodeId;
         return true;
      }

      leaderNodeId = Guid.Empty;
      return false;
   }
}
