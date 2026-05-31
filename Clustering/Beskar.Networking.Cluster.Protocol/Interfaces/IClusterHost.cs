using Beskar.Networking.Cluster.Protocol.Registries;

namespace Beskar.Networking.Cluster.Protocol.Interfaces;

public interface IClusterHost
{
   public Guid LocalNodeId { get; }

   public IClusterSessionRegistry SessionRegistry { get; }
   public IShardRoutingRegistry RoutingRegistry { get; }
   public ClusterMessageRegistry MessageRegistry { get; }
}
