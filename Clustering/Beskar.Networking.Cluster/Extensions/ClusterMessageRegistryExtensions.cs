using Beskar.Networking.Cluster.Handlers.Node;
using Beskar.Networking.Cluster.Protocol.Packets;
using Beskar.Networking.Cluster.Protocol.Packets.Node;
using Beskar.Networking.Cluster.Protocol.Registries;

namespace Beskar.Networking.Cluster.Extensions;

public static class ClusterMessageRegistryExtensions
{
   extension(ClusterMessageRegistry registry)
   {
      public void RegisterDefaultHandlers()
      {
         // Nodes
         registry.RegisterHandler<ClusterPacket<JoinClusterRequestPayload>>(
            static (ref ctx, ref pg, ct) => JoinClusterRequestHandler.Execute(ctx, pg, ct));

         // Roles

         // Shards
      }
   }
}
