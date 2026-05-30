using Beskar.Networking.Cluster.Protocol.Models;
using Beskar.Networking.Cluster.Protocol.Packets;
using Beskar.Networking.Cluster.Protocol.Packets.Node;

namespace Beskar.Networking.Cluster.Handlers.Node;

public class JoinClusterRequestHandler
{
   public static async ValueTask Execute(
      ClusterMessageContext context,
      ClusterPacket<JoinClusterRequestPayload> packet,
      CancellationToken ct)
   {
      if (context.IsJoined) return;


   }
}
