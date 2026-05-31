using Beskar.Networking.Cluster.Protocol.Enums;
using Beskar.Networking.Cluster.Protocol.Models;
using Beskar.Networking.Cluster.Protocol.Packets;
using Beskar.Networking.Cluster.Protocol.Packets.Node;
using Beskar.Utilities.Tracing;

namespace Beskar.Networking.Cluster.Handlers.Node;

public class JoinClusterRequestHandler
{
   public static async ValueTask Execute(
      ClusterMessageContext context,
      ClusterPacket<JoinClusterRequestPayload> packet,
      CancellationToken ct)
   {
      if (context.Validator.Validate(context, packet) is not PacketValidationResult.Valid and var reason)
      {
         TraceLogger.LogNeutralError("[]");
         return;
      }


   }
}
