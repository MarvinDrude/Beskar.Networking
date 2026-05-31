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
      if (context.Validator.Validate(context, packet) is not PacketValidationResult.Valid
          || context.Stream is null)
      {
         return;
      }

      context.Host.SessionRegistry.Register(packet.Payload.RequestingNodeId, context.Stream);
      context.IsJoined = true;

      var accept = new JoinClusterResponsePayload()
      {
         RespondingNodeId = context.Host.LocalNodeId,

         IsSuccess = true
      };
   }
}
