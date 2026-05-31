using Beskar.Memory.Code.PacketGenerator.Interfaces;
using Beskar.Networking.Cluster.Constants;
using Beskar.Networking.Cluster.Protocol.Enums;
using Beskar.Networking.Cluster.Protocol.Interfaces;
using Beskar.Networking.Cluster.Protocol.Models;
using Beskar.Networking.Cluster.Protocol.Packets;
using Beskar.Networking.Cluster.Protocol.Packets.Node;

namespace Beskar.Networking.Cluster.Engine;

public sealed class PacketValidator : IPacketValidator
{
   /// <summary>
   /// Validates a cluster packet.
   /// </summary>
   public PacketValidationResult Validate<TPayload>(
      ClusterMessageContext context,
      scoped in ClusterPacket<TPayload> packet)
      where TPayload : IClusterPacketPayload, IPacket
   {
      if (packet.MagicNumber != ClusterPacket<TPayload>.Magic)
      {
         // Always same magic required
         return PacketValidationResult.WrongMagic;
      }

      if (packet.Version != ClusterConstants.Version)
      {
         // The cluster version must match
         return PacketValidationResult.WrongVersion;
      }

      if (!context.IsJoined && packet.Payload is not JoinClusterRequestPayload)
      {
         // Only join requests are allowed when not joined
         return PacketValidationResult.NotJoinedYet;
      }

      return PacketValidationResult.Valid;
   }
}
