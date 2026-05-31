using Beskar.Memory.Code.PacketGenerator.Interfaces;
using Beskar.Networking.Cluster.Constants;
using Beskar.Networking.Cluster.Protocol.Enums;
using Beskar.Networking.Cluster.Protocol.Interfaces;
using Beskar.Networking.Cluster.Protocol.Models;
using Beskar.Networking.Cluster.Protocol.Packets;
using Beskar.Networking.Cluster.Protocol.Packets.Node;
using Beskar.Utilities.Tracing;

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
         TraceLogger.LogNeutralError("[{0}]: Invalid incoming packet {1}", context, PacketValidationResult.WrongMagic);
         return PacketValidationResult.WrongMagic;
      }

      if (packet.Version != ClusterConstants.Version)
      {
         // The cluster version must match
         TraceLogger.LogNeutralError("[{0}]: Invalid incoming packet {1}", context, PacketValidationResult.WrongVersion);
         return PacketValidationResult.WrongVersion;
      }

      if (!context.IsJoined && packet.Payload is not JoinClusterRequestPayload)
      {
         // Only join requests are allowed when not joined
         TraceLogger.LogNeutralError("[{0}]: Invalid incoming packet {1}", context, PacketValidationResult.NotJoinedYet);
         return PacketValidationResult.NotJoinedYet;
      }

      return PacketValidationResult.Valid;
   }
}
