using Beskar.Memory.Code.PacketGenerator.Interfaces;
using Beskar.Networking.Cluster.Protocol.Enums;
using Beskar.Networking.Cluster.Protocol.Models;
using Beskar.Networking.Cluster.Protocol.Packets;

namespace Beskar.Networking.Cluster.Protocol.Interfaces;

public interface IPacketValidator
{
   public PacketValidationResult Validate<TPayload>(ClusterMessageContext context, scoped in ClusterPacket<TPayload> packet)
      where TPayload : IClusterPacketPayload, IPacket;
}
