using Beskar.Memory.Code.PacketGenerator.Interfaces;
using Beskar.Networking.Cluster.Protocol.Packets;

namespace Beskar.Networking.Cluster.Protocol.Interfaces;

public interface IPacketValidator
{
   public bool Validate<TPayload>(scoped in ClusterPacket<TPayload> packet)
      where TPayload : IClusterPacketPayload, IPacket;
}
