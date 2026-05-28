
using Beskar.Memory.Serialization.Attributes;
using Beskar.Networking.Cluster.Protocol.Interfaces;

namespace Beskar.Networking.Cluster.Protocol.Packets;

[BeskarObject]
public struct ClusterPacket<TPacket>
   where TPacket : IClusterPacketPayload
{
   private const ushort Magic = 0xBE5C;

   [BeskarOrder(0)]
   public ushort MagicNumber { get; set; } = Magic;

   [BeskarOrder(1)]
   public ushort Version { get; set; }

   [BeskarOrder(2)]
   public Guid CorrelationId { get; set; }

   [BeskarOrder(3)]
   public int Length { get; set; }

   [BeskarOrder(4)]
   public required TPacket Payload { get; set; }

   public ClusterPacket()
   {

   }

   public readonly bool IsValid => MagicNumber == Magic;
}
