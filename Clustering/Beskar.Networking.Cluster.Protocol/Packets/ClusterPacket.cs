
namespace Beskar.Networking.Cluster.Protocol.Packets;

public struct ClusterPacket<TPacket>
{
   public const ushort Magic = 0xBE5C;

   public ushort MagicNumber { get; set; } = Magic;

   public ushort Version { get; set; }

   public Guid CorrelationId { get; set; }

   public int Length { get; set; }

   public required TPacket Packet { get; set; }

   public ClusterPacket()
   {

   }
}
