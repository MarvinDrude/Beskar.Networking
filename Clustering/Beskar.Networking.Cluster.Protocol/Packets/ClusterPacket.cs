using Beskar.Memory.Code.PacketGenerator.Interfaces;
using Beskar.Memory.Serialization.Attributes;
using Beskar.Networking.Cluster.Protocol.Interfaces;

namespace Beskar.Networking.Cluster.Protocol.Packets;

/// <summary>
/// All internal cluster packets are wrapped in this structure.
/// </summary>
[BeskarObject]
public readonly struct ClusterPacket<TPacket> : IPacket
   where TPacket : IClusterPacketPayload, IPacket
{
   private const ushort Magic = 0xBE5C;

   /// <summary>
   /// The magic number of the cluster packet.
   /// </summary>
   [BeskarOrder(0)]
   public ushort MagicNumber { get; init; } = Magic;

   /// <summary>
   /// The version of the cluster packet.
   /// </summary>
   [BeskarOrder(1)]
   public ushort Version { get; init; }

   /// <summary>
   /// The correlation ID of the cluster packet.
   /// </summary>
   [BeskarOrder(2)]
   public Guid CorrelationId { get; init; }

   /// <summary>
   /// The payload of the cluster packet.
   /// </summary>
   [BeskarOrder(3)]
   public required TPacket Payload { get; init; }

   /// <summary>
   /// The current epoch of the cluster.
   /// </summary>
   [BeskarOrder(4)]
   public long CurrentEpoch { get; init; }

   /// <summary>
   /// The identifier of the specific shard (consensus group) this packet is routed to.
   /// </summary>
   [BeskarOrder(5)]
   public Guid ShardId { get; init; }

   public ClusterPacket()
   {

   }

   public readonly bool IsValid => MagicNumber == Magic;
}
