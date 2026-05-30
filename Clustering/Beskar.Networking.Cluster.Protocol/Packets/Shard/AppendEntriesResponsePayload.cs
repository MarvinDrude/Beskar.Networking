using System;
using Beskar.Memory.Code.PacketGenerator.Attributes;
using Beskar.Memory.Code.PacketGenerator.Interfaces;
using Beskar.Memory.Serialization.Attributes;
using Beskar.Networking.Cluster.Protocol.Interfaces;
using Beskar.Networking.Cluster.Protocol.Registries;

namespace Beskar.Networking.Cluster.Protocol.Packets.Shard;

/// <summary>
/// Returned by a replica node to acknowledge receipt of replication payload or heartbeat.
/// </summary>
[BeskarObject]
[Packet(typeof(ClusterMessageRegistry), Wrapper = typeof(ClusterPacket<>))]
public struct AppendEntriesResponsePayload
   : IClusterPacketPayload, IPacket
{
   /// <summary>
   /// The unique identifier of the replica node responding.
   /// </summary>
   [BeskarOrder(0)]
   public Guid ReplicaNodeId { get; init; }

   /// <summary>
   /// The replica's current epoch (term).
   /// </summary>
   [BeskarOrder(1)]
   public long Term { get; init; }

   /// <summary>
   /// True if the replica successfully matched PrevLogIndex and PrevLogTerm; otherwise, false.
   /// </summary>
   [BeskarOrder(2)]
   public bool Success { get; init; }

   /// <summary>
   /// The highest log index of the replica that matches the leader's log.
   /// </summary>
   [BeskarOrder(3)]
   public long MatchIndex { get; init; }
}
