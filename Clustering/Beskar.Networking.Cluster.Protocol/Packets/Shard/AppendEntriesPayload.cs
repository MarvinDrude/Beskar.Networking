using System;
using Beskar.Memory.Serialization.Attributes;
using Beskar.Networking.Cluster.Protocol.Interfaces;

namespace Beskar.Networking.Cluster.Protocol.Packets.Shard;

/// <summary>
/// Sent by the shard leader to replicate state updates or to assert authority (heartbeat).
/// </summary>
[BeskarObject]
public struct AppendEntriesPayload
   : IClusterPacketPayload
{
   /// <summary>
   /// The unique identifier of the leader node.
   /// </summary>
   [BeskarOrder(0)]
   public Guid LeaderNodeId { get; init; }

   /// <summary>
   /// The index of the log entry immediately preceding the new ones.
   /// </summary>
   [BeskarOrder(1)]
   public long PrevLogIndex { get; init; }

   /// <summary>
   /// The epoch (term) of the PrevLogIndex entry.
   /// </summary>
   [BeskarOrder(2)]
   public long PrevLogTerm { get; init; }

   /// <summary>
   /// The leader's highest committed log index.
   /// </summary>
   [BeskarOrder(3)]
   public long LeaderCommitIndex { get; init; }

   /// <summary>
   /// The raw binary payload of the state machine updates (opaque to networking).
   /// </summary>
   [BeskarOrder(4)]
   public byte[]? Entries { get; init; }
}
