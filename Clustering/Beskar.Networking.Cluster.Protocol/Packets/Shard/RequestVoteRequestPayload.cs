using System;
using Beskar.Memory.Serialization.Attributes;
using Beskar.Networking.Cluster.Protocol.Interfaces;

namespace Beskar.Networking.Cluster.Protocol.Packets.Shard;

/// <summary>
/// Sent by a candidate node seeking votes to become the Leader for a specific shard.
/// </summary>
[BeskarObject]
public struct RequestVoteRequestPayload
   : IClusterPacketPayload
{
   /// <summary>
   /// The unique identifier of the candidate node requesting the vote.
   /// </summary>
   [BeskarOrder(0)]
   public Guid CandidateNodeId { get; init; }

   /// <summary>
   /// The candidate's current epoch (term).
   /// </summary>
   [BeskarOrder(1)]
   public long Term { get; init; }

   /// <summary>
   /// The index of the candidate's last log entry.
   /// </summary>
   [BeskarOrder(2)]
   public long LastLogIndex { get; init; }

   /// <summary>
   /// The epoch (term) of the candidate's last log entry.
   /// </summary>
   [BeskarOrder(3)]
   public long LastLogTerm { get; init; }
}
