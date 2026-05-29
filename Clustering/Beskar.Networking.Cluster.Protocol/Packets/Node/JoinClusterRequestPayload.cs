using System;
using Beskar.Memory.Serialization.Attributes;
using Beskar.Networking.Cluster.Protocol.Interfaces;

namespace Beskar.Networking.Cluster.Protocol.Packets.Node;

/// <summary>
/// Sent by a new node when attempting to bootstrap or connect
/// to an existing peer/seed node in the cluster.
/// </summary>
[BeskarObject]
public struct JoinClusterRequestPayload
   : IClusterPacketPayload
{
   /// <summary>
   /// The unique identifier of the requesting node.
   /// </summary>
   [BeskarOrder(0)]
   public Guid RequestingNodeId { get; init; }

   /// <summary>
   /// The address of the requesting node.
   /// </summary>
   [BeskarOrder(1)]
   public required string RequestingNodeAddress { get; init; }

   /// <summary>
   /// The name of the cluster to join.
   /// </summary>
   [BeskarOrder(2)]
   public required string ClusterName { get; init; }

   /// <summary>
   /// The authentication token used to join the cluster.
   /// </summary>
   [BeskarOrder(3)]
   public string? AuthenticationToken { get; init; }

   /// <summary>
   /// The timestamp of the request.
   /// </summary>
   [BeskarOrder(4)]
   public long Timestamp { get; init; }
}
