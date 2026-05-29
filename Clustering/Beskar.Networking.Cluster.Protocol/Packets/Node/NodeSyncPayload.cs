using System;
using Beskar.Memory.Serialization.Attributes;
using Beskar.Networking.Cluster.Protocol.Interfaces;
using Beskar.Networking.Cluster.Protocol.Models;

namespace Beskar.Networking.Cluster.Protocol.Packets.Node;

/// <summary>
/// Sent to synchronize the state of nodes in the cluster.
/// </summary>
[BeskarObject]
public struct NodeSyncPayload
   : IClusterPacketPayload
{
   /// <summary>
   /// The node that initiated this sync payload.
   /// </summary>
   [BeskarOrder(0)]
   public Guid SourceNodeId { get; init; }

   /// <summary>
   /// The list of node state updates.
   /// </summary>
   [BeskarOrder(1)]
   public required NodeStateDelta[] StateDeltas { get; init; }
}
