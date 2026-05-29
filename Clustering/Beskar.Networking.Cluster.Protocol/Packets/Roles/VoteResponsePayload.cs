using Beskar.Memory.Serialization.Attributes;
using Beskar.Networking.Cluster.Protocol.Interfaces;

namespace Beskar.Networking.Cluster.Protocol.Packets.Roles;

/// <summary>
/// Sent by any none candidate node to respond to a vote request.
/// </summary>
[BeskarObject]
public struct VoteResponsePayload
   : IClusterPacketPayload
{
   /// <summary>
   /// The unique identifier of the node that voted.
   /// </summary>
   [BeskarOrder(0)]
   public Guid VoteNoderId { get; init; }

   /// <summary>
   /// Whether the vote was granted.
   /// </summary>
   [BeskarOrder(1)]
   public bool VoteGranted { get; init; }
}
