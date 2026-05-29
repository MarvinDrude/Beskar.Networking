using Beskar.Memory.Code.EnumGenerator.Attributes;

namespace Beskar.Networking.Cluster.Protocol.Enums;

/// <summary>
/// Represents the status of a cluster node.
/// </summary>
[FastEnum]
public enum ClusterNodeStatus : byte
{
   /// <summary>
   /// The node is active and can receive and send messages.
   /// </summary>
   Active = 1,

   /// <summary>
   /// The node is suspected to have connectivity issues or is unresponsive.
   /// </summary>
   Suspect,

   /// <summary>
   /// The node is unresponsive and considered non-operational within the cluster.
   /// </summary>
   Dead,

   /// <summary>
   /// The node is leaving the cluster.
   /// </summary>
   Leaving,
}
