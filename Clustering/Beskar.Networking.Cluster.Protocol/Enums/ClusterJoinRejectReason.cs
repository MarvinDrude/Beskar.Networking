using Beskar.Memory.Code.EnumGenerator.Attributes;

namespace Beskar.Networking.Cluster.Protocol.Enums;

/// <summary>
/// The reason why a cluster node was rejected to join the cluster.
/// </summary>
[FastEnum]
public enum ClusterJoinRejectReason : byte
{
   /// <summary>
   /// No reason was specified.
   /// </summary>
   None = 0,

   /// <summary>
   /// The authentication failed.
   /// </summary>
   AuthFailed,

   /// <summary>
   /// The cluster name does not match.
   /// </summary>
   ClusterNameMismatch,

   /// <summary>
   /// The node ID is already in use.
   /// </summary>
   DuplicateNodeId,
}
