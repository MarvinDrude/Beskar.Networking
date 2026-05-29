using Beskar.Memory.Code.EnumGenerator.Attributes;

namespace Beskar.Networking.Cluster.Protocol.Enums;

/// <summary>
/// The reason why a cluster node was shutdown.
/// </summary>
[FastEnum]
public enum ClusterNodeShutdownReason : byte
{
   /// <summary>
   /// The reason is unknown.
   /// </summary>
   Unknown = 0,
   /// <summary>
   /// The node was shutdown gracefully.
   /// </summary>
   Graceful,
   /// <summary>
   /// The node was shutdown unexpectedly.
   /// </summary>
   Unexpected,
}
