using Beskar.Memory.Code.EnumGenerator.Attributes;

namespace Beskar.Networking.Cluster.Protocol.Enums;

/// <summary>
/// Represents the role of a cluster node.
/// </summary>
[FastEnum]
public enum ClusterNodeRole : byte
{
   /// <summary>
   /// The role is unknown.
   /// </summary>
   None = 0,

   /// <summary>
   /// The node is a leader.
   /// </summary>
   Leader,

   /// <summary>
   /// The node is a replica.
   /// </summary>
   Replica,

   /// <summary>
   /// The node is a candidate for leader election.
   /// </summary>
   Candidate,

   /// <summary>
   /// The node is an observer.
   /// </summary>
   Observer
}
