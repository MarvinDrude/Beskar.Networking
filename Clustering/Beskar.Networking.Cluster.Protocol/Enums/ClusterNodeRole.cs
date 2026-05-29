using Beskar.Memory.Code.EnumGenerator.Attributes;

namespace Beskar.Networking.Cluster.Protocol.Enums;

/// <summary>
/// Represents the operational role of a cluster node within a shard or consensus group.
/// </summary>
[FastEnum]
public enum ClusterNodeRole : byte
{
   /// <summary>
   /// The node is not a member of the cluster.
   /// </summary>
   None = 0,

   /// <summary>
   /// The node is a leader in the shard or consensus group.
   /// </summary>
   Leader = 1,

   /// <summary>
   /// The node is a replica in the shard or consensus group.
   /// </summary>
   Replica = 2,

   /// <summary>
   /// The node is a candidate in the shard or consensus group.
   /// </summary>
   Candidate = 3,

   /// <summary>
   /// The node is an observer in the shard or consensus group.
   /// </summary>
   Observer = 4
}
