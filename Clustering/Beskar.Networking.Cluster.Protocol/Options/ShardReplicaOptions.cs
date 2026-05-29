namespace Beskar.Networking.Cluster.Protocol.Options;

/// <summary>
/// Options for configuring the behavior of a shard replica.
/// </summary>
public sealed class ShardReplicaOptions
{
   /// <summary>
   /// The minimum election timeout duration for a shard replica.
   /// </summary>
   public TimeSpan MinElectionTimeout { get; set; } = TimeSpan.FromMilliseconds(150);

   /// <summary>
   /// The maximum election timeout duration for a shard replica.
   /// </summary>
   public TimeSpan MaxElectionTimeout { get; set; } = TimeSpan.FromMilliseconds(300);

   /// <summary>
   /// The heartbeat interval for a shard replica.
   /// </summary>
   public TimeSpan HeartbeatInterval { get; set; } = TimeSpan.FromMilliseconds(100);
}
