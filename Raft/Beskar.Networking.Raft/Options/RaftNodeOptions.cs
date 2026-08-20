namespace Beskar.Networking.Raft.Options;

/// <summary>
/// Configuration options for initializing a <see cref="RaftNode"/>.
/// </summary>
public sealed class RaftNodeOptions
{
   /// <summary>
   /// Unique identifier of this node within the Raft cluster.
   /// </summary>
   public string NodeId { get; set; } = Guid.NewGuid().ToString("N");

   /// <summary>
   /// List of peer identifiers that make up the rest of the Raft cluster.
   /// </summary>
   public IReadOnlyList<string> Peers { get; set; } = Array.Empty<string>();

   /// <summary>
   /// Minimum election timeout before starting a new election. Default is 150ms.
   /// </summary>
   public TimeSpan ElectionTimeoutMin { get; set; } = TimeSpan.FromMilliseconds(150);

   /// <summary>
   /// Maximum election timeout before starting a new election. Default is 300ms.
   /// </summary>
   public TimeSpan ElectionTimeoutMax { get; set; } = TimeSpan.FromMilliseconds(300);

   /// <summary>
   /// Interval between leader heartbeats. Default is 50ms.
   /// </summary>
   public TimeSpan HeartbeatInterval { get; set; } = TimeSpan.FromMilliseconds(50);

   /// <summary>
   /// Maximum number of log entries sent in a single AppendEntries batch. Default is 100.
   /// </summary>
   public int MaxAppendEntriesBatchSize { get; set; } = 100;

   /// <summary>
   /// Whether to run a non-disruptive Pre-Vote phase before incrementing term. Default is true.
   /// </summary>
   public bool EnablePreVote { get; set; } = true;
}
