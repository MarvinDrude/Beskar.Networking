namespace Beskar.Networking.Raft.Protocol.Messages;

/// <summary>
/// Invoked by leader to send chunks of a snapshot to a follower.
/// </summary>
public sealed class InstallSnapshotRequest
{
   /// <summary>
   /// The leader's term.
   /// </summary>
   public ulong Term { get; set; }

   /// <summary>
   /// Identifier of the leader so follower can redirect clients.
   /// </summary>
   public string LeaderId { get; set; } = string.Empty;

   /// <summary>
   /// The snapshot replaces all entries up through and including this index.
   /// </summary>
   public ulong LastIncludedIndex { get; set; }

   /// <summary>
   /// Term of <see cref="LastIncludedIndex"/>.
   /// </summary>
   public ulong LastIncludedTerm { get; set; }

   /// <summary>
   /// Raw snapshot byte payload.
   /// </summary>
   public ReadOnlyMemory<byte> Data { get; set; }
}
