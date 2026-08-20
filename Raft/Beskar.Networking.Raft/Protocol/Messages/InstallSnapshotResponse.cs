namespace Beskar.Networking.Raft.Protocol.Messages;

/// <summary>
/// Response to an <see cref="InstallSnapshotRequest"/> RPC.
/// </summary>
public sealed class InstallSnapshotResponse
{
   /// <summary>
   /// The responder's current term, for leader to update itself if behind.
   /// </summary>
   public ulong Term { get; set; }

   /// <summary>
   /// True if snapshot was successfully applied by the follower.
   /// </summary>
   public bool Success { get; set; }
}
