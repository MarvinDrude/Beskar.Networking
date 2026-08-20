namespace Beskar.Networking.Raft.Protocol.Models;

/// <summary>
/// Represents a single immutable log entry stored in the Raft distributed log.
/// </summary>
/// <param name="Term">The election term when this entry was received by the leader.</param>
/// <param name="Index">The 1-based monotonically increasing index in the log.</param>
/// <param name="Data">The command payload to apply to the state machine upon commitment.</param>
public readonly record struct RaftLogEntry(ulong Term, ulong Index, ReadOnlyMemory<byte> Data)
{
   /// <summary>
   /// Empty/null representation of a log entry.
   /// </summary>
   public static readonly RaftLogEntry Empty = new(0, 0, ReadOnlyMemory<byte>.Empty);
}
