namespace Beskar.Networking.Raft.StateMachine;

/// <summary>
/// Defines the replicated state machine contract applied deterministically by Raft on committed log entries.
/// </summary>
public interface IRaftStateMachine
{
   /// <summary>
   /// Applies a committed command payload to the state machine deterministically.
   /// </summary>
   /// <param name="command">The command payload stored in the committed log entry.</param>
   /// <param name="logIndex">The index of the log entry being applied.</param>
   /// <param name="ct">Cancellation token.</param>
   /// <returns>The result of applying the command.</returns>
   ValueTask<ReadOnlyMemory<byte>> ApplyAsync(ReadOnlyMemory<byte> command, ulong logIndex, CancellationToken ct = default);

   /// <summary>
   /// Takes a snapshot of the current state machine state.
   /// </summary>
   ValueTask<ReadOnlyMemory<byte>> TakeSnapshotAsync(CancellationToken ct = default)
      => ValueTask.FromResult(ReadOnlyMemory<byte>.Empty);

   /// <summary>
   /// Restores the state machine state from a given snapshot.
   /// </summary>
   ValueTask RestoreSnapshotAsync(ReadOnlyMemory<byte> snapshot, ulong lastIncludedIndex, ulong lastIncludedTerm, CancellationToken ct = default)
      => ValueTask.CompletedTask;
}
