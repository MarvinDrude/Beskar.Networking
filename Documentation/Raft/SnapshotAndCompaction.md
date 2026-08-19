# Raft Snapshotting & Log Compaction Guide

In a Raft cluster, retaining all committed log entries indefinitely would result in unbounded log growth, disk space exhaustion, and long node startup times due to replaying millions of log commands into the state machine.

`Beskar.Networking.Raft` provides built-in contracts and protocol messages to perform **State Machine Snapshotting**, **Log Compaction**, and **Snapshot Installation** (`InstallSnapshot`).

---

## 1. Overview of Snapshotting & Compaction

```
Before Snapshotting:
[Log #1: SET a=1] [Log #2: SET b=2] ... [Log #1000: SET a=500] --> Raft Log (Unbounded Growth)

After Snapshotting at Index #1000:
Snapshot Blob (State Machine: { a: 500, b: 2 }) + Raft Log Tail ([Log #1001: SET c=3] ...)
```

1. **State Machine Snapshot**: The current in-memory/on-disk state of the replicated state machine represents the cumulative outcome of all past applied commands.
2. **Log Truncation**: Once a snapshot is safely recorded up to `lastIncludedIndex`, historical log entries prior to `lastIncludedIndex` are compacted and discarded.
3. **Catching Up Lagging Followers**: If a follower node goes offline and falls behind beyond the leader's truncated log threshold, the leader sends an `InstallSnapshot` RPC to bring the follower up to date directly from the snapshot.

---

## 2. Core Snapshotting Contracts

### `IRaftStateMachine`

State machine implementations can opt into snapshot support by implementing `TakeSnapshotAsync` and `RestoreSnapshotAsync`:

```csharp
public interface IRaftStateMachine
{
   ValueTask<ReadOnlyMemory<byte>> ApplyAsync(ReadOnlyMemory<byte> command, ulong logIndex, CancellationToken ct = default);

   /// <summary>
   /// Takes a snapshot of the current state machine state.
   /// </summary>
   ValueTask<ReadOnlyMemory<byte>> TakeSnapshotAsync(CancellationToken ct = default);

   /// <summary>
   /// Restores the state machine state from a given snapshot payload.
   /// </summary>
   ValueTask RestoreSnapshotAsync(ReadOnlyMemory<byte> snapshot, ulong lastIncludedIndex, ulong lastIncludedTerm, CancellationToken ct = default);
}
```

### `IRaftLogStorage` Truncation

Log storage implementations (`FileRaftLogStorage`, `InMemoryRaftLogStorage`) expose `TruncateLogAsync`:

```csharp
// Truncates log entries from fromIndex onwards (inclusive)
ValueTask TruncateLogAsync(ulong fromIndex, CancellationToken ct = default);
```

---

## 3. Snapshot Installation RPC (`InstallSnapshot`)

When a follower node is too far behind to receive log entries via `AppendEntries`, `RaftNode` automatically handles `InstallSnapshot`:

| Field | Type | Description |
| :--- | :--- | :--- |
| `Term` | `ulong` | Leader's term. |
| `LeaderId` | `string` | Leader's node identifier. |
| `LastIncludedIndex` | `ulong` | The snapshot replaces all entries up to and including this index. |
| `LastIncludedTerm` | `ulong` | Term of `LastIncludedIndex`. |
| `Data` | `ReadOnlyMemory<byte>` | Raw binary payload of the state machine snapshot. |

Upon receiving `InstallSnapshotRequest`:
1. The follower restores its state machine using `StateMachine.RestoreSnapshotAsync(Data, LastIncludedIndex, LastIncludedTerm)`.
2. The follower truncates its log and advances its `CommitIndex` and `LastApplied` to `LastIncludedIndex`.

---

## 4. Usage Example

Here is a simplified snapshot-capable key-value state machine:

```csharp
using System.Text;
using System.Text.Json;
using Beskar.Networking.Raft.StateMachine;

public sealed class KeyValueStateMachine : IRaftStateMachine
{
   private Dictionary<string, string> _store = new();

   public ValueTask<ReadOnlyMemory<byte>> ApplyAsync(ReadOnlyMemory<byte> command, ulong logIndex, CancellationToken ct = default)
   {
      var text = Encoding.UTF8.GetString(command.Span);
      var parts = text.Split('=');
      if (parts.Length == 2)
      {
         _store[parts[0]] = parts[1];
      }
      return ValueTask.FromResult<ReadOnlyMemory<byte>>(Encoding.UTF8.GetBytes("OK"));
   }

   // 1. Capture snapshot of state
   public ValueTask<ReadOnlyMemory<byte>> TakeSnapshotAsync(CancellationToken ct = default)
   {
      var json = JsonSerializer.SerializeToUtf8Bytes(_store);
      return ValueTask.FromResult<ReadOnlyMemory<byte>>(json);
   }

   // 2. Restore state directly from snapshot blob
   public ValueTask RestoreSnapshotAsync(ReadOnlyMemory<byte> snapshot, ulong lastIncludedIndex, ulong lastIncludedTerm, CancellationToken ct = default)
   {
      if (!snapshot.IsEmpty)
      {
         var deserialized = JsonSerializer.Deserialize<Dictionary<string, string>>(snapshot.Span);
         if (deserialized != null)
         {
            _store = deserialized;
         }
      }
      return ValueTask.CompletedTask;
   }
}
```

---

## 5. Related Resources & Examples

- **Example Code**: [`Examples/Raft/Beskar.Raft.Example.SnapshotCompaction`](https://github.com/MarvinDrude/Beskar.Networking/tree/master/Examples/Raft/Beskar.Raft.Example.SnapshotCompaction)
- [Raft Consensus Overview](Overview.md)
- [Storage & Persistence Guide](StorageAndPersistence.md)
