# State Machine & Client Proposals

In the Raft consensus architecture, the **State Machine** represents your application's actual business data (e.g. key-value store, distributed database, banking ledger, or actor cluster state).

---

## 1. Implementing `IRaftStateMachine`

Application state machines implement the [`IRaftStateMachine`](https://github.com/MarvinDrude/Beskar.Networking/blob/master/Raft/Beskar.Networking.Raft/StateMachine/IRaftStateMachine.cs) contract:

```csharp
using System.Text;
using System.Text.Json;
using Beskar.Networking.Raft.StateMachine;

public sealed class KeyValueStateMachine : IRaftStateMachine
{
   private readonly Dictionary<string, string> _store = new();
   private readonly Lock _lock = new();

   public ValueTask<ReadOnlyMemory<byte>> ApplyAsync(
      ReadOnlyMemory<byte> command,
      ulong logIndex,
      CancellationToken ct = default)
   {
      var operation = JsonSerializer.Deserialize<KvOperation>(command.Span);
      if (operation == null)
      {
         return ValueTask.FromResult<ReadOnlyMemory<byte>>(Encoding.UTF8.GetBytes("ERR_INVALID"));
      }

      lock (_lock)
      {
         switch (operation.Action)
         {
            case "SET":
               _store[operation.Key] = operation.Value!;
               return ValueTask.FromResult<ReadOnlyMemory<byte>>(Encoding.UTF8.GetBytes("OK"));

            case "DEL":
               _store.Remove(operation.Key);
               return ValueTask.FromResult<ReadOnlyMemory<byte>>(Encoding.UTF8.GetBytes("OK"));

            default:
               return ValueTask.FromResult<ReadOnlyMemory<byte>>(Encoding.UTF8.GetBytes("ERR_UNKNOWN"));
         }
      }
   }
}
```

---

## 2. Submitting Proposals via `ProposeAsync`

To submit a mutation to the cluster, clients call `leader.ProposeAsync(command, cancellationToken)`.

### How Proposal Consensus Works:
1. **Validation**: The node verifies it is the current **Leader**. If called on a follower or candidate, it throws an `InvalidOperationException` containing the current `LeaderId`.
2. **Log Append**: The leader assigns the next sequential log index (`Index = LastLogIndex + 1`) and appends the entry to its local storage.
3. **Replication**: The leader broadcasts `AppendEntries` RPCs containing the new entry to all cluster followers over the network.
4. **Quorum Commit**: Once a majority ($\lfloor N/2 \rfloor + 1$) of nodes have acknowledged writing the entry to disk, the leader advances its `CommitIndex`.
5. **State Machine Execution**: The leader applies the command deterministically to its `IRaftStateMachine` and completes the `ProposeAsync` task with the returned result bytes.

```csharp
var commandBytes = JsonSerializer.SerializeToUtf8Bytes(new KvOperation("SET", "account:101", "1500"));

// Submit to leader
var responseBytes = await leader.ProposeAsync(commandBytes);
Console.WriteLine($"Result: {Encoding.UTF8.GetString(responseBytes.Span)}");
```

---

## 3. Events & Lifecycle Monitoring

`RaftNode` exposes a pipeline of strongly-typed events via `HandlerPipeline<T>`:

```csharp
// 1. Role transitions (Follower -> Candidate -> Leader)
node.Events.OnRoleChanged.Add((ctx, ct) =>
{
   Console.WriteLine($"Node '{ctx.NodeId}' transitioned: {ctx.OldRole} -> {ctx.NewRole} in Term {ctx.Term}");
   return ValueTask.CompletedTask;
});

// 2. Leader changes
node.Events.OnLeaderChanged.Add((ctx, ct) =>
{
   Console.WriteLine($"Node '{ctx.NodeId}' detected new leader: '{ctx.LeaderId}' in Term {ctx.Term}");
   return ValueTask.CompletedTask;
});

// 3. Entry commits
node.Events.OnEntryCommitted.Add((ctx, ct) =>
{
   Console.WriteLine($"Committed entry #{ctx.Entry.Index}: {Encoding.UTF8.GetString(ctx.Result.Span)}");
   return ValueTask.CompletedTask;
});
```

---

## 4. Leader Failover & Follower Reinstallation

When a leader crashes or network partitions occur:
1. The remaining followers detect the missing heartbeats and elect a new leader.
2. The new leader continues committing client commands with the remaining quorum nodes.
3. When the disconnected/crashed node comes back online, the new leader detects its `MatchIndex` is behind and automatically replicates all missing log entries in order, reinstalling the node's state up to the cluster's current commit index.
