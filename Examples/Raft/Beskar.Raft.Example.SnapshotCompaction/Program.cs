using System.Text;
using System.Text.Json;
using Beskar.Networking.Raft;
using Beskar.Networking.Raft.Options;
using Beskar.Networking.Raft.StateMachine;
using Beskar.Networking.Raft.Storage;
using Beskar.Networking.Raft.Transport;
using Beskar.Networking.Transports.Memory;

Console.WriteLine("=== Beskar Raft Log Compaction & Snapshotting Example ===\n");

var dataDir = Path.Combine(Path.GetTempPath(), $"raft_snapshot_demo_{Guid.NewGuid():N}");
Console.WriteLine($"Storage directory: {dataDir}\n");

try
{
   var options = new RaftNodeOptions
   {
      NodeId = "snapshot-node-1",
      Peers = [],
      ElectionTimeoutMin = TimeSpan.FromMilliseconds(50),
      ElectionTimeoutMax = TimeSpan.FromMilliseconds(100),
      HeartbeatInterval = TimeSpan.FromMilliseconds(20)
   };

   var storage = new FileRaftLogStorage(dataDir);
   var stateMachine = new SnapshotCapableStateMachine();
   var memoryOptions = new MemoryTransportOptions();
   var endPoint = new MemoryEndPoint($"raft-snapshot-{Guid.NewGuid():N}");
   var listener = new MemoryNetworkListener(endPoint, memoryOptions);
   var transport = new RaftNetworkTransport(listener, []);

   await using var node = new RaftNode(options, storage, stateMachine, transport);

   node.Events.OnEntryCommitted.Add((ctx, _) =>
   {
      Console.WriteLine($"[Committed] Log Index #{ctx.Entry.Index}: '{Encoding.UTF8.GetString(ctx.Entry.Data.Span)}'");
      return ValueTask.CompletedTask;
   });

   await node.StartAsync();
   await Task.Delay(150); // Wait for leader election

   // ====================================================
   // Phase 1: Propose Log Entries & Mutate State
   // ====================================================
   Console.WriteLine("--- Phase 1: Submitting 10 state mutations ---");
   for (var i = 1; i <= 10; i++)
   {
      var cmd = $"SET key{i}=value{i}";
      await node.ProposeAsync(Encoding.UTF8.GetBytes(cmd));
   }

   var lastIndexBefore = await storage.GetLastLogIndexAsync();
   Console.WriteLine($"\n[Log State Before Compaction] Last Log Index: {lastIndexBefore}");
   for (ulong i = 1; i <= lastIndexBefore; i++)
   {
      var entry = await storage.GetEntryAsync(i);
      Console.WriteLine($"  - Log #{i}: Term={entry?.Term}, Data='{(entry.HasValue ? Encoding.UTF8.GetString(entry.Value.Data.Span) : "<null>")}'");
   }

   // ====================================================
   // Phase 2: Prefix Log Compaction & Snapshotting
   // ====================================================
   Console.WriteLine("\n--- Phase 2: Performing Snapshot Prefix Compaction ---");
   var snapshotIndex = 7UL; // Snapshot captures state up to Log Index #7
   var snapshotTerm = node.CurrentTerm;

   // 1. Take snapshot of state machine state at Index #7
   var snapshotData = await stateMachine.TakeSnapshotAsync();
   Console.WriteLine($"State Machine Snapshot taken at Index #{snapshotIndex}, Term {snapshotTerm}.");
   Console.WriteLine($"Snapshot Blob Size: {snapshotData.Length} bytes.");

   // 2. Compact prefix log entries 1..7 (historical entries now saved in snapshot)
   Console.WriteLine($"Compacting prefix log entries #1 through #{snapshotIndex}...");
   await storage.CompactPrefixAsync(snapshotIndex);

   var lastIndexAfter = await storage.GetLastLogIndexAsync();
   Console.WriteLine($"\n[Log State After Prefix Compaction] Last Log Index in Storage: {lastIndexAfter}");
   for (ulong i = 1; i <= 10; i++)
   {
      var entry = await storage.GetEntryAsync(i);
      var status = entry.HasValue ? Encoding.UTF8.GetString(entry.Value.Data.Span) : "<compacted / discarded from disk>";
      Console.WriteLine($"  - Log #{i}: {status}");
   }

   // ====================================================
   // Phase 3: Restoring Fresh State Machine from Snapshot + Tail Replay
   // ====================================================
   Console.WriteLine("\n--- Phase 3: Restoring State Machine from Snapshot + Tail Replay ---");
   var restoredStateMachine = new SnapshotCapableStateMachine();

   // Step A: Load snapshot blob (restores key1..key7)
   await restoredStateMachine.RestoreSnapshotAsync(snapshotData, snapshotIndex, snapshotTerm);
   Console.WriteLine($"Restored base state from snapshot up to Index #{snapshotIndex}.");

   // Step B: Replay remaining tail entries (8..10) from log storage
   var tailEntries = await storage.GetEntriesAsync(snapshotIndex + 1, 100);
   Console.WriteLine($"Replaying {tailEntries.Count} remaining tail log entries from storage...");
   foreach (var tailEntry in tailEntries)
   {
      await restoredStateMachine.ApplyAsync(tailEntry.Data, tailEntry.Index);
   }

   Console.WriteLine("\nFinal Restored State Machine Key-Value Pairs:");
   foreach (var (key, value) in restoredStateMachine.Store)
   {
      Console.WriteLine($"  - {key} => {value}");
   }

   await node.StopAsync();
   Console.WriteLine("\nSnapshot compaction example completed successfully!");
}
finally
{
   if (Directory.Exists(dataDir))
   {
      Directory.Delete(dataDir, recursive: true);
   }
}

/// <summary>
/// A state machine implementation that supports capturing and restoring snapshots.
/// </summary>
internal sealed class SnapshotCapableStateMachine : IRaftStateMachine
{
   public Dictionary<string, string> Store { get; private set; } = new();

   public ValueTask<ReadOnlyMemory<byte>> ApplyAsync(ReadOnlyMemory<byte> command, ulong logIndex, CancellationToken ct = default)
   {
      var text = Encoding.UTF8.GetString(command.Span);
      if (text.StartsWith("SET "))
      {
         var parts = text[4..].Split('=');
         if (parts.Length == 2)
         {
            Store[parts[0]] = parts[1];
         }
      }

      return ValueTask.FromResult<ReadOnlyMemory<byte>>(Encoding.UTF8.GetBytes("OK"));
   }

   public ValueTask<ReadOnlyMemory<byte>> TakeSnapshotAsync(CancellationToken ct = default)
   {
      var json = JsonSerializer.SerializeToUtf8Bytes(Store);
      return ValueTask.FromResult<ReadOnlyMemory<byte>>(json);
   }

   public ValueTask RestoreSnapshotAsync(ReadOnlyMemory<byte> snapshot, ulong lastIncludedIndex, ulong lastIncludedTerm, CancellationToken ct = default)
   {
      if (!snapshot.IsEmpty)
      {
         var deserialized = JsonSerializer.Deserialize<Dictionary<string, string>>(snapshot.Span);
         if (deserialized != null)
         {
            Store = deserialized;
         }
      }

      return ValueTask.CompletedTask;
   }
}
