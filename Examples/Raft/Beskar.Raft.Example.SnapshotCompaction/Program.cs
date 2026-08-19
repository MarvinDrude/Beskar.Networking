using System.Text;
using System.Text.Json;
using Beskar.Networking.Raft;
using Beskar.Networking.Raft.Options;
using Beskar.Networking.Raft.StateMachine;
using Beskar.Networking.Raft.Storage;
using Beskar.Networking.Raft.Transport;
using Beskar.Networking.Transports.Memory;

Console.WriteLine("=== Beskar Raft Snapshot & Log Compaction Example ===\n");

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
   Console.WriteLine($"\n[Log State Before Snapshot] Last Log Index: {lastIndexBefore}");
   for (ulong i = 1; i <= lastIndexBefore; i++)
   {
      var entry = await storage.GetEntryAsync(i);
      Console.WriteLine($"  - Log #{i}: Term={entry?.Term}, Data='{(entry.HasValue ? Encoding.UTF8.GetString(entry.Value.Data.Span) : "<null>")}'");
   }

   // ====================================================
   // Phase 2: Create State Machine Snapshot
   // ====================================================
   Console.WriteLine("\n--- Phase 2: Creating State Machine Snapshot ---");
   var snapshotIndex = lastIndexBefore;
   var snapshotTerm = node.CurrentTerm;

   // Take snapshot of current state machine state (captures key1..key10)
   var snapshotData = await stateMachine.TakeSnapshotAsync();
   Console.WriteLine($"State Machine Snapshot taken at Index #{snapshotIndex}, Term {snapshotTerm}.");
   Console.WriteLine($"Snapshot Blob Size: {snapshotData.Length} bytes.");

   // Demonstrating conflict log truncation (e.g. discarding uncommitted entries 8..10)
   Console.WriteLine("\nTruncating uncommitted log entries 8..10 from log storage...");
   await storage.TruncateLogAsync(8);

   var lastIndexAfter = await storage.GetLastLogIndexAsync();
   Console.WriteLine($"\n[Log State After Log Truncation] Last Log Index in Storage: {lastIndexAfter}");
   for (ulong i = 1; i <= 10; i++)
   {
      var entry = await storage.GetEntryAsync(i);
      var status = entry.HasValue ? Encoding.UTF8.GetString(entry.Value.Data.Span) : "<truncated / missing from log>";
      Console.WriteLine($"  - Log #{i}: {status}");
   }

   // ====================================================
   // Phase 3: Restoring a Fresh State Machine from Snapshot
   // ====================================================
   Console.WriteLine("\n--- Phase 3: Restoring State Machine directly from Snapshot Payload ---");
   Console.WriteLine("Notice: Restoring from snapshot does NOT replay log storage!");
   Console.WriteLine("It loads state directly from the snapshot blob (which was taken at Index #10).\n");

   var restoredStateMachine = new SnapshotCapableStateMachine();
   await restoredStateMachine.RestoreSnapshotAsync(snapshotData, snapshotIndex, snapshotTerm);

   Console.WriteLine("Restored State Machine Key-Value Pairs:");
   foreach (var (key, value) in restoredStateMachine.Store)
   {
      Console.WriteLine($"  - {key} => {value}");
   }

   await node.StopAsync();
   Console.WriteLine("\nSnapshot example completed successfully!");
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
