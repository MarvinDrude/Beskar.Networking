using System.Text;
using Beskar.Networking.Raft;
using Beskar.Networking.Raft.Enums;
using Beskar.Networking.Raft.Options;
using Beskar.Networking.Raft.StateMachine;
using Beskar.Networking.Raft.Storage;
using Beskar.Networking.Raft.Transport;
using Beskar.Networking.Transports.Memory;

Console.WriteLine("=== Beskar Raft Disk-Backed Persistent Log Storage Example ===\n");

var dataDir = Path.Combine(Path.GetTempPath(), $"raft_persist_demo_{Guid.NewGuid():N}");
Console.WriteLine($"Storage directory: {dataDir}\n");

try
{
   // ==========================================
   // Phase 1: Start node, commit entries, stop
   // ==========================================
   Console.WriteLine("--- Phase 1: Boot node and persist state ---");
   {
      var options = new RaftNodeOptions
      {
         NodeId = "persistent-node-1",
         Peers = Array.Empty<string>(),
         ElectionTimeoutMin = TimeSpan.FromMilliseconds(50),
         ElectionTimeoutMax = TimeSpan.FromMilliseconds(100),
         HeartbeatInterval = TimeSpan.FromMilliseconds(20)
      };

      var storage = new FileRaftLogStorage(dataDir);
      var sm = new PersistentDemoStateMachine();
      var memoryOptions = new MemoryTransportOptions();
      var endPoint = new MemoryEndPoint($"raft-persist-{Guid.NewGuid():N}");
      var listener = new MemoryNetworkListener(endPoint, memoryOptions);
      var transport = new RaftNetworkTransport(listener, []);

      await using var node = new RaftNode(options, storage, sm, transport);

      node.Events.OnRoleChanged.Add((ctx, _) =>
      {
         Console.WriteLine($"[Event] Node role: {ctx.OldRole} -> {ctx.NewRole} in Term {ctx.Term}");
         return ValueTask.CompletedTask;
      });

      node.Events.OnEntryCommitted.Add((ctx, _) =>
      {
         Console.WriteLine($"[Event] Committed #{ctx.Entry.Index} to disk: '{Encoding.UTF8.GetString(ctx.Entry.Data.Span)}'");
         return ValueTask.CompletedTask;
      });

      await node.StartAsync();
      await Task.Delay(150); // Wait for leader election

      Console.WriteLine("\nSubmitting proposals to persist to disk...");
      await node.ProposeAsync(Encoding.UTF8.GetBytes("TRANSACTION:1001:DEPOSIT:500"));
      await node.ProposeAsync(Encoding.UTF8.GetBytes("TRANSACTION:1002:TRANSFER:250"));
      await node.ProposeAsync(Encoding.UTF8.GetBytes("TRANSACTION:1003:WITHDRAW:100"));

      Console.WriteLine($"Current Term before shutdown: {node.CurrentTerm}, Commit Index: {node.CommitIndex}");
      Console.WriteLine("Stopping node gracefully (simulating shutdown/crash)...");
      await node.StopAsync();
   }

   // Inspect files on disk
   Console.WriteLine("\n--- Disk Inspection ---");
   var metaFile = Path.Combine(dataDir, "metadata.bin");
   var logFile = Path.Combine(dataDir, "raft.log");
   Console.WriteLine($"metadata.bin size: {new FileInfo(metaFile).Length} bytes");
   Console.WriteLine($"raft.log size:     {new FileInfo(logFile).Length} bytes");

   // ====================================================
   // Phase 2: Restart node from same directory and verify
   // ====================================================
   Console.WriteLine("\n--- Phase 2: Restart node from existing disk files ---");
   {
      var options = new RaftNodeOptions
      {
         NodeId = "persistent-node-1",
         Peers = Array.Empty<string>(),
         ElectionTimeoutMin = TimeSpan.FromMilliseconds(50),
         ElectionTimeoutMax = TimeSpan.FromMilliseconds(100),
         HeartbeatInterval = TimeSpan.FromMilliseconds(20)
      };

      // Reopen file storage from the same directory!
      var storage = new FileRaftLogStorage(dataDir);
      Console.WriteLine($"Storage reloaded: Recovered Term = {await storage.GetCurrentTermAsync()}, Last Log Index = {await storage.GetLastLogIndexAsync()}");

      var sm = new PersistentDemoStateMachine();
      var memoryOptions = new MemoryTransportOptions();
      var endPoint = new MemoryEndPoint($"raft-persist-{Guid.NewGuid():N}");
      var listener = new MemoryNetworkListener(endPoint, memoryOptions);
      var transport = new RaftNetworkTransport(listener, Array.Empty<RaftPeerEndpoint>());

      await using var node = new RaftNode(options, storage, sm, transport);

      node.Events.OnRoleChanged.Add((ctx, _) =>
      {
         Console.WriteLine($"[Event] Node role: {ctx.OldRole} -> {ctx.NewRole} in Term {ctx.Term}");
         return ValueTask.CompletedTask;
      });

      node.Events.OnEntryCommitted.Add((ctx, _) =>
      {
         Console.WriteLine($"[Event] Committed #{ctx.Entry.Index} to disk: '{Encoding.UTF8.GetString(ctx.Entry.Data.Span)}'");
         return ValueTask.CompletedTask;
      });

      await node.StartAsync();
      await Task.Delay(150);

      // Verify log entries 1, 2, 3 still exist on disk
      for (ulong i = 1; i <= 3; i++)
      {
         var entry = await storage.GetEntryAsync(i);
         var text = entry.HasValue ? Encoding.UTF8.GetString(entry.Value.Data.Span) : "<null>";
         Console.WriteLine($"[Storage Verification] Entry #{i}: Term={entry?.Term}, Data='{text}'");
      }

      // Propose new command (should become index #4)
      Console.WriteLine("\nProposing new command after restart...");
      await node.ProposeAsync(Encoding.UTF8.GetBytes("TRANSACTION:1004:DEPOSIT:1000"));
      Console.WriteLine($"Final Commit Index after continuation: {node.CommitIndex}");

      await node.StopAsync();
   }

   Console.WriteLine("\nPersistence example completed successfully!");
}
finally
{
   if (Directory.Exists(dataDir))
   {
      Directory.Delete(dataDir, recursive: true);
   }
}

internal sealed class PersistentDemoStateMachine : IRaftStateMachine
{
   public ValueTask<ReadOnlyMemory<byte>> ApplyAsync(ReadOnlyMemory<byte> command, ulong logIndex, CancellationToken ct = default)
   {
      var text = Encoding.UTF8.GetString(command.Span);
      return ValueTask.FromResult<ReadOnlyMemory<byte>>(Encoding.UTF8.GetBytes($"PROCESSED:{text}"));
   }
}
