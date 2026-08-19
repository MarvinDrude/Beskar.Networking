using System.Collections.Concurrent;
using System.Text;
using Beskar.Networking.Raft;
using Beskar.Networking.Raft.Enums;
using Beskar.Networking.Raft.Options;
using Beskar.Networking.Raft.StateMachine;
using Beskar.Networking.Raft.Storage;
using Beskar.Networking.Raft.Transport;
using Beskar.Networking.Transports.Memory;

Console.WriteLine("=== Beskar Raft Leader Failover & Reinstallation Example ===");

var clusterId = Guid.NewGuid().ToString("N");
var nodeIds = new[] { "node-1", "node-2", "node-3" };
var memoryOptions = new MemoryTransportOptions();

var endpoints = nodeIds.ToDictionary(
   id => id,
   id => new MemoryEndPoint($"raft-failover-{clusterId}-{id}"));

var storages = nodeIds.ToDictionary(id => id, _ => new InMemoryRaftLogStorage());
var stateMachines = nodeIds.ToDictionary(id => id, id => new FailoverDemoStateMachine(id));

RaftNode CreateNode(string id)
{
   var peers = nodeIds.Where(x => x != id).ToList();
   var peerEndpoints = peers.Select(p => new RaftPeerEndpoint(
      p,
      endpoints[p],
      () => new MemoryNetworkClient(memoryOptions))).ToList();

   var listener = new MemoryNetworkListener(endpoints[id], memoryOptions);
   var transport = new RaftNetworkTransport(listener, peerEndpoints, TimeSpan.FromMilliseconds(150));

   var options = new RaftNodeOptions
   {
      NodeId = id,
      Peers = peers,
      ElectionTimeoutMin = TimeSpan.FromMilliseconds(100),
      ElectionTimeoutMax = TimeSpan.FromMilliseconds(200),
      HeartbeatInterval = TimeSpan.FromMilliseconds(30)
   };

   var node = new RaftNode(options, storages[id], stateMachines[id], transport);

   // Hook in all events with clear visual logging
   node.Events.OnRoleChanged.Add((ctx, _) =>
   {
      Console.ForegroundColor = ctx.NewRole switch
      {
         RaftRole.Leader => ConsoleColor.Green,
         RaftRole.Candidate => ConsoleColor.Yellow,
         RaftRole.Follower => ConsoleColor.Cyan,
         _ => ConsoleColor.Gray
      };
      Console.WriteLine($"[ROLE CHANGE] {ctx.NodeId,-6}: {ctx.OldRole} -> {ctx.NewRole} (Term {ctx.Term})");
      Console.ResetColor();
      return ValueTask.CompletedTask;
   });

   node.Events.OnLeaderChanged.Add((ctx, _) =>
   {
      Console.WriteLine($"[LEADER SEEN] {ctx.NodeId,-6} recognizes Leader '{ctx.LeaderId}' in Term {ctx.Term}");
      return ValueTask.CompletedTask;
   });

   node.Events.OnEntryCommitted.Add((ctx, _) =>
   {
      Console.WriteLine($"[COMMIT]      {ctx.NodeId,-6} committed Log #{ctx.Entry.Index} (Term {ctx.Entry.Term}): '{Encoding.UTF8.GetString(ctx.Entry.Data.Span)}'");
      return ValueTask.CompletedTask;
   });

   return node;
}

var activeNodes = new Dictionary<string, RaftNode>();
foreach (var id in nodeIds)
{
   activeNodes[id] = CreateNode(id);
}

// ----------------------------------------------------
// Step 1: Start cluster and elect initial leader
// ----------------------------------------------------
Console.WriteLine("\n--- Step 1: Starting cluster and electing initial leader ---");
foreach (var node in activeNodes.Values)
{
   await node.StartAsync();
}

RaftNode? leader1 = null;
var deadline = Environment.TickCount64 + 3000;
while (Environment.TickCount64 < deadline)
{
   leader1 = activeNodes.Values.FirstOrDefault(n => n.Role == RaftRole.Leader);
   if (leader1 != null) break;
   await Task.Delay(50);
}

var leader1Id = leader1!.Options.NodeId;
Console.WriteLine($"\n>>> Leader is '{leader1Id}'. Proposing Entry #1...");
await leader1.ProposeAsync(Encoding.UTF8.GetBytes("MSG_1_INITIAL_SETUP"));

await Task.Delay(150);

// ----------------------------------------------------
// Step 2: Crash the leader!
// ----------------------------------------------------
Console.WriteLine($"\n--- Step 2: Crashing leader '{leader1Id}' ---");
Console.ForegroundColor = ConsoleColor.Red;
Console.WriteLine($"[CRASH] Stopping leader '{leader1Id}'...");
Console.ResetColor();

await leader1.StopAsync();
activeNodes.Remove(leader1Id);

// ----------------------------------------------------
// Step 3: Remaining 2 nodes elect a new leader
// ----------------------------------------------------
Console.WriteLine("\n--- Step 3: Remaining 2 nodes detecting timeout & electing new leader ---");

RaftNode? leader2 = null;
var failoverDeadline = Environment.TickCount64 + 4000;
while (Environment.TickCount64 < failoverDeadline)
{
   leader2 = activeNodes.Values.FirstOrDefault(n => n.Role == RaftRole.Leader);
   if (leader2 != null) break;
   await Task.Delay(50);
}

var leader2Id = leader2!.Options.NodeId;
Console.ForegroundColor = ConsoleColor.Green;
Console.WriteLine($"\n>>> New Leader elected: '{leader2Id}' in Term {leader2.CurrentTerm}!");
Console.ResetColor();

// ----------------------------------------------------
// Step 4: Propose entries 2 & 3 while old leader is dead
// ----------------------------------------------------
Console.WriteLine($"\n--- Step 4: Committing entries while '{leader1Id}' is offline ---");
await leader2.ProposeAsync(Encoding.UTF8.GetBytes("MSG_2_DURING_PARTITION"));
await leader2.ProposeAsync(Encoding.UTF8.GetBytes("MSG_3_DURING_PARTITION"));

await Task.Delay(150);

Console.WriteLine($"Current applied logs on dead '{leader1Id}': [{string.Join(", ", stateMachines[leader1Id].AppliedLogs)}]");
Console.WriteLine($"Current applied logs on active '{leader2Id}': [{string.Join(", ", stateMachines[leader2Id].AppliedLogs)}]");

// ----------------------------------------------------
// Step 5: Revive crashed node and observe reinstallation
// ----------------------------------------------------
Console.WriteLine($"\n--- Step 5: Reviving '{leader1Id}' and watching leader catch it up ---");
Console.ForegroundColor = ConsoleColor.Magenta;
Console.WriteLine($"[REVIVE] Restarting '{leader1Id}'...");
Console.ResetColor();

var revivedNode1 = CreateNode(leader1Id);
activeNodes[leader1Id] = revivedNode1;
await revivedNode1.StartAsync();

// Wait for leader to detect revived node is behind and replicate entries #2 & #3
var catchUpDeadline = Environment.TickCount64 + 3000;
while (Environment.TickCount64 < catchUpDeadline && stateMachines[leader1Id].AppliedLogs.Count < 3)
{
   await Task.Delay(50);
}

Console.ForegroundColor = ConsoleColor.Green;
Console.WriteLine($"\n[CATCH-UP COMPLETE] '{leader1Id}' reinstalled and caught up to current commit index {revivedNode1.CommitIndex}!");
Console.ResetColor();

// ----------------------------------------------------
// Step 6: Propose entry #4 across all 3 nodes
// ----------------------------------------------------
Console.WriteLine("\n--- Step 6: Proposing Entry #4 across all 3 synchronized nodes ---");
await leader2.ProposeAsync(Encoding.UTF8.GetBytes("MSG_4_ALL_NODES_HEALTHY"));

await Task.Delay(200);

Console.WriteLine("\n--- Final Verification of Replicated Logs Across Cluster ---");
foreach (var (id, sm) in stateMachines)
{
   Console.WriteLine($"Node '{id}' Logs: [{string.Join(", ", sm.AppliedLogs)}]");
}

foreach (var node in activeNodes.Values)
{
   await node.DisposeAsync();
}

Console.WriteLine("\nDone!");

internal sealed class FailoverDemoStateMachine(string nodeId) : IRaftStateMachine
{
   private readonly string _nodeId = nodeId;
   public readonly List<string> AppliedLogs = [];
   private readonly Lock _lock = new();

   public ValueTask<ReadOnlyMemory<byte>> ApplyAsync(ReadOnlyMemory<byte> command, ulong logIndex, CancellationToken ct = default)
   {
      var str = Encoding.UTF8.GetString(command.Span);
      lock (_lock)
      {
         AppliedLogs.Add($"#{logIndex}:{str}");
      }
      return ValueTask.FromResult<ReadOnlyMemory<byte>>(Encoding.UTF8.GetBytes($"ACK:{str}"));
   }
}
