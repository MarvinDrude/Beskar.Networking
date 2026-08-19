using System.Collections.Concurrent;
using System.Text;
using System.Text.Json;
using Beskar.Networking.Raft;
using Beskar.Networking.Raft.Enums;
using Beskar.Networking.Raft.Options;
using Beskar.Networking.Raft.StateMachine;
using Beskar.Networking.Raft.Storage;
using Beskar.Networking.Raft.Transport;
using Beskar.Networking.Transports.Memory;

Console.WriteLine("=== Beskar Raft Distributed Key-Value Store Example ===");

var clusterId = Guid.NewGuid().ToString("N");
var nodeIds = new[] { "node-1", "node-2", "node-3" };
var memoryOptions = new MemoryTransportOptions();

var endpoints = nodeIds.ToDictionary(
   id => id,
   id => new MemoryEndPoint($"raft-kv-{clusterId}-{id}"));

var nodes = new List<RaftNode>();
var stateMachines = new Dictionary<string, KeyValueStateMachine>();

for (var i = 0; i < nodeIds.Length; i++)
{
   var id = nodeIds[i];
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

   var storage = new InMemoryRaftLogStorage();
   var sm = new KeyValueStateMachine(id);
   stateMachines[id] = sm;

   var node = new RaftNode(options, storage, sm, transport);
   nodes.Add(node);
}

// 1. Start all nodes in cluster
Console.WriteLine("Starting 3-node cluster...");
foreach (var node in nodes)
{
   await node.StartAsync();
}

// 2. Wait for leader election
RaftNode? leader = null;
var deadline = Environment.TickCount64 + 3000;
while (Environment.TickCount64 < deadline)
{
   leader = nodes.FirstOrDefault(n => n.Role == RaftRole.Leader);
   if (leader != null) break;
   await Task.Delay(50);
}

if (leader == null)
{
   Console.WriteLine("Leader election timed out.");
   return;
}

Console.WriteLine($"Cluster ready! Leader is '{leader.Options.NodeId}' in Term {leader.CurrentTerm}\n");

// 3. Propose KV mutations
async Task PutAsync(string key, string value)
{
   var currentLeader = nodes.FirstOrDefault(n => n.Role == RaftRole.Leader) ?? leader;
   var cmd = new KvCommand("PUT", key, value);
   var bytes = JsonSerializer.SerializeToUtf8Bytes(cmd);
   using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
   var responseBytes = await currentLeader.ProposeAsync(bytes, cts.Token);
   Console.WriteLine($"[Client] PUT {key} = {value} -> {Encoding.UTF8.GetString(responseBytes.Span)}");
}

async Task DeleteAsync(string key)
{
   var currentLeader = nodes.FirstOrDefault(n => n.Role == RaftRole.Leader) ?? leader;
   var cmd = new KvCommand("DEL", key, null);
   var bytes = JsonSerializer.SerializeToUtf8Bytes(cmd);
   using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
   var responseBytes = await currentLeader.ProposeAsync(bytes, cts.Token);
   Console.WriteLine($"[Client] DEL {key} -> {Encoding.UTF8.GetString(responseBytes.Span)}");
}

await PutAsync("cluster_name", "Beskar-Consensus-1");
await PutAsync("max_connections", "100000");
await PutAsync("region", "eu-central");
await DeleteAsync("region");

// Wait briefly for full quorum replication across all followers
var waitDeadline = Environment.TickCount64 + 2000;
while (Environment.TickCount64 < waitDeadline && stateMachines.Values.Any(sm => sm.Store.Count < 2))
{
   await Task.Delay(25);
}

// 4. Verify consistent state across all nodes
Console.WriteLine("\n--- Verifying Replicated State across all 3 nodes ---");
foreach (var (nodeId, sm) in stateMachines)
{
   Console.WriteLine($"Node '{nodeId}' Store Content: {string.Join(", ", sm.Store.Select(kv => $"{kv.Key}={kv.Value}"))}");
}

// Clean up
foreach (var node in nodes)
{
   await node.DisposeAsync();
}

Console.WriteLine("\nDone!");

public sealed record KvCommand(string Action, string Key, string? Value);

public sealed class KeyValueStateMachine(string nodeId) : IRaftStateMachine
{
   private readonly string _nodeId = nodeId;
   public readonly ConcurrentDictionary<string, string> Store = new();

   public ValueTask<ReadOnlyMemory<byte>> ApplyAsync(ReadOnlyMemory<byte> command, ulong logIndex, CancellationToken ct = default)
   {
      var cmd = JsonSerializer.Deserialize<KvCommand>(command.Span);
      if (cmd == null)
      {
         return ValueTask.FromResult<ReadOnlyMemory<byte>>(Encoding.UTF8.GetBytes("ERR_INVALID_COMMAND"));
      }

      switch (cmd.Action.ToUpperInvariant())
      {
         case "PUT" when cmd.Value != null:
            Store[cmd.Key] = cmd.Value;
            Console.WriteLine($"  [{_nodeId}] Applied log #{logIndex}: PUT {cmd.Key} = {cmd.Value}");
            return ValueTask.FromResult<ReadOnlyMemory<byte>>(Encoding.UTF8.GetBytes("OK"));

         case "DEL":
            Store.TryRemove(cmd.Key, out _);
            Console.WriteLine($"  [{_nodeId}] Applied log #{logIndex}: DEL {cmd.Key}");
            return ValueTask.FromResult<ReadOnlyMemory<byte>>(Encoding.UTF8.GetBytes("OK"));

         default:
            return ValueTask.FromResult<ReadOnlyMemory<byte>>(Encoding.UTF8.GetBytes("ERR_UNKNOWN_ACTION"));
      }
   }
}
