using System.Text;
using Beskar.Networking.Raft;
using Beskar.Networking.Raft.Enums;
using Beskar.Networking.Raft.Options;
using Beskar.Networking.Raft.StateMachine;
using Beskar.Networking.Raft.Storage;
using Beskar.Networking.Raft.Transport;
using Beskar.Networking.Transports.Memory;

Console.WriteLine("=== Beskar Raft QuickStart Example ===");

// 1. Define a minimal replicated State Machine
var stateMachine = new SimpleConsoleStateMachine();

// 2. Configure Raft Node Options
var options = new RaftNodeOptions
{
   NodeId = "node-alpha",
   Peers = [], // Single-node standalone cluster
   ElectionTimeoutMin = TimeSpan.FromMilliseconds(50),
   ElectionTimeoutMax = TimeSpan.FromMilliseconds(100),
   HeartbeatInterval = TimeSpan.FromMilliseconds(20)
};

// 3. Set up in-memory storage and transport
var storage = new InMemoryRaftLogStorage();
var memoryOptions = new MemoryTransportOptions();
var endPoint = new MemoryEndPoint("raft-quickstart");
var listener = new MemoryNetworkListener(endPoint, memoryOptions);
var transport = new RaftNetworkTransport(listener, Array.Empty<RaftPeerEndpoint>());

// 4. Create and start the Raft node
await using var node = new RaftNode(options, storage, stateMachine, transport);

node.Events.OnRoleChanged.Add((ctx, _) =>
{
   Console.WriteLine($"[Event] Node {ctx.NodeId} transitioned from {ctx.OldRole} -> {ctx.NewRole} (Term {ctx.Term})");
   return ValueTask.CompletedTask;
});

node.Events.OnLeaderChanged.Add((ctx, _) =>
{
   Console.WriteLine($"[Event] Leader elected: {ctx.LeaderId} in Term {ctx.Term}");
   return ValueTask.CompletedTask;
});

node.Events.OnEntryCommitted.Add((ctx, _) =>
{
   Console.WriteLine($"[Event] Entry #{ctx.Entry.Index} committed to quorum! Result: {Encoding.UTF8.GetString(ctx.Result.Span)}");
   return ValueTask.CompletedTask;
});

await node.StartAsync();

// Wait for leader election
await Task.Delay(150);

if (node.Role == RaftRole.Leader)
{
   Console.WriteLine("\nSubmitting commands to leader...");

   var result1 = await node.ProposeAsync(Encoding.UTF8.GetBytes("USER_CREATED:alice"));
   Console.WriteLine($"Propose response: {Encoding.UTF8.GetString(result1.Span)}");

   var result2 = await node.ProposeAsync(Encoding.UTF8.GetBytes("USER_CREATED:bob"));
   Console.WriteLine($"Propose response: {Encoding.UTF8.GetString(result2.Span)}");
}

await Task.Delay(100);
await node.StopAsync();
Console.WriteLine("Done!");

internal sealed class SimpleConsoleStateMachine : IRaftStateMachine
{
   public ValueTask<ReadOnlyMemory<byte>> ApplyAsync(ReadOnlyMemory<byte> command, ulong logIndex, CancellationToken ct = default)
   {
      var text = Encoding.UTF8.GetString(command.Span);
      Console.WriteLine($"[StateMachine] Applying log #{logIndex}: '{text}'");
      var response = Encoding.UTF8.GetBytes($"SUCCESS:{text}");
      return ValueTask.FromResult<ReadOnlyMemory<byte>>(response);
   }
}
