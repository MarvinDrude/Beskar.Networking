using System.Collections.Concurrent;
using System.Text;
using Beskar.Networking.Raft.Enums;
using Beskar.Networking.Raft.Options;
using Beskar.Networking.Raft.StateMachine;
using Beskar.Networking.Raft.Storage;
using Beskar.Networking.Raft.Transport;
using Beskar.Networking.Transports.Memory;

namespace Beskar.Networking.Raft.Tests;

[NotInParallel]
public class RaftClusterTests
{
   private sealed class TestStateMachine : IRaftStateMachine
   {
      public readonly ConcurrentDictionary<ulong, string> AppliedCommands = new();

      public ValueTask<ReadOnlyMemory<byte>> ApplyAsync(ReadOnlyMemory<byte> command, ulong logIndex, CancellationToken ct = default)
      {
         var str = Encoding.UTF8.GetString(command.Span);
         AppliedCommands[logIndex] = str;
         var response = Encoding.UTF8.GetBytes($"ACK:{str}");
         return ValueTask.FromResult<ReadOnlyMemory<byte>>(response);
      }
   }

   [Test]
   public async Task SingleNode_LeaderElectionAndProposal_Success()
   {
      var nodeId = "single-node";
      var options = new RaftNodeOptions
      {
         NodeId = nodeId,
         Peers = Array.Empty<string>(),
         ElectionTimeoutMin = TimeSpan.FromMilliseconds(50),
         ElectionTimeoutMax = TimeSpan.FromMilliseconds(100),
         HeartbeatInterval = TimeSpan.FromMilliseconds(20)
      };

      var memoryOptions = new MemoryTransportOptions();
      var endPoint = new MemoryEndPoint($"raft-{Guid.NewGuid():N}");
      var listener = new MemoryNetworkListener(endPoint, memoryOptions);
      var transport = new RaftNetworkTransport(listener, Array.Empty<RaftPeerEndpoint>());

      var storage = new InMemoryRaftLogStorage();
      var stateMachine = new TestStateMachine();

      await using var node = new RaftNode(options, storage, stateMachine, transport);

      await node.StartAsync();

      // Wait briefly for election
      await Task.Delay(150);

      await Assert.That(node.Role).IsEqualTo(RaftRole.Leader);
      await Assert.That(node.LeaderId).IsEqualTo(nodeId);

      var proposalData = Encoding.UTF8.GetBytes("INIT_SYSTEM");
      var result = await node.ProposeAsync(proposalData);

      await Assert.That(Encoding.UTF8.GetString(result.Span)).IsEqualTo("ACK:INIT_SYSTEM");
      await Assert.That(stateMachine.AppliedCommands.Count).IsEqualTo(1);
      await Assert.That(stateMachine.AppliedCommands[1]).IsEqualTo("INIT_SYSTEM");
      await Assert.That(node.CommitIndex).IsEqualTo(1UL);
   }

   [Test]
   public async Task ThreeNodeCluster_LeaderElectionAndLogReplication_Success()
   {
      var nodeIds = new[] { "node-1", "node-2", "node-3" };
      var memoryOptions = new MemoryTransportOptions();

      var endpoints = nodeIds.ToDictionary(
         id => id,
         id => new MemoryEndPoint($"raft-cluster-{id}-{Guid.NewGuid():N}"));

      var nodes = new List<RaftNode>();
      var stateMachines = new Dictionary<string, TestStateMachine>();

      for (var i = 0; i < nodeIds.Length; i++)
      {
         var id = nodeIds[i];
         var peers = nodeIds.Where(x => x != id).ToList();

         var peerEndpoints = peers.Select(p => new RaftPeerEndpoint(
            p,
            endpoints[p],
            () => new MemoryNetworkClient(memoryOptions))).ToList();

         var listener = new MemoryNetworkListener(endpoints[id], memoryOptions);
         var transport = new RaftNetworkTransport(listener, peerEndpoints);

         var options = new RaftNodeOptions
         {
            NodeId = id,
            Peers = peers,
            ElectionTimeoutMin = TimeSpan.FromMilliseconds(100),
            ElectionTimeoutMax = TimeSpan.FromMilliseconds(200),
            HeartbeatInterval = TimeSpan.FromMilliseconds(30)
         };

         var storage = new InMemoryRaftLogStorage();
         var sm = new TestStateMachine();
         stateMachines[id] = sm;

         var node = new RaftNode(options, storage, sm, transport);
         nodes.Add(node);
      }

      // Start all nodes
      for (var i = 0; i < nodes.Count; i++)
      {
         await nodes[i].StartAsync();
      }

      // Wait for leader election
      RaftNode? leader = null;
      var deadline = Environment.TickCount64 + 3000;
      while (Environment.TickCount64 < deadline)
      {
         leader = nodes.FirstOrDefault(n => n.Role == RaftRole.Leader);
         if (leader != null)
         {
            break;
         }
         await Task.Delay(50);
      }

      await Assert.That(leader).IsNotNull();
      await Assert.That(leader!.Role).IsEqualTo(RaftRole.Leader);

      // Propose command through leader
      var cmd1 = Encoding.UTF8.GetBytes("SET alpha 100");
      using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
      var result1 = await leader.ProposeAsync(cmd1, cts.Token);

      await Assert.That(Encoding.UTF8.GetString(result1.Span)).IsEqualTo("ACK:SET alpha 100");

      // Wait for log replication across all nodes
      var wait1Deadline = Environment.TickCount64 + 2000;
      while (Environment.TickCount64 < wait1Deadline && stateMachines.Values.Any(sm => !sm.AppliedCommands.ContainsKey(1)))
      {
         await Task.Delay(25);
      }

      foreach (var sm in stateMachines.Values)
      {
         await Assert.That(sm.AppliedCommands.ContainsKey(1)).IsTrue();
         await Assert.That(sm.AppliedCommands[1]).IsEqualTo("SET alpha 100");
      }

      // Propose second command
      var cmd2 = Encoding.UTF8.GetBytes("SET beta 200");
      var result2 = await leader.ProposeAsync(cmd2, cts.Token);

      await Assert.That(Encoding.UTF8.GetString(result2.Span)).IsEqualTo("ACK:SET beta 200");

      var wait2Deadline = Environment.TickCount64 + 2000;
      while (Environment.TickCount64 < wait2Deadline && stateMachines.Values.Any(sm => !sm.AppliedCommands.ContainsKey(2)))
      {
         await Task.Delay(25);
      }

      foreach (var sm in stateMachines.Values)
      {
         await Assert.That(sm.AppliedCommands.ContainsKey(2)).IsTrue();
         await Assert.That(sm.AppliedCommands[2]).IsEqualTo("SET beta 200");
      }

      // Clean up
      for (var i = 0; i < nodes.Count; i++)
      {
         await nodes[i].DisposeAsync();
      }
   }

   [Test]
   public async Task ThreeNodeCluster_LeaderFailover_ReElection_Success()
   {
      var nodeIds = new[] { "node-a", "node-b", "node-c" };
      var memoryOptions = new MemoryTransportOptions();

      var endpoints = nodeIds.ToDictionary(
         id => id,
         id => new MemoryEndPoint($"raft-failover-{id}-{Guid.NewGuid():N}"));

      var nodes = new List<RaftNode>();
      var stateMachines = new Dictionary<string, TestStateMachine>();

      for (var i = 0; i < nodeIds.Length; i++)
      {
         var id = nodeIds[i];
         var peers = nodeIds.Where(x => x != id).ToList();

         var peerEndpoints = peers.Select(p => new RaftPeerEndpoint(
            p,
            endpoints[p],
            () => new MemoryNetworkClient(memoryOptions))).ToList();

         var listener = new MemoryNetworkListener(endpoints[id], memoryOptions);
         var transport = new RaftNetworkTransport(listener, peerEndpoints, TimeSpan.FromMilliseconds(100));

         var options = new RaftNodeOptions
         {
            NodeId = id,
            Peers = peers,
            ElectionTimeoutMin = TimeSpan.FromMilliseconds(100),
            ElectionTimeoutMax = TimeSpan.FromMilliseconds(200),
            HeartbeatInterval = TimeSpan.FromMilliseconds(30)
         };

         var storage = new InMemoryRaftLogStorage();
         var sm = new TestStateMachine();
         stateMachines[id] = sm;

         var node = new RaftNode(options, storage, sm, transport);
         nodes.Add(node);
      }

      for (var i = 0; i < nodes.Count; i++)
      {
         await nodes[i].StartAsync();
      }

      // Wait for initial leader
      RaftNode? leader = null;
      var deadline = Environment.TickCount64 + 3000;
      while (Environment.TickCount64 < deadline)
      {
         leader = nodes.FirstOrDefault(n => n.Role == RaftRole.Leader);
         if (leader != null) break;
         await Task.Delay(50);
      }

      await Assert.That(leader).IsNotNull();
      var oldLeaderId = leader!.Options.NodeId;

      // Stop the leader to trigger failover
      await leader.StopAsync();

      // Wait for re-election among the remaining 2 nodes
      RaftNode? newLeader = null;
      var failoverDeadline = Environment.TickCount64 + 4000;
      while (Environment.TickCount64 < failoverDeadline)
      {
         newLeader = nodes.FirstOrDefault(n => n.Role == RaftRole.Leader && n.Options.NodeId != oldLeaderId);
         if (newLeader != null) break;
         await Task.Delay(50);
      }

      await Assert.That(newLeader).IsNotNull();
      await Assert.That(newLeader!.Role).IsEqualTo(RaftRole.Leader);
      await Assert.That(newLeader.Options.NodeId).IsNotEqualTo(oldLeaderId);

      // Clean up
      for (var i = 0; i < nodes.Count; i++)
      {
         await nodes[i].DisposeAsync();
      }
   }
}
