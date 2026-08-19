using System.Text;
using Beskar.Networking.Raft.Enums;
using Beskar.Networking.Raft.Options;
using Beskar.Networking.Raft.Protocol.Messages;
using Beskar.Networking.Raft.Storage;
using Beskar.Networking.Raft.Transport;
using Beskar.Networking.Transports.Memory;

namespace Beskar.Networking.Raft.Tests;

public class RaftLeaderFailoverTests
{
   [Test]
   public async Task ThreeNodeCluster_LeaderFailover_ElectsNewLeaderAndRecovers()
   {
      var memoryOptions = new MemoryTransportOptions();

      var ep1 = new MemoryEndPoint($"failover-node-1-{Guid.NewGuid():N}");
      var ep2 = new MemoryEndPoint($"failover-node-2-{Guid.NewGuid():N}");
      var ep3 = new MemoryEndPoint($"failover-node-3-{Guid.NewGuid():N}");

      var l1 = new MemoryNetworkListener(ep1, memoryOptions);
      var l2 = new MemoryNetworkListener(ep2, memoryOptions);
      var l3 = new MemoryNetworkListener(ep3, memoryOptions);

      var p1 = new List<RaftPeerEndpoint>
      {
         new("node-2", ep2, () => new MemoryNetworkClient(memoryOptions)),
         new("node-3", ep3, () => new MemoryNetworkClient(memoryOptions))
      };

      var p2 = new List<RaftPeerEndpoint>
      {
         new("node-1", ep1, () => new MemoryNetworkClient(memoryOptions)),
         new("node-3", ep3, () => new MemoryNetworkClient(memoryOptions))
      };

      var p3 = new List<RaftPeerEndpoint>
      {
         new("node-1", ep1, () => new MemoryNetworkClient(memoryOptions)),
         new("node-2", ep2, () => new MemoryNetworkClient(memoryOptions))
      };

      await using var fixture1 = new ClusterNodeFixture("node-1", l1, p1);
      await using var fixture2 = new ClusterNodeFixture("node-2", l2, p2);
      await using var fixture3 = new ClusterNodeFixture("node-3", l3, p3);

      await fixture1.Node.StartAsync();
      await fixture2.Node.StartAsync();
      await fixture3.Node.StartAsync();

      // Wait for initial election
      await Task.Delay(400);

      var nodes = new[] { fixture1.Node, fixture2.Node, fixture3.Node };
      var initialLeader = nodes.FirstOrDefault(n => n.Role == RaftRole.Leader);
      await Assert.That(initialLeader).IsNotNull();

      var initialLeaderId = initialLeader!.Options.NodeId;

      // Propose command on initial leader
      var res1 = await initialLeader.ProposeAsync("SET leader1=active"u8.ToArray());
      await Assert.That(Encoding.UTF8.GetString(res1.Span)).IsEqualTo("OK");

      // Kill the initial leader node
      var deadFixture = initialLeaderId switch
      {
         "node-1" => fixture1,
         "node-2" => fixture2,
         _ => fixture3
      };

      await deadFixture.Node.StopAsync();

      // Wait for remaining 2 nodes to detect heartbeat timeout and elect new leader
      await Task.Delay(500);

      var remainingFixtures = new[] { fixture1, fixture2, fixture3 }
         .Where(f => f.NodeId != initialLeaderId)
         .ToList();

      var newLeaderFixture = remainingFixtures.FirstOrDefault(f => f.Node.Role == RaftRole.Leader);
      await Assert.That(newLeaderFixture).IsNotNull();
      await Assert.That(newLeaderFixture!.NodeId).IsNotEqualTo(initialLeaderId);

      // Propose command on new leader
      var res2 = await newLeaderFixture.Node.ProposeAsync("SET leader2=active"u8.ToArray());
      await Assert.That(Encoding.UTF8.GetString(res2.Span)).IsEqualTo("OK");

      // Verify that both surviving nodes applied both proposals
      await Task.Delay(100);
      foreach (var survivor in remainingFixtures)
      {
         await Assert.That(survivor.StateMachine.Store.ContainsKey("leader1")).IsTrue();
         await Assert.That(survivor.StateMachine.Store.ContainsKey("leader2")).IsTrue();
      }
   }

   [Test]
   public async Task RejoiningLeader_StepsDownWhenHigherTermDiscovered()
   {
      var memoryOptions = new MemoryTransportOptions();

      var ep1 = new MemoryEndPoint($"stepdown-node-1-{Guid.NewGuid():N}");
      var ep2 = new MemoryEndPoint($"stepdown-node-2-{Guid.NewGuid():N}");

      var l1 = new MemoryNetworkListener(ep1, memoryOptions);
      var l2 = new MemoryNetworkListener(ep2, memoryOptions);

      var p1 = new List<RaftPeerEndpoint> { new("node-2", ep2, () => new MemoryNetworkClient(memoryOptions)) };
      var p2 = new List<RaftPeerEndpoint> { new("node-1", ep1, () => new MemoryNetworkClient(memoryOptions)) };

      await using var f1 = new ClusterNodeFixture("node-1", l1, p1);
      await using var f2 = new ClusterNodeFixture("node-2", l2, p2);

      await f1.Node.StartAsync();
      await f2.Node.StartAsync();

      await Task.Delay(300);

      // Advance f2's term directly in storage to term 50
      await f2.Storage.SetCurrentTermAsync(50);

      var req = new RequestVoteRequest
      {
         Term = 50,
         CandidateId = "node-2",
         LastLogIndex = 0,
         LastLogTerm = 0
      };

      var resp = await f2.Transport.RequestVoteAsync("node-1", req);

      await Assert.That(resp!.Term).IsGreaterThanOrEqualTo(50UL);
      await Assert.That(f1.Node.CurrentTerm).IsEqualTo(50UL);
      await Assert.That(f1.Node.Role).IsEqualTo(RaftRole.Follower);
   }

   private sealed class ClusterNodeFixture : IAsyncDisposable
   {
      public ClusterNodeFixture(string nodeId, MemoryNetworkListener listener, List<RaftPeerEndpoint> peers)
      {
         NodeId = nodeId;
         Storage = new InMemoryRaftLogStorage();
         StateMachine = new KeyValueTestStateMachine();
         Transport = new RaftNetworkTransport(listener, peers);

         var options = new RaftNodeOptions
         {
            NodeId = nodeId,
            Peers = peers.Select(p => p.PeerId).ToList(),
            ElectionTimeoutMin = TimeSpan.FromMilliseconds(100),
            ElectionTimeoutMax = TimeSpan.FromMilliseconds(200),
            HeartbeatInterval = TimeSpan.FromMilliseconds(30)
         };

         Node = new RaftNode(options, Storage, StateMachine, Transport);
      }

      public string NodeId { get; }
      public RaftNode Node { get; }
      public InMemoryRaftLogStorage Storage { get; }
      public KeyValueTestStateMachine StateMachine { get; }
      public RaftNetworkTransport Transport { get; }

      public async ValueTask DisposeAsync()
      {
         await Node.DisposeAsync();
      }
   }
}
