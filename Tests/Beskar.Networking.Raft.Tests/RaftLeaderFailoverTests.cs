using System.Text;
using Beskar.Networking.Raft.Enums;
using Beskar.Networking.Raft.Options;
using Beskar.Networking.Raft.Protocol.Messages;
using Beskar.Networking.Raft.StateMachine;
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

   [Test]
   [Timeout(5000)]
   public async Task CheckQuorum_PartitionedLeaderWithDelayedTransport_StepsDownWithinElectionTimeoutMax(
      CancellationToken ct)
   {
      var memoryOptions = new MemoryTransportOptions();
      var nodeEp = new MemoryEndPoint($"quorum-lag-n-{Guid.NewGuid():N}");
      var transport = new PartitionDelayMockTransport(
         new RaftNetworkTransport(new MemoryNetworkListener(nodeEp, memoryOptions), []));

      await using var node = new RaftNode(
         new RaftNodeOptions
         {
            NodeId = "node-1",
            Peers = ["peer-1", "peer-2"],
            ElectionTimeoutMin = TimeSpan.FromMilliseconds(80),
            ElectionTimeoutMax = TimeSpan.FromMilliseconds(150),
            HeartbeatInterval = TimeSpan.FromMilliseconds(25),
            EnablePreVote = false
         },
         new InMemoryRaftLogStorage(),
         new LeaderFailoverNoOpStateMachine(),
         transport);

      await node.StartAsync(ct);

      var deadline = DateTime.UtcNow.AddSeconds(2);
      while (node.Role != RaftRole.Leader && DateTime.UtcNow < deadline) await Task.Delay(15, ct);
      await Assert.That(node.Role).IsEqualTo(RaftRole.Leader);

      var steppedDownTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
      node.Events.OnRoleChanged.Add((ctx, _) =>
      {
         if (ctx.OldRole == RaftRole.Leader && ctx.NewRole == RaftRole.Follower) steppedDownTcs.TrySetResult();

         return ValueTask.CompletedTask;
      });

      // Partition both peers with RPC latency exceeding ElectionTimeoutMax (150ms)
      transport.EnablePartition(TimeSpan.FromMilliseconds(900));

      // Leader must step down within ElectionTimeoutMax (+ heartbeat interval grace) instead of blocking CheckQuorum on 900ms RPCs
      await steppedDownTcs.Task.WaitAsync(TimeSpan.FromMilliseconds(350), ct);
   }

   [Test]
   [Timeout(5000)]
   public async Task StartElection_SameTermAppendEntriesStepDown_CancelsPendingVoteRpcs(CancellationToken ct)
   {
      var memoryOptions = new MemoryTransportOptions();
      var nodeEp = new MemoryEndPoint($"unblock-elec-n-{Guid.NewGuid():N}");

      var term1VoteStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
      var term1RpcCancelled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
      var releaseTerm1Votes = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

      var transport = new ElectionUnblockTestTransport(
         new RaftNetworkTransport(new MemoryNetworkListener(nodeEp, memoryOptions), []),
         term1VoteStarted,
         term1RpcCancelled,
         releaseTerm1Votes);

      await using var node = new RaftNode(
         new RaftNodeOptions
         {
            NodeId = "node-1",
            Peers = ["peer-1", "peer-2"],
            ElectionTimeoutMin = TimeSpan.FromMilliseconds(250),
            ElectionTimeoutMax = TimeSpan.FromMilliseconds(350),
            HeartbeatInterval = TimeSpan.FromMilliseconds(20),
            EnablePreVote = false
         },
         new InMemoryRaftLogStorage(),
         new LeaderFailoverNoOpStateMachine(),
         transport);

      var clientTransport = new RaftNetworkTransport(
         new MemoryNetworkListener(new MemoryEndPoint($"unblock-elec-c-{Guid.NewGuid():N}"), memoryOptions),
         [new RaftPeerEndpoint("node-1", nodeEp, () => new MemoryNetworkClient(memoryOptions))]);
      await using var _ = clientTransport;

      try
      {
         await node.StartAsync(ct);
         await term1VoteStarted.Task.WaitAsync(ct);

         // Same-term leader peer-1 sends AppendEntries while node-1's Term 1 RequestVote RPCs are still blocked
         var aeResp = await clientTransport.AppendEntriesAsync("node-1", new AppendEntriesRequest
         {
            Term = 1,
            LeaderId = "peer-1",
            Entries = []
         }, ct);
         await Assert.That(aeResp!.Success).IsTrue();
         await Assert.That(node.Role).IsEqualTo(RaftRole.Follower);

         // Stepping down must promptly unblock StartElectionAsync and cancel pending Term 1 RequestVote RPCs
         await Task.Delay(50, ct);

         await Assert.That(term1RpcCancelled.Task.IsCompleted).IsTrue();
      }
      finally
      {
         releaseTerm1Votes.TrySetResult();
      }
   }

   private sealed class LeaderFailoverNoOpStateMachine : IRaftStateMachine
   {
      public ValueTask<ReadOnlyMemory<byte>> ApplyAsync(ReadOnlyMemory<byte> command, ulong logIndex,
         CancellationToken ct = default)
      {
         return ValueTask.FromResult<ReadOnlyMemory<byte>>("OK"u8.ToArray());
      }
   }

   private sealed class PartitionDelayMockTransport(IRaftTransport inner) : IRaftTransport
   {
      private TimeSpan _partitionDelay;
      private int _partitioned;

      public ValueTask StartAsync(Func<RaftRpcRequest, ValueTask<RaftRpcResponse>> rpcHandler,
         CancellationToken ct = default)
      {
         return inner.StartAsync(rpcHandler, ct);
      }

      public ValueTask StopAsync(CancellationToken ct = default)
      {
         return inner.StopAsync(ct);
      }

      public ValueTask<RequestVoteResponse?> RequestVoteAsync(string peerId, RequestVoteRequest request,
         CancellationToken ct = default)
      {
         if (Volatile.Read(ref _partitioned) == 1) return ValueTask.FromResult<RequestVoteResponse?>(null);

         return ValueTask.FromResult<RequestVoteResponse?>(new RequestVoteResponse
            { Term = request.Term, VoteGranted = true });
      }

      public async ValueTask<AppendEntriesResponse?> AppendEntriesAsync(string peerId, AppendEntriesRequest request,
         CancellationToken ct = default)
      {
         if (Volatile.Read(ref _partitioned) == 1)
         {
            try
            {
               await Task.Delay(_partitionDelay, ct);
            }
            catch (OperationCanceledException)
            {
            }

            return null;
         }

         return new AppendEntriesResponse
         {
            Term = request.Term,
            Success = true,
            MatchIndex = request.PrevLogIndex + (ulong)request.Entries.Count
         };
      }

      public ValueTask<InstallSnapshotResponse?> InstallSnapshotAsync(string peerId, InstallSnapshotRequest request,
         CancellationToken ct = default)
      {
         return inner.InstallSnapshotAsync(peerId, request, ct);
      }

      public ValueTask DisposeAsync()
      {
         return inner.DisposeAsync();
      }

      public void EnablePartition(TimeSpan delay)
      {
         _partitionDelay = delay;
         Volatile.Write(ref _partitioned, 1);
      }
   }

   private sealed class ElectionUnblockTestTransport(
      IRaftTransport inner,
      TaskCompletionSource term1VoteStarted,
      TaskCompletionSource term1RpcCancelled,
      TaskCompletionSource releaseTerm1Votes) : IRaftTransport
   {
      public ValueTask StartAsync(Func<RaftRpcRequest, ValueTask<RaftRpcResponse>> rpcHandler,
         CancellationToken ct = default)
      {
         return inner.StartAsync(rpcHandler, ct);
      }

      public ValueTask StopAsync(CancellationToken ct = default)
      {
         return inner.StopAsync(ct);
      }

      public async ValueTask<RequestVoteResponse?> RequestVoteAsync(string peerId, RequestVoteRequest request,
         CancellationToken ct = default)
      {
         if (request.Term == 1)
         {
            term1VoteStarted.TrySetResult();
            using var reg = ct.Register(() =>
            {
               term1RpcCancelled.TrySetResult();
               releaseTerm1Votes.TrySetResult();
            });
            await releaseTerm1Votes.Task;
            return null;
         }

         return null;
      }

      public ValueTask<AppendEntriesResponse?> AppendEntriesAsync(string peerId, AppendEntriesRequest request,
         CancellationToken ct = default)
      {
         return ValueTask.FromResult<AppendEntriesResponse?>(null);
      }

      public ValueTask<InstallSnapshotResponse?> InstallSnapshotAsync(string peerId, InstallSnapshotRequest request,
         CancellationToken ct = default)
      {
         return inner.InstallSnapshotAsync(peerId, request, ct);
      }

      public ValueTask DisposeAsync()
      {
         return inner.DisposeAsync();
      }
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
