using Beskar.Networking.Raft.Enums;
using Beskar.Networking.Raft.Options;
using Beskar.Networking.Raft.Protocol.Messages;
using Beskar.Networking.Raft.Protocol.Models;
using Beskar.Networking.Raft.StateMachine;
using Beskar.Networking.Raft.Storage;
using Beskar.Networking.Raft.Transport;
using Beskar.Networking.Transports.Memory;

namespace Beskar.Networking.Raft.Tests;

public class RaftElectionTests
{
   private static (RaftNode Node, IRaftLogStorage Storage, IRaftTransport ClientTransport) CreateNodeAndSender(
      string nodeId, IEnumerable<string> peers, IRaftLogStorage? storage = null)
   {
      var memoryOptions = new MemoryTransportOptions();
      var nodeEndpoint = new MemoryEndPoint($"node-ep-{nodeId}-{Guid.NewGuid():N}");
      var listener = new MemoryNetworkListener(nodeEndpoint, memoryOptions);
      var nodeTransport = new RaftNetworkTransport(listener, []);

      var options = new RaftNodeOptions
      {
         NodeId = nodeId,
         Peers = peers.ToList(),
         ElectionTimeoutMin = TimeSpan.FromMilliseconds(50),
         ElectionTimeoutMax = TimeSpan.FromMilliseconds(100),
         HeartbeatInterval = TimeSpan.FromMilliseconds(20)
      };

      var s = storage ?? new InMemoryRaftLogStorage();
      var sm = new TestRaftStateMachine();
      var node = new RaftNode(options, s, sm, nodeTransport);

      var clientListenerEndpoint = new MemoryEndPoint($"client-ep-{Guid.NewGuid():N}");
      var clientListener = new MemoryNetworkListener(clientListenerEndpoint, memoryOptions);
      var clientPeerEndpoints = new List<RaftPeerEndpoint>
      {
         new(nodeId, nodeEndpoint, () => new MemoryNetworkClient(memoryOptions))
      };
      var clientTransport = new RaftNetworkTransport(clientListener, clientPeerEndpoints);

      return (node, s, clientTransport);
   }

   [Test]
   public async Task SingleNode_BecomesLeaderImmediatelyOnStart()
   {
      var (node, _, clientTransport) = CreateNodeAndSender("node-1", []);
      await using var _ = clientTransport;
      await using var __ = node;

      await node.StartAsync();
      await Task.Delay(150);

      await Assert.That(node.Role).IsEqualTo(RaftRole.Leader);
      await Assert.That(node.LeaderId).IsEqualTo("node-1");
      await Assert.That(node.CurrentTerm).IsGreaterThan(0UL);
   }

   [Test]
   public async Task Follower_StartsAsFollowerInTermZero()
   {
      var (node, _, clientTransport) = CreateNodeAndSender("node-1", ["node-2"]);
      await using var _ = clientTransport;
      await using var __ = node;

      await Assert.That(node.Role).IsEqualTo(RaftRole.Stopped);

      await node.StartAsync();

      await Assert.That(node.Role).IsEqualTo(RaftRole.Follower);
      await Assert.That(node.LeaderId).IsNull();
   }

   [Test]
   [Arguments(1, 0, 1, 0, true)]
   [Arguments(1, 10, 1, 10, true)]
   [Arguments(2, 5, 1, 10, true)]
   [Arguments(1, 10, 2, 5, false)]
   [Arguments(1, 5, 1, 10, false)]
   [Arguments(0, 0, 1, 0, false)]
   public async Task HandleRequestVote_LogUpToDateness_SafetyRule(
      ulong candidateTerm, ulong candidateIndex, ulong localTerm, ulong localIndex, bool expectedVote)
   {
      var storage = new InMemoryRaftLogStorage();
      await storage.SetCurrentTermAsync(1);

      if (localIndex > 0)
      {
         var entries = new List<RaftLogEntry>();
         for (ulong i = 1; i <= localIndex; i++) entries.Add(new RaftLogEntry(localTerm, i, "cmd"u8.ToArray()));
         await storage.AppendEntriesAsync(entries);
      }

      var (node, _, clientTransport) = CreateNodeAndSender("follower-1", ["candidate-1"], storage);
      await using var _ = clientTransport;
      await using var __ = node;

      await node.StartAsync();

      var request = new RequestVoteRequest
      {
         Term = candidateTerm,
         CandidateId = "candidate-1",
         LastLogIndex = candidateIndex,
         LastLogTerm = candidateTerm
      };

      var response = await clientTransport.RequestVoteAsync("follower-1", request);

      await Assert.That(response).IsNotNull();
      await Assert.That(response!.VoteGranted).IsEqualTo(expectedVote);
   }

   [Test]
   public async Task HandleRequestVote_RejectsLowerTerm()
   {
      var storage = new InMemoryRaftLogStorage();
      await storage.SetCurrentTermAsync(5);

      var (node, _, clientTransport) = CreateNodeAndSender("follower-1", ["candidate-1"], storage);
      await using var _ = clientTransport;
      await using var __ = node;

      await node.StartAsync();

      var request = new RequestVoteRequest
      {
         Term = 4,
         CandidateId = "candidate-1",
         LastLogIndex = 10,
         LastLogTerm = 4
      };

      var response = await clientTransport.RequestVoteAsync("follower-1", request);

      await Assert.That(response).IsNotNull();
      await Assert.That(response!.VoteGranted).IsFalse();
      await Assert.That(response.Term).IsEqualTo(5UL);
   }

   [Test]
   public async Task HandleRequestVote_GrantsVoteOnlyOncePerTerm()
   {
      var storage = new InMemoryRaftLogStorage();
      await storage.SetCurrentTermAsync(2);

      var (node, _, clientTransport) = CreateNodeAndSender("follower-1", ["cand-1", "cand-2"], storage);
      await using var _ = clientTransport;
      await using var __ = node;

      await node.StartAsync();

      var request1 = new RequestVoteRequest
      {
         Term = 3,
         CandidateId = "cand-1",
         LastLogIndex = 1,
         LastLogTerm = 3
      };

      var resp1 = await clientTransport.RequestVoteAsync("follower-1", request1);
      await Assert.That(resp1!.VoteGranted).IsTrue();

      var request2 = new RequestVoteRequest
      {
         Term = 3,
         CandidateId = "cand-2",
         LastLogIndex = 1,
         LastLogTerm = 3
      };

      var resp2 = await clientTransport.RequestVoteAsync("follower-1", request2);
      await Assert.That(resp2!.VoteGranted).IsFalse();

      var resp1Repeat = await clientTransport.RequestVoteAsync("follower-1", request1);
      await Assert.That(resp1Repeat!.VoteGranted).IsTrue();
   }

   [Test]
   public async Task HandleRequestVote_StepsDownIfHigherTermReceived()
   {
      var storage = new InMemoryRaftLogStorage();
      await storage.SetCurrentTermAsync(2);

      var (node, _, clientTransport) = CreateNodeAndSender("follower-1", ["cand-1"], storage);
      await using var _ = clientTransport;
      await using var __ = node;

      await node.StartAsync();

      var request = new RequestVoteRequest
      {
         Term = 10,
         CandidateId = "cand-1",
         LastLogIndex = 1,
         LastLogTerm = 10
      };

      var resp = await clientTransport.RequestVoteAsync("follower-1", request);

      await Assert.That(resp!.VoteGranted).IsTrue();
      await Assert.That(node.CurrentTerm).IsEqualTo(10UL);
   }

   [Test]
   [Arguments(3, 2)]
   [Arguments(5, 3)]
   [Arguments(7, 4)]
   public async Task Quorum_Calculation_RequiresMajority(int totalClusterNodes, int expectedQuorum)
   {
      var calculatedQuorum = totalClusterNodes / 2 + 1;
      await Assert.That(calculatedQuorum).IsEqualTo(expectedQuorum);
   }

   [Test]
   public async Task Election_Timer_ResetsOnValidHeartbeat()
   {
      var (node, _, clientTransport) = CreateNodeAndSender("follower-1", ["leader-1"]);
      await using var _ = clientTransport;
      await using var __ = node;

      await node.StartAsync();

      for (var i = 0; i < 5; i++)
      {
         await Task.Delay(30);
         var heartbeat = new AppendEntriesRequest
         {
            Term = 1,
            LeaderId = "leader-1",
            PrevLogIndex = 0,
            PrevLogTerm = 0,
            LeaderCommitIndex = 0,
            Entries = []
         };
         var resp = await clientTransport.AppendEntriesAsync("follower-1", heartbeat);
         await Assert.That(resp!.Success).IsTrue();
      }

      await Assert.That(node.Role).IsEqualTo(RaftRole.Follower);
      await Assert.That(node.LeaderId).IsEqualTo("leader-1");
   }
}

internal sealed class TestRaftStateMachine : IRaftStateMachine
{
   public ValueTask<ReadOnlyMemory<byte>> ApplyAsync(ReadOnlyMemory<byte> command, ulong logIndex,
      CancellationToken ct = default)
   {
      return ValueTask.FromResult<ReadOnlyMemory<byte>>("OK"u8.ToArray());
   }
}
