using System.Text;
using Beskar.Networking.Raft.Enums;
using Beskar.Networking.Raft.Options;
using Beskar.Networking.Raft.Protocol.Messages;
using Beskar.Networking.Raft.Protocol.Models;
using Beskar.Networking.Raft.StateMachine;
using Beskar.Networking.Raft.Storage;
using Beskar.Networking.Raft.Transport;
using Beskar.Networking.Transports.Memory;

namespace Beskar.Networking.Raft.Tests;

public class RaftConcurrencyTests
{
   [Test]
   [Arguments(10)]
   [Arguments(25)]
   [Arguments(50)]
   public async Task ConcurrentAppends_InMemoryStorage_ThreadSafety(int threadCount)
   {
      await using var storage = new InMemoryRaftLogStorage();

      var tasks = Enumerable.Range(1, threadCount).Select(threadId => Task.Run(async () =>
      {
         for (var i = 1; i <= 20; i++)
         {
            await storage.SetCurrentTermAsync((ulong)(threadId * 100 + i));
            await storage.SetVotedForAsync($"node-{threadId}");
         }
      })).ToList();

      await Task.WhenAll(tasks);

      await Assert.That(await storage.GetCurrentTermAsync()).IsGreaterThan(0UL);
   }

   [Test]
   [Arguments(5)]
   [Arguments(10)]
   [Arguments(20)]
   public async Task RapidStartStop_RaftNode_HandlesChurnCleanly(int churnIterations)
   {
      var options = new RaftNodeOptions
      {
         NodeId = "churn-node",
         Peers = [],
         ElectionTimeoutMin = TimeSpan.FromMilliseconds(50),
         ElectionTimeoutMax = TimeSpan.FromMilliseconds(100),
         HeartbeatInterval = TimeSpan.FromMilliseconds(20)
      };

      var memoryOptions = new MemoryTransportOptions();

      for (var i = 0; i < churnIterations; i++)
      {
         var storage = new InMemoryRaftLogStorage();
         var sm = new TestRaftStateMachine();
         var endPoint = new MemoryEndPoint($"raft-churn-{i}-{Guid.NewGuid():N}");
         var listener = new MemoryNetworkListener(endPoint, memoryOptions);
         var transport = new RaftNetworkTransport(listener, []);

         await using var node = new RaftNode(options, storage, sm, transport);
         await node.StartAsync();
         await Task.Delay(10);
         await node.StopAsync();

         await Assert.That(node.Role).IsEqualTo(RaftRole.Stopped);
      }
   }

   [Test]
   public async Task ConcurrentStorageReadsAndWrites_FileStorage_NoRaceConditions()
   {
      var testDir = Path.Combine(Path.GetTempPath(), $"raft_conc_file_{Guid.NewGuid():N}");
      try
      {
         await using (var storage = new FileRaftLogStorage(testDir))
         {
            var writeTask = Task.Run(async () =>
            {
               for (ulong i = 1; i <= 100; i++)
               {
                  var entry = new RaftLogEntry(1, i, Encoding.UTF8.GetBytes($"DATA_{i}"));
                  await storage.AppendEntriesAsync([entry]);
                  await Task.Yield();
               }
            });

            var readTask = Task.Run(async () =>
            {
               for (var i = 0; i < 50; i++)
               {
                  var lastIdx = await storage.GetLastLogIndexAsync();
                  if (lastIdx > 0) await storage.GetEntryAsync(lastIdx);
                  await Task.Yield();
               }
            });

            await Task.WhenAll(writeTask, readTask);

            await Assert.That(await storage.GetLastLogIndexAsync()).IsEqualTo(100UL);
         }
      }
      finally
      {
         if (Directory.Exists(testDir)) Directory.Delete(testDir, true);
      }
   }

   [Test]
   [Timeout(5000)]
   public async Task ProposeAsync_ConcurrentWithStepDown_FaultsInsteadOfHangingOrAppendingStaleEntry(
      CancellationToken ct)
   {
      var memoryOptions = new MemoryTransportOptions();
      var nodeEp = new MemoryEndPoint($"prop-step-n-{Guid.NewGuid():N}");
      var baseTransport = new RaftNetworkTransport(
         new MemoryNetworkListener(nodeEp, memoryOptions), []);
      var transport = new VotingMockRaftTransport(baseTransport);

      var enteredGetLastDuringPropose = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
      var releasePropose = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
      var storage = new ProposeGateStorage(new InMemoryRaftLogStorage(), enteredGetLastDuringPropose, releasePropose);

      await using var node = new RaftNode(
         new RaftNodeOptions
         {
            NodeId = "node-1",
            Peers = ["peer-1"],
            ElectionTimeoutMin = TimeSpan.FromMilliseconds(40),
            ElectionTimeoutMax = TimeSpan.FromMilliseconds(70),
            HeartbeatInterval = TimeSpan.FromMilliseconds(20),
            EnablePreVote = false
         },
         storage,
         new NoOpConcurrencyStateMachine(),
         transport);

      var clientTransport = new RaftNetworkTransport(
         new MemoryNetworkListener(new MemoryEndPoint($"prop-step-c-{Guid.NewGuid():N}"), memoryOptions),
         [new RaftPeerEndpoint("node-1", nodeEp, () => new MemoryNetworkClient(memoryOptions))]);
      await using var _ = clientTransport;

      await node.StartAsync(ct);

      var deadline = DateTime.UtcNow.AddSeconds(2);
      while (node.Role != RaftRole.Leader && DateTime.UtcNow < deadline) await Task.Delay(15, ct);
      await Assert.That(node.Role).IsEqualTo(RaftRole.Leader);

      var priorLastIndex = await storage.GetLastLogIndexAsync(ct);

      storage.ArmGate();
      var proposeTask = node.ProposeAsync("stale-cmd"u8.ToArray(), ct).AsTask();

      await enteredGetLastDuringPropose.Task.WaitAsync(ct);

      // Concurrent higher-term RPC forces node-1 to step down BEFORE ProposeAsync appends or registers pending proposal
      var voteResp = await clientTransport.RequestVoteAsync("node-1", new RequestVoteRequest
      {
         Term = node.CurrentTerm + 5,
         CandidateId = "peer-1",
         LastLogIndex = priorLastIndex,
         LastLogTerm = node.CurrentTerm
      }, ct);
      await Assert.That(voteResp!.VoteGranted).IsTrue();
      await Assert.That(node.Role).IsEqualTo(RaftRole.Follower);

      releasePropose.TrySetResult();

      var completed = await Task.WhenAny(proposeTask, Task.Delay(400, ct));
      await Assert.That(completed == proposeTask).IsTrue();

      var rejected = false;
      try
      {
         await proposeTask;
      }
      catch (InvalidOperationException)
      {
         rejected = true;
      }

      await Assert.That(rejected).IsTrue();
      await Assert.That(await storage.GetLastLogIndexAsync(ct)).IsEqualTo(priorLastIndex);
   }

   [Test]
   [Timeout(5000)]
   public async Task HandleAppendEntries_ConcurrentHigherTermAdvance_DelayedLowerTermDoesNotOverwriteLeaderOrLog(
      CancellationToken ct)
   {
      var memoryOptions = new MemoryTransportOptions();
      var nodeEp = new MemoryEndPoint($"ae-term-race-n-{Guid.NewGuid():N}");
      var nodeTransport = new RaftNetworkTransport(
         new MemoryNetworkListener(nodeEp, memoryOptions), []);
      var storage = new InMemoryRaftLogStorage();

      await using var node = new RaftNode(
         new RaftNodeOptions
         {
            NodeId = "node-1",
            Peers = ["peer-1", "peer-2"],
            ElectionTimeoutMin = TimeSpan.FromSeconds(10),
            ElectionTimeoutMax = TimeSpan.FromSeconds(15),
            HeartbeatInterval = TimeSpan.FromSeconds(2),
            EnablePreVote = false
         },
         storage,
         new NoOpConcurrencyStateMachine(),
         nodeTransport);

      var pausedOnTerm2 = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
      var releaseTerm2 = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

      node.Events.OnLeaderChanged.Add(async (ctx, token) =>
      {
         if (ctx.Term == 2 && ctx.LeaderId == "peer-1")
         {
            pausedOnTerm2.TrySetResult();
            await releaseTerm2.Task.WaitAsync(token);
         }
      });

      var client1 = new RaftNetworkTransport(
         new MemoryNetworkListener(new MemoryEndPoint($"ae-term-c1-{Guid.NewGuid():N}"), memoryOptions),
         [new RaftPeerEndpoint("node-1", nodeEp, () => new MemoryNetworkClient(memoryOptions))]);
      var client2 = new RaftNetworkTransport(
         new MemoryNetworkListener(new MemoryEndPoint($"ae-term-c2-{Guid.NewGuid():N}"), memoryOptions),
         [new RaftPeerEndpoint("node-1", nodeEp, () => new MemoryNetworkClient(memoryOptions))]);
      await using var _ = client1;
      await using var __ = client2;

      await node.StartAsync(ct);

      // Delayed lower-term (Term 2) AppendEntries pauses at OnLeaderChanged
      var term2Task = client1.AppendEntriesAsync("node-1", new AppendEntriesRequest
      {
         Term = 2,
         LeaderId = "peer-1",
         PrevLogIndex = 0,
         PrevLogTerm = 0,
         Entries = [new RaftLogEntry(2, 1, "stale-t2"u8.ToArray())],
         LeaderCommitIndex = 1
      }, ct).AsTask();

      await pausedOnTerm2.Task.WaitAsync(ct);

      // Higher-term (Term 5) leader writes committed entry at index 1
      var term5Resp = await client2.AppendEntriesAsync("node-1", new AppendEntriesRequest
      {
         Term = 5,
         LeaderId = "peer-2",
         PrevLogIndex = 0,
         PrevLogTerm = 0,
         Entries = [new RaftLogEntry(5, 1, "newer-t5"u8.ToArray())],
         LeaderCommitIndex = 1
      }, ct);
      await Assert.That(term5Resp!.Success).IsTrue();

      releaseTerm2.TrySetResult();
      var term2Resp = await term2Task;

      await Assert.That(term2Resp!.Success).IsFalse();
      await Assert.That(node.CurrentTerm).IsEqualTo(5UL);
      var entry1 = await storage.GetEntryAsync(1, ct);
      await Assert.That(entry1.HasValue).IsTrue();
      await Assert.That(entry1!.Value.Term).IsEqualTo(5UL);
   }

   [Test]
   [Timeout(5000)]
   public async Task HandleInstallSnapshot_ConcurrentHigherTermAdvance_DelayedLowerTermDoesNotOverwriteState(
      CancellationToken ct)
   {
      var memoryOptions = new MemoryTransportOptions();
      var nodeEp = new MemoryEndPoint($"snap-term-race-n-{Guid.NewGuid():N}");
      var nodeTransport = new RaftNetworkTransport(
         new MemoryNetworkListener(nodeEp, memoryOptions), []);
      var storage = new InMemoryRaftLogStorage();

      await using var node = new RaftNode(
         new RaftNodeOptions
         {
            NodeId = "node-1",
            Peers = ["peer-1", "peer-2"],
            ElectionTimeoutMin = TimeSpan.FromSeconds(10),
            ElectionTimeoutMax = TimeSpan.FromSeconds(15),
            HeartbeatInterval = TimeSpan.FromSeconds(2),
            EnablePreVote = false
         },
         storage,
         new NoOpConcurrencyStateMachine(),
         nodeTransport);

      var pausedOnTerm2 = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
      var releaseTerm2 = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

      node.Events.OnLeaderChanged.Add(async (ctx, token) =>
      {
         if (ctx.Term == 2 && ctx.LeaderId == "peer-1")
         {
            pausedOnTerm2.TrySetResult();
            await releaseTerm2.Task.WaitAsync(token);
         }
      });

      var client1 = new RaftNetworkTransport(
         new MemoryNetworkListener(new MemoryEndPoint($"snap-term-c1-{Guid.NewGuid():N}"), memoryOptions),
         [new RaftPeerEndpoint("node-1", nodeEp, () => new MemoryNetworkClient(memoryOptions))]);
      var client2 = new RaftNetworkTransport(
         new MemoryNetworkListener(new MemoryEndPoint($"snap-term-c2-{Guid.NewGuid():N}"), memoryOptions),
         [new RaftPeerEndpoint("node-1", nodeEp, () => new MemoryNetworkClient(memoryOptions))]);
      await using var _ = client1;
      await using var __ = client2;

      await node.StartAsync(ct);

      var snapTask = client1.InstallSnapshotAsync("node-1", new InstallSnapshotRequest
      {
         Term = 2,
         LeaderId = "peer-1",
         LastIncludedIndex = 10,
         LastIncludedTerm = 2,
         Data = "old-snap"u8.ToArray()
      }, ct).AsTask();

      await pausedOnTerm2.Task.WaitAsync(ct);

      var term5Resp = await client2.AppendEntriesAsync("node-1", new AppendEntriesRequest
      {
         Term = 5,
         LeaderId = "peer-2",
         Entries = []
      }, ct);
      await Assert.That(term5Resp!.Success).IsTrue();

      releaseTerm2.TrySetResult();
      await snapTask;

      await Assert.That(node.CurrentTerm).IsEqualTo(5UL);
      await Assert.That(await storage.GetCompactedUntilIndexAsync(ct)).IsEqualTo(0UL);
      await Assert.That(node.CommitIndex).IsEqualTo(0UL);
   }

   [Test]
   [Timeout(5000)]
   public async Task StepDownAndRequestVote_ConcurrentHigherTermPersistence_DoesNotRegressStorageTermOrVote(
      CancellationToken ct)
   {
      var pausedTerm5 = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
      var releaseTerm5 = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
      var baseStorage = new InMemoryRaftLogStorage();
      var gatedStorage = new TermPersistenceGateStorage(baseStorage, 5, pausedTerm5, releaseTerm5);

      var memoryOptions = new MemoryTransportOptions();
      var nodeEp = new MemoryEndPoint($"term-regress-n-{Guid.NewGuid():N}");
      var nodeTransport = new RaftNetworkTransport(
         new MemoryNetworkListener(nodeEp, memoryOptions), []);

      await using var node = new RaftNode(
         new RaftNodeOptions
         {
            NodeId = "node-1",
            Peers = ["peer-1", "peer-2"],
            ElectionTimeoutMin = TimeSpan.FromSeconds(10),
            ElectionTimeoutMax = TimeSpan.FromSeconds(15),
            HeartbeatInterval = TimeSpan.FromSeconds(2),
            EnablePreVote = false
         },
         gatedStorage,
         new NoOpConcurrencyStateMachine(),
         nodeTransport);

      var client1 = new RaftNetworkTransport(
         new MemoryNetworkListener(new MemoryEndPoint($"term-regress-c1-{Guid.NewGuid():N}"), memoryOptions),
         [new RaftPeerEndpoint("node-1", nodeEp, () => new MemoryNetworkClient(memoryOptions))]);
      var client2 = new RaftNetworkTransport(
         new MemoryNetworkListener(new MemoryEndPoint($"term-regress-c2-{Guid.NewGuid():N}"), memoryOptions),
         [new RaftPeerEndpoint("node-1", nodeEp, () => new MemoryNetworkClient(memoryOptions))]);
      await using var _ = client1;
      await using var __ = client2;

      await node.StartAsync(ct);

      // Start lower-term (5) RequestVote, pausing while writing term 5 to storage
      var vote5Task = client1.RequestVoteAsync("node-1", new RequestVoteRequest
      {
         Term = 5,
         CandidateId = "peer-1"
      }, ct).AsTask();

      await pausedTerm5.Task.WaitAsync(ct);

      // Higher-term (10) RequestVote completes and persists (10, "peer-2")
      var vote10Resp = await client2.RequestVoteAsync("node-1", new RequestVoteRequest
      {
         Term = 10,
         CandidateId = "peer-2"
      }, ct);
      await Assert.That(vote10Resp!.VoteGranted).IsTrue();

      // Release delayed term 5 write
      releaseTerm5.TrySetResult();
      await vote5Task;

      await Assert.That(await baseStorage.GetCurrentTermAsync(ct)).IsEqualTo(10UL);
      await Assert.That(await baseStorage.GetVotedForAsync(ct)).IsEqualTo("peer-2");
   }

   [Test]
   [Timeout(5000)]
   public async Task HandleRequestVote_ConcurrentLogAppend_EvaluatesLatestLogStateAtomically(CancellationToken ct)
   {
      var baseStorage = new InMemoryRaftLogStorage();
      await baseStorage.AppendEntriesAsync([
         new RaftLogEntry(1, 1, "a"u8.ToArray()),
         new RaftLogEntry(1, 2, "b"u8.ToArray())
      ], ct);

      var pausedMidRewrite = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
      var releaseMidRewrite = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
      var storage = new TruncatePauseStorage(baseStorage, pausedMidRewrite, releaseMidRewrite);

      var memoryOptions = new MemoryTransportOptions();
      var nodeEp = new MemoryEndPoint($"vote-atom-n-{Guid.NewGuid():N}");
      var nodeTransport = new RaftNetworkTransport(
         new MemoryNetworkListener(nodeEp, memoryOptions), []);

      await using var node = new RaftNode(
         new RaftNodeOptions
         {
            NodeId = "node-1",
            Peers = ["peer-1", "peer-2"],
            ElectionTimeoutMin = TimeSpan.FromSeconds(10),
            ElectionTimeoutMax = TimeSpan.FromSeconds(15),
            HeartbeatInterval = TimeSpan.FromSeconds(2),
            EnablePreVote = false
         },
         storage,
         new NoOpConcurrencyStateMachine(),
         nodeTransport);

      var client1 = new RaftNetworkTransport(
         new MemoryNetworkListener(new MemoryEndPoint($"vote-atom-c1-{Guid.NewGuid():N}"), memoryOptions),
         [new RaftPeerEndpoint("node-1", nodeEp, () => new MemoryNetworkClient(memoryOptions))]);
      var client2 = new RaftNetworkTransport(
         new MemoryNetworkListener(new MemoryEndPoint($"vote-atom-c2-{Guid.NewGuid():N}"), memoryOptions),
         [new RaftPeerEndpoint("node-1", nodeEp, () => new MemoryNetworkClient(memoryOptions))]);
      await using var _ = client1;
      await using var __ = client2;

      await node.StartAsync(ct);

      // Leader replaces entries 1..2 (term 1) with (term 2) under _proposalLock; storage pauses right after TruncateLogAsync(1)
      var appendTask = client1.AppendEntriesAsync("node-1", new AppendEntriesRequest
      {
         Term = 2,
         LeaderId = "peer-1",
         PrevLogIndex = 0,
         PrevLogTerm = 0,
         Entries =
         [
            new RaftLogEntry(2, 1, "x"u8.ToArray()),
            new RaftLogEntry(2, 2, "y"u8.ToArray())
         ]
      }, ct).AsTask();

      await pausedMidRewrite.Task.WaitAsync(ct);

      // Candidate with stale log (LastLogTerm=1, LastLogIndex=1) is stale BOTH before [(1,1),(1,2)] and after [(2,1),(2,2)]
      var voteTask = client2.RequestVoteAsync("node-1", new RequestVoteRequest
      {
         Term = 3,
         CandidateId = "peer-2",
         LastLogIndex = 1,
         LastLogTerm = 1
      }, ct).AsTask();

      // Give RequestVote time to read torn state if it skips _proposalLock
      await Task.Delay(50, ct);
      releaseMidRewrite.TrySetResult();

      await appendTask;
      var voteResp = await voteTask;

      await Assert.That(voteResp!.VoteGranted).IsFalse();
   }

   [Test]
   [Timeout(5000)]
   public async Task Replication_ConcurrentTriggersWithSlowPeer_SerializesInFlightRpcPerPeer(CancellationToken ct)
   {
      var memoryOptions = new MemoryTransportOptions();
      var nodeEp = new MemoryEndPoint($"slow-peer-n-{Guid.NewGuid():N}");
      var slowTransport = new SlowPeerTrackingTransport(
         new RaftNetworkTransport(
            new MemoryNetworkListener(nodeEp, memoryOptions), []));

      await using var node = new RaftNode(
         new RaftNodeOptions
         {
            NodeId = "node-1",
            Peers = ["peer-fast", "peer-slow"],
            ElectionTimeoutMin = TimeSpan.FromMilliseconds(40),
            ElectionTimeoutMax = TimeSpan.FromMilliseconds(70),
            HeartbeatInterval = TimeSpan.FromMilliseconds(15),
            EnablePreVote = false
         },
         new InMemoryRaftLogStorage(),
         new NoOpConcurrencyStateMachine(),
         slowTransport);

      await node.StartAsync(ct);

      var deadline = DateTime.UtcNow.AddSeconds(2);
      while (node.Role != RaftRole.Leader && DateTime.UtcNow < deadline) await Task.Delay(15, ct);
      await Assert.That(node.Role).IsEqualTo(RaftRole.Leader);

      // Rapidly trigger replication via proposals + heartbeats while peer-slow takes 90ms per RPC
      await node.ProposeAsync("p1"u8.ToArray(), ct);
      await node.ProposeAsync("p2"u8.ToArray(), ct);
      await Task.Delay(200, ct);

      await Assert.That(slowTransport.SlowPeerCalls).IsGreaterThanOrEqualTo(2);
      await Assert.That(slowTransport.MaxConcurrentSlowPeerInFlight).IsLessThanOrEqualTo(1);
   }
}

internal sealed class NoOpConcurrencyStateMachine : IRaftStateMachine
{
   public ValueTask<ReadOnlyMemory<byte>> ApplyAsync(ReadOnlyMemory<byte> command, ulong logIndex,
      CancellationToken ct = default)
   {
      return ValueTask.FromResult<ReadOnlyMemory<byte>>("OK"u8.ToArray());
   }
}

internal sealed class VotingMockRaftTransport(IRaftTransport inner)
   : IRaftTransport
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

   public ValueTask<RequestVoteResponse?> RequestVoteAsync(
      string peerId, RequestVoteRequest request, CancellationToken ct = default)
   {
      return ValueTask.FromResult<RequestVoteResponse?>(
         new RequestVoteResponse { Term = request.Term, VoteGranted = true });
   }

   public ValueTask<AppendEntriesResponse?> AppendEntriesAsync(
      string peerId, AppendEntriesRequest request, CancellationToken ct = default)
   {
      return ValueTask.FromResult<AppendEntriesResponse?>(
         new AppendEntriesResponse
         {
            Term = request.Term,
            Success = true,
            MatchIndex = request.PrevLogIndex + (ulong)request.Entries.Count
         });
   }

   public ValueTask<InstallSnapshotResponse?> InstallSnapshotAsync(
      string peerId, InstallSnapshotRequest request, CancellationToken ct = default)
   {
      return inner.InstallSnapshotAsync(peerId, request, ct);
   }

   public ValueTask DisposeAsync()
   {
      return inner.DisposeAsync();
   }
}

internal sealed class SlowPeerTrackingTransport(IRaftTransport inner)
   : IRaftTransport
{
   private int _currentSlowInFlight;
   private int _maxSlowInFlight;
   private int _slowCalls;

   public int MaxConcurrentSlowPeerInFlight => Volatile.Read(ref _maxSlowInFlight);
   public int SlowPeerCalls => Volatile.Read(ref _slowCalls);

   public ValueTask StartAsync(Func<RaftRpcRequest, ValueTask<RaftRpcResponse>> rpcHandler,
      CancellationToken ct = default)
   {
      return inner.StartAsync(rpcHandler, ct);
   }

   public ValueTask StopAsync(CancellationToken ct = default)
   {
      return inner.StopAsync(ct);
   }

   public ValueTask<RequestVoteResponse?> RequestVoteAsync(
      string peerId, RequestVoteRequest request, CancellationToken ct = default)
   {
      return ValueTask.FromResult<RequestVoteResponse?>(
         new RequestVoteResponse
         {
            Term = request.Term,
            VoteGranted = peerId == "peer-fast"
         });
   }

   public async ValueTask<AppendEntriesResponse?> AppendEntriesAsync(
      string peerId, AppendEntriesRequest request, CancellationToken ct = default)
   {
      if (peerId == "peer-slow")
      {
         Interlocked.Increment(ref _slowCalls);
         var inFlight = Interlocked.Increment(ref _currentSlowInFlight);
         while (true)
         {
            var prev = Volatile.Read(ref _maxSlowInFlight);
            if (inFlight <= prev || Interlocked.CompareExchange(ref _maxSlowInFlight, inFlight, prev) == prev) break;
         }

         try
         {
            await Task.Delay(90, ct);
         }
         finally
         {
            Interlocked.Decrement(ref _currentSlowInFlight);
         }
      }

      return new AppendEntriesResponse
      {
         Term = request.Term,
         Success = true,
         MatchIndex = request.PrevLogIndex + (ulong)request.Entries.Count
      };
   }

   public ValueTask<InstallSnapshotResponse?> InstallSnapshotAsync(
      string peerId, InstallSnapshotRequest request, CancellationToken ct = default)
   {
      return inner.InstallSnapshotAsync(peerId, request, ct);
   }

   public ValueTask DisposeAsync()
   {
      return inner.DisposeAsync();
   }
}

internal sealed class ProposeGateStorage(
   IRaftLogStorage inner,
   TaskCompletionSource enteredGetLast,
   TaskCompletionSource releasePropose) : IRaftLogStorage
{
   private int _armed;
   private int _triggered;

   public ValueTask<ulong> GetCurrentTermAsync(CancellationToken ct = default)
   {
      return inner.GetCurrentTermAsync(ct);
   }

   public ValueTask SetCurrentTermAsync(ulong term, CancellationToken ct = default)
   {
      return inner.SetCurrentTermAsync(term, ct);
   }

   public ValueTask<string?> GetVotedForAsync(CancellationToken ct = default)
   {
      return inner.GetVotedForAsync(ct);
   }

   public ValueTask SetVotedForAsync(string? candidateId, CancellationToken ct = default)
   {
      return inner.SetVotedForAsync(candidateId, ct);
   }

   public ValueTask SetTermAndVoteAsync(ulong term, string? candidateId, CancellationToken ct = default)
   {
      return inner.SetTermAndVoteAsync(term, candidateId, ct);
   }

   public async ValueTask<ulong> GetLastLogIndexAsync(CancellationToken ct = default)
   {
      var result = await inner.GetLastLogIndexAsync(ct);
      if (Volatile.Read(ref _armed) == 1 && Interlocked.Exchange(ref _triggered, 1) == 0)
      {
         enteredGetLast.TrySetResult();
         await releasePropose.Task.WaitAsync(ct);
      }

      return result;
   }

   public ValueTask<ulong> GetLastLogTermAsync(CancellationToken ct = default)
   {
      return inner.GetLastLogTermAsync(ct);
   }

   public ValueTask<RaftLogEntry?> GetEntryAsync(ulong index, CancellationToken ct = default)
   {
      return inner.GetEntryAsync(index, ct);
   }

   public ValueTask<IReadOnlyList<RaftLogEntry>> GetEntriesAsync(ulong fromIndex, int maxCount,
      CancellationToken ct = default)
   {
      return inner.GetEntriesAsync(fromIndex, maxCount, ct);
   }

   public ValueTask AppendEntriesAsync(IReadOnlyList<RaftLogEntry> entries, CancellationToken ct = default)
   {
      return inner.AppendEntriesAsync(entries, ct);
   }

   public ValueTask TruncateLogAsync(ulong fromIndex, CancellationToken ct = default)
   {
      return inner.TruncateLogAsync(fromIndex, ct);
   }

   public ValueTask CompactPrefixAsync(ulong untilIndex, ulong untilTerm = 0, CancellationToken ct = default)
   {
      return inner.CompactPrefixAsync(untilIndex, untilTerm, ct);
   }

   public ValueTask<ulong> GetCompactedUntilIndexAsync(CancellationToken ct = default)
   {
      return inner.GetCompactedUntilIndexAsync(ct);
   }

   public ValueTask<ulong> GetCompactedUntilTermAsync(CancellationToken ct = default)
   {
      return inner.GetCompactedUntilTermAsync(ct);
   }

   public ValueTask DisposeAsync()
   {
      return inner.DisposeAsync();
   }

   public void ArmGate()
   {
      Interlocked.Exchange(ref _armed, 1);
   }
}

internal sealed class TermPersistenceGateStorage(
   IRaftLogStorage inner,
   ulong gateTerm,
   TaskCompletionSource paused,
   TaskCompletionSource release) : IRaftLogStorage
{
   private int _gated;

   public ValueTask<ulong> GetCurrentTermAsync(CancellationToken ct = default)
   {
      return inner.GetCurrentTermAsync(ct);
   }

   public ValueTask SetCurrentTermAsync(ulong term, CancellationToken ct = default)
   {
      return inner.SetCurrentTermAsync(term, ct);
   }

   public ValueTask<string?> GetVotedForAsync(CancellationToken ct = default)
   {
      return inner.GetVotedForAsync(ct);
   }

   public ValueTask SetVotedForAsync(string? candidateId, CancellationToken ct = default)
   {
      return inner.SetVotedForAsync(candidateId, ct);
   }

   public async ValueTask SetTermAndVoteAsync(ulong term, string? candidateId, CancellationToken ct = default)
   {
      if (term == gateTerm && Interlocked.Exchange(ref _gated, 1) == 0)
      {
         paused.TrySetResult();
         await release.Task.WaitAsync(ct);
      }

      await inner.SetTermAndVoteAsync(term, candidateId, ct);
   }

   public ValueTask<ulong> GetLastLogIndexAsync(CancellationToken ct = default)
   {
      return inner.GetLastLogIndexAsync(ct);
   }

   public ValueTask<ulong> GetLastLogTermAsync(CancellationToken ct = default)
   {
      return inner.GetLastLogTermAsync(ct);
   }

   public ValueTask<RaftLogEntry?> GetEntryAsync(ulong index, CancellationToken ct = default)
   {
      return inner.GetEntryAsync(index, ct);
   }

   public ValueTask<IReadOnlyList<RaftLogEntry>> GetEntriesAsync(ulong fromIndex, int maxCount,
      CancellationToken ct = default)
   {
      return inner.GetEntriesAsync(fromIndex, maxCount, ct);
   }

   public ValueTask AppendEntriesAsync(IReadOnlyList<RaftLogEntry> entries, CancellationToken ct = default)
   {
      return inner.AppendEntriesAsync(entries, ct);
   }

   public ValueTask TruncateLogAsync(ulong fromIndex, CancellationToken ct = default)
   {
      return inner.TruncateLogAsync(fromIndex, ct);
   }

   public ValueTask CompactPrefixAsync(ulong untilIndex, ulong untilTerm = 0, CancellationToken ct = default)
   {
      return inner.CompactPrefixAsync(untilIndex, untilTerm, ct);
   }

   public ValueTask<ulong> GetCompactedUntilIndexAsync(CancellationToken ct = default)
   {
      return inner.GetCompactedUntilIndexAsync(ct);
   }

   public ValueTask<ulong> GetCompactedUntilTermAsync(CancellationToken ct = default)
   {
      return inner.GetCompactedUntilTermAsync(ct);
   }

   public ValueTask DisposeAsync()
   {
      return inner.DisposeAsync();
   }
}

internal sealed class TruncatePauseStorage(
   IRaftLogStorage inner,
   TaskCompletionSource pausedMidRewrite,
   TaskCompletionSource releaseMidRewrite) : IRaftLogStorage
{
   private int _paused;

   public ValueTask<ulong> GetCurrentTermAsync(CancellationToken ct = default)
   {
      return inner.GetCurrentTermAsync(ct);
   }

   public ValueTask SetCurrentTermAsync(ulong term, CancellationToken ct = default)
   {
      return inner.SetCurrentTermAsync(term, ct);
   }

   public ValueTask<string?> GetVotedForAsync(CancellationToken ct = default)
   {
      return inner.GetVotedForAsync(ct);
   }

   public ValueTask SetVotedForAsync(string? candidateId, CancellationToken ct = default)
   {
      return inner.SetVotedForAsync(candidateId, ct);
   }

   public ValueTask SetTermAndVoteAsync(ulong term, string? candidateId, CancellationToken ct = default)
   {
      return inner.SetTermAndVoteAsync(term, candidateId, ct);
   }

   public ValueTask<ulong> GetLastLogIndexAsync(CancellationToken ct = default)
   {
      return inner.GetLastLogIndexAsync(ct);
   }

   public ValueTask<ulong> GetLastLogTermAsync(CancellationToken ct = default)
   {
      return inner.GetLastLogTermAsync(ct);
   }

   public ValueTask<RaftLogEntry?> GetEntryAsync(ulong index, CancellationToken ct = default)
   {
      return inner.GetEntryAsync(index, ct);
   }

   public ValueTask<IReadOnlyList<RaftLogEntry>> GetEntriesAsync(ulong fromIndex, int maxCount,
      CancellationToken ct = default)
   {
      return inner.GetEntriesAsync(fromIndex, maxCount, ct);
   }

   public ValueTask AppendEntriesAsync(IReadOnlyList<RaftLogEntry> entries, CancellationToken ct = default)
   {
      return inner.AppendEntriesAsync(entries, ct);
   }

   public async ValueTask TruncateLogAsync(ulong fromIndex, CancellationToken ct = default)
   {
      await inner.TruncateLogAsync(fromIndex, ct);
      if (Interlocked.Exchange(ref _paused, 1) == 0)
      {
         pausedMidRewrite.TrySetResult();
         await releaseMidRewrite.Task.WaitAsync(ct);
      }
   }

   public ValueTask CompactPrefixAsync(ulong untilIndex, ulong untilTerm = 0, CancellationToken ct = default)
   {
      return inner.CompactPrefixAsync(untilIndex, untilTerm, ct);
   }

   public ValueTask<ulong> GetCompactedUntilIndexAsync(CancellationToken ct = default)
   {
      return inner.GetCompactedUntilIndexAsync(ct);
   }

   public ValueTask<ulong> GetCompactedUntilTermAsync(CancellationToken ct = default)
   {
      return inner.GetCompactedUntilTermAsync(ct);
   }

   public ValueTask DisposeAsync()
   {
      return inner.DisposeAsync();
   }
}
