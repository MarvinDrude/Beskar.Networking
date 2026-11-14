using System.Text;
using System.Text.Json;
using Beskar.Networking.Raft.Enums;
using Beskar.Networking.Raft.Options;
using Beskar.Networking.Raft.Protocol.Messages;
using Beskar.Networking.Raft.Protocol.Models;
using Beskar.Networking.Raft.StateMachine;
using Beskar.Networking.Raft.Storage;
using Beskar.Networking.Raft.Transport;
using Beskar.Networking.Transports.Memory;

namespace Beskar.Networking.Raft.Tests;

public class RaftSnapshotTests
{
   private static (RaftNode Node, IRaftLogStorage Storage, SnapshotStateMachine StateMachine, IRaftTransport
      ClientTransport) CreateNodeAndSender(
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
      var sm = new SnapshotStateMachine();
      var node = new RaftNode(options, s, sm, nodeTransport);

      var clientListenerEndpoint = new MemoryEndPoint($"client-ep-{Guid.NewGuid():N}");
      var clientListener = new MemoryNetworkListener(clientListenerEndpoint, memoryOptions);
      var clientPeerEndpoints = new List<RaftPeerEndpoint>
      {
         new(nodeId, nodeEndpoint, () => new MemoryNetworkClient(memoryOptions))
      };
      var clientTransport = new RaftNetworkTransport(clientListener, clientPeerEndpoints);

      return (node, s, sm, clientTransport);
   }

   [Test]
   public async Task StateMachine_TakeAndRestoreSnapshot_RoundtripsAccurately()
   {
      var sm1 = new SnapshotStateMachine();
      await sm1.ApplyAsync("SET a=100"u8.ToArray(), 1);
      await sm1.ApplyAsync("SET b=200"u8.ToArray(), 2);

      var snapshotBytes = await sm1.TakeSnapshotAsync();
      await Assert.That(snapshotBytes.Length).IsGreaterThan(0);

      var sm2 = new SnapshotStateMachine();
      await sm2.RestoreSnapshotAsync(snapshotBytes, 2, 1);

      await Assert.That(sm2.Store.Count).IsEqualTo(2);
      await Assert.That(sm2.Store["a"]).IsEqualTo("100");
      await Assert.That(sm2.Store["b"]).IsEqualTo("200");
   }

   [Test]
   public async Task HandleInstallSnapshot_AcceptsValidSnapshot_AndRestoresState()
   {
      var storage = new InMemoryRaftLogStorage();
      var (node, _, sm, clientTransport) = CreateNodeAndSender("follower-1", ["leader-1"], storage);
      await using var _ = clientTransport;
      await using var __ = node;

      await node.StartAsync();

      var initialEntries = new List<RaftLogEntry>();
      for (ulong i = 1; i <= 5; i++)
         initialEntries.Add(new RaftLogEntry(1, i, Encoding.UTF8.GetBytes($"SET key{i}=val{i}")));
      await storage.AppendEntriesAsync(initialEntries);

      var dict = new Dictionary<string, string>
      {
         ["key1"] = "val1",
         ["key2"] = "val2",
         ["key3"] = "val3",
         ["key4"] = "val4",
         ["key5"] = "val5"
      };
      var snapshotData = JsonSerializer.SerializeToUtf8Bytes(dict);

      var request = new InstallSnapshotRequest
      {
         Term = 1,
         LeaderId = "leader-1",
         LastIncludedIndex = 5,
         LastIncludedTerm = 1,
         Data = snapshotData
      };

      var response = await clientTransport.InstallSnapshotAsync("follower-1", request);

      await Assert.That(response).IsNotNull();
      await Assert.That(response!.Success).IsTrue();
      await Assert.That(node.CommitIndex).IsEqualTo(5UL);
      await Assert.That(node.LastApplied).IsEqualTo(5UL);

      for (ulong i = 1; i <= 5; i++)
      {
         var entry = await storage.GetEntryAsync(i);
         await Assert.That(entry.HasValue).IsFalse();
      }

      await Assert.That(sm.Store.Count).IsEqualTo(5);
      await Assert.That(sm.Store["key5"]).IsEqualTo("val5");
   }

   [Test]
   public async Task HandleInstallSnapshot_RejectsLowerTerm()
   {
      var storage = new InMemoryRaftLogStorage();
      await storage.SetCurrentTermAsync(10);

      var (node, _, sm, clientTransport) = CreateNodeAndSender("follower-1", ["leader-1"], storage);
      await using var _ = clientTransport;
      await using var __ = node;

      await node.StartAsync();

      var request = new InstallSnapshotRequest
      {
         Term = 9,
         LeaderId = "leader-1",
         LastIncludedIndex = 100,
         LastIncludedTerm = 9,
         Data = "DUMMY_SNAPSHOT"u8.ToArray()
      };

      var response = await clientTransport.InstallSnapshotAsync("follower-1", request);

      await Assert.That(response).IsNotNull();
      await Assert.That(response!.Success).IsFalse();
      await Assert.That(response.Term).IsEqualTo(10UL);
   }

   [Test]
   public async Task HandleInstallSnapshot_LargePayload_RestoresWithoutCorruption()
   {
      var (node, _, sm, clientTransport) = CreateNodeAndSender("follower-1", ["leader-1"]);
      await using var _ = clientTransport;
      await using var __ = node;

      await node.StartAsync();

      var dict = new Dictionary<string, string>();
      for (var i = 1; i <= 2000; i++) dict[$"bulk_key_{i}"] = new string('x', 50);
      var snapshotData = JsonSerializer.SerializeToUtf8Bytes(dict);

      var request = new InstallSnapshotRequest
      {
         Term = 2,
         LeaderId = "leader-1",
         LastIncludedIndex = 2000,
         LastIncludedTerm = 2,
         Data = snapshotData
      };

      var response = await clientTransport.InstallSnapshotAsync("follower-1", request);

      await Assert.That(response!.Success).IsTrue();
      await Assert.That(sm.Store.Count).IsEqualTo(2000);
      await Assert.That(sm.Store["bulk_key_1500"]).IsEqualTo(new string('x', 50));
   }

   [Test]
   public async Task Replication_LeaderCompactedEntries_FallbackToInstallSnapshot()
   {
      var memoryOptions = new MemoryTransportOptions();
      var epLeader = new MemoryEndPoint($"leader-ep-{Guid.NewGuid():N}");
      var epFollower = new MemoryEndPoint($"follower-ep-{Guid.NewGuid():N}");

      var listenerLeader = new MemoryNetworkListener(epLeader, memoryOptions);
      var listenerFollower = new MemoryNetworkListener(epFollower, memoryOptions);

      var peersForLeader = new List<RaftPeerEndpoint>
      {
         new("follower-1", epFollower, () => new MemoryNetworkClient(memoryOptions))
      };

      var peersForFollower = new List<RaftPeerEndpoint>
      {
         new("leader-1", epLeader, () => new MemoryNetworkClient(memoryOptions))
      };

      var transportLeader = new RaftNetworkTransport(listenerLeader, peersForLeader);
      var transportFollower = new RaftNetworkTransport(listenerFollower, peersForFollower);

      var storageLeader = new InMemoryRaftLogStorage();
      var storageFollower = new InMemoryRaftLogStorage();

      var smLeader = new SnapshotStateMachine();
      var smFollower = new SnapshotStateMachine();

      var optionsLeader = new RaftNodeOptions
      {
         NodeId = "leader-1",
         Peers = ["follower-1"],
         ElectionTimeoutMin = TimeSpan.FromMilliseconds(50),
         ElectionTimeoutMax = TimeSpan.FromMilliseconds(100),
         HeartbeatInterval = TimeSpan.FromMilliseconds(20)
      };

      var optionsFollower = new RaftNodeOptions
      {
         NodeId = "follower-1",
         Peers = ["leader-1"],
         ElectionTimeoutMin = TimeSpan.FromMilliseconds(200),
         ElectionTimeoutMax = TimeSpan.FromMilliseconds(400),
         HeartbeatInterval = TimeSpan.FromMilliseconds(20)
      };

      await using var leader = new RaftNode(optionsLeader, storageLeader, smLeader, transportLeader);
      await using var follower = new RaftNode(optionsFollower, storageFollower, smFollower, transportFollower);

      await leader.StartAsync();
      await follower.StartAsync();

      await Task.Delay(150);

      // Propose entries on leader while follower is active
      await leader.ProposeAsync("SET key1=val1"u8.ToArray());

      // Leader compacts log entries up to index 1
      await storageLeader.CompactPrefixAsync(1);

      // Now propose entry 2. Leader's log no longer has entry 1 (compacted).
      // TriggerReplicationAsync should automatically detect missing prevEntry and send InstallSnapshot!
      await leader.ProposeAsync("SET key2=val2"u8.ToArray());

      await Task.Delay(150);

      // Follower must have received InstallSnapshot and caught up to latest state
      await Assert.That(smFollower.Store.ContainsKey("key1")).IsTrue();
      await Assert.That(smFollower.Store["key1"]).IsEqualTo("val1");
   }

   [Test]
   public async Task Replication_AppendEntriesImmediatelyFollowingCompactedBoundary_DoesNotLoopSnapshots()
   {
      var memoryOptions = new MemoryTransportOptions();
      var epLeader = new MemoryEndPoint($"leader-ep-{Guid.NewGuid():N}");
      var epFollower = new MemoryEndPoint($"follower-ep-{Guid.NewGuid():N}");

      var listenerLeader = new MemoryNetworkListener(epLeader, memoryOptions);
      var listenerFollower = new MemoryNetworkListener(epFollower, memoryOptions);

      var peersForLeader = new List<RaftPeerEndpoint>
      {
         new("follower-1", epFollower, () => new MemoryNetworkClient(memoryOptions))
      };

      var peersForFollower = new List<RaftPeerEndpoint>
      {
         new("leader-1", epLeader, () => new MemoryNetworkClient(memoryOptions))
      };

      var transportLeader = new RaftNetworkTransport(listenerLeader, peersForLeader);
      var transportFollower = new RaftNetworkTransport(listenerFollower, peersForFollower);

      var storageLeader = new InMemoryRaftLogStorage();
      var storageFollower = new InMemoryRaftLogStorage();

      var smLeader = new SnapshotStateMachine();
      var smFollower = new SnapshotStateMachine();

      var optionsLeader = new RaftNodeOptions
      {
         NodeId = "leader-1",
         Peers = ["follower-1"],
         ElectionTimeoutMin = TimeSpan.FromMilliseconds(50),
         ElectionTimeoutMax = TimeSpan.FromMilliseconds(100),
         HeartbeatInterval = TimeSpan.FromMilliseconds(20)
      };

      var optionsFollower = new RaftNodeOptions
      {
         NodeId = "follower-1",
         Peers = ["leader-1"],
         ElectionTimeoutMin = TimeSpan.FromMilliseconds(200),
         ElectionTimeoutMax = TimeSpan.FromMilliseconds(400),
         HeartbeatInterval = TimeSpan.FromMilliseconds(20)
      };

      await using var leader = new RaftNode(optionsLeader, storageLeader, smLeader, transportLeader);
      await using var follower = new RaftNode(optionsFollower, storageFollower, smFollower, transportFollower);

      await leader.StartAsync();
      await follower.StartAsync();

      await Task.Delay(150);

      // 1. Propose entry 1 and 2
      await leader.ProposeAsync("SET key1=val1"u8.ToArray());
      await leader.ProposeAsync("SET key2=val2"u8.ToArray());

      await Task.Delay(100);

      // 2. Both leader and follower compact entries up to index 2 (term 1)
      await storageLeader.CompactPrefixAsync(2, leader.CurrentTerm);
      await storageFollower.CompactPrefixAsync(2, follower.CurrentTerm);

      // 3. Leader proposes entry 3. PrevLogIndex is 2 (compacted boundary).
      // Leader and follower must successfully append entry 3 via standard AppendEntries without infinite snapshotting!
      await leader.ProposeAsync("SET key3=val3"u8.ToArray());

      await Task.Delay(150);

      await Assert.That(smFollower.Store.ContainsKey("key3")).IsTrue();
      await Assert.That(smFollower.Store["key3"]).IsEqualTo("val3");
      await Assert.That(follower.CommitIndex).IsEqualTo(3UL);
      await Assert.That(follower.LastApplied).IsEqualTo(3UL);
   }

   [Test]
   public async Task StartAsync_RestoresCommitIndexAndLastApplied_FromCompactedStorage()
   {
      var storage = new InMemoryRaftLogStorage();
      await storage.CompactPrefixAsync(50, 3);

      var (node, _, _, clientTransport) = CreateNodeAndSender("node-restart", [], storage);
      await using var _ = clientTransport;
      await using var __ = node;

      await node.StartAsync();

      await Assert.That(node.CommitIndex).IsEqualTo(50UL);
      await Assert.That(node.LastApplied).IsEqualTo(50UL);
   }

   [Test]
   [Timeout(8000)]
   public async Task Replication_FollowerCompactedAheadOfLeaderNextIndex_ConvergesWithoutLooping(CancellationToken ct)
   {
      var memoryOptions = new MemoryTransportOptions();
      var ep1 = new MemoryEndPoint($"snap-loop-1-{Guid.NewGuid():N}");
      var ep2 = new MemoryEndPoint($"snap-loop-2-{Guid.NewGuid():N}");
      var ep3 = new MemoryEndPoint($"snap-loop-3-{Guid.NewGuid():N}");

      var storage1 = new InMemoryRaftLogStorage();
      var storage2 = new InMemoryRaftLogStorage();
      var storage3 = new InMemoryRaftLogStorage();

      // Leader (node-1) and node-3 start with entries 1..5 in term 2
      var initEntries = Enumerable.Range(1, 5)
         .Select(i => new RaftLogEntry(2, (ulong)i, Encoding.UTF8.GetBytes($"init-{i}")))
         .ToList();
      await storage1.SetTermAndVoteAsync(2, "node-1", ct);
      await storage1.AppendEntriesAsync(initEntries, ct);
      await storage3.SetTermAndVoteAsync(2, "node-1", ct);
      await storage3.AppendEntriesAsync(initEntries, ct);

      var t1 = new RaftNetworkTransport(new MemoryNetworkListener(ep1, memoryOptions), [
         new RaftPeerEndpoint("node-2", ep2, () => new MemoryNetworkClient(memoryOptions)),
         new RaftPeerEndpoint("node-3", ep3, () => new MemoryNetworkClient(memoryOptions))
      ]);
      var t2 = new RaftNetworkTransport(new MemoryNetworkListener(ep2, memoryOptions), [
         new RaftPeerEndpoint("node-1", ep1, () => new MemoryNetworkClient(memoryOptions)),
         new RaftPeerEndpoint("node-3", ep3, () => new MemoryNetworkClient(memoryOptions))
      ]);
      var t3 = new RaftNetworkTransport(new MemoryNetworkListener(ep3, memoryOptions), [
         new RaftPeerEndpoint("node-1", ep1, () => new MemoryNetworkClient(memoryOptions)),
         new RaftPeerEndpoint("node-2", ep2, () => new MemoryNetworkClient(memoryOptions))
      ]);

      await using var node1 = new RaftNode(
         new RaftNodeOptions
         {
            NodeId = "node-1",
            Peers = ["node-2", "node-3"],
            ElectionTimeoutMin = TimeSpan.FromMilliseconds(40),
            ElectionTimeoutMax = TimeSpan.FromMilliseconds(70),
            HeartbeatInterval = TimeSpan.FromMilliseconds(15),
            MaxAppendEntriesBatchSize = 3,
            EnablePreVote = false
         },
         storage1, new TrackingSnapshotStateMachine(), t1);

      await using var node3 = new RaftNode(
         new RaftNodeOptions
         {
            NodeId = "node-3",
            Peers = ["node-1", "node-2"],
            ElectionTimeoutMin = TimeSpan.FromMilliseconds(500),
            ElectionTimeoutMax = TimeSpan.FromMilliseconds(800),
            HeartbeatInterval = TimeSpan.FromMilliseconds(15),
            EnablePreVote = false
         },
         storage3, new TrackingSnapshotStateMachine(), t3);

      await node3.StartAsync(ct);
      await node1.StartAsync(ct);

      // Wait until node-1 wins election in term 3 (before node-2 starts, so node-1's NextIndex[node-2] = 7)
      var deadline = DateTime.UtcNow.AddSeconds(2);
      while (node1.Role != RaftRole.Leader && DateTime.UtcNow < deadline) await Task.Delay(15, ct);
      await Assert.That(node1.Role).IsEqualTo(RaftRole.Leader);
      var leaderTerm = node1.CurrentTerm;

      // Propose entries 7..15 on node-1 (committed via node-3)
      for (var i = 7; i <= 15; i++) await node1.ProposeAsync(Encoding.UTF8.GetBytes($"cmd-{i}"), ct);

      // Now prepare node-2 compacted up to index 10 in leaderTerm (> NextIndex["node-2"] = 7, > MaxAppendEntriesBatchSize = 3)
      await storage2.SetTermAndVoteAsync(leaderTerm, null, ct);
      await storage2.CompactPrefixAsync(10, leaderTerm, ct);

      await using var node2 = new RaftNode(
         new RaftNodeOptions
         {
            NodeId = "node-2",
            Peers = ["node-1", "node-3"],
            ElectionTimeoutMin = TimeSpan.FromMilliseconds(500),
            ElectionTimeoutMax = TimeSpan.FromMilliseconds(800),
            HeartbeatInterval = TimeSpan.FromMilliseconds(15),
            EnablePreVote = false
         },
         storage2, new TrackingSnapshotStateMachine(), t2);

      await node2.StartAsync(ct);

      var expectedLast = await storage1.GetLastLogIndexAsync(ct);
      deadline = DateTime.UtcNow.AddSeconds(2);
      while (await storage2.GetLastLogIndexAsync(ct) < expectedLast && DateTime.UtcNow < deadline)
         await Task.Delay(20, ct);

      await Assert.That(await storage2.GetLastLogIndexAsync(ct)).IsEqualTo(expectedLast);
   }

   [Test]
   [Timeout(5000)]
   public async Task Replication_ConcurrentCompactionDuringSnapshot_SendsMatchingTerm(CancellationToken ct)
   {
      var baseStorage = new InMemoryRaftLogStorage();
      await baseStorage.SetTermAndVoteAsync(1, null, ct);
      await baseStorage.CompactPrefixAsync(3, 1, ct);

      var enteredApply4 = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
      var readCompactedTermDuringApply = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
      var snapshotSentTcs =
         new TaskCompletionSource<InstallSnapshotRequest>(TaskCreationOptions.RunContinuationsAsynchronously);

      var interceptStorage = new SnapshotRaceStorage(
         baseStorage,
         () =>
         {
            if (enteredApply4.Task.IsCompleted) readCompactedTermDuringApply.TrySetResult();
         });

      var memoryOptions = new MemoryTransportOptions();
      var nodeEp = new MemoryEndPoint($"snap-race-{Guid.NewGuid():N}");
      var transport = new SnapshotCapturingTransport(
         new RaftNetworkTransport(new MemoryNetworkListener(nodeEp, memoryOptions), []),
         (peerId, req) =>
         {
            if (peerId == "peer-2" && req.LastIncludedIndex >= 4) snapshotSentTcs.TrySetResult(req);
         });

      var sm = new Apply4CompactingStateMachine(enteredApply4, readCompactedTermDuringApply, baseStorage);

      await using var node = new RaftNode(
         new RaftNodeOptions
         {
            NodeId = "node-1",
            Peers = ["peer-1", "peer-2"],
            ElectionTimeoutMin = TimeSpan.FromMilliseconds(40),
            ElectionTimeoutMax = TimeSpan.FromMilliseconds(70),
            HeartbeatInterval = TimeSpan.FromMilliseconds(20),
            EnablePreVote = false
         },
         interceptStorage,
         sm,
         transport);

      await node.StartAsync(ct);

      var deadline = DateTime.UtcNow.AddSeconds(2);
      while (node.Role != RaftRole.Leader && DateTime.UtcNow < deadline) await Task.Delay(15, ct);
      await Assert.That(node.Role).IsEqualTo(RaftRole.Leader);

      var leaderTerm = node.CurrentTerm;
      sm.TargetTerm = leaderTerm;

      var proposeTask = node.ProposeAsync("cmd-4"u8.ToArray(), ct).AsTask();
      var snapReq = await snapshotSentTcs.Task.WaitAsync(ct);
      await proposeTask;

      var expectedEntry = await baseStorage.GetEntryAsync(snapReq.LastIncludedIndex, ct);
      var actualTermAtIncludedIndex = snapReq.LastIncludedIndex == await baseStorage.GetCompactedUntilIndexAsync(ct)
         ? await baseStorage.GetCompactedUntilTermAsync(ct)
         : expectedEntry?.Term ?? 0;

      await Assert.That(snapReq.LastIncludedTerm).IsEqualTo(actualTermAtIncludedIndex);
   }

   [Test]
   [Timeout(5000)]
   public async Task HandleInstallSnapshot_ConcurrentWithAppendEntries_PreservesSnapshotBoundary(CancellationToken ct)
   {
      var appendEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
      var releaseAppend = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
      var storage = new AppendAndCompactOverlapStorage(new InMemoryRaftLogStorage(), appendEntered, releaseAppend);

      var memoryOptions = new MemoryTransportOptions();
      var nodeEp = new MemoryEndPoint($"snap-lock-{Guid.NewGuid():N}");
      var transport = new RaftNetworkTransport(new MemoryNetworkListener(nodeEp, memoryOptions), []);

      await using var node = new RaftNode(
         new RaftNodeOptions
         {
            NodeId = "node-1",
            Peers = ["peer-1"],
            ElectionTimeoutMin = TimeSpan.FromSeconds(10),
            ElectionTimeoutMax = TimeSpan.FromSeconds(15),
            HeartbeatInterval = TimeSpan.FromSeconds(2),
            EnablePreVote = false
         },
         storage,
         new TrackingSnapshotStateMachine(),
         transport);

      var clientTransport1 = new RaftNetworkTransport(
         new MemoryNetworkListener(new MemoryEndPoint($"snap-lock-c1-{Guid.NewGuid():N}"), memoryOptions),
         [new RaftPeerEndpoint("node-1", nodeEp, () => new MemoryNetworkClient(memoryOptions))]);
      var clientTransport2 = new RaftNetworkTransport(
         new MemoryNetworkListener(new MemoryEndPoint($"snap-lock-c2-{Guid.NewGuid():N}"), memoryOptions),
         [new RaftPeerEndpoint("node-1", nodeEp, () => new MemoryNetworkClient(memoryOptions))]);
      await using var _ = clientTransport1;
      await using var __ = clientTransport2;

      await node.StartAsync(ct);

      var appendTask = clientTransport1.AppendEntriesAsync("node-1", new AppendEntriesRequest
      {
         Term = 2,
         LeaderId = "peer-1",
         PrevLogIndex = 0,
         PrevLogTerm = 0,
         Entries = [new RaftLogEntry(2, 1, "e1"u8.ToArray())],
         LeaderCommitIndex = 0
      }, ct).AsTask();

      await appendEntered.Task.WaitAsync(ct);

      var snapTask = clientTransport2.InstallSnapshotAsync("node-1", new InstallSnapshotRequest
      {
         Term = 2,
         LeaderId = "peer-1",
         LastIncludedIndex = 10,
         LastIncludedTerm = 2,
         Data = "snap"u8.ToArray()
      }, ct).AsTask();

      // Give HandleInstallSnapshotAsync time to reach CompactPrefixAsync if it skips _proposalLock
      await Task.Delay(50, ct);
      releaseAppend.TrySetResult();

      await Task.WhenAll(appendTask, snapTask);

      await Assert.That(storage.ConcurrentCompactDuringAppendDetected).IsFalse();
   }
}

internal sealed class TrackingSnapshotStateMachine : IRaftStateMachine
{
   public ValueTask<ReadOnlyMemory<byte>> ApplyAsync(ReadOnlyMemory<byte> command, ulong logIndex,
      CancellationToken ct = default)
   {
      return ValueTask.FromResult<ReadOnlyMemory<byte>>("OK"u8.ToArray());
   }

   public ValueTask<ReadOnlyMemory<byte>> TakeSnapshotAsync(CancellationToken ct = default)
   {
      return ValueTask.FromResult<ReadOnlyMemory<byte>>("SNAP"u8.ToArray());
   }

   public ValueTask RestoreSnapshotAsync(ReadOnlyMemory<byte> snapshot, ulong lastIncludedIndex,
      ulong lastIncludedTerm, CancellationToken ct = default)
   {
      return ValueTask.CompletedTask;
   }
}

internal sealed class Apply4CompactingStateMachine(
   TaskCompletionSource enteredApply4,
   TaskCompletionSource readCompactedTermDuringApply,
   InMemoryRaftLogStorage baseStorage) : IRaftStateMachine
{
   public ulong TargetTerm { get; set; }

   public async ValueTask<ReadOnlyMemory<byte>> ApplyAsync(ReadOnlyMemory<byte> command, ulong logIndex,
      CancellationToken ct = default)
   {
      if (logIndex == 4)
      {
         enteredApply4.TrySetResult();
         await readCompactedTermDuringApply.Task.WaitAsync(ct);
         await baseStorage.CompactPrefixAsync(4, TargetTerm, ct);
      }

      return "OK"u8.ToArray();
   }

   public ValueTask<ReadOnlyMemory<byte>> TakeSnapshotAsync(CancellationToken ct = default)
   {
      return ValueTask.FromResult<ReadOnlyMemory<byte>>("SNAP4"u8.ToArray());
   }

   public ValueTask RestoreSnapshotAsync(ReadOnlyMemory<byte> snapshot, ulong lastIncludedIndex,
      ulong lastIncludedTerm, CancellationToken ct = default)
   {
      return ValueTask.CompletedTask;
   }
}

internal sealed class SnapshotRaceStorage(
   IRaftLogStorage inner,
   Action onGetCompactedUntilTerm) : IRaftLogStorage
{
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
      onGetCompactedUntilTerm();
      return inner.GetCompactedUntilTermAsync(ct);
   }

   public ValueTask DisposeAsync()
   {
      return inner.DisposeAsync();
   }
}

internal sealed class SnapshotCapturingTransport(
   IRaftTransport inner,
   Action<string, InstallSnapshotRequest> onInstallSnapshot) : IRaftTransport
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

   public ValueTask<RequestVoteResponse?> RequestVoteAsync(string peerId, RequestVoteRequest request,
      CancellationToken ct = default)
   {
      return ValueTask.FromResult<RequestVoteResponse?>(new RequestVoteResponse
         { Term = request.Term, VoteGranted = true });
   }

   public ValueTask<AppendEntriesResponse?> AppendEntriesAsync(string peerId, AppendEntriesRequest request,
      CancellationToken ct = default)
   {
      if (peerId == "peer-1")
         return ValueTask.FromResult<AppendEntriesResponse?>(new AppendEntriesResponse
         {
            Term = request.Term,
            Success = true,
            MatchIndex = request.PrevLogIndex + (ulong)request.Entries.Count
         });

      // peer-2 rejects AppendEntries so NextIndex drops to <= CompactedUntilIndex
      return ValueTask.FromResult<AppendEntriesResponse?>(new AppendEntriesResponse
      {
         Term = request.Term,
         Success = false,
         MatchIndex = 0
      });
   }

   public ValueTask<InstallSnapshotResponse?> InstallSnapshotAsync(string peerId, InstallSnapshotRequest request,
      CancellationToken ct = default)
   {
      onInstallSnapshot(peerId, request);
      return ValueTask.FromResult<InstallSnapshotResponse?>(new InstallSnapshotResponse
      {
         Term = request.Term,
         Success = request.LastIncludedIndex >= 4
      });
   }

   public ValueTask DisposeAsync()
   {
      return inner.DisposeAsync();
   }
}

internal sealed class AppendAndCompactOverlapStorage(
   IRaftLogStorage inner,
   TaskCompletionSource appendEntered,
   TaskCompletionSource releaseAppend) : IRaftLogStorage
{
   private int _inAppend;
   public bool ConcurrentCompactDuringAppendDetected { get; private set; }

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

   public async ValueTask AppendEntriesAsync(IReadOnlyList<RaftLogEntry> entries, CancellationToken ct = default)
   {
      if (entries.Count > 0)
      {
         Interlocked.Exchange(ref _inAppend, 1);
         try
         {
            appendEntered.TrySetResult();
            await releaseAppend.Task.WaitAsync(ct);
            await inner.AppendEntriesAsync(entries, ct);
         }
         finally
         {
            Interlocked.Exchange(ref _inAppend, 0);
         }
      }
      else
      {
         await inner.AppendEntriesAsync(entries, ct);
      }
   }

   public ValueTask TruncateLogAsync(ulong fromIndex, CancellationToken ct = default)
   {
      return inner.TruncateLogAsync(fromIndex, ct);
   }

   public ValueTask CompactPrefixAsync(ulong untilIndex, ulong untilTerm = 0, CancellationToken ct = default)
   {
      if (Volatile.Read(ref _inAppend) == 1) ConcurrentCompactDuringAppendDetected = true;

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

internal sealed class SnapshotStateMachine : IRaftStateMachine
{
   public Dictionary<string, string> Store { get; private set; } = new();

   public ValueTask<ReadOnlyMemory<byte>> ApplyAsync(ReadOnlyMemory<byte> command, ulong logIndex,
      CancellationToken ct = default)
   {
      var text = Encoding.UTF8.GetString(command.Span);
      if (text.StartsWith("SET "))
      {
         var parts = text[4..].Split('=');
         if (parts.Length == 2) Store[parts[0]] = parts[1];
      }

      return ValueTask.FromResult<ReadOnlyMemory<byte>>("OK"u8.ToArray());
   }

   public ValueTask<ReadOnlyMemory<byte>> TakeSnapshotAsync(CancellationToken ct = default)
   {
      var json = JsonSerializer.SerializeToUtf8Bytes(Store);
      return ValueTask.FromResult<ReadOnlyMemory<byte>>(json);
   }

   public ValueTask RestoreSnapshotAsync(ReadOnlyMemory<byte> snapshot, ulong lastIncludedIndex, ulong lastIncludedTerm,
      CancellationToken ct = default)
   {
      if (!snapshot.IsEmpty)
      {
         var deserialized = JsonSerializer.Deserialize<Dictionary<string, string>>(snapshot.Span);
         if (deserialized != null) Store = deserialized;
      }

      return ValueTask.CompletedTask;
   }
}
