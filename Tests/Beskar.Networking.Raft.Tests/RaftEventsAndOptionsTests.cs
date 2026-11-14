using System.Text;
using Beskar.Networking.Raft.Enums;
using Beskar.Networking.Raft.Events;
using Beskar.Networking.Raft.Options;
using Beskar.Networking.Raft.Protocol.Messages;
using Beskar.Networking.Raft.Protocol.Models;
using Beskar.Networking.Raft.StateMachine;
using Beskar.Networking.Raft.Storage;
using Beskar.Networking.Raft.Transport;
using Beskar.Networking.Transports.Memory;

namespace Beskar.Networking.Raft.Tests;

public class RaftEventsAndOptionsTests
{
   [Test]
   public async Task RaftNodeOptions_Defaults_AreSetSensibly()
   {
      var options = new RaftNodeOptions
      {
         NodeId = "test-node"
      };

      await Assert.That(options.NodeId).IsEqualTo("test-node");
      await Assert.That(options.Peers.Count).IsEqualTo(0);
      await Assert.That(options.ElectionTimeoutMin).IsGreaterThan(TimeSpan.Zero);
      await Assert.That(options.ElectionTimeoutMax).IsGreaterThan(options.ElectionTimeoutMin);
      await Assert.That(options.HeartbeatInterval).IsGreaterThan(TimeSpan.Zero);
      await Assert.That(options.MaxAppendEntriesBatchSize).IsGreaterThan(0);
   }

   [Test]
   public async Task RaftNode_RoleChangedEvent_FiresOnLifecycleTransitions()
   {
      var options = new RaftNodeOptions
      {
         NodeId = "solo-node",
         Peers = [],
         ElectionTimeoutMin = TimeSpan.FromMilliseconds(50),
         ElectionTimeoutMax = TimeSpan.FromMilliseconds(100),
         HeartbeatInterval = TimeSpan.FromMilliseconds(20)
      };

      var storage = new InMemoryRaftLogStorage();
      var sm = new TestRaftStateMachine();
      var memoryOptions = new MemoryTransportOptions();
      var endPoint = new MemoryEndPoint($"raft-evt-{Guid.NewGuid():N}");
      var listener = new MemoryNetworkListener(endPoint, memoryOptions);
      var transport = new RaftNetworkTransport(listener, []);

      await using var node = new RaftNode(options, storage, sm, transport);

      var roleTransitions = new List<(RaftRole OldRole, RaftRole NewRole)>();
      node.Events.OnRoleChanged.Add((ctx, _) =>
      {
         roleTransitions.Add((ctx.OldRole, ctx.NewRole));
         return ValueTask.CompletedTask;
      });

      await node.StartAsync();
      await Task.Delay(150); // Becomes follower -> candidate -> leader

      await node.StopAsync();

      await Assert.That(roleTransitions.Count).IsGreaterThanOrEqualTo(2);
      await Assert.That(roleTransitions[0]).IsEqualTo((RaftRole.Stopped, RaftRole.Follower));
   }

   [Test]
   public async Task RaftNode_EntryCommittedEvent_FiresWithCorrectContext()
   {
      var options = new RaftNodeOptions
      {
         NodeId = "solo-node",
         Peers = [],
         ElectionTimeoutMin = TimeSpan.FromMilliseconds(50),
         ElectionTimeoutMax = TimeSpan.FromMilliseconds(100),
         HeartbeatInterval = TimeSpan.FromMilliseconds(20)
      };

      var storage = new InMemoryRaftLogStorage();
      var sm = new TestRaftStateMachine();
      var memoryOptions = new MemoryTransportOptions();
      var endPoint = new MemoryEndPoint($"raft-evt-commit-{Guid.NewGuid():N}");
      var listener = new MemoryNetworkListener(endPoint, memoryOptions);
      var transport = new RaftNetworkTransport(listener, []);

      await using var node = new RaftNode(options, storage, sm, transport);

      RaftEntryCommittedContext? committedCtx = null;
      node.Events.OnEntryCommitted.Add((ctx, _) =>
      {
         committedCtx = ctx;
         return ValueTask.CompletedTask;
      });

      await node.StartAsync();
      await Task.Delay(150);

      await node.ProposeAsync("TEST_PROPOSAL"u8.ToArray());

      await Assert.That(committedCtx).IsNotNull();
      await Assert.That(committedCtx!.NodeId).IsEqualTo("solo-node");
      await Assert.That(committedCtx.Entry.Index).IsEqualTo(1UL);
   }

   [Test]
   public async Task ContextModels_ConstructAndExposePropertiesCorrectly()
   {
      var roleCtx = new RaftRoleChangedContext("node-1", RaftRole.Follower, RaftRole.Candidate, 5);
      await Assert.That(roleCtx.NodeId).IsEqualTo("node-1");
      await Assert.That(roleCtx.OldRole).IsEqualTo(RaftRole.Follower);
      await Assert.That(roleCtx.NewRole).IsEqualTo(RaftRole.Candidate);
      await Assert.That(roleCtx.Term).IsEqualTo(5UL);

      var leaderCtx = new RaftLeaderChangedContext("node-1", "leader-99", 10);
      await Assert.That(leaderCtx.NodeId).IsEqualTo("node-1");
      await Assert.That(leaderCtx.LeaderId).IsEqualTo("leader-99");
      await Assert.That(leaderCtx.Term).IsEqualTo(10UL);

      var entry = new RaftLogEntry(1, 2, "cmd"u8.ToArray());
      var commitCtx = new RaftEntryCommittedContext("node-1", entry, "RESULT"u8.ToArray());
      await Assert.That(commitCtx.NodeId).IsEqualTo("node-1");
      await Assert.That(commitCtx.Entry.Index).IsEqualTo(2UL);
   }

   [Test]
   [Timeout(5000)]
   public async Task RaftNode_RestartAfterStop_StopsAndUnbindsCleanlyOnSecondStop(CancellationToken ct)
   {
      var memoryOptions = new MemoryTransportOptions();
      var ep = new MemoryEndPoint($"node-restart-stop-{Guid.NewGuid():N}");
      var listener = new MemoryNetworkListener(ep, memoryOptions);
      var transport = new RaftNetworkTransport(listener, []);

      await using var node = new RaftNode(
         new RaftNodeOptions
         {
            NodeId = "node-1",
            Peers = [],
            ElectionTimeoutMin = TimeSpan.FromMilliseconds(40),
            ElectionTimeoutMax = TimeSpan.FromMilliseconds(70),
            HeartbeatInterval = TimeSpan.FromMilliseconds(20),
            EnablePreVote = false
         },
         new InMemoryRaftLogStorage(),
         new LifecycleNoOpStateMachine(),
         transport);

      try
      {
         await node.StartAsync(ct);
         await Assert.That(listener.IsBound).IsTrue();

         await node.StopAsync(ct);
         await Assert.That(node.Role).IsEqualTo(RaftRole.Stopped);
         await Assert.That(listener.IsBound).IsFalse();

         await node.StartAsync(ct);
         await Assert.That(listener.IsBound).IsTrue();

         await node.StopAsync(ct);
         await Assert.That(node.Role).IsEqualTo(RaftRole.Stopped);
         await Assert.That(listener.IsBound).IsFalse();
      }
      finally
      {
         if (listener.IsBound) await listener.UnbindAsync(CancellationToken.None);
      }
   }

   [Test]
   [Timeout(5000)]
   public async Task RaftNode_FailedStartAsync_RollsBackRoleToStoppedAndAllowsRetry(CancellationToken ct)
   {
      var memoryOptions = new MemoryTransportOptions();
      var ep = new MemoryEndPoint($"node-fail-start-{Guid.NewGuid():N}");
      var transport = new RaftNetworkTransport(
         new MemoryNetworkListener(ep, memoryOptions), []);
      var storage = new ThrowOnceOnStartupStorage(new InMemoryRaftLogStorage());

      await using var node = new RaftNode(
         new RaftNodeOptions
         {
            NodeId = "node-1",
            Peers = [],
            ElectionTimeoutMin = TimeSpan.FromMilliseconds(40),
            ElectionTimeoutMax = TimeSpan.FromMilliseconds(70),
            HeartbeatInterval = TimeSpan.FromMilliseconds(20),
            EnablePreVote = false
         },
         storage,
         new LifecycleNoOpStateMachine(),
         transport);

      await Assert.ThrowsAsync<IOException>(async () => await node.StartAsync(ct));
      await Assert.That(node.Role).IsEqualTo(RaftRole.Stopped);

      // Retry after transient failure must succeed cleanly
      await node.StartAsync(ct);

      var deadline = DateTime.UtcNow.AddSeconds(2);
      while (node.Role != RaftRole.Leader && DateTime.UtcNow < deadline) await Task.Delay(15, ct);

      await Assert.That(node.Role).IsEqualTo(RaftRole.Leader);
   }

   [Test]
   [Timeout(5000)]
   public async Task RaftNode_DoubleDisposeAsync_DisposesDependenciesOnce(CancellationToken ct)
   {
      var memoryOptions = new MemoryTransportOptions();
      var ep = new MemoryEndPoint($"node-dbl-disp-{Guid.NewGuid():N}");
      var transport = new DisposeCountingTransport(
         new RaftNetworkTransport(
            new MemoryNetworkListener(ep, memoryOptions), []));
      var storage = new DisposeCountingStorage(new InMemoryRaftLogStorage());

      var node = new RaftNode(
         new RaftNodeOptions { NodeId = "node-1", Peers = [] },
         storage,
         new LifecycleNoOpStateMachine(),
         transport);

      await node.StartAsync(ct);
      await node.DisposeAsync();
      await node.DisposeAsync();

      await Assert.That(storage.DisposeCount).IsEqualTo(1);
      await Assert.That(transport.DisposeCount).IsEqualTo(1);
   }

   [Test]
   [Timeout(5000)]
   public async Task RaftNode_NonPositiveBatchSize_ThrowsArgumentException(CancellationToken ct)
   {
      await using var transport = new RaftNetworkTransport(
         new MemoryNetworkListener(
            new MemoryEndPoint($"opt-batch-{Guid.NewGuid():N}"),
            new MemoryTransportOptions()),
         []);
      await using var storage = new InMemoryRaftLogStorage();

      var threw = false;
      try
      {
         _ = new RaftNode(
            new RaftNodeOptions { NodeId = "node-1", Peers = [], MaxAppendEntriesBatchSize = 0 },
            storage,
            new LifecycleNoOpStateMachine(),
            transport);
      }
      catch (ArgumentException)
      {
         threw = true;
      }

      await Assert.That(threw).IsTrue();
   }

   [Test]
   [Timeout(5000)]
   public async Task RaftNode_InvalidIntervals_ThrowsArgumentException(CancellationToken ct)
   {
      await using var transport = new RaftNetworkTransport(
         new MemoryNetworkListener(
            new MemoryEndPoint($"opt-interval-{Guid.NewGuid():N}"),
            new MemoryTransportOptions()),
         []);
      await using var storage = new InMemoryRaftLogStorage();

      var threwHeartbeat = false;
      try
      {
         _ = new RaftNode(
            new RaftNodeOptions { NodeId = "node-1", Peers = [], HeartbeatInterval = TimeSpan.Zero },
            storage,
            new LifecycleNoOpStateMachine(),
            transport);
      }
      catch (ArgumentException)
      {
         threwHeartbeat = true;
      }

      await Assert.That(threwHeartbeat).IsTrue();

      var threwMinZero = false;
      try
      {
         _ = new RaftNode(
            new RaftNodeOptions { NodeId = "node-1", Peers = [], ElectionTimeoutMin = TimeSpan.Zero },
            storage,
            new LifecycleNoOpStateMachine(),
            transport);
      }
      catch (ArgumentException)
      {
         threwMinZero = true;
      }

      await Assert.That(threwMinZero).IsTrue();

      var threwMinGtMax = false;
      try
      {
         _ = new RaftNode(
            new RaftNodeOptions
            {
               NodeId = "node-1",
               Peers = [],
               ElectionTimeoutMin = TimeSpan.FromMilliseconds(300),
               ElectionTimeoutMax = TimeSpan.FromMilliseconds(100)
            },
            storage,
            new LifecycleNoOpStateMachine(),
            transport);
      }
      catch (ArgumentException)
      {
         threwMinGtMax = true;
      }

      await Assert.That(threwMinGtMax).IsTrue();
   }

   [Test]
   [Timeout(5000)]
   public async Task RaftNode_SelfOrDuplicatePeerInOptions_ThrowsArgumentException(CancellationToken ct)
   {
      await using var transport = new RaftNetworkTransport(
         new MemoryNetworkListener(
            new MemoryEndPoint($"opt-peers-{Guid.NewGuid():N}"),
            new MemoryTransportOptions()),
         []);
      await using var storage = new InMemoryRaftLogStorage();

      var threwSelf = false;
      try
      {
         _ = new RaftNode(
            new RaftNodeOptions { NodeId = "node-1", Peers = ["node-1"] },
            storage,
            new LifecycleNoOpStateMachine(),
            transport);
      }
      catch (ArgumentException)
      {
         threwSelf = true;
      }

      await Assert.That(threwSelf).IsTrue();

      var threwDuplicate = false;
      try
      {
         _ = new RaftNode(
            new RaftNodeOptions { NodeId = "node-1", Peers = ["peer-1", "peer-1"] },
            storage,
            new LifecycleNoOpStateMachine(),
            transport);
      }
      catch (ArgumentException)
      {
         threwDuplicate = true;
      }

      await Assert.That(threwDuplicate).IsTrue();
   }

   [Test]
   [Timeout(5000)]
   public async Task AdvanceCommitIndex_OnEntryCommittedThrows_ContinuesApplyingSubsequentEntries(CancellationToken ct)
   {
      var memoryOptions = new MemoryTransportOptions();
      var nodeEp = new MemoryEndPoint($"commit-evt-n-{Guid.NewGuid():N}");
      var nodeTransport = new RaftNetworkTransport(
         new MemoryNetworkListener(nodeEp, memoryOptions), []);
      var sm = new RecordingLifecycleStateMachine();

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
         new InMemoryRaftLogStorage(),
         sm,
         nodeTransport);

      node.Events.OnEntryCommitted.Add((ctx, _) =>
      {
         if (Encoding.UTF8.GetString(ctx.Entry.Data.Span) == "FIRST")
            throw new InvalidOperationException("Subscriber failure");

         return ValueTask.CompletedTask;
      });

      var clientTransport = new RaftNetworkTransport(
         new MemoryNetworkListener(
            new MemoryEndPoint($"commit-evt-c-{Guid.NewGuid():N}"), memoryOptions),
         [new RaftPeerEndpoint("node-1", nodeEp, () => new MemoryNetworkClient(memoryOptions))]);
      await using var _ = clientTransport;

      await node.StartAsync(ct);

      try
      {
         await clientTransport.AppendEntriesAsync("node-1", new AppendEntriesRequest
         {
            Term = 1,
            LeaderId = "peer-1",
            PrevLogIndex = 0,
            PrevLogTerm = 0,
            Entries =
            [
               new RaftLogEntry(1, 1, "FIRST"u8.ToArray()),
               new RaftLogEntry(1, 2, "SECOND"u8.ToArray())
            ],
            LeaderCommitIndex = 2
         }, ct);
      }
      catch (InvalidOperationException)
      {
      }

      await Assert.That(node.LastApplied).IsEqualTo(2UL);
      await Assert.That(sm.Applied).Contains("SECOND");
   }
}

internal sealed class LifecycleNoOpStateMachine : IRaftStateMachine
{
   public ValueTask<ReadOnlyMemory<byte>> ApplyAsync(ReadOnlyMemory<byte> command, ulong logIndex,
      CancellationToken ct = default)
   {
      return ValueTask.FromResult<ReadOnlyMemory<byte>>("OK"u8.ToArray());
   }
}

internal sealed class RecordingLifecycleStateMachine : IRaftStateMachine
{
   public List<string> Applied { get; } = [];

   public ValueTask<ReadOnlyMemory<byte>> ApplyAsync(ReadOnlyMemory<byte> command, ulong logIndex,
      CancellationToken ct = default)
   {
      Applied.Add(Encoding.UTF8.GetString(command.Span));
      return ValueTask.FromResult<ReadOnlyMemory<byte>>("OK"u8.ToArray());
   }
}

internal sealed class ThrowOnceOnStartupStorage(IRaftLogStorage inner)
   : IRaftLogStorage
{
   private int _threw;

   public ValueTask<ulong> GetCurrentTermAsync(CancellationToken ct = default)
   {
      if (Interlocked.Exchange(ref _threw, 1) == 0) throw new IOException("Transient startup storage failure");

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
      return inner.GetCompactedUntilTermAsync(ct);
   }

   public ValueTask DisposeAsync()
   {
      return inner.DisposeAsync();
   }
}

internal sealed class DisposeCountingStorage(IRaftLogStorage inner)
   : IRaftLogStorage
{
   private int _disposeCount;
   public int DisposeCount => Volatile.Read(ref _disposeCount);

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
      return inner.GetCompactedUntilTermAsync(ct);
   }

   public ValueTask DisposeAsync()
   {
      Interlocked.Increment(ref _disposeCount);
      return inner.DisposeAsync();
   }
}

internal sealed class DisposeCountingTransport(IRaftTransport inner)
   : IRaftTransport
{
   private int _disposeCount;
   public int DisposeCount => Volatile.Read(ref _disposeCount);

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
      return inner.RequestVoteAsync(peerId, request, ct);
   }

   public ValueTask<AppendEntriesResponse?> AppendEntriesAsync(string peerId, AppendEntriesRequest request,
      CancellationToken ct = default)
   {
      return inner.AppendEntriesAsync(peerId, request, ct);
   }

   public ValueTask<InstallSnapshotResponse?> InstallSnapshotAsync(string peerId, InstallSnapshotRequest request,
      CancellationToken ct = default)
   {
      return inner.InstallSnapshotAsync(peerId, request, ct);
   }

   public ValueTask DisposeAsync()
   {
      Interlocked.Increment(ref _disposeCount);
      return inner.DisposeAsync();
   }
}
