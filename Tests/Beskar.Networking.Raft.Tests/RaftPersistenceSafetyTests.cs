using System.Collections.Concurrent;
using Beskar.Networking.Raft.Enums;
using Beskar.Networking.Raft.Options;
using Beskar.Networking.Raft.Protocol.Enums;
using Beskar.Networking.Raft.Protocol.Messages;
using Beskar.Networking.Raft.Protocol.Models;
using Beskar.Networking.Raft.StateMachine;
using Beskar.Networking.Raft.Storage;
using Beskar.Networking.Raft.Transport;
using Beskar.Networking.Transports.Memory;

namespace Beskar.Networking.Raft.Tests;

[NotInParallel]
public class RaftPersistenceSafetyTests
{
   [Test]
   [Timeout(10000)]
   public async Task StepDown_OnAppendEntriesHigherTerm_PersistsTermWhenStorageHonorsCancellation(CancellationToken ct)
   {
      var innerStorage = new InMemoryRaftLogStorage();
      var term10Persisted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
      var storage = new CancellationObservingLogStorage(innerStorage, targetTerm: 10, onTargetAttempt: () => term10Persisted.TrySetResult());

      var transport = new ScriptedPeerTransport
      {
         OnRequestVote = (_, req) => new RequestVoteResponse { Term = req.Term, VoteGranted = true },
         OnAppendEntries = (_, _) => new AppendEntriesResponse { Term = 10, Success = false, MatchIndex = 0 }
      };

      var options = new RaftNodeOptions
      {
         NodeId = "node-1",
         Peers = ["node-2"],
         ElectionTimeoutMin = TimeSpan.FromMilliseconds(150),
         ElectionTimeoutMax = TimeSpan.FromMilliseconds(220),
         HeartbeatInterval = TimeSpan.FromMilliseconds(25),
         EnablePreVote = false
      };

      await using var node = new RaftNode(options, storage, new PassThroughStateMachine(), transport);
      await node.StartAsync(ct);

      await term10Persisted.Task.WaitAsync(ct);
      await Task.Delay(25, ct);

      await Assert.That(storage.CancelledPersistenceAttempts).IsEqualTo(0);
      await Assert.That(await innerStorage.GetCurrentTermAsync(ct)).IsEqualTo(10UL);
      await Assert.That(node.CurrentTerm).IsEqualTo(10UL);
      await Assert.That(node.Role).IsEqualTo(RaftRole.Follower);
   }

   [Test]
   [Timeout(10000)]
   public async Task StepDown_OnInstallSnapshotHigherTerm_PersistsTermWhenStorageHonorsCancellation(CancellationToken ct)
   {
      var innerStorage = new InMemoryRaftLogStorage();
      await innerStorage.SetTermAndVoteAsync(1, null, ct);
      await innerStorage.AppendEntriesAsync([
         new RaftLogEntry(1, 1, "e1"u8.ToArray()),
         new RaftLogEntry(1, 2, "e2"u8.ToArray())
      ], ct);
      await innerStorage.CompactPrefixAsync(2, 1, ct);

      var term15Persisted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
      var storage = new CancellationObservingLogStorage(innerStorage, targetTerm: 15, onTargetAttempt: () => term15Persisted.TrySetResult());

      var transport = new ScriptedPeerTransport
      {
         OnRequestVote = (_, req) => new RequestVoteResponse { Term = req.Term, VoteGranted = true },
         // Return MatchIndex = 0 so NextIndex drops to 1 (<= compactedUntilIndex=2), triggering SendInstallSnapshotStepAsync
         OnAppendEntries = (_, req) => new AppendEntriesResponse { Term = req.Term, Success = false, MatchIndex = 0 },
         OnInstallSnapshot = (_, _) => new InstallSnapshotResponse { Term = 15, Success = false }
      };

      var options = new RaftNodeOptions
      {
         NodeId = "node-1",
         Peers = ["node-2"],
         ElectionTimeoutMin = TimeSpan.FromMilliseconds(150),
         ElectionTimeoutMax = TimeSpan.FromMilliseconds(220),
         HeartbeatInterval = TimeSpan.FromMilliseconds(25),
         EnablePreVote = false
      };

      await using var node = new RaftNode(options, storage, new PassThroughStateMachine(), transport);
      await node.StartAsync(ct);

      await term15Persisted.Task.WaitAsync(ct);
      await Task.Delay(25, ct);

      await Assert.That(storage.CancelledPersistenceAttempts).IsEqualTo(0);
      await Assert.That(await innerStorage.GetCurrentTermAsync(ct)).IsEqualTo(15UL);
      await Assert.That(node.CurrentTerm).IsEqualTo(15UL);
      await Assert.That(node.Role).IsEqualTo(RaftRole.Follower);
   }

   [Test]
   [Timeout(10000)]
   public async Task ConcurrentTermUpdates_MaintainMonotonicPersistedTermWhenVoteGranted(CancellationToken ct)
   {
      var memoryOptions = new MemoryTransportOptions();
      var nodeEndpoint = new MemoryEndPoint($"raft-persist-mono-{Guid.NewGuid():N}");
      var nodeListener = new MemoryNetworkListener(nodeEndpoint, memoryOptions);
      var nodeTransport = new RaftNetworkTransport(nodeListener, []);

      var innerStorage = new InMemoryRaftLogStorage();
      var write2Entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
      var releaseWrite2 = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
      var storage = new BlockingTermLogStorage(innerStorage, blockTerm: 2, blockVotedFor: null, write2Entered, releaseWrite2);

      var options = new RaftNodeOptions
      {
         NodeId = "node-1",
         Peers = ["peer-a"],
         ElectionTimeoutMin = TimeSpan.FromMilliseconds(5000),
         ElectionTimeoutMax = TimeSpan.FromMilliseconds(6000),
         HeartbeatInterval = TimeSpan.FromMilliseconds(100),
         EnablePreVote = false
      };

      await using var node = new RaftNode(options, storage, new PassThroughStateMachine(), nodeTransport);
      await node.StartAsync(ct);

      var clientEndpoint1 = new MemoryEndPoint($"client-mono-1-{Guid.NewGuid():N}");
      var clientListener1 = new MemoryNetworkListener(clientEndpoint1, memoryOptions);
      await using var clientTransport1 = new RaftNetworkTransport(clientListener1, [
         new RaftPeerEndpoint("node-1", nodeEndpoint, () => new MemoryNetworkClient(memoryOptions))
      ]);

      var clientEndpoint2 = new MemoryEndPoint($"client-mono-2-{Guid.NewGuid():N}");
      var clientListener2 = new MemoryNetworkListener(clientEndpoint2, memoryOptions);
      await using var clientTransport2 = new RaftNetworkTransport(clientListener2, [
         new RaftPeerEndpoint("node-1", nodeEndpoint, () => new MemoryNetworkClient(memoryOptions))
      ]);

      try
      {
         var appendTerm2Task = clientTransport1.AppendEntriesAsync("node-1", new AppendEntriesRequest
         {
            Term = 2,
            LeaderId = "peer-a",
            PrevLogIndex = 0,
            PrevLogTerm = 0,
            LeaderCommitIndex = 0,
            Entries = []
         }, ct).AsTask();

         await write2Entered.Task.WaitAsync(ct);

         var voteTerm3Task = clientTransport2.RequestVoteAsync("node-1", new RequestVoteRequest
         {
            Term = 3,
            CandidateId = "cand-3",
            LastLogIndex = 0,
            LastLogTerm = 0,
            IsPreVote = false
         }, ct).AsTask();

         await Task.WhenAny(voteTerm3Task, Task.Delay(80, ct));
         releaseWrite2.TrySetResult();

         await Task.WhenAll(appendTerm2Task, voteTerm3Task);

         var voteResp = await voteTerm3Task;
         await Assert.That(voteResp).IsNotNull();
         await Assert.That(voteResp!.VoteGranted).IsTrue();
         await Assert.That(storage.TermRegressedOnDisk).IsFalse();
         await Assert.That(await innerStorage.GetCurrentTermAsync(ct)).IsEqualTo(3UL);
         await Assert.That(await innerStorage.GetVotedForAsync(ct)).IsEqualTo("cand-3");
      }
      finally
      {
         releaseWrite2.TrySetResult();
      }
   }

   [Test]
   [Timeout(10000)]
   public async Task AppendEntries_InNewlySteppedUpTerm_WaitsForTermPersistenceBeforeReplyingSuccess(CancellationToken ct)
   {
      var memoryOptions = new MemoryTransportOptions();
      var nodeEndpoint = new MemoryEndPoint($"raft-persist-gate-{Guid.NewGuid():N}");
      var nodeListener = new MemoryNetworkListener(nodeEndpoint, memoryOptions);
      var nodeTransport = new RaftNetworkTransport(nodeListener, []);

      var innerStorage = new InMemoryRaftLogStorage();
      var write2Entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
      var releaseWrite2 = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
      var storage = new BlockingTermLogStorage(innerStorage, blockTerm: 2, blockVotedFor: null, write2Entered, releaseWrite2);

      var options = new RaftNodeOptions
      {
         NodeId = "node-1",
         Peers = ["peer-a"],
         ElectionTimeoutMin = TimeSpan.FromMilliseconds(5000),
         ElectionTimeoutMax = TimeSpan.FromMilliseconds(6000),
         HeartbeatInterval = TimeSpan.FromMilliseconds(100),
         EnablePreVote = false
      };

      await using var node = new RaftNode(options, storage, new PassThroughStateMachine(), nodeTransport);
      await node.StartAsync(ct);

      var clientEndpoint1 = new MemoryEndPoint($"client-gate-1-{Guid.NewGuid():N}");
      var clientListener1 = new MemoryNetworkListener(clientEndpoint1, memoryOptions);
      await using var clientTransport1 = new RaftNetworkTransport(clientListener1, [
         new RaftPeerEndpoint("node-1", nodeEndpoint, () => new MemoryNetworkClient(memoryOptions))
      ]);

      var clientEndpoint2 = new MemoryEndPoint($"client-gate-2-{Guid.NewGuid():N}");
      var clientListener2 = new MemoryNetworkListener(clientEndpoint2, memoryOptions);
      await using var clientTransport2 = new RaftNetworkTransport(clientListener2, [
         new RaftPeerEndpoint("node-1", nodeEndpoint, () => new MemoryNetworkClient(memoryOptions))
      ]);

      try
      {
         var stepUpRpcTask = clientTransport1.AppendEntriesAsync("node-1", new AppendEntriesRequest
         {
            Term = 2,
            LeaderId = "peer-a",
            PrevLogIndex = 0,
            PrevLogTerm = 0,
            LeaderCommitIndex = 0,
            Entries = []
         }, ct).AsTask();

         await write2Entered.Task.WaitAsync(ct);

         var concurrentAppendTask = clientTransport2.AppendEntriesAsync("node-1", new AppendEntriesRequest
         {
            Term = 2,
            LeaderId = "peer-a",
            PrevLogIndex = 0,
            PrevLogTerm = 0,
            LeaderCommitIndex = 0,
            Entries = [new RaftLogEntry(2, 1, "cmd-1"u8.ToArray())]
         }, ct).AsTask();

         await Task.WhenAny(concurrentAppendTask, Task.Delay(80, ct));
         var durableTermAtAck = await innerStorage.GetCurrentTermAsync(ct);
         var ackedBeforeDurable = concurrentAppendTask.IsCompleted && durableTermAtAck < 2UL;

         releaseWrite2.TrySetResult();
         await Task.WhenAll(stepUpRpcTask, concurrentAppendTask);

         var appendResp = await concurrentAppendTask;
         await Assert.That(appendResp).IsNotNull();
         await Assert.That(appendResp!.Success).IsTrue();
         await Assert.That(ackedBeforeDurable).IsFalse();
      }
      finally
      {
         releaseWrite2.TrySetResult();
      }
   }

   private sealed class PassThroughStateMachine : IRaftStateMachine
   {
      public ValueTask<ReadOnlyMemory<byte>> ApplyAsync(ReadOnlyMemory<byte> command, ulong logIndex, CancellationToken ct = default)
         => ValueTask.FromResult(command);
   }

   private sealed class CancellationObservingLogStorage(
      IRaftLogStorage inner,
      ulong targetTerm,
      Action onTargetAttempt) : IRaftLogStorage
   {
      private int _cancelledAttempts;
      public int CancelledPersistenceAttempts => Volatile.Read(ref _cancelledAttempts);

      public ValueTask<ulong> GetCurrentTermAsync(CancellationToken ct = default) => inner.GetCurrentTermAsync(ct);

      public async ValueTask SetCurrentTermAsync(ulong term, CancellationToken ct = default)
      {
         if (ct.IsCancellationRequested)
         {
            Interlocked.Increment(ref _cancelledAttempts);
            if (term == targetTerm) onTargetAttempt();
            ct.ThrowIfCancellationRequested();
         }

         await inner.SetCurrentTermAsync(term, ct);
         if (term == targetTerm) onTargetAttempt();
      }

      public ValueTask<string?> GetVotedForAsync(CancellationToken ct = default) => inner.GetVotedForAsync(ct);

      public ValueTask SetVotedForAsync(string? candidateId, CancellationToken ct = default)
      {
         if (ct.IsCancellationRequested)
         {
            Interlocked.Increment(ref _cancelledAttempts);
            ct.ThrowIfCancellationRequested();
         }

         return inner.SetVotedForAsync(candidateId, ct);
      }

      public async ValueTask SetTermAndVoteAsync(ulong term, string? candidateId, CancellationToken ct = default)
      {
         if (ct.IsCancellationRequested)
         {
            Interlocked.Increment(ref _cancelledAttempts);
            if (term == targetTerm) onTargetAttempt();
            ct.ThrowIfCancellationRequested();
         }

         await inner.SetTermAndVoteAsync(term, candidateId, ct);
         if (term == targetTerm) onTargetAttempt();
      }

      public ValueTask<ulong> GetLastLogIndexAsync(CancellationToken ct = default) => inner.GetLastLogIndexAsync(ct);
      public ValueTask<ulong> GetLastLogTermAsync(CancellationToken ct = default) => inner.GetLastLogTermAsync(ct);
      public ValueTask<RaftLogEntry?> GetEntryAsync(ulong index, CancellationToken ct = default) => inner.GetEntryAsync(index, ct);
      public ValueTask<IReadOnlyList<RaftLogEntry>> GetEntriesAsync(ulong fromIndex, int maxCount, CancellationToken ct = default) => inner.GetEntriesAsync(fromIndex, maxCount, ct);
      public ValueTask AppendEntriesAsync(IReadOnlyList<RaftLogEntry> entries, CancellationToken ct = default) => inner.AppendEntriesAsync(entries, ct);
      public ValueTask TruncateLogAsync(ulong fromIndex, CancellationToken ct = default) => inner.TruncateLogAsync(fromIndex, ct);
      public ValueTask CompactPrefixAsync(ulong untilIndex, ulong untilTerm = 0, CancellationToken ct = default) => inner.CompactPrefixAsync(untilIndex, untilTerm, ct);
      public ValueTask<ulong> GetCompactedUntilIndexAsync(CancellationToken ct = default) => inner.GetCompactedUntilIndexAsync(ct);
      public ValueTask<ulong> GetCompactedUntilTermAsync(CancellationToken ct = default) => inner.GetCompactedUntilTermAsync(ct);
      public ValueTask DisposeAsync() => inner.DisposeAsync();
   }

   private sealed class BlockingTermLogStorage(
      IRaftLogStorage inner,
      ulong blockTerm,
      string? blockVotedFor,
      TaskCompletionSource blockEntered,
      TaskCompletionSource releaseBlock) : IRaftLogStorage
   {
      private readonly Lock _historyLock = new();
      private int _blockedOnce;
      private ulong _highestCommittedTerm;
      private bool _regressed;

      public bool TermRegressedOnDisk
      {
         get
         {
            lock (_historyLock) return _regressed;
         }
      }

      public ValueTask<ulong> GetCurrentTermAsync(CancellationToken ct = default) => inner.GetCurrentTermAsync(ct);

      public async ValueTask SetCurrentTermAsync(ulong term, CancellationToken ct = default)
      {
         await inner.SetCurrentTermAsync(term, ct);
         RecordTermWrite(await inner.GetCurrentTermAsync(ct));
      }

      public ValueTask<string?> GetVotedForAsync(CancellationToken ct = default) => inner.GetVotedForAsync(ct);
      public ValueTask SetVotedForAsync(string? candidateId, CancellationToken ct = default) => inner.SetVotedForAsync(candidateId, ct);

      public async ValueTask SetTermAndVoteAsync(ulong term, string? candidateId, CancellationToken ct = default)
      {
         if (term == blockTerm && candidateId == blockVotedFor && Interlocked.Exchange(ref _blockedOnce, 1) == 0)
         {
            blockEntered.TrySetResult();
            await Task.WhenAny(releaseBlock.Task, Task.Delay(2000, ct));
         }

         await inner.SetTermAndVoteAsync(term, candidateId, ct);
         RecordTermWrite(await inner.GetCurrentTermAsync(ct));
      }

      private void RecordTermWrite(ulong term)
      {
         lock (_historyLock)
         {
            if (term < _highestCommittedTerm)
            {
               _regressed = true;
            }

            if (term > _highestCommittedTerm)
            {
               _highestCommittedTerm = term;
            }
         }
      }

      public ValueTask<ulong> GetLastLogIndexAsync(CancellationToken ct = default) => inner.GetLastLogIndexAsync(ct);
      public ValueTask<ulong> GetLastLogTermAsync(CancellationToken ct = default) => inner.GetLastLogTermAsync(ct);
      public ValueTask<RaftLogEntry?> GetEntryAsync(ulong index, CancellationToken ct = default) => inner.GetEntryAsync(index, ct);
      public ValueTask<IReadOnlyList<RaftLogEntry>> GetEntriesAsync(ulong fromIndex, int maxCount, CancellationToken ct = default) => inner.GetEntriesAsync(fromIndex, maxCount, ct);
      public ValueTask AppendEntriesAsync(IReadOnlyList<RaftLogEntry> entries, CancellationToken ct = default) => inner.AppendEntriesAsync(entries, ct);
      public ValueTask TruncateLogAsync(ulong fromIndex, CancellationToken ct = default) => inner.TruncateLogAsync(fromIndex, ct);
      public ValueTask CompactPrefixAsync(ulong untilIndex, ulong untilTerm = 0, CancellationToken ct = default) => inner.CompactPrefixAsync(untilIndex, untilTerm, ct);
      public ValueTask<ulong> GetCompactedUntilIndexAsync(CancellationToken ct = default) => inner.GetCompactedUntilIndexAsync(ct);
      public ValueTask<ulong> GetCompactedUntilTermAsync(CancellationToken ct = default) => inner.GetCompactedUntilTermAsync(ct);
      public ValueTask DisposeAsync() => inner.DisposeAsync();
   }

   private sealed class ScriptedPeerTransport : IRaftTransport
   {
      public Func<string, RequestVoteRequest, RequestVoteResponse?>? OnRequestVote { get; init; }
      public Func<string, AppendEntriesRequest, AppendEntriesResponse?>? OnAppendEntries { get; init; }
      public Func<string, InstallSnapshotRequest, InstallSnapshotResponse?>? OnInstallSnapshot { get; init; }

      public ValueTask StartAsync(Func<RaftRpcRequest, ValueTask<RaftRpcResponse>> rpcHandler, CancellationToken ct = default)
         => ValueTask.CompletedTask;

      public ValueTask StopAsync(CancellationToken ct = default) => ValueTask.CompletedTask;

      public ValueTask<RequestVoteResponse?> RequestVoteAsync(string peerId, RequestVoteRequest request, CancellationToken ct = default)
         => ValueTask.FromResult(OnRequestVote?.Invoke(peerId, request));

      public ValueTask<AppendEntriesResponse?> AppendEntriesAsync(string peerId, AppendEntriesRequest request, CancellationToken ct = default)
         => ValueTask.FromResult(OnAppendEntries?.Invoke(peerId, request));

      public ValueTask<InstallSnapshotResponse?> InstallSnapshotAsync(string peerId, InstallSnapshotRequest request, CancellationToken ct = default)
         => ValueTask.FromResult(OnInstallSnapshot?.Invoke(peerId, request));

      public ValueTask DisposeAsync() => ValueTask.CompletedTask;
   }
}
