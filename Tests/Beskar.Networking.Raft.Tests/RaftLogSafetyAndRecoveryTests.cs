using System.Text;
using Beskar.Networking.Raft.Enums;
using Beskar.Networking.Raft.Options;
using Beskar.Networking.Raft.Protocol.Enums;
using Beskar.Networking.Raft.Protocol.Messages;
using Beskar.Networking.Raft.Protocol.Models;
using Beskar.Networking.Raft.StateMachine;
using Beskar.Networking.Raft.Storage;
using Beskar.Networking.Raft.Transport;

namespace Beskar.Networking.Raft.Tests;

[NotInParallel]
public class RaftLogSafetyAndRecoveryTests
{
   [Test]
   [Timeout(10000)]
   public async Task AppendEntries_ConcurrentWithElectionAndProposal_DoesNotAckStaleTermOrOverwriteLogIndex(CancellationToken ct)
   {
      var innerStorage = new InMemoryRaftLogStorage();
      await innerStorage.SetTermAndVoteAsync(1, null, ct);

      var append1Entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
      var releaseAppend1 = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
      var storage = new GatedAppendLogStorage(innerStorage, targetTerm: 1, append1Entered, releaseAppend1, releaseTimeoutMs: 250);

      var transport = new ControlledRpcTransport
      {
         OnRequestVote = (_, req) => new RequestVoteResponse { Term = req.Term, VoteGranted = true },
         OnAppendEntries = (_, req) =>
         {
            var lastIdx = req.Entries.Count > 0 ? req.Entries[^1].Index : req.PrevLogIndex;
            return new AppendEntriesResponse { Term = req.Term, Success = true, MatchIndex = lastIdx };
         }
      };

      var options = new RaftNodeOptions
      {
         NodeId = "node-1",
         Peers = ["node-2"],
         ElectionTimeoutMin = TimeSpan.FromMilliseconds(55),
         ElectionTimeoutMax = TimeSpan.FromMilliseconds(85),
         HeartbeatInterval = TimeSpan.FromMilliseconds(15),
         EnablePreVote = false
      };

      await using var node = new RaftNode(options, storage, new PassThroughStateMachine(), transport);
      await node.StartAsync(ct);

      try
      {
         var staleAppendTask = transport.DispatchIncomingAsync(new RaftRpcRequest(
            RaftMessageType.AppendEntries,
            new AppendEntriesRequest
            {
               Term = 1,
               LeaderId = "node-2",
               PrevLogIndex = 0,
               PrevLogTerm = 0,
               LeaderCommitIndex = 0,
               Entries = [new RaftLogEntry(1, 1, "stale-term1"u8.ToArray())]
            })).AsTask();

         await append1Entered.Task.WaitAsync(ct);

         var deadline = Environment.TickCount64 + 220;
         while (Environment.TickCount64 < deadline && !(node.Role == RaftRole.Leader && node.CurrentTerm >= 2))
         {
            await Task.Delay(10, ct);
         }

         var becameLeaderBeforeAppendFinished = node.Role == RaftRole.Leader && node.CurrentTerm >= 2 && !staleAppendTask.IsCompleted;
         Task<ReadOnlyMemory<byte>>? proposeTask = null;
         if (node.Role == RaftRole.Leader)
         {
            proposeTask = node.ProposeAsync("leader-term2"u8.ToArray(), ct).AsTask();
            await Task.Delay(10, ct);
         }

         releaseAppend1.TrySetResult();

         var rpcResp = await staleAppendTask;
         var appendResp = (AppendEntriesResponse)rpcResp.Payload;

         if (proposeTask != null)
         {
            using var proposeTimeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
            proposeTimeout.CancelAfter(1000);
            await proposeTask.WaitAsync(proposeTimeout.Token);
         }

         var entryAt1 = await innerStorage.GetEntryAsync(1, ct);

         await Assert.That(becameLeaderBeforeAppendFinished && appendResp.Success).IsFalse();
         if (appendResp.Success)
         {
            await Assert.That(entryAt1).IsNotNull();
            await Assert.That(entryAt1!.Value.Term).IsEqualTo(1UL);
            await Assert.That(Encoding.UTF8.GetString(entryAt1.Value.Data.Span)).IsEqualTo("stale-term1");
         }
      }
      finally
      {
         releaseAppend1.TrySetResult();
      }
   }

   [Test]
   [Timeout(10000)]
   public async Task StartElection_CapturesConsistentLastLogIndexAndTerm_UnderConcurrentAppend(CancellationToken ct)
   {
      var innerStorage = new InMemoryRaftLogStorage();
      await innerStorage.SetTermAndVoteAsync(2, null, ct);
      await innerStorage.AppendEntriesAsync([new RaftLogEntry(1, 1, "e1"u8.ToArray())], ct);

      var append2Entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
      var releaseAppend2 = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
      var append2Committed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
      var capturedRequests = new List<RequestVoteRequest>();

      RaftNode? nodeRef = null;
      var storage = new SplitTailReadLogStorage(
         innerStorage,
         appendTargetTerm: 2,
         append2Entered,
         releaseAppend2,
         append2Committed,
         isTerm3ElectionActive: () => nodeRef is { Role: RaftRole.Candidate, CurrentTerm: >= 3 });

      var transport = new ControlledRpcTransport
      {
         OnRequestVote = (_, req) =>
         {
            lock (capturedRequests) capturedRequests.Add(req);
            return new RequestVoteResponse { Term = req.Term, VoteGranted = false };
         }
      };

      var options = new RaftNodeOptions
      {
         NodeId = "node-1",
         Peers = ["node-2"],
         ElectionTimeoutMin = TimeSpan.FromMilliseconds(45),
         ElectionTimeoutMax = TimeSpan.FromMilliseconds(70),
         HeartbeatInterval = TimeSpan.FromMilliseconds(15),
         EnablePreVote = false
      };

      await using var node = new RaftNode(options, storage, new PassThroughStateMachine(), transport);
      nodeRef = node;
      await node.StartAsync(ct);

      try
      {
         var appendTask = transport.DispatchIncomingAsync(new RaftRpcRequest(
            RaftMessageType.AppendEntries,
            new AppendEntriesRequest
            {
               Term = 2,
               LeaderId = "node-2",
               PrevLogIndex = 1,
               PrevLogTerm = 1,
               LeaderCommitIndex = 0,
               Entries = [new RaftLogEntry(2, 2, "e2"u8.ToArray())]
            })).AsTask();

         await append2Entered.Task.WaitAsync(ct);

         var waitVoteDeadline = Environment.TickCount64 + 1000;
         while (Environment.TickCount64 < waitVoteDeadline)
         {
            lock (capturedRequests)
            {
               if (capturedRequests.Count > 0) break;
            }

            await Task.Delay(10, ct);
         }

         releaseAppend2.TrySetResult();
         await appendTask;

         RequestVoteRequest? sentVote;
         lock (capturedRequests) sentVote = capturedRequests.FirstOrDefault();

         await Assert.That(sentVote).IsNotNull();
         var entryAtReportedIndex = await innerStorage.GetEntryAsync(sentVote!.LastLogIndex, ct);
         await Assert.That(entryAtReportedIndex).IsNotNull();
         await Assert.That(sentVote.LastLogTerm).IsEqualTo(entryAtReportedIndex!.Value.Term);
      }
      finally
      {
         releaseAppend2.TrySetResult();
      }
   }

   [Test]
   [Timeout(10000)]
   public async Task FileStorage_ReopenAfterConflictingCompactionBeforeLogTruncate_DiscardsStaleTailEntries(CancellationToken ct)
   {
      var testDir = Path.Combine(Path.GetTempPath(), $"raft_compact_recover_{Guid.NewGuid():N}");
      var preTruncateLogBackup = Path.Combine(testDir, "raft.log.pre_truncate_backup");

      try
      {
         await using (var initial = new FileRaftLogStorage(testDir))
         {
            await initial.AppendEntriesAsync([
               new RaftLogEntry(1, 1, "branchA-1"u8.ToArray()),
               new RaftLogEntry(1, 2, "branchA-2"u8.ToArray()),
               new RaftLogEntry(1, 3, "branchA-3"u8.ToArray())
            ], ct);
         }

         // Preserve raft.log contents prior to truncation
         File.Copy(Path.Combine(testDir, "raft.log"), preTruncateLogBackup, overwrite: true);

         // Compact with conflicting term at index 2 (term 2 != entry 2's term 1), updating metadata.bin
         await using (var compacted = new FileRaftLogStorage(testDir))
         {
            await compacted.CompactPrefixAsync(untilIndex: 2, untilTerm: 2, ct);
         }

         // Restore pre-truncation raft.log alongside updated metadata.bin to model a crash between WriteMetadata and SetLength(0)
         File.Copy(preTruncateLogBackup, Path.Combine(testDir, "raft.log"), overwrite: true);
         File.Delete(preTruncateLogBackup);

         await using var recovered = new FileRaftLogStorage(testDir);
         await Assert.That(await recovered.GetCompactedUntilIndexAsync(ct)).IsEqualTo(2UL);
         await Assert.That(await recovered.GetCompactedUntilTermAsync(ct)).IsEqualTo(2UL);
         await Assert.That(await recovered.GetLastLogIndexAsync(ct)).IsEqualTo(2UL);
         await Assert.That(await recovered.GetLastLogTermAsync(ct)).IsEqualTo(2UL);
         await Assert.That(await recovered.GetEntryAsync(3, ct)).IsNull();
      }
      finally
      {
         if (Directory.Exists(testDir))
         {
            Directory.Delete(testDir, recursive: true);
         }
      }
   }

   private sealed class PassThroughStateMachine : IRaftStateMachine
   {
      public ValueTask<ReadOnlyMemory<byte>> ApplyAsync(ReadOnlyMemory<byte> command, ulong logIndex, CancellationToken ct = default)
         => ValueTask.FromResult(command);
   }

   private sealed class GatedAppendLogStorage(
      IRaftLogStorage inner,
      ulong targetTerm,
      TaskCompletionSource appendEntered,
      TaskCompletionSource releaseAppend,
      int releaseTimeoutMs) : IRaftLogStorage
   {
      private int _gatedOnce;

      public ValueTask<ulong> GetCurrentTermAsync(CancellationToken ct = default) => inner.GetCurrentTermAsync(ct);
      public ValueTask SetCurrentTermAsync(ulong term, CancellationToken ct = default) => inner.SetCurrentTermAsync(term, ct);
      public ValueTask<string?> GetVotedForAsync(CancellationToken ct = default) => inner.GetVotedForAsync(ct);
      public ValueTask SetVotedForAsync(string? candidateId, CancellationToken ct = default) => inner.SetVotedForAsync(candidateId, ct);
      public ValueTask SetTermAndVoteAsync(ulong term, string? candidateId, CancellationToken ct = default) => inner.SetTermAndVoteAsync(term, candidateId, ct);
      public ValueTask<ulong> GetLastLogIndexAsync(CancellationToken ct = default) => inner.GetLastLogIndexAsync(ct);
      public ValueTask<ulong> GetLastLogTermAsync(CancellationToken ct = default) => inner.GetLastLogTermAsync(ct);
      public ValueTask<RaftLogEntry?> GetEntryAsync(ulong index, CancellationToken ct = default) => inner.GetEntryAsync(index, ct);
      public ValueTask<IReadOnlyList<RaftLogEntry>> GetEntriesAsync(ulong fromIndex, int maxCount, CancellationToken ct = default) => inner.GetEntriesAsync(fromIndex, maxCount, ct);

      public async ValueTask AppendEntriesAsync(IReadOnlyList<RaftLogEntry> entries, CancellationToken ct = default)
      {
         if (entries.Count > 0 && entries[0].Term == targetTerm && Interlocked.Exchange(ref _gatedOnce, 1) == 0)
         {
            appendEntered.TrySetResult();
            await Task.WhenAny(releaseAppend.Task, Task.Delay(releaseTimeoutMs, CancellationToken.None));
         }

         await inner.AppendEntriesAsync(entries, ct);
      }

      public ValueTask TruncateLogAsync(ulong fromIndex, CancellationToken ct = default) => inner.TruncateLogAsync(fromIndex, ct);
      public ValueTask CompactPrefixAsync(ulong untilIndex, ulong untilTerm = 0, CancellationToken ct = default) => inner.CompactPrefixAsync(untilIndex, untilTerm, ct);
      public ValueTask<ulong> GetCompactedUntilIndexAsync(CancellationToken ct = default) => inner.GetCompactedUntilIndexAsync(ct);
      public ValueTask<ulong> GetCompactedUntilTermAsync(CancellationToken ct = default) => inner.GetCompactedUntilTermAsync(ct);
      public ValueTask DisposeAsync() => inner.DisposeAsync();
   }

   private sealed class SplitTailReadLogStorage(
      IRaftLogStorage inner,
      ulong appendTargetTerm,
      TaskCompletionSource appendEntered,
      TaskCompletionSource releaseAppend,
      TaskCompletionSource appendCommitted,
      Func<bool> isTerm3ElectionActive) : IRaftLogStorage
   {
      private int _gatedAppendOnce;
      private int _waitedInTermReadOnce;

      public ValueTask<ulong> GetCurrentTermAsync(CancellationToken ct = default) => inner.GetCurrentTermAsync(ct);
      public ValueTask SetCurrentTermAsync(ulong term, CancellationToken ct = default) => inner.SetCurrentTermAsync(term, ct);
      public ValueTask<string?> GetVotedForAsync(CancellationToken ct = default) => inner.GetVotedForAsync(ct);
      public ValueTask SetVotedForAsync(string? candidateId, CancellationToken ct = default) => inner.SetVotedForAsync(candidateId, ct);
      public ValueTask SetTermAndVoteAsync(ulong term, string? candidateId, CancellationToken ct = default) => inner.SetTermAndVoteAsync(term, candidateId, ct);
      public ValueTask<ulong> GetLastLogIndexAsync(CancellationToken ct = default) => inner.GetLastLogIndexAsync(ct);

      public async ValueTask<ulong> GetLastLogTermAsync(CancellationToken ct = default)
      {
         if (isTerm3ElectionActive() && Interlocked.Exchange(ref _waitedInTermReadOnce, 1) == 0)
         {
            releaseAppend.TrySetResult();
            await Task.WhenAny(appendCommitted.Task, Task.Delay(200, CancellationToken.None));
         }

         return await inner.GetLastLogTermAsync(ct);
      }

      public ValueTask<RaftLogEntry?> GetEntryAsync(ulong index, CancellationToken ct = default) => inner.GetEntryAsync(index, ct);
      public ValueTask<IReadOnlyList<RaftLogEntry>> GetEntriesAsync(ulong fromIndex, int maxCount, CancellationToken ct = default) => inner.GetEntriesAsync(fromIndex, maxCount, ct);

      public async ValueTask AppendEntriesAsync(IReadOnlyList<RaftLogEntry> entries, CancellationToken ct = default)
      {
         if (entries.Count > 0 && entries[0].Term == appendTargetTerm && Interlocked.Exchange(ref _gatedAppendOnce, 1) == 0)
         {
            appendEntered.TrySetResult();
            await Task.WhenAny(releaseAppend.Task, Task.Delay(150, CancellationToken.None));
            await inner.AppendEntriesAsync(entries, ct);
            appendCommitted.TrySetResult();
            return;
         }

         await inner.AppendEntriesAsync(entries, ct);
      }

      public ValueTask TruncateLogAsync(ulong fromIndex, CancellationToken ct = default) => inner.TruncateLogAsync(fromIndex, ct);
      public ValueTask CompactPrefixAsync(ulong untilIndex, ulong untilTerm = 0, CancellationToken ct = default) => inner.CompactPrefixAsync(untilIndex, untilTerm, ct);
      public ValueTask<ulong> GetCompactedUntilIndexAsync(CancellationToken ct = default) => inner.GetCompactedUntilIndexAsync(ct);
      public ValueTask<ulong> GetCompactedUntilTermAsync(CancellationToken ct = default) => inner.GetCompactedUntilTermAsync(ct);
      public ValueTask DisposeAsync() => inner.DisposeAsync();
   }

   private sealed class ControlledRpcTransport : IRaftTransport
   {
      private Func<RaftRpcRequest, ValueTask<RaftRpcResponse>>? _handler;

      public Func<string, RequestVoteRequest, RequestVoteResponse?>? OnRequestVote { get; init; }
      public Func<string, AppendEntriesRequest, AppendEntriesResponse?>? OnAppendEntries { get; init; }
      public Func<string, InstallSnapshotRequest, InstallSnapshotResponse?>? OnInstallSnapshot { get; init; }

      public ValueTask StartAsync(Func<RaftRpcRequest, ValueTask<RaftRpcResponse>> rpcHandler, CancellationToken ct = default)
      {
         _handler = rpcHandler;
         return ValueTask.CompletedTask;
      }

      public ValueTask<RaftRpcResponse> DispatchIncomingAsync(RaftRpcRequest request)
      {
         var handler = _handler ?? throw new InvalidOperationException("Transport not started.");
         return handler(request);
      }

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
