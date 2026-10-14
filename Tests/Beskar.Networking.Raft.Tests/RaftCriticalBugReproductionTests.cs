using System.Reflection;
using System.Text;
using System.Text.Json;
using Beskar.Networking.Raft.Enums;
using Beskar.Networking.Raft.Options;
using Beskar.Networking.Raft.Protocol.Messages;
using Beskar.Networking.Raft.Protocol.Models;
using Beskar.Networking.Raft.Storage;
using Beskar.Networking.Raft.Transport;
using Beskar.Networking.Transports.Memory;

namespace Beskar.Networking.Raft.Tests;

[NotInParallel]
public class RaftCriticalBugReproductionTests
{
   [Test]
   [Timeout(5000)]
   public async Task Follower_ShouldNotCommitUnverifiedTailEntriesFromPreviousTerm_WhenLeaderSendsHeartbeat(CancellationToken ct)
   {
      var memoryOptions = new MemoryTransportOptions();
      var ep = new MemoryEndPoint($"follower-safety-{Guid.NewGuid():N}");
      var listener = new MemoryNetworkListener(ep, memoryOptions);
      var transport = new RaftNetworkTransport(listener, []);

      var storage = new InMemoryRaftLogStorage();
      await storage.SetCurrentTermAsync(1, ct);
      // Follower has entry 1 (term 1) and an uncommitted conflicting entry 2 (term 1) from a deposed leader
      await storage.AppendEntriesAsync([
         new RaftLogEntry(1, 1, "SET k1=v1"u8.ToArray()),
         new RaftLogEntry(1, 2, "SET k2=UNCOMMITTED_CONFLICT_FROM_DEPOSED_LEADER"u8.ToArray())
      ], ct);

      var sm = new SnapshotStateMachine();
      var options = new RaftNodeOptions
      {
         NodeId = "follower-1",
         Peers = ["leader-2"],
         ElectionTimeoutMin = TimeSpan.FromMilliseconds(500),
         ElectionTimeoutMax = TimeSpan.FromMilliseconds(1000),
         HeartbeatInterval = TimeSpan.FromMilliseconds(50)
      };

      await using var follower = new RaftNode(options, storage, sm, transport);
      await follower.StartAsync(ct);

      var clientEp = new MemoryEndPoint($"leader-sender-{Guid.NewGuid():N}");
      var clientListener = new MemoryNetworkListener(clientEp, memoryOptions);
      await using var clientTransport = new RaftNetworkTransport(clientListener, [
         new RaftPeerEndpoint("follower-1", ep, () => new MemoryNetworkClient(memoryOptions))
      ]);

      // New leader in Term 2 sends a heartbeat (or partial batch) matching PrevLogIndex=1, PrevLogTerm=1,
      // with Entries=[] and LeaderCommitIndex=2 (because leader committed a Term 2 entry at index 2 on another quorum).
      // Per Raft Figure 2 Receiver Implementation Rule 5:
      // commitIndex = min(leaderCommit, index of last new entry) = min(2, 1) = 1.
      var resp = await clientTransport.AppendEntriesAsync("follower-1", new AppendEntriesRequest
      {
         Term = 2,
         LeaderId = "leader-2",
         PrevLogIndex = 1,
         PrevLogTerm = 1,
         LeaderCommitIndex = 2,
         Entries = []
      }, ct);

      await Assert.That(resp).IsNotNull();
      await Assert.That(resp!.Success).IsTrue();

      // Follower MUST only commit up to index 1 (replicatedMatchIndex), NOT index 2!
      await Assert.That(follower.CommitIndex).IsEqualTo(1UL);
      await Assert.That(sm.Store.ContainsKey("k2")).IsFalse();
   }

   [Test]
   [Timeout(5000)]
   public async Task StepDownAsync_WithStaleTerm_WhileLeaderOrCandidate_ShouldNeverRegressCurrentTerm(CancellationToken ct)
   {
      var memoryOptions = new MemoryTransportOptions();
      var ep = new MemoryEndPoint($"node-term-regress-{Guid.NewGuid():N}");
      var listener = new MemoryNetworkListener(ep, memoryOptions);
      var transport = new RaftNetworkTransport(listener, []);

      var storage = new InMemoryRaftLogStorage();
      await storage.SetTermAndVoteAsync(5, "node-1", ct);

      var sm = new SnapshotStateMachine();
      var options = new RaftNodeOptions
      {
         NodeId = "node-1",
         Peers = [], // Single node becomes Leader immediately
         ElectionTimeoutMin = TimeSpan.FromMilliseconds(50),
         ElectionTimeoutMax = TimeSpan.FromMilliseconds(100),
         HeartbeatInterval = TimeSpan.FromMilliseconds(20)
      };

      await using var node = new RaftNode(options, storage, sm, transport);
      await node.StartAsync(ct);

      await Task.Delay(120, ct);
      await Assert.That(node.Role).IsEqualTo(RaftRole.Leader);
      var currentTermBefore = node.CurrentTerm;
      await Assert.That(currentTermBefore >= 5UL).IsTrue();

      // Simulate an in-flight task from an earlier term (Term 2) calling StepDownAsync(2)
      var stepDownMethod = typeof(RaftNode).GetMethod("StepDownAsync", BindingFlags.NonPublic | BindingFlags.Instance)!;
      var task = (Task)stepDownMethod.Invoke(node, [2UL, ct])!;
      await task;

      // CurrentTerm must NEVER decrease backwards from >=5 to 2!
      await Assert.That(node.CurrentTerm).IsEqualTo(currentTermBefore);
      await Assert.That(await storage.GetCurrentTermAsync(ct)).IsEqualTo(currentTermBefore);
   }

   [Test]
   [Timeout(5000)]
   public async Task AdvanceCommitIndexAsync_ShouldNotCompletePendingProposalFromEarlierTerm_WhenNewLeaderCommitsDifferentEntryAtSameIndex(CancellationToken ct)
   {
      var memoryOptions = new MemoryTransportOptions();
      var ep = new MemoryEndPoint($"node-proposal-term-mismatch-{Guid.NewGuid():N}");
      var listener = new MemoryNetworkListener(ep, memoryOptions);
      var transport = new RaftNetworkTransport(listener, []);

      var storage = new InMemoryRaftLogStorage();
      await storage.SetCurrentTermAsync(1, ct);

      var sm = new SnapshotStateMachine();
      var options = new RaftNodeOptions
      {
         NodeId = "node-1",
         Peers = ["leader-2"],
         ElectionTimeoutMin = TimeSpan.FromMilliseconds(500),
         ElectionTimeoutMax = TimeSpan.FromMilliseconds(1000),
         HeartbeatInterval = TimeSpan.FromMilliseconds(50)
      };

      await using var node = new RaftNode(options, storage, sm, transport);
      await node.StartAsync(ct);

      // Advance node to Term 2 first (simulating the race where ProposeAsync added to _pendingProposals after StepDownAsync,
      // or an unverified entry from Term 1 was overwritten by Term 2 while its proposal TCS remained in _pendingProposals)
      var clientEp = new MemoryEndPoint($"leader2-sender-{Guid.NewGuid():N}");
      var clientListener = new MemoryNetworkListener(clientEp, memoryOptions);
      await using var clientTransport = new RaftNetworkTransport(clientListener, [
         new RaftPeerEndpoint("node-1", ep, () => new MemoryNetworkClient(memoryOptions))
      ]);

      await clientTransport.AppendEntriesAsync("node-1", new AppendEntriesRequest
      {
         Term = 2,
         LeaderId = "leader-2",
         PrevLogIndex = 0,
         PrevLogTerm = 0,
         LeaderCommitIndex = 0,
         Entries = []
      }, ct);

      // Simulate a pending proposal TCS at index 1 that was created when node-1 was Leader in Term 1
      var pendingField = typeof(RaftNode).GetField("_pendingProposals", BindingFlags.NonPublic | BindingFlags.Instance)!;
      var pendingObj = pendingField.GetValue(node)!;
      var tcs = new TaskCompletionSource<ReadOnlyMemory<byte>>(TaskCreationOptions.RunContinuationsAsynchronously);
      var tryAddMethod = pendingObj.GetType().GetMethod("TryAdd")!;
      tryAddMethod.Invoke(pendingObj, [1UL, tcs]);

      // Now leader-2 in Term 2 replicates and commits a DIFFERENT command at index 1 (Term 2)
      var resp = await clientTransport.AppendEntriesAsync("node-1", new AppendEntriesRequest
      {
         Term = 2,
         LeaderId = "leader-2",
         PrevLogIndex = 0,
         PrevLogTerm = 0,
         LeaderCommitIndex = 1,
         Entries = [new RaftLogEntry(2, 1, "SET k1=OTHER_CLIENT_COMMAND"u8.ToArray())]
      }, ct);

      await Assert.That(resp!.Success).IsTrue();

      // Wait briefly for AdvanceCommitIndexAsync to apply index 1
      await Task.Delay(50, ct);

      // The Term 1 proposal must NOT succeed with the result of Term 2's entry!
      await Assert.That(tcs.Task.IsCompletedSuccessfully).IsFalse();
   }

   [Test]
   [Timeout(5000)]
   public async Task HandleInstallSnapshotAsync_ShouldRecheckLastAppliedInsideApplyLock_AndNotRollbackStateMachine(CancellationToken ct)
   {
      var memoryOptions = new MemoryTransportOptions();
      var ep = new MemoryEndPoint($"node-snap-race-{Guid.NewGuid():N}");
      var listener = new MemoryNetworkListener(ep, memoryOptions);
      var transport = new RaftNetworkTransport(listener, []);

      var storage = new InMemoryRaftLogStorage();
      await storage.SetCurrentTermAsync(1, ct);
      await storage.AppendEntriesAsync([
         new RaftLogEntry(1, 1, "SET k1=v1"u8.ToArray()),
         new RaftLogEntry(1, 2, "SET k2=v2"u8.ToArray()),
         new RaftLogEntry(1, 3, "SET k3=LATEST_APPLIED"u8.ToArray())
      ], ct);

      var sm = new SnapshotStateMachine();
      var options = new RaftNodeOptions
      {
         NodeId = "node-1",
         Peers = ["leader-1"],
         ElectionTimeoutMin = TimeSpan.FromMilliseconds(500),
         ElectionTimeoutMax = TimeSpan.FromMilliseconds(1000),
         HeartbeatInterval = TimeSpan.FromMilliseconds(50)
      };

      await using var node = new RaftNode(options, storage, sm, transport);
      await node.StartAsync(ct);

      // Hold _applyLock to simulate AdvanceCommitIndexAsync currently applying entries 1..3
      var applyLockField = typeof(RaftNode).GetField("_applyLock", BindingFlags.NonPublic | BindingFlags.Instance)!;
      var applyLock = (SemaphoreSlim)applyLockField.GetValue(node)!;
      var commitIndexField = typeof(RaftNode).GetField("_commitIndex", BindingFlags.NonPublic | BindingFlags.Instance)!;
      var lastAppliedField = typeof(RaftNode).GetField("_lastApplied", BindingFlags.NonPublic | BindingFlags.Instance)!;

      await applyLock.WaitAsync(ct);

      var clientEp = new MemoryEndPoint($"leader-snap-sender-{Guid.NewGuid():N}");
      var clientListener = new MemoryNetworkListener(clientEp, memoryOptions);
      await using var clientTransport = new RaftNetworkTransport(clientListener, [
         new RaftPeerEndpoint("node-1", ep, () => new MemoryNetworkClient(memoryOptions))
      ]);

      // Start an InstallSnapshot RPC for LastIncludedIndex = 2 while CommitIndex is still 0 (passes initial check at line 964)
      var olderSnapshotBytes = JsonSerializer.SerializeToUtf8Bytes(new Dictionary<string, string>
      {
         ["k1"] = "v1",
         ["k2"] = "v2"
      });

      var installTask = clientTransport.InstallSnapshotAsync("node-1", new InstallSnapshotRequest
      {
         Term = 1,
         LeaderId = "leader-1",
         LastIncludedIndex = 2,
         LastIncludedTerm = 1,
         Data = olderSnapshotBytes
      }, ct);

      // Give HandleInstallSnapshotAsync time to pass the pre-lock check and wait on _applyLock
      await Task.Delay(50, ct);

      // Simulate AdvanceCommitIndexAsync finishing application up to index 3 before releasing _applyLock
      sm.Store["k1"] = "v1";
      sm.Store["k2"] = "v2";
      sm.Store["k3"] = "LATEST_APPLIED";
      commitIndexField.SetValue(node, 3UL);
      lastAppliedField.SetValue(node, 3UL);

      applyLock.Release();

      var resp = await installTask;
      await Assert.That(resp).IsNotNull();

      // State machine must still contain k3="LATEST_APPLIED" and NOT have been rolled back to index 2!
      await Assert.That(sm.Store.ContainsKey("k3")).IsTrue();
      await Assert.That(sm.Store["k3"]).IsEqualTo("LATEST_APPLIED");
   }

   [Test]
   public async Task FileRaftLogStorage_WhenLogEndsWithPartialCrashedHeader_ShouldTruncateCorruptTailBeforeAppendingNewEntries(CancellationToken ct)
   {
      var tempDir = Path.Combine(Path.GetTempPath(), $"raft_partial_hdr_{Guid.NewGuid():N}");
      Directory.CreateDirectory(tempDir);

      try
      {
         // 1. Write valid entry 1
         await using (var storage1 = new FileRaftLogStorage(tempDir))
         {
            await storage1.AppendEntriesAsync([new RaftLogEntry(1, 1, "entry-1"u8.ToArray())], ct);
         }

         // 2. Simulate a power failure / crash that wrote 8 bytes of a 20-byte header at the end of raft.log
         var logPath = Path.Combine(tempDir, "raft.log");
         await using (var fs = new FileStream(logPath, FileMode.Append, FileAccess.Write, FileShare.ReadWrite))
         {
            await fs.WriteAsync(new byte[] { 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08 }, ct);
            await fs.FlushAsync(ct);
         }

         // 3. Reopen storage and append entry 2
         await using (var storage2 = new FileRaftLogStorage(tempDir))
         {
            await storage2.AppendEntriesAsync([new RaftLogEntry(1, 2, "entry-2"u8.ToArray())], ct);
         }

         // 4. Reopen storage again and verify both entry 1 and entry 2 are intact
         await using (var storage3 = new FileRaftLogStorage(tempDir))
         {
            var lastIdx = await storage3.GetLastLogIndexAsync(ct);
            await Assert.That(lastIdx).IsEqualTo(2UL);

            var e1 = await storage3.GetEntryAsync(1, ct);
            var e2 = await storage3.GetEntryAsync(2, ct);

            await Assert.That(e1).IsNotNull();
            await Assert.That(e2).IsNotNull();
            await Assert.That(Encoding.UTF8.GetString(e1!.Value.Data.Span)).IsEqualTo("entry-1");
            await Assert.That(Encoding.UTF8.GetString(e2!.Value.Data.Span)).IsEqualTo("entry-2");
         }
      }
      finally
      {
         if (Directory.Exists(tempDir))
         {
            Directory.Delete(tempDir, recursive: true);
         }
      }
   }
}
