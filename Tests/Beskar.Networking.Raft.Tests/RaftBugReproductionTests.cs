using System.Reflection;
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

public class RaftBugReproductionTests
{
   [Test]
   [Timeout(3000)]
   public async Task Bug1_CompactedLeader_FreshFollowerWithNextIndex1_FailsToInstallSnapshot(CancellationToken ct)
   {
      var memoryOptions = new MemoryTransportOptions();
      var epLeader = new MemoryEndPoint($"leader-bug1-{Guid.NewGuid():N}");
      var epFollower = new MemoryEndPoint($"follower-bug1-{Guid.NewGuid():N}");

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
         ElectionTimeoutMin = TimeSpan.FromMilliseconds(300),
         ElectionTimeoutMax = TimeSpan.FromMilliseconds(600),
         HeartbeatInterval = TimeSpan.FromMilliseconds(20)
      };

      await using var leader = new RaftNode(optionsLeader, storageLeader, smLeader, transportLeader);
      await using var follower = new RaftNode(optionsFollower, storageFollower, smFollower, transportFollower);

      await leader.StartAsync(ct);
      await follower.StartAsync(ct);

      // Wait for leader election
      await Task.Delay(150, ct);
      await Assert.That(leader.Role).IsEqualTo(RaftRole.Leader);

      // Propose entries 1..5
      for (var i = 1; i <= 5; i++)
      {
         await leader.ProposeAsync(Encoding.UTF8.GetBytes($"SET key{i}=val{i}"), ct);
      }

      // Compact leader log up to index 5
      await storageLeader.CompactPrefixAsync(5, leader.CurrentTerm, ct);

      // Clear follower storage and state machine to simulate a wiped/fresh node reconnecting with NextIndex = 1
      await storageFollower.TruncateLogAsync(1, ct);
      await storageFollower.CompactPrefixAsync(0, 0, ct);
      smFollower.Store.Clear();

      // Reset leader's tracker for follower-1 to NextIndex = 1, MatchIndex = 0
      var trackersField = typeof(RaftNode).GetField("_peerTrackers", BindingFlags.NonPublic | BindingFlags.Instance)!;
      var trackers = (System.Collections.Concurrent.ConcurrentDictionary<string, Internal.RaftPeerTracker>)trackersField.GetValue(leader)!;
      if (trackers.TryGetValue("follower-1", out var tracker))
      {
         tracker.NextIndex = 1;
         tracker.MatchIndex = 0;
      }

      // Wait for leader heartbeat / replication to catch follower up
      var deadline = Environment.TickCount64 + 1500;
      while (Environment.TickCount64 < deadline && !ct.IsCancellationRequested)
      {
         if (smFollower.Store.ContainsKey("key5"))
         {
            break;
         }
         await Task.Delay(50, ct);
      }

      await Assert.That(smFollower.Store.ContainsKey("key5")).IsTrue();
      await Assert.That(smFollower.Store["key5"]).IsEqualTo("val5");
   }

   [Test]
   [Timeout(3000)]
   public async Task Bug2_ConflictingTermAtSameLengthLog_RejectionBacktrackInfiniteLoop(CancellationToken ct)
   {
      var memoryOptions = new MemoryTransportOptions();
      var epLeader = new MemoryEndPoint($"leader-bug2-{Guid.NewGuid():N}");
      var epFollower = new MemoryEndPoint($"follower-bug2-{Guid.NewGuid():N}");

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

      // Pre-populate follower log with 3 entries from Term 1
      await storageFollower.SetCurrentTermAsync(1, ct);
      await storageFollower.AppendEntriesAsync([
         new RaftLogEntry(1, 1, "SET k1=v1"u8.ToArray()),
         new RaftLogEntry(1, 2, "SET k2=OLD"u8.ToArray()),
         new RaftLogEntry(1, 3, "SET k3=OLD"u8.ToArray())
      ], ct);

      // Pre-populate leader with entry 1 in Term 1, and entries 2 and 3 in Term 2
      await storageLeader.SetCurrentTermAsync(2, ct);
      await storageLeader.AppendEntriesAsync([
         new RaftLogEntry(1, 1, "SET k1=v1"u8.ToArray()),
         new RaftLogEntry(2, 2, "SET k2=NEW"u8.ToArray()),
         new RaftLogEntry(2, 3, "SET k3=NEW"u8.ToArray())
      ], ct);

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
         ElectionTimeoutMin = TimeSpan.FromMilliseconds(500),
         ElectionTimeoutMax = TimeSpan.FromMilliseconds(1000),
         HeartbeatInterval = TimeSpan.FromMilliseconds(20)
      };

      await using var leader = new RaftNode(optionsLeader, storageLeader, smLeader, transportLeader);
      await using var follower = new RaftNode(optionsFollower, storageFollower, smFollower, transportFollower);

      await leader.StartAsync(ct);
      await follower.StartAsync(ct);

      // Wait for leader to establish leadership in Term 3
      await Task.Delay(150, ct);

      // Leader proposes new entry 4
      await leader.ProposeAsync("SET k4=v4"u8.ToArray(), ct);

      // Wait for follower to converge
      var deadline = Environment.TickCount64 + 1500;
      while (Environment.TickCount64 < deadline && !ct.IsCancellationRequested)
      {
         if (smFollower.Store.ContainsKey("k4") && smFollower.Store.GetValueOrDefault("k3") == "NEW")
         {
            break;
         }
         await Task.Delay(50, ct);
      }

      await Assert.That(smFollower.Store.GetValueOrDefault("k3")).IsEqualTo("NEW");
      await Assert.That(smFollower.Store.GetValueOrDefault("k4")).IsEqualTo("v4");
   }

   [Test]
   public async Task Bug3_AdvanceCommitIndex_StarvesUnappliedEntries_WhenNewCommitLessThanOrEqualToCommitIndex(CancellationToken ct)
   {
      var memoryOptions = new MemoryTransportOptions();
      var ep = new MemoryEndPoint($"node-bug3-{Guid.NewGuid():N}");
      var listener = new MemoryNetworkListener(ep, memoryOptions);
      var transport = new RaftNetworkTransport(listener, []);

      var storage = new InMemoryRaftLogStorage();
      await storage.AppendEntriesAsync([
         new RaftLogEntry(1, 1, "SET k1=v1"u8.ToArray()),
         new RaftLogEntry(1, 2, "SET k2=v2"u8.ToArray()),
         new RaftLogEntry(1, 3, "SET k3=v3"u8.ToArray())
      ], ct);

      var sm = new SnapshotStateMachine();

      var options = new RaftNodeOptions
      {
         NodeId = "node-1",
         Peers = ["leader-remote"],
         ElectionTimeoutMin = TimeSpan.FromMilliseconds(500),
         ElectionTimeoutMax = TimeSpan.FromMilliseconds(1000),
         HeartbeatInterval = TimeSpan.FromMilliseconds(50)
      };

      await using var node = new RaftNode(options, storage, sm, transport);
      await node.StartAsync(ct);

      // Simulate client transport connecting to node-1
      var clientListenerEndpoint = new MemoryEndPoint($"client-ep-bug3-{Guid.NewGuid():N}");
      var clientListener = new MemoryNetworkListener(clientListenerEndpoint, memoryOptions);
      var clientPeerEndpoints = new List<RaftPeerEndpoint>
      {
         new("node-1", ep, () => new MemoryNetworkClient(memoryOptions))
      };
      await using var clientTransport = new RaftNetworkTransport(clientListener, clientPeerEndpoints);

      // Manually set _commitIndex = 3 but _lastApplied = 0 on node-1
      var commitField = typeof(RaftNode).GetField("_commitIndex", BindingFlags.NonPublic | BindingFlags.Instance)!;
      commitField.SetValue(node, 3UL);

      // Now send AppendEntries with LeaderCommitIndex = 3
      var req = new AppendEntriesRequest
      {
         Term = 1,
         LeaderId = "leader-remote",
         PrevLogIndex = 0,
         PrevLogTerm = 0,
         LeaderCommitIndex = 3,
         Entries = [
            new RaftLogEntry(1, 1, "SET k1=v1"u8.ToArray()),
            new RaftLogEntry(1, 2, "SET k2=v2"u8.ToArray()),
            new RaftLogEntry(1, 3, "SET k3=v3"u8.ToArray())
         ]
      };
      var resp = await clientTransport.AppendEntriesAsync("node-1", req, ct);
      await Assert.That(resp!.Success).IsTrue();

      await Assert.That(node.LastApplied).IsEqualTo(3UL);
      await Assert.That(sm.Store.Count).IsEqualTo(3);
   }

   [Test]
   public async Task Bug4_HandleInstallSnapshotAsync_RacesWithStateMachineApply_WithoutApplyLock(CancellationToken ct)
   {
      var memoryOptions = new MemoryTransportOptions();
      var ep = new MemoryEndPoint($"node-bug4-{Guid.NewGuid():N}");
      var listener = new MemoryNetworkListener(ep, memoryOptions);
      var transport = new RaftNetworkTransport(listener, []);

      var storage = new InMemoryRaftLogStorage();
      await storage.AppendEntriesAsync([
         new RaftLogEntry(1, 1, "SET k1=v1"u8.ToArray())
      ], ct);

      var sm = new ConcurrencyDetectingStateMachine();

      var options = new RaftNodeOptions
      {
         NodeId = "node-1",
         Peers = ["leader-remote"],
         ElectionTimeoutMin = TimeSpan.FromMilliseconds(500),
         ElectionTimeoutMax = TimeSpan.FromMilliseconds(1000),
         HeartbeatInterval = TimeSpan.FromMilliseconds(50)
      };

      await using var node = new RaftNode(options, storage, sm, transport);
      await node.StartAsync(ct);

      var clientListenerEndpoint = new MemoryEndPoint($"client-ep-bug4-{Guid.NewGuid():N}");
      var clientListener = new MemoryNetworkListener(clientListenerEndpoint, memoryOptions);
      var clientPeerEndpoints = new List<RaftPeerEndpoint>
      {
         new("node-1", ep, () => new MemoryNetworkClient(memoryOptions))
      };
      await using var clientTransport = new RaftNetworkTransport(clientListener, clientPeerEndpoints);

      var applyLockField = typeof(RaftNode).GetField("_applyLock", BindingFlags.NonPublic | BindingFlags.Instance)!;
      var applyLock = (SemaphoreSlim)applyLockField.GetValue(node)!;

      // Lock _applyLock
      await applyLock.WaitAsync(ct);
      try
      {
         // Send InstallSnapshot while _applyLock is held
         var snapshotData = System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(new Dictionary<string, string> { ["k1"] = "snap" });
         var snapReq = new InstallSnapshotRequest
         {
            Term = 1,
            LeaderId = "leader-remote",
            LastIncludedIndex = 1,
            LastIncludedTerm = 1,
            Data = snapshotData
         };

         var snapTask = clientTransport.InstallSnapshotAsync("node-1", snapReq, ct).AsTask();
         var delayTask = Task.Delay(100, ct);
         var completed = await Task.WhenAny(snapTask, delayTask);

         await Assert.That(completed).IsEqualTo(delayTask);
      }
      finally
      {
         applyLock.Release();
      }
   }

   private sealed class ConcurrencyDetectingStateMachine : IRaftStateMachine
   {
      private int _activeOperations;
      public bool ConcurrencyDetected { get; private set; }

      public async ValueTask<ReadOnlyMemory<byte>> ApplyAsync(ReadOnlyMemory<byte> command, ulong logIndex, CancellationToken ct = default)
      {
         if (Interlocked.Increment(ref _activeOperations) > 1)
         {
            ConcurrencyDetected = true;
         }
         await Task.Delay(50, ct);
         Interlocked.Decrement(ref _activeOperations);
         return command;
      }

      public async ValueTask RestoreSnapshotAsync(ReadOnlyMemory<byte> snapshot, ulong lastIncludedIndex, ulong lastIncludedTerm, CancellationToken ct = default)
      {
         if (Interlocked.Increment(ref _activeOperations) > 1)
         {
            ConcurrencyDetected = true;
         }
         await Task.Delay(50, ct);
         Interlocked.Decrement(ref _activeOperations);
      }
   }
}
