using System.Text;
using System.Text.Json;
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
