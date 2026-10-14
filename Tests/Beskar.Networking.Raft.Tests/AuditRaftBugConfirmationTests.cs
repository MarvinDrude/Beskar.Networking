using System.Collections.Concurrent;
using System.Text;
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
public class AuditRaftBugConfirmationTests
{
   [Test]
   [Timeout(5000)]
   public async Task FileRaftLogStorage_OutOfOrderOrConcurrentAppendEntries_ShouldNotCorruptO1IndexLookup(CancellationToken ct)
   {
      var tempDir = Path.Combine(Path.GetTempPath(), $"raft-bug7-{Guid.NewGuid():N}");
      try
      {
         await using var storage = new FileRaftLogStorage(tempDir);

         await storage.AppendEntriesAsync([new RaftLogEntry(1, 1, "entry-1"u8.ToArray())], ct);
         await storage.AppendEntriesAsync([new RaftLogEntry(1, 3, "entry-3"u8.ToArray())], ct);

         var entryAt2 = await storage.GetEntryAsync(2, ct);
         if (entryAt2.HasValue)
         {
            await Assert.That(entryAt2.Value.Index).IsEqualTo(2UL);
         }
      }
      finally
      {
         if (Directory.Exists(tempDir))
         {
            try { Directory.Delete(tempDir, recursive: true); } catch { }
         }
      }
   }

   [Test]
   [Timeout(5000)]
   public async Task RaftNode_SingleUnresponsivePeerInThreeNodeCluster_ShouldElectLeaderAndCommitWithin600Ms(CancellationToken ct)
   {
      var nodeIds = new[] { "node-1", "node-2", "node-3-hung" };
      var memoryOptions = new MemoryTransportOptions();
      var endpoints = nodeIds.ToDictionary(
         id => id,
         id => new MemoryEndPoint($"raft-hung-{id}-{Guid.NewGuid():N}"));

      // Bind node-3's listener without starting a RaftNode on it so ConnectAsync succeeds
      // and RPCs to node-3 wait for RaftNetworkTransport's default 1000ms _rpcTimeout.
      var hungListener = new MemoryNetworkListener(endpoints["node-3-hung"], memoryOptions);
      await hungListener.BindAsync();

      var nodes = new List<RaftNode>();
      for (var i = 0; i < 2; i++)
      {
         var id = nodeIds[i];
         var peers = nodeIds.Where(x => x != id).ToList();
         var peerEndpoints = peers.Select(p => new RaftPeerEndpoint(
            p,
            endpoints[p],
            () => new MemoryNetworkClient(memoryOptions))).ToList();

         var listener = new MemoryNetworkListener(endpoints[id], memoryOptions);
         // Uses default RaftNetworkTransport timeout (1000ms)
         var transport = new RaftNetworkTransport(listener, peerEndpoints);
         var options = new RaftNodeOptions
         {
            NodeId = id,
            Peers = peers,
            ElectionTimeoutMin = TimeSpan.FromMilliseconds(80),
            ElectionTimeoutMax = TimeSpan.FromMilliseconds(150),
            HeartbeatInterval = TimeSpan.FromMilliseconds(25)
         };

         nodes.Add(new RaftNode(options, new InMemoryRaftLogStorage(), new NoOpStateMachine(), transport));
      }

      try
      {
         foreach (var n in nodes) await n.StartAsync(ct);

         // Wait up to 600ms for the 2 healthy nodes (2/3 majority) to elect a leader and commit a proposal
         RaftNode? leader = null;
         var deadline = Environment.TickCount64 + 600;
         while (Environment.TickCount64 < deadline && leader == null)
         {
            leader = nodes.FirstOrDefault(n => n.Role == RaftRole.Leader);
            if (leader == null) await Task.Delay(20, ct);
         }

         await Assert.That(leader).IsNotNull();

         using var proposeCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
         proposeCts.CancelAfter(TimeSpan.FromMilliseconds(400));
         await leader!.ProposeAsync("quorum-write"u8.ToArray(), proposeCts.Token);

         await Assert.That(leader.CommitIndex).IsGreaterThanOrEqualTo(1UL);
      }
      finally
      {
         foreach (var n in nodes) await n.DisposeAsync();
         await hungListener.UnbindAsync();
         await hungListener.DisposeAsync();
      }
   }

   [Test]
   [Timeout(5000)]
   public async Task RaftLogStorage_OutOfOrderOrConcurrentAppendEntries_ShouldNotCorruptO1IndexLookup(CancellationToken ct)
   {
      await using var storage = new InMemoryRaftLogStorage();

      await storage.AppendEntriesAsync([new RaftLogEntry(1, 1, "entry-1"u8.ToArray())], ct);
      await storage.AppendEntriesAsync([new RaftLogEntry(1, 3, "entry-3"u8.ToArray())], ct);

      var entryAt2 = await storage.GetEntryAsync(2, ct);
      if (entryAt2.HasValue)
      {
         await Assert.That(entryAt2.Value.Index).IsEqualTo(2UL);
      }
   }

   [Test]
   [Timeout(5000)]
   public async Task RaftNode_BecomeLeaderInNewTerm_ShouldCommitPriorTermEntryViaNoOpWithoutNewClientProposals(CancellationToken ct)
   {
      var nodeIds = new[] { "node-1", "node-2", "node-3" };
      var memoryOptions = new MemoryTransportOptions();
      var endpoints = nodeIds.ToDictionary(
         id => id,
         id => new MemoryEndPoint($"raft-noop-{id}-{Guid.NewGuid():N}"));

      var storages = new Dictionary<string, InMemoryRaftLogStorage>
      {
         ["node-1"] = new(),
         ["node-2"] = new(),
         ["node-3"] = new()
      };

      // Pre-populate node-1 with an uncommitted entry from Term 1 at Index 1
      await storages["node-1"].SetTermAndVoteAsync(1, "node-1", ct);
      await storages["node-1"].AppendEntriesAsync([new RaftLogEntry(1, 1, "term1-entry"u8.ToArray())], ct);
      await storages["node-2"].SetTermAndVoteAsync(1, null, ct);
      await storages["node-3"].SetTermAndVoteAsync(1, null, ct);

      var nodes = new List<RaftNode>();
      for (var i = 0; i < nodeIds.Length; i++)
      {
         var id = nodeIds[i];
         var peers = nodeIds.Where(x => x != id).ToList();
         var peerEndpoints = peers.Select(p => new RaftPeerEndpoint(
            p,
            endpoints[p],
            () => new MemoryNetworkClient(memoryOptions))).ToList();

         var listener = new MemoryNetworkListener(endpoints[id], memoryOptions);
         var transport = new RaftNetworkTransport(listener, peerEndpoints, TimeSpan.FromMilliseconds(100));
         var options = new RaftNodeOptions
         {
            NodeId = id,
            Peers = peers,
            ElectionTimeoutMin = TimeSpan.FromMilliseconds(60),
            ElectionTimeoutMax = TimeSpan.FromMilliseconds(120),
            HeartbeatInterval = TimeSpan.FromMilliseconds(20)
         };

         nodes.Add(new RaftNode(options, storages[id], new NoOpStateMachine(), transport));
      }

      try
      {
         foreach (var n in nodes) await n.StartAsync(ct);

         var deadline = Environment.TickCount64 + 600;
         while (Environment.TickCount64 < deadline && nodes[0].CommitIndex < 1)
         {
            await Task.Delay(25, ct);
         }

         await Assert.That(nodes[0].Role).IsEqualTo(RaftRole.Leader);
         await Assert.That(nodes[0].CommitIndex).IsGreaterThanOrEqualTo(1UL);
      }
      finally
      {
         foreach (var n in nodes) await n.DisposeAsync();
      }
   }

   private sealed class NoOpStateMachine : IRaftStateMachine
   {
      public ValueTask<ReadOnlyMemory<byte>> ApplyAsync(ReadOnlyMemory<byte> command, ulong logIndex, CancellationToken ct = default)
         => ValueTask.FromResult(command);
   }
}
