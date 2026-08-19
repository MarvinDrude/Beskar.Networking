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

public class RaftReplicationTests
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
         HeartbeatInterval = TimeSpan.FromMilliseconds(20),
         MaxAppendEntriesBatchSize = 10
      };

      var s = storage ?? new InMemoryRaftLogStorage();
      var sm = new KeyValueTestStateMachine();
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
   public async Task ProposeAsync_OnNonLeader_ThrowsInvalidOperationException()
   {
      var (node, _, clientTransport) = CreateNodeAndSender("follower-1", ["node-2"]);
      await using var _ = clientTransport;
      await using var __ = node;

      await node.StartAsync();

      await Assert.That(async () => await node.ProposeAsync("cmd"u8.ToArray()))
         .Throws<InvalidOperationException>();
   }

   [Test]
   public async Task SingleNode_ProposeAsync_CommitsAndAppliesImmediately()
   {
      var (node, storage, clientTransport) = CreateNodeAndSender("node-1", []);
      await using var _ = clientTransport;
      await using var __ = node;

      await node.StartAsync();
      await Task.Delay(150);

      var result = await node.ProposeAsync("SET name=Alice"u8.ToArray());

      await Assert.That(Encoding.UTF8.GetString(result.Span)).IsEqualTo("OK");
      await Assert.That(node.CommitIndex).IsEqualTo(1UL);
      await Assert.That(node.LastApplied).IsEqualTo(1UL);

      var entry = await storage.GetEntryAsync(1);
      await Assert.That(entry.HasValue).IsTrue();
      await Assert.That(Encoding.UTF8.GetString(entry!.Value.Data.Span)).IsEqualTo("SET name=Alice");
   }

   [Test]
   public async Task SingleNode_MultipleProposals_MaintainSequentialIndexes()
   {
      var (node, storage, clientTransport) = CreateNodeAndSender("node-1", []);
      await using var _ = clientTransport;
      await using var __ = node;

      await node.StartAsync();
      await Task.Delay(150);

      for (var i = 1; i <= 10; i++)
      {
         var res = await node.ProposeAsync(Encoding.UTF8.GetBytes($"SET k{i}=v{i}"));
         await Assert.That(Encoding.UTF8.GetString(res.Span)).IsEqualTo("OK");
         await Assert.That(node.CommitIndex).IsEqualTo((ulong)i);
      }

      await Assert.That(await storage.GetLastLogIndexAsync()).IsEqualTo(10UL);
   }

   [Test]
   public async Task HandleAppendEntries_ValidMatchingIndex_AcceptsEntries()
   {
      var (node, _, clientTransport) = CreateNodeAndSender("follower-1", ["leader-1"]);
      await using var _ = clientTransport;
      await using var __ = node;

      await node.StartAsync();

      var request = new AppendEntriesRequest
      {
         Term = 1,
         LeaderId = "leader-1",
         PrevLogIndex = 0,
         PrevLogTerm = 0,
         LeaderCommitIndex = 1,
         Entries = [new RaftLogEntry(1, 1, "SET key1=val1"u8.ToArray())]
      };

      var response = await clientTransport.AppendEntriesAsync("follower-1", request);

      await Assert.That(response).IsNotNull();
      await Assert.That(response!.Success).IsTrue();
      await Assert.That(response.MatchIndex).IsEqualTo(1UL);
      await Assert.That(node.CommitIndex).IsEqualTo(1UL);
      await Assert.That(node.LastApplied).IsEqualTo(1UL);
   }

   [Test]
   public async Task HandleAppendEntries_MissingPrevLogIndex_Rejects()
   {
      var (node, _, clientTransport) = CreateNodeAndSender("follower-1", ["leader-1"]);
      await using var _ = clientTransport;
      await using var __ = node;

      await node.StartAsync();

      var request = new AppendEntriesRequest
      {
         Term = 1,
         LeaderId = "leader-1",
         PrevLogIndex = 5,
         PrevLogTerm = 1,
         LeaderCommitIndex = 5,
         Entries = [new RaftLogEntry(1, 6, "SET key6=val6"u8.ToArray())]
      };

      var response = await clientTransport.AppendEntriesAsync("follower-1", request);

      await Assert.That(response).IsNotNull();
      await Assert.That(response!.Success).IsFalse();
      await Assert.That(response.MatchIndex).IsEqualTo(0UL);
   }

   [Test]
   public async Task HandleAppendEntries_ConflictingEntry_TruncatesAndOverwrites()
   {
      var storage = new InMemoryRaftLogStorage();
      await storage.AppendEntriesAsync([
         new RaftLogEntry(1, 1, "SET k1=v1"u8.ToArray()),
         new RaftLogEntry(1, 2, "SET k2=OLD"u8.ToArray())
      ]);

      var (node, _, clientTransport) = CreateNodeAndSender("follower-1", ["leader-1"], storage);
      await using var _ = clientTransport;
      await using var __ = node;

      await node.StartAsync();

      var request = new AppendEntriesRequest
      {
         Term = 2,
         LeaderId = "leader-1",
         PrevLogIndex = 1,
         PrevLogTerm = 1,
         LeaderCommitIndex = 2,
         Entries = [new RaftLogEntry(2, 2, "SET k2=NEW"u8.ToArray())]
      };

      var response = await clientTransport.AppendEntriesAsync("follower-1", request);

      await Assert.That(response!.Success).IsTrue();
      await Assert.That(response.MatchIndex).IsEqualTo(2UL);

      var entry2 = await storage.GetEntryAsync(2);
      await Assert.That(entry2.HasValue).IsTrue();
      await Assert.That(entry2!.Value.Term).IsEqualTo(2UL);
      await Assert.That(Encoding.UTF8.GetString(entry2.Value.Data.Span)).IsEqualTo("SET k2=NEW");
   }

   [Test]
   public async Task StopAsync_CancelsPendingProposals()
   {
      var (node, _, clientTransport) = CreateNodeAndSender("leader-1", ["peer-1", "peer-2"]);
      await using var _ = clientTransport;

      await node.StartAsync();
      await Task.Delay(150);

      if (node.Role == RaftRole.Leader)
      {
         var proposalTask = node.ProposeAsync("cmd"u8.ToArray());
         await node.StopAsync();

         await Assert.That(async () => await proposalTask).Throws<TaskCanceledException>();
      }
   }

   [Test]
   public async Task ProposeAsync_CancelledByToken_ThrowsTaskCanceledException()
   {
      var (node, _, clientTransport) = CreateNodeAndSender("leader-1", ["peer-1"]);
      await using var _ = clientTransport;
      await using var __ = node;

      await node.StartAsync();
      await Task.Delay(150);

      if (node.Role == RaftRole.Leader)
      {
         using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(50));
         await Assert.That(async () => await node.ProposeAsync("cmd"u8.ToArray(), cts.Token))
            .Throws<TaskCanceledException>();
      }
   }
}

internal sealed class KeyValueTestStateMachine : IRaftStateMachine
{
   public Dictionary<string, string> Store { get; } = new();

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
}
