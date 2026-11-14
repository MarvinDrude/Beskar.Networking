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

   [Test]
   public async Task LeaderStepDown_PendingProposalsFaultImmediately()
   {
      var (node, _, clientTransport) = CreateNodeAndSender("leader-1", ["peer-1", "peer-2"]);
      await using var _ = clientTransport;
      await using var __ = node;

      await node.StartAsync();
      await Task.Delay(150);

      if (node.Role == RaftRole.Leader)
      {
         var proposalTask = node.ProposeAsync("cmd-to-fail"u8.ToArray());

         // Send higher term AppendEntries from another leader to force step-down
         var higherTermReq = new AppendEntriesRequest
         {
            Term = 50,
            LeaderId = "peer-1",
            PrevLogIndex = 0,
            PrevLogTerm = 0,
            LeaderCommitIndex = 0,
            Entries = []
         };
         var resp = await clientTransport.AppendEntriesAsync("leader-1", higherTermReq);
         await Assert.That(resp!.Success).IsTrue();
         await Assert.That(node.Role).IsEqualTo(RaftRole.Follower);

         // Proposal task must fault immediately with InvalidOperationException
         await Assert.That(async () => await proposalTask)
            .Throws<InvalidOperationException>();
      }
   }

   [Test]
   public async Task AppendEntries_ReplicatedMatchIndexAccuratelyReflectsRequestEntries()
   {
      var storage = new InMemoryRaftLogStorage();
      // Pre-populate follower log with 10 old entries
      var oldEntries = new List<RaftLogEntry>();
      for (ulong i = 1; i <= 10; i++)
         oldEntries.Add(new RaftLogEntry(1, i, Encoding.UTF8.GetBytes($"OLD_{i}")));
      await storage.AppendEntriesAsync(oldEntries);

      var (node, _, clientTransport) = CreateNodeAndSender("follower-1", ["leader-1"], storage);
      await using var _ = clientTransport;
      await using var __ = node;

      await node.StartAsync();

      // Leader in term 2 sends entries 1..3
      var request = new AppendEntriesRequest
      {
         Term = 2,
         LeaderId = "leader-1",
         PrevLogIndex = 0,
         PrevLogTerm = 0,
         LeaderCommitIndex = 3,
         Entries =
         [
            new RaftLogEntry(2, 1, "NEW_1"u8.ToArray()),
            new RaftLogEntry(2, 2, "NEW_2"u8.ToArray()),
            new RaftLogEntry(2, 3, "NEW_3"u8.ToArray())
         ]
      };

      var response = await clientTransport.AppendEntriesAsync("follower-1", request);

      await Assert.That(response!.Success).IsTrue();
      // Response MatchIndex must be 3 (the end of leader's replicated batch), NOT 10 (follower's old trailing uncommitted index)!
      await Assert.That(response.MatchIndex).IsEqualTo(3UL);
   }

   [Test]
   public async Task AdvanceCommitIndexAsync_ConcurrentCalls_ApplyDeterministically()
   {
      var (node, storage, clientTransport) = CreateNodeAndSender("node-1", []);
      await using var _ = clientTransport;
      await using var __ = node;

      await node.StartAsync();
      await Task.Delay(150);

      var tasks = Enumerable.Range(1, 30).Select(i => Task.Run(async () =>
      {
         var res = await node.ProposeAsync(Encoding.UTF8.GetBytes($"SET concurrent_{i}={i}"));
         await Assert.That(Encoding.UTF8.GetString(res.Span)).IsEqualTo("OK");
      })).ToList();

      await Task.WhenAll(tasks);

      await Assert.That(node.CommitIndex).IsEqualTo(30UL);
      await Assert.That(node.LastApplied).IsEqualTo(30UL);
      await Assert.That(await storage.GetLastLogIndexAsync()).IsEqualTo(30UL);
   }

   [Test]
   [Timeout(5000)]
   public async Task Cluster_EmptyCommandProposal_AppliesDeterministicallyAcrossAllNodes(CancellationToken ct)
   {
      var memoryOptions = new MemoryTransportOptions();
      var ep1 = new MemoryEndPoint($"empty-cmd-1-{Guid.NewGuid():N}");
      var ep2 = new MemoryEndPoint($"empty-cmd-2-{Guid.NewGuid():N}");

      var sm1 = new CommandRecordingStateMachine();
      var sm2 = new CommandRecordingStateMachine();

      var transport1 = new RaftNetworkTransport(
         new MemoryNetworkListener(ep1, memoryOptions),
         [new RaftPeerEndpoint("node-2", ep2, () => new MemoryNetworkClient(memoryOptions))]);

      var transport2 = new RaftNetworkTransport(
         new MemoryNetworkListener(ep2, memoryOptions),
         [new RaftPeerEndpoint("node-1", ep1, () => new MemoryNetworkClient(memoryOptions))]);

      await using var node1 = new RaftNode(
         new RaftNodeOptions
         {
            NodeId = "node-1",
            Peers = ["node-2"],
            ElectionTimeoutMin = TimeSpan.FromMilliseconds(40),
            ElectionTimeoutMax = TimeSpan.FromMilliseconds(70),
            HeartbeatInterval = TimeSpan.FromMilliseconds(15),
            EnablePreVote = false
         },
         new InMemoryRaftLogStorage(),
         sm1,
         transport1);

      await using var node2 = new RaftNode(
         new RaftNodeOptions
         {
            NodeId = "node-2",
            Peers = ["node-1"],
            ElectionTimeoutMin = TimeSpan.FromMilliseconds(500),
            ElectionTimeoutMax = TimeSpan.FromMilliseconds(800),
            HeartbeatInterval = TimeSpan.FromMilliseconds(15),
            EnablePreVote = false
         },
         new InMemoryRaftLogStorage(),
         sm2,
         transport2);

      await node2.StartAsync(ct);
      await node1.StartAsync(ct);

      // Wait for node-1 to become leader
      var deadline = DateTime.UtcNow.AddSeconds(2);
      while (node1.Role != RaftRole.Leader && DateTime.UtcNow < deadline) await Task.Delay(15, ct);
      await Assert.That(node1.Role).IsEqualTo(RaftRole.Leader);

      // Propose an empty client command followed by a marker command
      await node1.ProposeAsync(ReadOnlyMemory<byte>.Empty, ct);
      await node1.ProposeAsync("MARKER"u8.ToArray(), ct);

      // Wait until follower applies MARKER (meaning all preceding entries including Empty have been processed)
      deadline = DateTime.UtcNow.AddSeconds(2);
      while (!sm2.AppliedCommands.Any(c => Encoding.UTF8.GetString(c) == "MARKER") && DateTime.UtcNow < deadline)
         await Task.Delay(15, ct);

      await Assert.That(sm1.AppliedCommands.Count(c => c.Length == 0)).IsEqualTo(1);
      await Assert.That(sm2.AppliedCommands.Count(c => c.Length == 0)).IsEqualTo(1);
   }

   [Test]
   [Timeout(5000)]
   public async Task SingleNode_RestartWithExistingLog_CommitsAndAppliesEntriesOnElection(CancellationToken ct)
   {
      var storage = new InMemoryRaftLogStorage();
      await storage.SetTermAndVoteAsync(1, "node-1", ct);
      await storage.AppendEntriesAsync([
         new RaftLogEntry(1, 1, "cmd-1"u8.ToArray()),
         new RaftLogEntry(1, 2, "cmd-2"u8.ToArray())
      ], ct);

      var sm = new CommandRecordingStateMachine();
      var memoryOptions = new MemoryTransportOptions();
      var ep = new MemoryEndPoint($"single-restart-{Guid.NewGuid():N}");
      var transport = new RaftNetworkTransport(new MemoryNetworkListener(ep, memoryOptions), []);

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
         sm,
         transport);

      await node.StartAsync(ct);

      var deadline = DateTime.UtcNow.AddSeconds(2);
      while (node.Role != RaftRole.Leader && DateTime.UtcNow < deadline) await Task.Delay(15, ct);
      await Assert.That(node.Role).IsEqualTo(RaftRole.Leader);

      await Task.Delay(150, ct);

      await Assert.That(node.CommitIndex).IsGreaterThanOrEqualTo(2UL);
      await Assert.That(node.LastApplied).IsGreaterThanOrEqualTo(2UL);
      await Assert.That(sm.AppliedCommands.Count).IsGreaterThanOrEqualTo(2);
   }

   [Test]
   [Timeout(5000)]
   public async Task RaftNode_StoppedNode_IgnoresIncomingRpcs(CancellationToken ct)
   {
      var memoryOptions = new MemoryTransportOptions();
      var ep = new MemoryEndPoint($"stopped-rpc-{Guid.NewGuid():N}");
      var innerTransport = new RaftNetworkTransport(new MemoryNetworkListener(ep, memoryOptions), []);
      var capturingTransport = new HandlerCapturingTransport(innerTransport);
      var storage = new InMemoryRaftLogStorage();

      await using var node = new RaftNode(
         new RaftNodeOptions
         {
            NodeId = "node-1",
            Peers = ["peer-1"],
            ElectionTimeoutMin = TimeSpan.FromMilliseconds(50),
            ElectionTimeoutMax = TimeSpan.FromMilliseconds(100),
            HeartbeatInterval = TimeSpan.FromMilliseconds(20),
            EnablePreVote = false
         },
         storage,
         new CommandRecordingStateMachine(),
         capturingTransport);

      await node.StartAsync(ct);
      await node.StopAsync(ct);
      await Assert.That(node.Role).IsEqualTo(RaftRole.Stopped);
      var capturedHandler = capturingTransport.Handler;
      await Assert.That(capturedHandler is not null).IsTrue();

      var termBefore = node.CurrentTerm;

      RequestVoteResponse? voteRespPayload = null;
      try
      {
         var voteResp = await capturedHandler!(new RaftRpcRequest(
            RaftMessageType.RequestVote,
            new RequestVoteRequest
            {
               Term = termBefore + 10,
               CandidateId = "peer-1",
               LastLogIndex = 0,
               LastLogTerm = 0
            }));
         voteRespPayload = voteResp.Payload as RequestVoteResponse;
      }
      catch (Exception ex) when (ex is OperationCanceledException or ObjectDisposedException
                                    or InvalidOperationException)
      {
         // Throwing when stopped is acceptable rejection
      }

      if (voteRespPayload is not null) await Assert.That(voteRespPayload.VoteGranted).IsFalse();

      AppendEntriesResponse? appendRespPayload = null;
      try
      {
         var appendResp = await capturedHandler!(new RaftRpcRequest(
            RaftMessageType.AppendEntries,
            new AppendEntriesRequest
            {
               Term = termBefore + 10,
               LeaderId = "peer-1",
               Entries = [new RaftLogEntry(termBefore + 10, 1, "x"u8.ToArray())]
            }));
         appendRespPayload = appendResp.Payload as AppendEntriesResponse;
      }
      catch (Exception ex) when (ex is OperationCanceledException or ObjectDisposedException
                                    or InvalidOperationException)
      {
      }

      if (appendRespPayload is not null) await Assert.That(appendRespPayload.Success).IsFalse();

      try
      {
         await capturedHandler!(new RaftRpcRequest(
            RaftMessageType.InstallSnapshot,
            new InstallSnapshotRequest
            {
               Term = termBefore + 10,
               LeaderId = "peer-1",
               LastIncludedIndex = 5,
               LastIncludedTerm = termBefore + 10,
               Data = "snap"u8.ToArray()
            }));
      }
      catch (Exception ex) when (ex is OperationCanceledException or ObjectDisposedException
                                    or InvalidOperationException)
      {
      }

      await Assert.That(node.Role).IsEqualTo(RaftRole.Stopped);
      await Assert.That(node.CurrentTerm).IsEqualTo(termBefore);
      await Assert.That(await storage.GetVotedForAsync(ct)).IsNull();
      await Assert.That(await storage.GetLastLogIndexAsync(ct)).IsEqualTo(0UL);
      await Assert.That(await storage.GetCompactedUntilIndexAsync(ct)).IsEqualTo(0UL);
   }

   [Test]
   [Timeout(5000)]
   public async Task SingleNode_ProposeCancelledDuringApply_AppliesCommittedEntryWithoutLeavingLag(CancellationToken ct)
   {
      var applyEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
      var sm = new CancellableDelayStateMachine(applyEntered, TimeSpan.FromMilliseconds(80));
      var memoryOptions = new MemoryTransportOptions();
      var ep = new MemoryEndPoint($"cancel-apply-{Guid.NewGuid():N}");
      var transport = new RaftNetworkTransport(new MemoryNetworkListener(ep, memoryOptions), []);

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
         sm,
         transport);

      await node.StartAsync(ct);

      var deadline = DateTime.UtcNow.AddSeconds(2);
      while (node.Role != RaftRole.Leader && DateTime.UtcNow < deadline) await Task.Delay(15, ct);
      await Assert.That(node.Role).IsEqualTo(RaftRole.Leader);

      using var proposeCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
      var proposeTask = node.ProposeAsync("cmd-1"u8.ToArray(), proposeCts.Token).AsTask();

      await applyEntered.Task.WaitAsync(ct);
      proposeCts.Cancel();

      var cancelled = false;
      try
      {
         await proposeTask;
      }
      catch (OperationCanceledException)
      {
         cancelled = true;
      }

      await Assert.That(cancelled).IsTrue();

      // Wait past the 80ms apply delay so the node's own apply path finishes cleanly
      await Task.Delay(150, ct);

      await Assert.That(node.LastApplied).IsEqualTo(node.CommitIndex);
      await Assert.That(sm.CompletedApplyCount).IsEqualTo(1);
   }
}

internal sealed class CommandRecordingStateMachine : IRaftStateMachine
{
   private readonly List<byte[]> _applied = [];

   public IReadOnlyList<byte[]> AppliedCommands
   {
      get
      {
         lock (_applied)
         {
            return _applied.ToList();
         }
      }
   }

   public ValueTask<ReadOnlyMemory<byte>> ApplyAsync(ReadOnlyMemory<byte> command, ulong logIndex,
      CancellationToken ct = default)
   {
      lock (_applied)
      {
         _applied.Add(command.ToArray());
      }

      return ValueTask.FromResult<ReadOnlyMemory<byte>>("OK"u8.ToArray());
   }
}

internal sealed class CancellableDelayStateMachine(
   TaskCompletionSource applyEntered,
   TimeSpan delay) : IRaftStateMachine
{
   private int _completed;
   public int CompletedApplyCount => Volatile.Read(ref _completed);

   public async ValueTask<ReadOnlyMemory<byte>> ApplyAsync(ReadOnlyMemory<byte> command, ulong logIndex,
      CancellationToken ct = default)
   {
      applyEntered.TrySetResult();
      await Task.Delay(delay, ct);
      Interlocked.Increment(ref _completed);
      return "OK"u8.ToArray();
   }
}

internal sealed class HandlerCapturingTransport(IRaftTransport inner) : IRaftTransport
{
   public Func<RaftRpcRequest, ValueTask<RaftRpcResponse>>? Handler { get; private set; }

   public ValueTask StartAsync(Func<RaftRpcRequest, ValueTask<RaftRpcResponse>> rpcHandler,
      CancellationToken ct = default)
   {
      Handler = rpcHandler;
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
      return inner.DisposeAsync();
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
