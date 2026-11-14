using System.Collections.Concurrent;
using System.IO.Pipelines;
using System.Net;
using System.Text;
using System.Text.Json;
using Beskar.Memory.Results;
using Beskar.Networking.Abstractions.Enums;
using Beskar.Networking.Abstractions.Errors;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Abstractions.Models;
using Beskar.Networking.Abstractions.Threading;
using Beskar.Networking.Raft.Enums;
using Beskar.Networking.Raft.Options;
using Beskar.Networking.Raft.Protocol.Messages;
using Beskar.Networking.Raft.Protocol.Models;
using Beskar.Networking.Raft.StateMachine;
using Beskar.Networking.Raft.Storage;
using Beskar.Networking.Raft.Transport;
using Beskar.Networking.Transports.Memory;

namespace Beskar.Networking.Raft.Tests;

[NotInParallel]
public class RaftFailoverAndLifecycleTests
{
   [Test]
   [Timeout(10000)]
   public async Task LeaderFailover_WithUncommittedPriorTermEntry_AppliesAllEntriesOnJsonStateMachine(CancellationToken ct)
   {
      var nodeIds = new[] { "node-1", "node-2", "node-3" };
      var memoryOptions = new MemoryTransportOptions();
      var endpoints = nodeIds.ToDictionary(
         id => id,
         id => new MemoryEndPoint($"raft-json-failover-{id}-{Guid.NewGuid():N}"));

      var storages = new Dictionary<string, InMemoryRaftLogStorage>
      {
         ["node-1"] = new(),
         ["node-2"] = new(),
         ["node-3"] = new()
      };

      var cmd1 = JsonSerializer.SerializeToUtf8Bytes(new KvCommand("PUT", "k1", "v1"));
      await storages["node-1"].SetTermAndVoteAsync(1, "node-1", ct);
      await storages["node-1"].AppendEntriesAsync([new RaftLogEntry(1, 1, cmd1)], ct);
      await storages["node-2"].SetTermAndVoteAsync(1, null, ct);
      await storages["node-3"].SetTermAndVoteAsync(1, null, ct);

      var nodes = new List<RaftNode>();
      var stateMachines = new Dictionary<string, StrictJsonKvStateMachine>();

      for (var i = 0; i < nodeIds.Length; i++)
      {
         var id = nodeIds[i];
         var peers = nodeIds.Where(x => x != id).ToList();
         var peerEndpoints = peers.Select(p => new RaftPeerEndpoint(
            p,
            endpoints[p],
            () => new MemoryNetworkClient(memoryOptions))).ToList();

         var listener = new MemoryNetworkListener(endpoints[id], memoryOptions);
         var transport = new RaftNetworkTransport(listener, peerEndpoints, TimeSpan.FromMilliseconds(150));
         var options = new RaftNodeOptions
         {
            NodeId = id,
            Peers = peers,
            ElectionTimeoutMin = TimeSpan.FromMilliseconds(55),
            ElectionTimeoutMax = TimeSpan.FromMilliseconds(110),
            HeartbeatInterval = TimeSpan.FromMilliseconds(20)
         };

         var sm = new StrictJsonKvStateMachine();
         stateMachines[id] = sm;
         nodes.Add(new RaftNode(options, storages[id], sm, transport));
      }

      try
      {
         foreach (var n in nodes) await n.StartAsync(ct);

         var deadline = Environment.TickCount64 + 1500;
         RaftNode? leader = null;
         while (Environment.TickCount64 < deadline)
         {
            if (stateMachines.Values.Sum(s => s.ExceptionsThrown) > 0) break;
            leader = nodes.FirstOrDefault(n => n.Role == RaftRole.Leader && n.CommitIndex >= 2 && n.LastApplied >= 2);
            if (leader != null) break;
            await Task.Delay(20, ct);
         }

         await Assert.That(stateMachines.Values.Sum(s => s.ExceptionsThrown)).IsEqualTo(0);
         await Assert.That(leader).IsNotNull();

         using var proposeCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
         proposeCts.CancelAfter(1000);
         var cmd2 = JsonSerializer.SerializeToUtf8Bytes(new KvCommand("PUT", "k2", "v2"));
         var result = await leader!.ProposeAsync(cmd2, proposeCts.Token);

         var leaderSm = stateMachines[leader.Options.NodeId];
         await Assert.That(Encoding.UTF8.GetString(result.Span)).IsEqualTo("OK");
         await Assert.That(leaderSm.ExceptionsThrown).IsEqualTo(0);
         await Assert.That(leader.LastApplied).IsEqualTo(leader.CommitIndex);
         await Assert.That(leaderSm.Store["k1"]).IsEqualTo("v1");
         await Assert.That(leaderSm.Store["k2"]).IsEqualTo("v2");
      }
      finally
      {
         foreach (var n in nodes) await n.DisposeAsync();
      }
   }

   [Test]
   [Timeout(10000)]
   public async Task ElectionAndLeadership_ContinueNormalOperation_WhenRoleOrLeaderEventsThrow(CancellationToken ct)
   {
      var memoryOptions = new MemoryTransportOptions();
      var endpoint = new MemoryEndPoint($"raft-events-leader-{Guid.NewGuid():N}");
      var listener = new MemoryNetworkListener(endpoint, memoryOptions);
      var transport = new RaftNetworkTransport(listener, []);

      var options = new RaftNodeOptions
      {
         NodeId = "single-node",
         Peers = [],
         ElectionTimeoutMin = TimeSpan.FromMilliseconds(35),
         ElectionTimeoutMax = TimeSpan.FromMilliseconds(60),
         HeartbeatInterval = TimeSpan.FromMilliseconds(15)
      };

      await using var node = new RaftNode(options, new InMemoryRaftLogStorage(), new PassThroughStateMachine(), transport);

      var throwOnTransition = false;
      node.Events.OnRoleChanged.Add((ctx, _) =>
      {
         if (throwOnTransition && ctx.NewRole != RaftRole.Stopped)
         {
            throw new InvalidOperationException("Role subscriber failure");
         }

         return ValueTask.CompletedTask;
      });
      node.Events.OnLeaderChanged.Add((_, _) =>
      {
         if (throwOnTransition) throw new InvalidOperationException("Leader subscriber failure");
         return ValueTask.CompletedTask;
      });

      await node.StartAsync(ct);
      throwOnTransition = true;

      var deadline = Environment.TickCount64 + 350;
      while (Environment.TickCount64 < deadline && node.Role != RaftRole.Leader)
      {
         await Task.Delay(20, ct);
      }

      await Assert.That(node.Role).IsEqualTo(RaftRole.Leader);
      var result = await node.ProposeAsync("ok"u8.ToArray(), ct);
      await Assert.That(Encoding.UTF8.GetString(result.Span)).IsEqualTo("ok");
   }

   [Test]
   [Timeout(10000)]
   public async Task FollowerAppendEntries_CompletesSuccessfully_WhenOnLeaderChangedThrows(CancellationToken ct)
   {
      var memoryOptions = new MemoryTransportOptions();
      var nodeEndpoint = new MemoryEndPoint($"raft-events-follower-{Guid.NewGuid():N}");
      var nodeListener = new MemoryNetworkListener(nodeEndpoint, memoryOptions);
      var nodeTransport = new RaftNetworkTransport(nodeListener, []);

      var storage = new InMemoryRaftLogStorage();
      var options = new RaftNodeOptions
      {
         NodeId = "follower-1",
         Peers = ["leader-1"],
         ElectionTimeoutMin = TimeSpan.FromMilliseconds(500),
         ElectionTimeoutMax = TimeSpan.FromMilliseconds(1000),
         HeartbeatInterval = TimeSpan.FromMilliseconds(50)
      };

      await using var follower = new RaftNode(options, storage, new PassThroughStateMachine(), nodeTransport);
      follower.Events.OnLeaderChanged.Add((_, _) => throw new InvalidOperationException("LeaderChanged subscriber failure"));

      await follower.StartAsync(ct);

      var clientEndpoint = new MemoryEndPoint($"client-events-{Guid.NewGuid():N}");
      var clientListener = new MemoryNetworkListener(clientEndpoint, memoryOptions);
      await using var clientTransport = new RaftNetworkTransport(clientListener, [
         new RaftPeerEndpoint("follower-1", nodeEndpoint, () => new MemoryNetworkClient(memoryOptions))
      ]);

      var resp = await clientTransport.AppendEntriesAsync("follower-1", new AppendEntriesRequest
      {
         Term = 1,
         LeaderId = "leader-1",
         PrevLogIndex = 0,
         PrevLogTerm = 0,
         LeaderCommitIndex = 1,
         Entries = [new RaftLogEntry(1, 1, "payload"u8.ToArray())]
      }, ct);

      await Assert.That(resp).IsNotNull();
      await Assert.That(resp!.Success).IsTrue();
      await Assert.That(await storage.GetLastLogIndexAsync(ct)).IsEqualTo(1UL);
   }

   [Test]
   [Timeout(10000)]
   public async Task NodeDisposeAsync_AwaitsInFlightReplicationWorkers_BeforeDisposingStorage(CancellationToken ct)
   {
      var replicationStorageEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
      RaftNode? nodeRef = null;
      var storage = new ReplicationLifecycleLogStorage(
         new InMemoryRaftLogStorage(),
         isLeaderActive: () => nodeRef is { Role: RaftRole.Leader },
         onReplicationReadEntered: () => replicationStorageEntered.TrySetResult(),
         readDelayMs: 120);

      var transport = new FastElectionMockTransport();
      var options = new RaftNodeOptions
      {
         NodeId = "node-1",
         Peers = ["node-2"],
         ElectionTimeoutMin = TimeSpan.FromMilliseconds(30),
         ElectionTimeoutMax = TimeSpan.FromMilliseconds(55),
         HeartbeatInterval = TimeSpan.FromMilliseconds(15),
         EnablePreVote = false
      };

      var node = new RaftNode(options, storage, new PassThroughStateMachine(), transport);
      nodeRef = node;
      await node.StartAsync(ct);

      await replicationStorageEntered.Task.WaitAsync(ct);
      await node.DisposeAsync();

      await Assert.That(storage.ActiveReplicationReads).IsEqualTo(0);
      await Assert.That(storage.DisposedWhileStorageInUse).IsFalse();
   }

   [Test]
   [Timeout(10000)]
   public async Task TransportStopAsync_CleansUpStreamsOpenedConcurrentlyDuringShutdown(CancellationToken ct)
   {
      var memoryOptions = new MemoryTransportOptions();
      var serverEndpoint = new MemoryEndPoint($"raft-tx-server-{Guid.NewGuid():N}");
      var serverListener = new MemoryNetworkListener(serverEndpoint, memoryOptions);
      await serverListener.BindAsync(ct);

      using var serverCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
      var acceptLoop = Task.Run(async () =>
      {
         while (!serverCts.IsCancellationRequested)
         {
            var res = await serverListener.AcceptSessionAsync(serverCts.Token);
            if (res.Failed || res.Success == null) break;
            _ = Task.Run(async () =>
            {
               try
               {
                  await res.Success.AcceptStreamAsync(serverCts.Token);
               }
               catch
               {
               }
            }, CancellationToken.None);
         }
      }, CancellationToken.None);

      var flushEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
      var stopDone = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
      var tracker = new ResourceTracker();

      var clientEndpoint = new MemoryEndPoint($"raft-tx-client-{Guid.NewGuid():N}");
      var clientListener = new MemoryNetworkListener(clientEndpoint, memoryOptions);
      var transport = new RaftNetworkTransport(
         clientListener,
         [
            new RaftPeerEndpoint(
               "peer-1",
               serverEndpoint,
               () => new TrackingNetworkClient(new MemoryNetworkClient(memoryOptions), tracker, flushEntered, stopDone))
         ],
         TimeSpan.FromMilliseconds(500));

      try
      {
         await transport.StartAsync(_ => ValueTask.FromResult(RaftRpcResponse.FromRequestVote(new RequestVoteResponse())), ct);

         var rpcTask = transport.RequestVoteAsync("peer-1", new RequestVoteRequest
         {
            Term = 1,
            CandidateId = "node-1",
            LastLogIndex = 0,
            LastLogTerm = 0
         }, ct).AsTask();

         await flushEntered.Task.WaitAsync(ct);

         var stopTask = transport.StopAsync(ct).AsTask();
         await Task.WhenAny(stopTask, Task.Delay(40, ct));
         var stopReturnedWhileStreamActive = stopTask.IsCompleted && !tracker.FlushCompleted;
         stopDone.TrySetResult();

         await Task.WhenAll(stopTask, rpcTask);
         await transport.DisposeAsync();

         await Assert.That(tracker.ClientsCreated).IsGreaterThan(0);
         await Assert.That(tracker.StreamsCreated).IsGreaterThan(0);
         await Assert.That(stopReturnedWhileStreamActive).IsFalse();
         await Assert.That(tracker.DisposedWhileStreamActive).IsFalse();
         await Assert.That(tracker.ClientsDisposed).IsEqualTo(tracker.ClientsCreated);
         await Assert.That(tracker.StreamsDisposed).IsEqualTo(tracker.StreamsCreated);
      }
      finally
      {
         stopDone.TrySetResult();
         serverCts.Cancel();
         await serverListener.UnbindAsync(CancellationToken.None);
         await serverListener.DisposeAsync();
         try { await acceptLoop; } catch { }
      }
   }

   private sealed record KvCommand(string Action, string Key, string? Value);

   private sealed class StrictJsonKvStateMachine : IRaftStateMachine
   {
      private int _exceptions;
      public ConcurrentDictionary<string, string> Store { get; } = new();
      public int ExceptionsThrown => Volatile.Read(ref _exceptions);

      public ValueTask<ReadOnlyMemory<byte>> ApplyAsync(ReadOnlyMemory<byte> command, ulong logIndex, CancellationToken ct = default)
      {
         try
         {
            var cmd = JsonSerializer.Deserialize<KvCommand>(command.Span);
            if (cmd == null)
            {
               return ValueTask.FromResult<ReadOnlyMemory<byte>>("ERR"u8.ToArray());
            }

            if (cmd.Action == "PUT" && cmd.Value != null)
            {
               Store[cmd.Key] = cmd.Value;
               return ValueTask.FromResult<ReadOnlyMemory<byte>>("OK"u8.ToArray());
            }

            return ValueTask.FromResult<ReadOnlyMemory<byte>>("UNKNOWN"u8.ToArray());
         }
         catch
         {
            Interlocked.Increment(ref _exceptions);
            throw;
         }
      }
   }

   private sealed class PassThroughStateMachine : IRaftStateMachine
   {
      public ValueTask<ReadOnlyMemory<byte>> ApplyAsync(ReadOnlyMemory<byte> command, ulong logIndex, CancellationToken ct = default)
         => ValueTask.FromResult(command);
   }

   private sealed class FastElectionMockTransport : IRaftTransport
   {
      public ValueTask StartAsync(Func<RaftRpcRequest, ValueTask<RaftRpcResponse>> rpcHandler, CancellationToken ct = default)
         => ValueTask.CompletedTask;

      public ValueTask StopAsync(CancellationToken ct = default) => ValueTask.CompletedTask;

      public ValueTask<RequestVoteResponse?> RequestVoteAsync(string peerId, RequestVoteRequest request, CancellationToken ct = default)
         => ValueTask.FromResult<RequestVoteResponse?>(new RequestVoteResponse { Term = request.Term, VoteGranted = true });

      public ValueTask<AppendEntriesResponse?> AppendEntriesAsync(string peerId, AppendEntriesRequest request, CancellationToken ct = default)
         => ValueTask.FromResult<AppendEntriesResponse?>(new AppendEntriesResponse { Term = request.Term, Success = true, MatchIndex = request.PrevLogIndex });

      public ValueTask<InstallSnapshotResponse?> InstallSnapshotAsync(string peerId, InstallSnapshotRequest request, CancellationToken ct = default)
         => ValueTask.FromResult<InstallSnapshotResponse?>(new InstallSnapshotResponse { Term = request.Term, Success = true });

      public ValueTask DisposeAsync() => ValueTask.CompletedTask;
   }

   private sealed class ReplicationLifecycleLogStorage(
      IRaftLogStorage inner,
      Func<bool> isLeaderActive,
      Action onReplicationReadEntered,
      int readDelayMs) : IRaftLogStorage
   {
      private int _activeReads;
      private int _disposed;
      private int _delayedOnce;
      private bool _disposedWhileInUse;

      public int ActiveReplicationReads => Volatile.Read(ref _activeReads);
      public bool DisposedWhileStorageInUse => Volatile.Read(ref _disposedWhileInUse);

      public ValueTask<ulong> GetCurrentTermAsync(CancellationToken ct = default) => inner.GetCurrentTermAsync(ct);
      public ValueTask SetCurrentTermAsync(ulong term, CancellationToken ct = default) => inner.SetCurrentTermAsync(term, ct);
      public ValueTask<string?> GetVotedForAsync(CancellationToken ct = default) => inner.GetVotedForAsync(ct);
      public ValueTask SetVotedForAsync(string? candidateId, CancellationToken ct = default) => inner.SetVotedForAsync(candidateId, ct);
      public ValueTask SetTermAndVoteAsync(ulong term, string? candidateId, CancellationToken ct = default) => inner.SetTermAndVoteAsync(term, candidateId, ct);
      public ValueTask<ulong> GetLastLogIndexAsync(CancellationToken ct = default) => inner.GetLastLogIndexAsync(ct);
      public ValueTask<ulong> GetLastLogTermAsync(CancellationToken ct = default) => inner.GetLastLogTermAsync(ct);
      public ValueTask<RaftLogEntry?> GetEntryAsync(ulong index, CancellationToken ct = default) => inner.GetEntryAsync(index, ct);

      public async ValueTask<IReadOnlyList<RaftLogEntry>> GetEntriesAsync(ulong fromIndex, int maxCount, CancellationToken ct = default)
      {
         if (isLeaderActive() && Interlocked.Exchange(ref _delayedOnce, 1) == 0)
         {
            Interlocked.Increment(ref _activeReads);
            try
            {
               onReplicationReadEntered();
               await Task.Delay(readDelayMs, CancellationToken.None);
               if (Volatile.Read(ref _disposed) != 0)
               {
                  Volatile.Write(ref _disposedWhileInUse, true);
               }

               return [];
            }
            finally
            {
               Interlocked.Decrement(ref _activeReads);
            }
         }

         return await inner.GetEntriesAsync(fromIndex, maxCount, ct);
      }

      public ValueTask AppendEntriesAsync(IReadOnlyList<RaftLogEntry> entries, CancellationToken ct = default) => inner.AppendEntriesAsync(entries, ct);
      public ValueTask TruncateLogAsync(ulong fromIndex, CancellationToken ct = default) => inner.TruncateLogAsync(fromIndex, ct);
      public ValueTask CompactPrefixAsync(ulong untilIndex, ulong untilTerm = 0, CancellationToken ct = default) => inner.CompactPrefixAsync(untilIndex, untilTerm, ct);
      public ValueTask<ulong> GetCompactedUntilIndexAsync(CancellationToken ct = default) => inner.GetCompactedUntilIndexAsync(ct);
      public ValueTask<ulong> GetCompactedUntilTermAsync(CancellationToken ct = default) => inner.GetCompactedUntilTermAsync(ct);

      public ValueTask DisposeAsync()
      {
         Volatile.Write(ref _disposed, 1);
         if (Volatile.Read(ref _activeReads) > 0)
         {
            Volatile.Write(ref _disposedWhileInUse, true);
         }

         return inner.DisposeAsync();
      }
   }

   private sealed class ResourceTracker
   {
      private int _clientsCreated;
      private int _clientsDisposed;
      private int _streamsCreated;
      private int _streamsDisposed;
      private int _activeFlushCount;
      private bool _flushCompleted;
      private bool _disposedWhileStreamActive;

      public int ClientsCreated => Volatile.Read(ref _clientsCreated);
      public int ClientsDisposed => Volatile.Read(ref _clientsDisposed);
      public int StreamsCreated => Volatile.Read(ref _streamsCreated);
      public int StreamsDisposed => Volatile.Read(ref _streamsDisposed);
      public bool FlushCompleted => Volatile.Read(ref _flushCompleted);
      public bool DisposedWhileStreamActive => Volatile.Read(ref _disposedWhileStreamActive);

      public void RecordClientCreated() => Interlocked.Increment(ref _clientsCreated);

      public void RecordClientDisposed()
      {
         if (Volatile.Read(ref _activeFlushCount) > 0)
         {
            Volatile.Write(ref _disposedWhileStreamActive, true);
         }

         Interlocked.Increment(ref _clientsDisposed);
      }

      public void RecordStreamCreated() => Interlocked.Increment(ref _streamsCreated);

      public void RecordStreamDisposed()
      {
         if (Volatile.Read(ref _activeFlushCount) > 0)
         {
            Volatile.Write(ref _disposedWhileStreamActive, true);
         }

         Interlocked.Increment(ref _streamsDisposed);
      }

      public void EnterFlush() => Interlocked.Increment(ref _activeFlushCount);

      public void ExitFlush()
      {
         Volatile.Write(ref _flushCompleted, true);
         Interlocked.Decrement(ref _activeFlushCount);
      }
   }

   private sealed class TrackingNetworkClient : INetworkClient
   {
      private readonly INetworkClient _inner;
      private readonly ResourceTracker _tracker;
      private readonly TaskCompletionSource _flushEntered;
      private readonly TaskCompletionSource _stopDone;
      private TrackingNetworkSession? _session;
      private int _disposed;

      public TrackingNetworkClient(
         INetworkClient inner,
         ResourceTracker tracker,
         TaskCompletionSource flushEntered,
         TaskCompletionSource stopDone)
      {
         _inner = inner;
         _tracker = tracker;
         _flushEntered = flushEntered;
         _stopDone = stopDone;
         _tracker.RecordClientCreated();
      }

      public TransportKind Transport => _inner.Transport;
      public bool IsConnected => _inner.IsConnected;
      public NetworkClientStats Stats => _inner.Stats;
      public INetworkSession? Session => _session;
      public EndPoint? LocalAddress => _inner.LocalAddress;
      public EndPoint? RemoteAddress => _inner.RemoteAddress;

      public async ValueTask<Result<INetworkSession, NetworkCodeError>> ConnectAsync(EndPoint endPoint, CancellationToken ct = default)
      {
         var result = await _inner.ConnectAsync(endPoint, ct);
         if (result.Failed || result.Success == null)
         {
            return result;
         }

         _session = new TrackingNetworkSession(result.Success, _tracker, _flushEntered, _stopDone);
         return _session;
      }

      public ValueTask DisconnectAsync(CancellationToken ct = default) => _inner.DisconnectAsync(ct);

      public ValueTask DisposeAsync()
      {
         if (Interlocked.Exchange(ref _disposed, 1) == 0)
         {
            _tracker.RecordClientDisposed();
         }

         return _inner.DisposeAsync();
      }
   }

   private sealed class TrackingNetworkSession(
      INetworkSession inner,
      ResourceTracker tracker,
      TaskCompletionSource flushEntered,
      TaskCompletionSource stopDone) : INetworkSession
   {
      public Guid Id => inner.Id;
      public EndPoint RemoteAddress => inner.RemoteAddress;
      public EndPoint LocalAddress => inner.LocalAddress;
      public bool IsSupportingMultiplexing => inner.IsSupportingMultiplexing;
      public bool IsSupportingUnidirectional => inner.IsSupportingUnidirectional;
      public CancellationToken SessionClosedToken => inner.SessionClosedToken;
      public INetworkPropertyStore Properties => inner.Properties;
      public NetworkStats Stats => inner.Stats;
      public DateTimeOffset CreatedAt => inner.CreatedAt;
      public TransportKind Transport => inner.Transport;
      public NetworkSecurityInfo SecurityInfo => inner.SecurityInfo;
      public NetworkSessionStats SessionStats => inner.SessionStats;
      public IReadOnlyCollection<INetworkStream> ActiveStreams => inner.ActiveStreams;

      public ValueTask<Result<INetworkStream, NetworkCodeError>> AcceptStreamAsync(CancellationToken ct = default)
         => inner.AcceptStreamAsync(ct);

      public async ValueTask<Result<INetworkStream, NetworkCodeError>> OpenStreamAsync(
         NetworkStreamDirection direction = NetworkStreamDirection.Bidirectional,
         CancellationToken ct = default)
      {
         var res = await inner.OpenStreamAsync(direction, ct);
         if (res.Failed || res.Success == null)
         {
            return res;
         }

         return new TrackingNetworkStream(res.Success, this, tracker, flushEntered, stopDone);
      }

      public ValueTask DisposeAsync() => inner.DisposeAsync();
   }

   private sealed class TrackingNetworkStream : INetworkStream
   {
      private readonly INetworkStream _inner;
      private readonly ResourceTracker _tracker;
      private int _disposed;

      public TrackingNetworkStream(
         INetworkStream inner,
         INetworkSession session,
         ResourceTracker tracker,
         TaskCompletionSource flushEntered,
         TaskCompletionSource stopDone)
      {
         _inner = inner;
         _tracker = tracker;
         Session = session;
         Transport = new FlushWaitingDuplexPipe(inner.Transport, tracker, flushEntered, stopDone);
         _tracker.RecordStreamCreated();
      }

      public long StreamId => _inner.StreamId;
      public INetworkSession Session { get; }
      public NetworkStreamDirection Direction => _inner.Direction;
      public IDuplexPipe Transport { get; }
      public NetworkStats Stats { get => _inner.Stats; set => _inner.Stats = value; }
      public DateTimeOffset CreatedAt => _inner.CreatedAt;

      public ValueTask<LockReleaser> AcquireWriterLock(CancellationToken cancellationToken = default)
         => _inner.AcquireWriterLock(cancellationToken);

      public ValueTask DisposeAsync()
      {
         if (Interlocked.Exchange(ref _disposed, 1) == 0)
         {
            _tracker.RecordStreamDisposed();
         }

         return _inner.DisposeAsync();
      }
   }

   private sealed class FlushWaitingDuplexPipe(
      IDuplexPipe inner,
      ResourceTracker tracker,
      TaskCompletionSource flushEntered,
      TaskCompletionSource stopDone) : IDuplexPipe
   {
      public PipeReader Input => inner.Input;
      public PipeWriter Output { get; } = new FlushWaitingPipeWriter(inner.Output, tracker, flushEntered, stopDone);

      private sealed class FlushWaitingPipeWriter(
         PipeWriter innerWriter,
         ResourceTracker tracker,
         TaskCompletionSource flushEntered,
         TaskCompletionSource stopDone) : PipeWriter
      {
         private int _waitedOnce;

         public override void Advance(int bytes) => innerWriter.Advance(bytes);
         public override void CancelPendingFlush() => innerWriter.CancelPendingFlush();
         public override void Complete(Exception? exception = null) => innerWriter.Complete(exception);
         public override Memory<byte> GetMemory(int sizeHint = 0) => innerWriter.GetMemory(sizeHint);
         public override Span<byte> GetSpan(int sizeHint = 0) => innerWriter.GetSpan(sizeHint);

         public override async ValueTask<FlushResult> FlushAsync(CancellationToken cancellationToken = default)
         {
            if (Interlocked.Exchange(ref _waitedOnce, 1) == 0)
            {
               tracker.EnterFlush();
               try
               {
                  var result = await innerWriter.FlushAsync(CancellationToken.None);
                  flushEntered.TrySetResult();
                  await Task.WhenAny(stopDone.Task, Task.Delay(200, CancellationToken.None));
                  return result;
               }
               finally
               {
                  tracker.ExitFlush();
               }
            }

            return await innerWriter.FlushAsync(cancellationToken);
         }
      }
   }
}
