using System.Collections.Concurrent;
using Beskar.Memory.Threading;
using Beskar.Networking.Raft.Enums;
using Beskar.Networking.Raft.Events;
using Beskar.Networking.Raft.Internal;
using Beskar.Networking.Raft.Options;
using Beskar.Networking.Raft.Protocol.Enums;
using Beskar.Networking.Raft.Protocol.Messages;
using Beskar.Networking.Raft.Protocol.Models;
using Beskar.Networking.Raft.StateMachine;
using Beskar.Networking.Raft.Storage;
using Beskar.Networking.Raft.Transport;

namespace Beskar.Networking.Raft;

/// <summary>
/// High-performance, low-allocation Raft consensus engine node managing distributed state transitions,
/// election timers, quorum consensus, log replication, and state machine application.
/// </summary>
public sealed class RaftNode : IAsyncDisposable
{
   public RaftNodeOptions Options { get; }
   public IRaftLogStorage Storage { get; }
   public IRaftStateMachine StateMachine { get; }
   public IRaftTransport Transport { get; }
   public RaftNodeEvents Events { get; } = new();

   public RaftRole Role => (RaftRole)Volatile.Read(ref _role);
   public ulong CurrentTerm => Volatile.Read(ref _currentTerm);
   public string? LeaderId => Volatile.Read(ref _leaderId);
   public ulong CommitIndex => Volatile.Read(ref _commitIndex);
   public ulong LastApplied => Volatile.Read(ref _lastApplied);

   private int _role = (int)RaftRole.Stopped;
   private ulong _currentTerm;
   private string? _votedFor;
   private string? _leaderId;
   private ulong _commitIndex;
   private ulong _lastApplied;

   private long _lastHeartbeatTicks;
   private long _lastLeaderContactTicks;
   private long _lastQuorumHeartbeatTicks;
   private int _electionTimeoutMs;
   private long _termStateVersion;
   private long _persistedTermStateVersion;
   private long _activePersistTaskVersion;
   private Task? _activePersistTask;
   private int _activeReplicationWorkers;
   private TaskCompletionSource? _replicationDrainedTcs;

   private readonly Lock _stateLock = new();
   private readonly SemaphoreSlim _lifecycleLock = new(1, 1);
   private readonly SemaphoreSlim _proposalLock = new(1, 1);
   private readonly SemaphoreSlim _logLock = new(1, 1);
   private readonly SemaphoreSlim _applyLock = new(1, 1);
   private readonly ConcurrentDictionary<string, RaftPeerTracker> _peerTrackers = new();
   private readonly ConcurrentDictionary<ulong, TaskCompletionSource<ReadOnlyMemory<byte>>> _pendingProposals = new();
   private readonly ConcurrentDictionary<ulong, ulong> _pendingProposalTerms = new();

   private CancellationTokenSource? _nodeCts;
   private CancellationTokenSource? _leaderCts;
   private CancellationTokenSource? _activeElectionCts;
   private TaskCompletionSource? _activeElectionTcs;
   private CancellationTokenSource? _activePreVoteCts;
   private TaskCompletionSource? _activePreVoteTcs;
   private Task? _timerLoopTask;
   private Task? _heartbeatLoopTask;
   private int _disposed;

   public RaftNode(
      RaftNodeOptions options,
      IRaftLogStorage storage,
      IRaftStateMachine stateMachine,
      IRaftTransport transport)
   {
      ValidateOptions(options);
      ArgumentNullException.ThrowIfNull(storage);
      ArgumentNullException.ThrowIfNull(stateMachine);
      ArgumentNullException.ThrowIfNull(transport);

      Options = options;
      Storage = storage;
      StateMachine = stateMachine;
      Transport = transport;
      _electionTimeoutMs = GetNextElectionTimeoutMs();
   }

   private static void ValidateOptions(RaftNodeOptions options)
   {
      ArgumentNullException.ThrowIfNull(options);
      ArgumentNullException.ThrowIfNull(options.Peers);

      if (string.IsNullOrWhiteSpace(options.NodeId))
      {
         throw new ArgumentException("NodeId cannot be null or whitespace.", nameof(options));
      }

      if (options.MaxAppendEntriesBatchSize <= 0)
      {
         throw new ArgumentOutOfRangeException(nameof(options), options.MaxAppendEntriesBatchSize, "MaxAppendEntriesBatchSize must be positive.");
      }

      if (options.HeartbeatInterval <= TimeSpan.Zero)
      {
         throw new ArgumentOutOfRangeException(nameof(options), options.HeartbeatInterval, "HeartbeatInterval must be greater than zero.");
      }

      if (options.ElectionTimeoutMin <= TimeSpan.Zero)
      {
         throw new ArgumentOutOfRangeException(nameof(options), options.ElectionTimeoutMin, "ElectionTimeoutMin must be greater than zero.");
      }

      if (options.ElectionTimeoutMax <= TimeSpan.Zero)
      {
         throw new ArgumentOutOfRangeException(nameof(options), options.ElectionTimeoutMax, "ElectionTimeoutMax must be greater than zero.");
      }

      if (options.ElectionTimeoutMin > options.ElectionTimeoutMax)
      {
         throw new ArgumentException("ElectionTimeoutMin cannot exceed ElectionTimeoutMax.", nameof(options));
      }

      var seenPeers = new HashSet<string>(StringComparer.Ordinal);
      for (var i = 0; i < options.Peers.Count; i++)
      {
         var peer = options.Peers[i];
         if (string.IsNullOrWhiteSpace(peer))
         {
            throw new ArgumentException("Peer identifier cannot be null or whitespace.", nameof(options));
         }

         if (string.Equals(peer, options.NodeId, StringComparison.Ordinal))
         {
            throw new ArgumentException($"Peers cannot include this node's own NodeId '{options.NodeId}'.", nameof(options));
         }

         if (!seenPeers.Add(peer))
         {
            throw new ArgumentException($"Duplicate peer identifier '{peer}' in Peers.", nameof(options));
         }
      }
   }

   public async ValueTask StartAsync(CancellationToken ct = default)
   {
      ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);

      ulong startedTerm;
      await _lifecycleLock.WaitAsync(ct);
      try
      {
         ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
         if (Role != RaftRole.Stopped)
         {
            return;
         }

         var transportStarted = false;
         CancellationTokenSource? nodeCts = null;

         try
         {
            var currentTerm = await Storage.GetCurrentTermAsync(ct);
            var votedFor = await Storage.GetVotedForAsync(ct);
            var compactedIndex = await Storage.GetCompactedUntilIndexAsync(ct);

            var timeoutMs = GetNextElectionTimeoutMs();
            if (currentTerm > 0)
            {
               if (votedFor == Options.NodeId)
               {
                  timeoutMs = (int)Options.ElectionTimeoutMin.TotalMilliseconds;
               }
               else if (string.IsNullOrEmpty(votedFor))
               {
                  timeoutMs = (int)Options.ElectionTimeoutMax.TotalMilliseconds;
               }
            }

            var now = Environment.TickCount64;
            nodeCts = new CancellationTokenSource();

            lock (_stateLock)
            {
               _currentTerm = currentTerm;
               _votedFor = votedFor;
               _termStateVersion = 0;
               _persistedTermStateVersion = 0;
               _leaderId = null;
               _peerTrackers.Clear();
               _commitIndex = compactedIndex;
               _lastApplied = compactedIndex;
               _electionTimeoutMs = timeoutMs;
               _lastHeartbeatTicks = now;
               _lastLeaderContactTicks = now - Math.Max(1L, (long)Options.ElectionTimeoutMax.TotalMilliseconds + 1000L);
               _lastQuorumHeartbeatTicks = now;
               _nodeCts = nodeCts;
               _role = (int)RaftRole.Follower;
            }

            startedTerm = currentTerm;
            await Transport.StartAsync(HandleIncomingRpcAsync, ct);
            transportStarted = true;

            _timerLoopTask = Task.Run(() => RunElectionTimerLoopAsync(nodeCts.Token), CancellationToken.None);
            _heartbeatLoopTask = Task.Run(() => RunHeartbeatLoopAsync(nodeCts.Token), CancellationToken.None);
         }
         catch
         {
            lock (_stateLock)
            {
               _role = (int)RaftRole.Stopped;
               _leaderId = null;
               _nodeCts = null;
            }

            if (transportStarted)
            {
               try
               {
                  await Transport.StopAsync(CancellationToken.None);
               }
               catch
               {
               }
            }

            if (nodeCts != null)
            {
               try
               {
                  await nodeCts.CancelAsync();
               }
               catch
               {
               }
               nodeCts.Dispose();
            }

            throw;
         }
      }
      finally
      {
         _lifecycleLock.Release();
      }

      await TryExecuteEventAsync(Events.OnRoleChanged, new RaftRoleChangedContext(Options.NodeId, RaftRole.Stopped, RaftRole.Follower, startedTerm), ct);
   }

   public async ValueTask StopAsync(CancellationToken ct = default)
   {
      RaftRole oldRole;
      ulong termAtStop;

      await _lifecycleLock.WaitAsync(ct);
      try
      {
         CancellationTokenSource? nodeCts;
         CancellationTokenSource? leaderCts;
         CancellationTokenSource? electionCts;
         TaskCompletionSource? electionTcs;
         CancellationTokenSource? preVoteCts;
         TaskCompletionSource? preVoteTcs;
         Task? timerLoopTask;
         Task? heartbeatLoopTask;

         lock (_stateLock)
         {
            oldRole = Role;
            if (oldRole == RaftRole.Stopped)
            {
               return;
            }

            termAtStop = _currentTerm;
            _role = (int)RaftRole.Stopped;
            _leaderId = null;

            nodeCts = _nodeCts;
            _nodeCts = null;
            leaderCts = _leaderCts;
            _leaderCts = null;
            electionCts = _activeElectionCts;
            _activeElectionCts = null;
            electionTcs = _activeElectionTcs;
            _activeElectionTcs = null;
            preVoteCts = _activePreVoteCts;
            _activePreVoteCts = null;
            preVoteTcs = _activePreVoteTcs;
            _activePreVoteTcs = null;

            timerLoopTask = _timerLoopTask;
            _timerLoopTask = null;
            heartbeatLoopTask = _heartbeatLoopTask;
            _heartbeatLoopTask = null;
         }

         AbortWaitersAndCancel(electionTcs, electionCts);
         AbortWaitersAndCancel(preVoteTcs, preVoteCts);

         if (leaderCts != null)
         {
            try { await leaderCts.CancelAsync(); } catch (ObjectDisposedException) { }
         }

         if (nodeCts != null)
         {
            try { await nodeCts.CancelAsync(); } catch (ObjectDisposedException) { }
         }

         try
         {
            await Transport.StopAsync(ct);
         }
         catch
         {
         }

         foreach (var kvp in _pendingProposals)
         {
            kvp.Value.TrySetCanceled(ct);
         }
         _pendingProposals.Clear();
         _pendingProposalTerms.Clear();

         if (timerLoopTask != null)
         {
            try { await timerLoopTask; } catch { }
         }

         if (heartbeatLoopTask != null)
         {
            try { await heartbeatLoopTask; } catch { }
         }

         await WaitForReplicationWorkersDrainedAsync();

         leaderCts?.Dispose();
         nodeCts?.Dispose();
      }
      finally
      {
         _lifecycleLock.Release();
      }

      await TryExecuteEventAsync(Events.OnRoleChanged, new RaftRoleChangedContext(Options.NodeId, oldRole, RaftRole.Stopped, termAtStop), ct);
   }

   private static void AbortWaitersAndCancel(TaskCompletionSource? tcs, CancellationTokenSource? cts)
   {
      tcs?.TrySetResult();
      if (cts != null)
      {
         try
         {
            cts.Cancel();
         }
         catch (ObjectDisposedException)
         {
         }
      }
   }

   /// <summary>
   /// Submits a command payload to the cluster leader for consensus replication and state machine execution.
   /// </summary>
   public async ValueTask<ReadOnlyMemory<byte>> ProposeAsync(ReadOnlyMemory<byte> command, CancellationToken ct = default)
   {
      if (Role != RaftRole.Leader)
      {
         throw new InvalidOperationException($"Node is not leader. Current leader is '{LeaderId ?? "unknown"}'.");
      }

      ulong newIndex;
      TaskCompletionSource<ReadOnlyMemory<byte>> tcs;

      await _proposalLock.WaitAsync(ct);
      try
      {
         ulong currentTerm;
         lock (_stateLock)
         {
            if (Role != RaftRole.Leader)
            {
               throw new InvalidOperationException($"Node is not leader. Current leader is '{LeaderId ?? "unknown"}'.");
            }

            currentTerm = _currentTerm;
         }

         // Read log tail before _logLock so step-downs during blocked storage reads do not stall on _logLock,
         // then refresh under _logLock after validating leadership and term so entries cannot collide with concurrent appends.
         _ = await Storage.GetLastLogIndexAsync(ct);
         tcs = new TaskCompletionSource<ReadOnlyMemory<byte>>(TaskCreationOptions.RunContinuationsAsynchronously);

         await _logLock.WaitAsync(ct);
         try
         {
            lock (_stateLock)
            {
               if (Role != RaftRole.Leader || _currentTerm != currentTerm)
               {
                  throw new InvalidOperationException($"Node is not leader. Current leader is '{LeaderId ?? "unknown"}'.");
               }
            }

            var lastIndex = await Storage.GetLastLogIndexAsync(ct);
            newIndex = lastIndex + 1;

            lock (_stateLock)
            {
               if (Role != RaftRole.Leader || _currentTerm != currentTerm)
               {
                  throw new InvalidOperationException($"Node is not leader. Current leader is '{LeaderId ?? "unknown"}'.");
               }

               _pendingProposalTerms[newIndex] = currentTerm;
               _pendingProposals[newIndex] = tcs;
            }

            try
            {
               var entry = new RaftLogEntry(currentTerm, newIndex, command);
               await Storage.AppendEntriesAsync([entry], ct);
            }
            catch
            {
               _pendingProposalTerms.TryRemove(newIndex, out _);
               _pendingProposals.TryRemove(newIndex, out _);
               throw;
            }

            if (Role != RaftRole.Leader || CurrentTerm != currentTerm)
            {
               _pendingProposalTerms.TryRemove(newIndex, out _);
               if (_pendingProposals.TryRemove(newIndex, out var removedTcs))
               {
                  removedTcs.TrySetException(new InvalidOperationException("Node stepped down from leader before proposal was committed."));
               }
               throw new InvalidOperationException($"Node is not leader. Current leader is '{LeaderId ?? "unknown"}'.");
            }
         }
         finally
         {
            _logLock.Release();
         }
      }
      finally
      {
         _proposalLock.Release();
      }

      // If single-node cluster, commit and apply immediately using node lifetime cancellation so caller cancellation does not leave apply lag.
      if (Options.Peers.Count == 0)
      {
         var nodeToken = GetNodeToken();
         var applyTask = AdvanceCommitIndexAsync(newIndex, nodeToken);
         _ = applyTask.ContinueWith(static t => _ = t.Exception, TaskContinuationOptions.OnlyOnFaulted);

         if (ct.CanBeCanceled)
         {
            await applyTask.WaitAsync(ct);
            return await tcs.Task.WaitAsync(ct);
         }

         await applyTask;
         return await tcs.Task;
      }

      // Trigger replication immediately
      _ = TriggerReplicationAsync();

      await using var reg = ct.Register(() =>
      {
         _pendingProposalTerms.TryRemove(newIndex, out _);
         if (_pendingProposals.TryRemove(newIndex, out var p))
         {
            p.TrySetCanceled(ct);
         }
      });

      return await tcs.Task;
   }

   private async Task RunElectionTimerLoopAsync(CancellationToken cancellationToken)
   {
      while (!cancellationToken.IsCancellationRequested)
      {
         try
         {
            await Task.Delay(10, cancellationToken);
         }
         catch
         {
            break;
         }

         if (Role is RaftRole.Leader or RaftRole.Stopped)
         {
            continue;
         }

         var elapsedMs = Environment.TickCount64 - Volatile.Read(ref _lastHeartbeatTicks);
         if (elapsedMs >= Volatile.Read(ref _electionTimeoutMs))
         {
            try
            {
               await StartPreVoteOrElectionAsync(cancellationToken);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
               break;
            }
            catch
            {
               // Transient storage or transport failures must not kill the election timer loop.
            }
         }
      }
   }

   private async Task RunHeartbeatLoopAsync(CancellationToken cancellationToken)
   {
      while (!cancellationToken.IsCancellationRequested)
      {
         try
         {
            await Task.Delay(Options.HeartbeatInterval, cancellationToken);
         }
         catch
         {
            break;
         }

         if (Role == RaftRole.Leader)
         {
            try
            {
               _ = TriggerReplicationAsync();

               if (Options.Peers.Count > 0)
               {
                  var elapsedSinceQuorum = Environment.TickCount64 - Volatile.Read(ref _lastQuorumHeartbeatTicks);
                  if (elapsedSinceQuorum >= (long)Options.ElectionTimeoutMax.TotalMilliseconds)
                  {
                     await StepDownAsync(CurrentTerm, cancellationToken);
                  }
               }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
               break;
            }
            catch
            {
               // Keep heartbeat loop alive on transient errors.
            }
         }
      }
   }

   private async Task StartPreVoteOrElectionAsync(CancellationToken cancellationToken)
   {
      ulong currentTerm;
      long preVoteStartContactTicks;

      lock (_stateLock)
      {
         if (Role is RaftRole.Leader or RaftRole.Stopped)
         {
            return;
         }

         currentTerm = _currentTerm;
         preVoteStartContactTicks = Volatile.Read(ref _lastLeaderContactTicks);
         // The previously observed leader timed out; clear cached LeaderId so peers are not falsely blocked from Pre-Vote.
         _leaderId = null;
      }

      if (Options.Peers.Count == 0 || !Options.EnablePreVote)
      {
         await StartElectionAsync(cancellationToken);
         return;
      }

      var preVoteTerm = currentTerm + 1;
      ulong lastLogIndex;
      ulong lastLogTerm;
      await _logLock.WaitAsync(cancellationToken);

      try
      {
         lastLogIndex = await Storage.GetLastLogIndexAsync(cancellationToken);
         lastLogTerm = await Storage.GetLastLogTermAsync(cancellationToken);
      }
      finally
      {
         _logLock.Release();
      }

      var preVotesGranted = 1; // Vote for self
      var totalNodes = Options.Peers.Count + 1;
      var requiredQuorum = (totalNodes / 2) + 1;

      var preVoteRequest = new RequestVoteRequest
      {
         Term = preVoteTerm,
         CandidateId = Options.NodeId,
         LastLogIndex = lastLogIndex,
         LastLogTerm = lastLogTerm,
         IsPreVote = true
      };

      var quorumOrDoneTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
      var preVoteCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

      lock (_stateLock)
      {
         if (Role is RaftRole.Leader or RaftRole.Stopped ||
             _currentTerm != currentTerm ||
             Volatile.Read(ref _lastLeaderContactTicks) != preVoteStartContactTicks ||
             _leaderId != null)
         {
            preVoteCts.Dispose();
            return;
         }

         preVoteCts.CancelAfter(Volatile.Read(ref _electionTimeoutMs));
         _activePreVoteCts = preVoteCts;
         _activePreVoteTcs = quorumOrDoneTcs;
      }

      var rpcToken = preVoteCts.Token;
      var peerTasks = Options.Peers.Select(async peerId =>
      {
         try
         {
            var response = await Transport.RequestVoteAsync(peerId, preVoteRequest, rpcToken);
            if (response == null)
            {
               return;
            }

            if (response.Term > currentTerm)
            {
               await StepDownAsync(response.Term, cancellationToken);
               quorumOrDoneTcs.TrySetResult();
               return;
            }

            if (response.VoteGranted)
            {
               if (Interlocked.Increment(ref preVotesGranted) >= requiredQuorum)
               {
                  quorumOrDoneTcs.TrySetResult();
               }
            }
         }
         catch
         {
            // Peer unreachable
         }
      }).ToArray();

      _ = Task.WhenAll(peerTasks).ContinueWith(
         static (_, state) =>
         {
            var (tcs, cts) = ((TaskCompletionSource, CancellationTokenSource))state!;
            tcs.TrySetResult();
            cts.Dispose();
         },
         (quorumOrDoneTcs, preVoteCts),
         CancellationToken.None,
         TaskContinuationOptions.ExecuteSynchronously,
         TaskScheduler.Default);

      try
      {
         await quorumOrDoneTcs.Task;
         try { await preVoteCts.CancelAsync(); } catch (ObjectDisposedException) { }
      }
      finally
      {
         lock (_stateLock)
         {
            if (ReferenceEquals(_activePreVoteCts, preVoteCts))
            {
               _activePreVoteCts = null;
               _activePreVoteTcs = null;
            }
         }
      }

      var abortedByLeaderOrStateChange = false;
      lock (_stateLock)
      {
         abortedByLeaderOrStateChange =
            Role is RaftRole.Leader or RaftRole.Stopped ||
            _currentTerm != currentTerm ||
            Volatile.Read(ref _lastLeaderContactTicks) != preVoteStartContactTicks ||
            _leaderId != null;
      }

      if (abortedByLeaderOrStateChange)
      {
         return;
      }

      if (Volatile.Read(ref preVotesGranted) >= requiredQuorum)
      {
         await StartElectionAsync(cancellationToken);
      }
      else
      {
         lock (_stateLock)
         {
            _leaderId = null;
            _lastHeartbeatTicks = Environment.TickCount64;
            _electionTimeoutMs = GetNextElectionTimeoutMs();
         }
      }
   }

   private async Task StartElectionAsync(CancellationToken cancellationToken)
   {
      ulong electionTerm;
      RaftRole previousRole;
      int electionTimeoutMs;
      ulong lastLogIndex;
      ulong lastLogTerm;

      await _logLock.WaitAsync(cancellationToken);
      try
      {
         lock (_stateLock)
         {
            if (Role is RaftRole.Leader or RaftRole.Stopped)
            {
               return;
            }

            previousRole = Role;
            _role = (int)RaftRole.Candidate;
            _currentTerm++;
            _votedFor = Options.NodeId;
            _termStateVersion++;
            electionTerm = _currentTerm;
            _leaderId = null;
            _lastHeartbeatTicks = Environment.TickCount64;
            _electionTimeoutMs = GetNextElectionTimeoutMs();
            electionTimeoutMs = _electionTimeoutMs;
         }

         lastLogIndex = await Storage.GetLastLogIndexAsync(cancellationToken);
         lastLogTerm = await Storage.GetLastLogTermAsync(cancellationToken);
      }
      finally
      {
         _logLock.Release();
      }

      await PersistTermAndVoteAsync(CancellationToken.None);

      if (Role == RaftRole.Candidate && CurrentTerm == electionTerm)
      {
         await TryExecuteEventAsync(Events.OnRoleChanged, new RaftRoleChangedContext(Options.NodeId, previousRole, RaftRole.Candidate, electionTerm), cancellationToken);
      }

      if (Role != RaftRole.Candidate || CurrentTerm != electionTerm)
      {
         return;
      }

      // Single-node cluster wins election instantly
      if (Options.Peers.Count == 0)
      {
         await BecomeLeaderAsync(electionTerm, lastLogIndex, cancellationToken);
         return;
      }

      var votesGranted = 1; // Vote for self
      var totalNodes = Options.Peers.Count + 1;
      var requiredQuorum = (totalNodes / 2) + 1;
      var electionDecided = 0;

      var voteRequest = new RequestVoteRequest
      {
         Term = electionTerm,
         CandidateId = Options.NodeId,
         LastLogIndex = lastLogIndex,
         LastLogTerm = lastLogTerm,
         IsPreVote = false
      };

      var quorumOrDoneTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
      var electionCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

      lock (_stateLock)
      {
         if (Role != RaftRole.Candidate || _currentTerm != electionTerm)
         {
            electionCts.Dispose();
            return;
         }

         electionCts.CancelAfter(electionTimeoutMs);
         _activeElectionCts = electionCts;
         _activeElectionTcs = quorumOrDoneTcs;
      }

      var rpcToken = electionCts.Token;
      var peerTasks = Options.Peers.Select(async peerId =>
      {
         try
         {
            var response = await Transport.RequestVoteAsync(peerId, voteRequest, rpcToken);
            if (response == null)
            {
               return;
            }

            if (response.Term > electionTerm)
            {
               await StepDownAsync(response.Term, cancellationToken);
               quorumOrDoneTcs.TrySetResult();
               return;
            }

            if (response.VoteGranted && response.Term == electionTerm)
            {
               var currentVotes = Interlocked.Increment(ref votesGranted);
               if (currentVotes >= requiredQuorum &&
                   Role == RaftRole.Candidate &&
                   CurrentTerm == electionTerm &&
                   Interlocked.Exchange(ref electionDecided, 1) == 0)
               {
                  await BecomeLeaderAsync(electionTerm, lastLogIndex, cancellationToken);
                  quorumOrDoneTcs.TrySetResult();
               }
            }
         }
         catch
         {
            // Peer unreachable
         }
      }).ToArray();

      _ = Task.WhenAll(peerTasks).ContinueWith(
         static (_, state) =>
         {
            var (tcs, cts) = ((TaskCompletionSource, CancellationTokenSource))state!;
            tcs.TrySetResult();
            cts.Dispose();
         },
         (quorumOrDoneTcs, electionCts),
         CancellationToken.None,
         TaskContinuationOptions.ExecuteSynchronously,
         TaskScheduler.Default);

      try
      {
         await quorumOrDoneTcs.Task;
         try { await electionCts.CancelAsync(); } catch (ObjectDisposedException) { }
      }
      finally
      {
         lock (_stateLock)
         {
            if (ReferenceEquals(_activeElectionCts, electionCts))
            {
               _activeElectionCts = null;
               _activeElectionTcs = null;
            }
         }
      }
   }

   private async Task BecomeLeaderAsync(ulong term, ulong lastLogIndex, CancellationToken cancellationToken)
   {
      RaftRole previousRole;
      CancellationTokenSource? oldLeaderCts;
      CancellationTokenSource? electionCts;
      TaskCompletionSource? electionTcs;
      CancellationTokenSource? preVoteCts;
      TaskCompletionSource? preVoteTcs;
      CancellationToken leaderToken;

      lock (_stateLock)
      {
         if (Role != RaftRole.Candidate || _currentTerm != term || _nodeCts == null)
         {
            return;
         }

         previousRole = Role;
         _role = (int)RaftRole.Leader;
         _leaderId = Options.NodeId;
         _lastQuorumHeartbeatTicks = Environment.TickCount64;

         oldLeaderCts = _leaderCts;
         var nodeToken = _nodeCts.Token;
         var newLeaderCts = cancellationToken.CanBeCanceled && cancellationToken != nodeToken
            ? CancellationTokenSource.CreateLinkedTokenSource(nodeToken, cancellationToken)
            : CancellationTokenSource.CreateLinkedTokenSource(nodeToken);
         _leaderCts = newLeaderCts;
         leaderToken = newLeaderCts.Token;

         electionCts = _activeElectionCts;
         _activeElectionCts = null;
         electionTcs = _activeElectionTcs;
         _activeElectionTcs = null;
         preVoteCts = _activePreVoteCts;
         _activePreVoteCts = null;
         preVoteTcs = _activePreVoteTcs;
         _activePreVoteTcs = null;

         _peerTrackers.Clear();
         foreach (var peer in Options.Peers)
         {
            _peerTrackers[peer] = new RaftPeerTracker(peer, lastLogIndex + 1);
         }
      }

      AbortWaitersAndCancel(electionTcs, electionCts);
      AbortWaitersAndCancel(preVoteTcs, preVoteCts);
      CancelAndDisposeWhenDrained(oldLeaderCts);

      if (lastLogIndex > CommitIndex)
      {
         try
         {
            await _proposalLock.WaitAsync(leaderToken);
            try
            {
               await _logLock.WaitAsync(leaderToken);
               try
               {
                  if (Role == RaftRole.Leader && CurrentTerm == term)
                  {
                     var currentLastIndex = await Storage.GetLastLogIndexAsync(leaderToken);
                     var currentLastTerm = await Storage.GetLastLogTermAsync(leaderToken);

                     if (currentLastIndex > CommitIndex && currentLastTerm < term)
                     {
                        var noOpIndex = currentLastIndex + 1;
                        var noOpEntry = new RaftLogEntry(term, noOpIndex, ReadOnlyMemory<byte>.Empty, IsNoOp: true);
                        await Storage.AppendEntriesAsync([noOpEntry], leaderToken);
                     }
                  }
               }
               finally
               {
                  _logLock.Release();
               }
            }
            finally
            {
               _proposalLock.Release();
            }
         }
         catch (OperationCanceledException) when (leaderToken.IsCancellationRequested)
         {
            return;
         }
      }

      if (Role != RaftRole.Leader || CurrentTerm != term)
      {
         return;
      }

      await TryExecuteEventAsync(
         Events.OnRoleChanged,
         new RaftRoleChangedContext(Options.NodeId, previousRole, RaftRole.Leader, term),
         leaderToken);

      if (Role != RaftRole.Leader || CurrentTerm != term)
      {
         return;
      }

      await TryExecuteEventAsync(
         Events.OnLeaderChanged,
         new RaftLeaderChangedContext(Options.NodeId, Options.NodeId, term),
         leaderToken);

      if (Role != RaftRole.Leader || CurrentTerm != term)
      {
         return;
      }

      if (Options.Peers.Count == 0)
      {
         await CheckAndUpdateCommitIndexAsync(leaderToken);
      }
      else
      {
         // Send initial AppendEntries (heartbeats / no-op) immediately without blocking leadership establishment on slow peers.
         _ = TriggerReplicationAsync();
      }
   }

   private async Task StepDownAsync(ulong newTerm, CancellationToken cancellationToken)
   {
      RaftRole previousRole = RaftRole.Stopped;
      ulong effectiveTerm = 0;
      var termIncreased = false;
      var needsWaitOnly = false;
      CancellationTokenSource? leaderCts = null;
      CancellationTokenSource? electionCts = null;
      TaskCompletionSource? electionTcs = null;
      CancellationTokenSource? preVoteCts = null;
      TaskCompletionSource? preVoteTcs = null;

      lock (_stateLock)
      {
         if (Role == RaftRole.Stopped || newTerm < _currentTerm)
         {
            return;
         }

         if (_currentTerm == newTerm && Role == RaftRole.Follower)
         {
            needsWaitOnly = true;
         }
         else
         {
            previousRole = Role;
            _role = (int)RaftRole.Follower;

            if (newTerm > _currentTerm)
            {
               _currentTerm = newTerm;
               _votedFor = null;
               _termStateVersion++;
               termIncreased = true;
            }

            effectiveTerm = _currentTerm;
            _leaderId = null;
            _lastHeartbeatTicks = Environment.TickCount64;
            _electionTimeoutMs = GetNextElectionTimeoutMs();

            leaderCts = _leaderCts;
            _leaderCts = null;

            electionCts = _activeElectionCts;
            _activeElectionCts = null;

            electionTcs = _activeElectionTcs;
            _activeElectionTcs = null;

            preVoteCts = _activePreVoteCts;
            _activePreVoteCts = null;

            preVoteTcs = _activePreVoteTcs;
            _activePreVoteTcs = null;
         }
      }

      if (needsWaitOnly)
      {
         await EnsureTermAndVotePersistedAsync();
         return;
      }

      AbortWaitersAndCancel(electionTcs, electionCts);
      AbortWaitersAndCancel(preVoteTcs, preVoteCts);
      CancelAndDisposeWhenDrained(leaderCts);

      if (previousRole == RaftRole.Leader)
      {
         FailPendingProposals("Node stepped down from leader before proposal was committed.");
      }

      if (termIncreased)
      {
         await PersistTermAndVoteAsync(CancellationToken.None);
      }
      else
      {
         await EnsureTermAndVotePersistedAsync();
      }

      if (previousRole == RaftRole.Leader && Role != RaftRole.Leader)
      {
         FailPendingProposals("Node stepped down from leader before proposal was committed.");
      }

      if (previousRole != RaftRole.Follower)
      {
         await TryExecuteEventAsync(
            Events.OnRoleChanged,
            new RaftRoleChangedContext(Options.NodeId, previousRole, RaftRole.Follower, effectiveTerm),
            GetNodeToken());
      }
   }

   private Task EnsureTermAndVotePersistedAsync()
   {
      return PersistTermAndVoteAsync(CancellationToken.None);
   }

   private Task PersistTermAndVoteAsync(CancellationToken cancellationToken)
   {
      long targetVersion;
      ulong targetTerm;
      string? targetVotedFor;
      TaskCompletionSource persistTcs;

      lock (_stateLock)
      {
         if (_termStateVersion <= _persistedTermStateVersion)
         {
            return Task.CompletedTask;
         }

         if (_activePersistTask != null &&
             !_activePersistTask.IsCompleted &&
             _activePersistTaskVersion == _termStateVersion)
         {
            return _activePersistTask;
         }

         targetVersion = _termStateVersion;
         targetTerm = _currentTerm;
         targetVotedFor = _votedFor;

         persistTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
         _activePersistTaskVersion = targetVersion;
         _activePersistTask = persistTcs.Task;
      }

      _ = RunPersistLoopAsync(targetVersion, targetTerm, targetVotedFor, persistTcs);
      return persistTcs.Task;
   }

   private async Task RunPersistLoopAsync(
      long version,
      ulong term,
      string? votedFor,
      TaskCompletionSource tcs)
   {
      while (true)
      {
         try
         {
            // Never hold locks across storage I/O so higher-term updates never deadlock on stalled lower-term writes,
            // and use CancellationToken.None so cancelled leader/caller tokens cannot abort term/vote durability.
            await Storage.SetTermAndVoteAsync(term, votedFor, CancellationToken.None);
         }
         catch (Exception ex)
         {
            lock (_stateLock)
            {
               if (ReferenceEquals(_activePersistTask, tcs.Task))
               {
                  _activePersistTask = null;
               }
            }

            tcs.TrySetException(ex);
            return;
         }

         lock (_stateLock)
         {
            if (_termStateVersion == version)
            {
               if (version > _persistedTermStateVersion)
               {
                  _persistedTermStateVersion = version;
               }

               if (ReferenceEquals(_activePersistTask, tcs.Task))
               {
                  _activePersistTask = null;
               }

               tcs.TrySetResult();
               return;
            }

            version = _termStateVersion;
            term = _currentTerm;
            votedFor = _votedFor;
         }
      }
   }

   private static async Task TryExecuteEventAsync<T>(
      HandlerPipeline<T> pipeline,
      T context,
      CancellationToken cancellationToken)
   {
      if (pipeline.Count == 0)
      {
         return;
      }

      try
      {
         await pipeline.ExecuteAsync(context, cancellationToken: cancellationToken);
      }
      catch
      {
         // Subscriber exceptions must not disrupt core Raft state transitions or RPC handling.
      }
   }

   private CancellationToken GetNodeToken()
   {
      lock (_stateLock)
      {
         return _nodeCts != null ? _nodeCts.Token : CancellationToken.None;
      }
   }

   private Task WaitForReplicationWorkersDrainedAsync()
   {
      lock (_stateLock)
      {
         if (_activeReplicationWorkers == 0)
         {
            return Task.CompletedTask;
         }

         _replicationDrainedTcs ??= new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
         return _replicationDrainedTcs.Task;
      }
   }

   private void DecrementReplicationWorker()
   {
      TaskCompletionSource? tcsToSignal = null;
      lock (_stateLock)
      {
         if (--_activeReplicationWorkers == 0)
         {
            tcsToSignal = _replicationDrainedTcs;
            _replicationDrainedTcs = null;
         }
      }

      tcsToSignal?.TrySetResult();
   }

   private void CancelAndDisposeWhenDrained(CancellationTokenSource? cts)
   {
      if (cts == null)
      {
         return;
      }

      try
      {
         cts.Cancel();
      }
      catch (ObjectDisposedException)
      {
         return;
      }

      Task drainTask;
      lock (_stateLock)
      {
         if (_activeReplicationWorkers == 0)
         {
            cts.Dispose();
            return;
         }

         _replicationDrainedTcs ??= new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
         drainTask = _replicationDrainedTcs.Task;
      }

      _ = drainTask.ContinueWith(
         static (_, state) =>
         {
            try
            {
               ((CancellationTokenSource)state!).Dispose();
            }
            catch (ObjectDisposedException)
            {
            }
         },
         cts,
         CancellationToken.None,
         TaskContinuationOptions.ExecuteSynchronously,
         TaskScheduler.Default);
   }

   private void FailPendingProposals(string reason)
   {
      foreach (var kvp in _pendingProposals)
      {
         _pendingProposalTerms.TryRemove(kvp.Key, out _);
         if (_pendingProposals.TryRemove(kvp.Key, out var tcs))
         {
            tcs.TrySetException(new InvalidOperationException(reason));
         }
      }
   }

   private Task TriggerReplicationAsync()
   {
      ulong currentTerm;
      CancellationToken effectiveToken;
      List<(string PeerId, RaftPeerTracker Tracker)>? peersToStart = null;

      lock (_stateLock)
      {
         if (Role != RaftRole.Leader || _leaderCts == null || _leaderCts.IsCancellationRequested)
         {
            return Task.CompletedTask;
         }

         currentTerm = _currentTerm;
         effectiveToken = _leaderCts.Token;

         foreach (var peerId in Options.Peers)
         {
            if (!_peerTrackers.TryGetValue(peerId, out var tracker))
            {
               continue;
            }

            tracker.RequestReplication();
            if (tracker.TryBeginReplication())
            {
               peersToStart ??= new List<(string, RaftPeerTracker)>(Options.Peers.Count);
               peersToStart.Add((peerId, tracker));
               _activeReplicationWorkers++;
            }
         }
      }

      if (peersToStart == null)
      {
         return Task.CompletedTask;
      }

      var workerTasks = new List<Task>(peersToStart.Count);
      for (var i = 0; i < peersToStart.Count; i++)
      {
         var (peerId, tracker) = peersToStart[i];
         workerTasks.Add(RunTrackedReplicationWorkerAsync(peerId, tracker, currentTerm, effectiveToken));
      }

      return Task.WhenAll(workerTasks);
   }

   private async Task RunTrackedReplicationWorkerAsync(
      string peerId,
      RaftPeerTracker tracker,
      ulong term,
      CancellationToken cancellationToken)
   {
      try
      {
         await ReplicatePeerLoopAsync(peerId, tracker, term, cancellationToken);
      }
      finally
      {
         DecrementReplicationWorker();
      }
   }

   private async Task ReplicatePeerLoopAsync(
      string peerId,
      RaftPeerTracker tracker,
      ulong term,
      CancellationToken cancellationToken)
   {
      while (!cancellationToken.IsCancellationRequested && Role == RaftRole.Leader && CurrentTerm == term)
      {
         try
         {
            while (!cancellationToken.IsCancellationRequested &&
                   Role == RaftRole.Leader &&
                   CurrentTerm == term &&
                   tracker.ConsumeReplicationRequest())
            {
               await SendPeerReplicationStepAsync(peerId, tracker, term, cancellationToken);
            }
         }
         catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
         {
            break;
         }
         catch
         {
            // Keep replication loop safe; EndReplication runs in finally.
         }
         finally
         {
            tracker.EndReplication();
         }

         if (cancellationToken.IsCancellationRequested ||
             Role != RaftRole.Leader ||
             CurrentTerm != term ||
             !tracker.HasPendingRequest ||
             !tracker.TryBeginReplication())
         {
            break;
         }
      }
   }

   private async Task SendPeerReplicationStepAsync(
      string peerId,
      RaftPeerTracker tracker,
      ulong term,
      CancellationToken cancellationToken)
   {
      try
      {
         var compactedIndex = await Storage.GetCompactedUntilIndexAsync(cancellationToken);
         var compactedTerm = await Storage.GetCompactedUntilTermAsync(cancellationToken);
         var nextIndex = tracker.NextIndex;

         if (compactedIndex > 0 && nextIndex <= compactedIndex)
         {
            await SendInstallSnapshotStepAsync(peerId, tracker, term, compactedTerm, cancellationToken);
            return;
         }

         if (Role != RaftRole.Leader || CurrentTerm != term)
         {
            return;
         }

         var prevLogIndex = nextIndex > 0 ? nextIndex - 1 : 0;
         ulong prevLogTerm = 0;

         if (prevLogIndex > 0)
         {
            if (prevLogIndex == compactedIndex)
            {
               prevLogTerm = compactedTerm;
            }
            else
            {
               var prevEntry = await Storage.GetEntryAsync(prevLogIndex, cancellationToken);
               if (prevEntry.HasValue)
               {
                  prevLogTerm = prevEntry.Value.Term;
               }
               else
               {
                  var updatedCompactedIndex = await Storage.GetCompactedUntilIndexAsync(cancellationToken);
                  var updatedCompactedTerm = await Storage.GetCompactedUntilTermAsync(cancellationToken);
                  if (updatedCompactedIndex > 0 && nextIndex <= updatedCompactedIndex)
                  {
                     tracker.RequestReplication();
                     return;
                  }

                  if (prevLogIndex == updatedCompactedIndex)
                  {
                     prevLogTerm = updatedCompactedTerm;
                  }
               }
            }
         }

         var entries = await Storage.GetEntriesAsync(nextIndex, Options.MaxAppendEntriesBatchSize, cancellationToken);
         if (entries.Count == 0 && nextIndex > 0)
         {
            var latestCompactedIndex = await Storage.GetCompactedUntilIndexAsync(cancellationToken);
            if (latestCompactedIndex > 0 && nextIndex <= latestCompactedIndex)
            {
               tracker.RequestReplication();
               return;
            }
         }

         if (Role != RaftRole.Leader || CurrentTerm != term)
         {
            return;
         }

         var request = new AppendEntriesRequest
         {
            Term = term,
            LeaderId = Options.NodeId,
            PrevLogIndex = prevLogIndex,
            PrevLogTerm = prevLogTerm,
            LeaderCommitIndex = CommitIndex,
            Entries = entries
         };

         var response = await Transport.AppendEntriesAsync(peerId, request, cancellationToken);
         if (response == null)
         {
            return;
         }

         if (response.Term > term)
         {
            await StepDownAsync(response.Term, GetNodeToken());
            return;
         }

         if (Role != RaftRole.Leader || CurrentTerm != term)
         {
            return;
         }

         UpdateQuorumContact(tracker, term);

         if (response.Success)
         {
            var updatedMatch = Math.Max(tracker.MatchIndex, response.MatchIndex);
            tracker.MatchIndex = updatedMatch;
            tracker.NextIndex = updatedMatch + 1;

            var leaderLastIndex = await Storage.GetLastLogIndexAsync(cancellationToken);
            if (leaderLastIndex >= tracker.NextIndex)
            {
               tracker.RequestReplication();
            }

            await CheckAndUpdateCommitIndexAsync(cancellationToken);
         }
         else
         {
            var previousNextIndex = tracker.NextIndex;
            if (response.MatchIndex >= prevLogIndex && response.MatchIndex > 0)
            {
               tracker.NextIndex = Math.Max(previousNextIndex, response.MatchIndex + 1);
            }
            else if (response.MatchIndex < prevLogIndex)
            {
               tracker.NextIndex = Math.Max(1, response.MatchIndex + 1);
            }
            else if (prevLogIndex > 0)
            {
               tracker.NextIndex = Math.Min(previousNextIndex, prevLogIndex);
            }

            if (tracker.NextIndex != previousNextIndex)
            {
               tracker.RequestReplication();
            }
         }
      }
      catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
      {
         throw;
      }
      catch
      {
         // Peer unreachable or transient RPC failure
      }
   }

   private async Task SendInstallSnapshotStepAsync(
      string peerId,
      RaftPeerTracker tracker,
      ulong term,
      ulong fallbackCompactedTerm,
      CancellationToken cancellationToken)
   {
      ReadOnlyMemory<byte> snapshotData;
      ulong lastApplied;
      ulong lastAppliedTerm;

      await _applyLock.WaitAsync(cancellationToken);
      try
      {
         snapshotData = await StateMachine.TakeSnapshotAsync(cancellationToken);
         lastApplied = Volatile.Read(ref _lastApplied);
         var lastAppliedEntry = await Storage.GetEntryAsync(lastApplied, cancellationToken);
         if (lastAppliedEntry.HasValue)
         {
            lastAppliedTerm = lastAppliedEntry.Value.Term;
         }
         else
         {
            var latestCompactedIndex = await Storage.GetCompactedUntilIndexAsync(cancellationToken);
            var latestCompactedTerm = await Storage.GetCompactedUntilTermAsync(cancellationToken);
            lastAppliedTerm = lastApplied == latestCompactedIndex ? latestCompactedTerm : fallbackCompactedTerm;
         }
      }
      finally
      {
         _applyLock.Release();
      }

      if (Role != RaftRole.Leader || CurrentTerm != term)
      {
         return;
      }

      var snapshotReq = new InstallSnapshotRequest
      {
         Term = term,
         LeaderId = Options.NodeId,
         LastIncludedIndex = lastApplied,
         LastIncludedTerm = lastAppliedTerm,
         Data = snapshotData
      };

      var snapshotResp = await Transport.InstallSnapshotAsync(peerId, snapshotReq, cancellationToken);
      if (snapshotResp == null)
      {
         return;
      }

      if (snapshotResp.Term > term)
      {
         await StepDownAsync(snapshotResp.Term, GetNodeToken());
         return;
      }

      if (Role != RaftRole.Leader || CurrentTerm != term)
      {
         return;
      }

      UpdateQuorumContact(tracker, term);

      if (snapshotResp.Success)
      {
         tracker.MatchIndex = Math.Max(tracker.MatchIndex, lastApplied);
         tracker.NextIndex = tracker.MatchIndex + 1;

         var leaderLastIndex = await Storage.GetLastLogIndexAsync(cancellationToken);
         if (leaderLastIndex >= tracker.NextIndex)
         {
            tracker.RequestReplication();
         }

         await CheckAndUpdateCommitIndexAsync(cancellationToken);
      }
   }

   private void UpdateQuorumContact(RaftPeerTracker tracker, ulong term)
   {
      var now = Environment.TickCount64;
      tracker.RecordContact(now);

      if (Role != RaftRole.Leader || CurrentTerm != term)
      {
         return;
      }

      var count = _peerTrackers.Count + 1;
      var contacts = count <= 32 ? stackalloc long[count] : new long[count];
      contacts[0] = now;
      var idx = 1;
      foreach (var peer in _peerTrackers.Values)
      {
         if (idx >= contacts.Length)
         {
            break;
         }

         contacts[idx++] = peer.LastContactTicks;
      }

      var quorumTicks = SelectQuorumMedianDescending(contacts[..idx]);

      if (Role != RaftRole.Leader || CurrentTerm != term)
      {
         return;
      }

      while (true)
      {
         var current = Volatile.Read(ref _lastQuorumHeartbeatTicks);
         if (quorumTicks <= current ||
             Interlocked.CompareExchange(ref _lastQuorumHeartbeatTicks, quorumTicks, current) == current)
         {
            break;
         }
      }
   }

   private static T SelectQuorumMedianDescending<T>(Span<T> values) where T : IComparable<T>
   {
      values.Sort();
      return values[values.Length - 1 - (values.Length / 2)];
   }

   private async Task CheckAndUpdateCommitIndexAsync(CancellationToken cancellationToken)
   {
      if (Role != RaftRole.Leader || _peerTrackers.Count != Options.Peers.Count)
      {
         return;
      }

      var currentTerm = CurrentTerm;
      var lastLogIndex = await Storage.GetLastLogIndexAsync(cancellationToken);
      var currentCommit = CommitIndex;

      var count = _peerTrackers.Count + 1;
      var matchIndexes = count <= 32 ? stackalloc ulong[count] : new ulong[count];
      matchIndexes[0] = lastLogIndex;
      var idx = 1;
      foreach (var tracker in _peerTrackers.Values)
      {
         if (idx >= matchIndexes.Length)
         {
            break;
         }

         matchIndexes[idx++] = tracker.MatchIndex;
      }

      var quorumIndex = SelectQuorumMedianDescending(matchIndexes[..idx]);

      if (quorumIndex > currentCommit)
      {
         var entry = await Storage.GetEntryAsync(quorumIndex, cancellationToken);
         if (entry != null && entry.Value.Term == currentTerm && Role == RaftRole.Leader && CurrentTerm == currentTerm)
         {
            await AdvanceCommitIndexAsync(quorumIndex, GetNodeToken());
            if (Options.Peers.Count > 0 && Role == RaftRole.Leader && CurrentTerm == currentTerm)
            {
               _ = TriggerReplicationAsync();
            }
         }
      }
   }

   private async Task AdvanceCommitIndexAsync(ulong newCommitIndex, CancellationToken cancellationToken)
   {
      lock (_stateLock)
      {
         if (Role == RaftRole.Stopped)
         {
            return;
         }

         if (newCommitIndex <= _commitIndex && Volatile.Read(ref _lastApplied) >= _commitIndex)
         {
            return;
         }

         _commitIndex = Math.Max(_commitIndex, newCommitIndex);
      }

      await _applyLock.WaitAsync(CancellationToken.None);
      try
      {
         while (Role != RaftRole.Stopped && Volatile.Read(ref _lastApplied) < Volatile.Read(ref _commitIndex))
         {
            var applyIndex = Volatile.Read(ref _lastApplied) + 1;
            var entry = await Storage.GetEntryAsync(applyIndex, cancellationToken);
            if (entry == null)
            {
               break;
            }

            var applyResult = entry.Value.IsNoOp
               ? ReadOnlyMemory<byte>.Empty
               : await StateMachine.ApplyAsync(entry.Value.Data, applyIndex, cancellationToken);
            Volatile.Write(ref _lastApplied, applyIndex);

            if (_pendingProposals.TryRemove(applyIndex, out var tcs))
            {
               var hasRecordedTerm = _pendingProposalTerms.TryRemove(applyIndex, out var proposalTerm);
               if ((hasRecordedTerm && entry.Value.Term == proposalTerm) ||
                   (!hasRecordedTerm && Role == RaftRole.Leader && entry.Value.Term == CurrentTerm))
               {
                  tcs.TrySetResult(applyResult);
               }
               else
               {
                  tcs.TrySetException(new InvalidOperationException("Proposal was overwritten by a different term."));
               }
            }

            await TryExecuteEventAsync(
               Events.OnEntryCommitted,
               new RaftEntryCommittedContext(Options.NodeId, entry.Value, applyResult),
               cancellationToken);
         }
      }
      finally
      {
         _applyLock.Release();
      }
   }

   private async ValueTask<RaftRpcResponse> HandleIncomingRpcAsync(RaftRpcRequest request)
   {
      return request.MessageType switch
      {
         RaftMessageType.RequestVote => RaftRpcResponse.FromRequestVote(await HandleRequestVoteAsync(request.AsRequestVote())),
         RaftMessageType.AppendEntries => RaftRpcResponse.FromAppendEntries(await HandleAppendEntriesAsync(request.AsAppendEntries())),
         RaftMessageType.InstallSnapshot => RaftRpcResponse.FromInstallSnapshot(await HandleInstallSnapshotAsync(request.AsInstallSnapshot())),
         _ => throw new NotSupportedException($"Unsupported Raft RPC message type: {request.MessageType}")
      };
   }

   private async ValueTask<RequestVoteResponse> HandleRequestVoteAsync(RequestVoteRequest request)
   {
      if (Role == RaftRole.Stopped)
      {
         return new RequestVoteResponse { Term = CurrentTerm, VoteGranted = false };
      }

      if (request.IsPreVote)
      {
         await _logLock.WaitAsync();
         try
         {
            var localLogIndex = await Storage.GetLastLogIndexAsync();
            var localLogTerm = await Storage.GetLastLogTermAsync();

            lock (_stateLock)
            {
               if (Role == RaftRole.Stopped)
               {
                  return new RequestVoteResponse { Term = _currentTerm, VoteGranted = false };
               }

               if (request.Term < _currentTerm ||
                   (request.Term == _currentTerm && !string.IsNullOrEmpty(_votedFor) && _votedFor != request.CandidateId))
               {
                  return new RequestVoteResponse { Term = _currentTerm, VoteGranted = false };
               }

               var isLogUpToDate = request.LastLogTerm > localLogTerm ||
                                   (request.LastLogTerm == localLogTerm && request.LastLogIndex >= localLogIndex);

               var hasActiveLeader = Role == RaftRole.Leader ||
                                     (_leaderId != null && Role == RaftRole.Follower &&
                                      (Environment.TickCount64 - Volatile.Read(ref _lastLeaderContactTicks) < (long)Options.ElectionTimeoutMin.TotalMilliseconds));

               return new RequestVoteResponse
               {
                  Term = _currentTerm,
                  VoteGranted = isLogUpToDate && !hasActiveLeader
               };
            }
         }
         finally
         {
            _logLock.Release();
         }
      }

      RaftRole previousRole;
      var termUpdated = false;
      var needsPersist = false;
      ulong responseTerm;
      var voteGranted = false;

      CancellationTokenSource? leaderCts = null;
      CancellationTokenSource? electionCts = null;
      TaskCompletionSource? electionTcs = null;
      CancellationTokenSource? preVoteCts = null;
      TaskCompletionSource? preVoteTcs = null;

      await _logLock.WaitAsync();
      try
      {
         var lastLogIndex = await Storage.GetLastLogIndexAsync();
         var lastLogTerm = await Storage.GetLastLogTermAsync();

         var isUpToDate = request.LastLogTerm > lastLogTerm ||
                          (request.LastLogTerm == lastLogTerm && request.LastLogIndex >= lastLogIndex);

         lock (_stateLock)
         {
            previousRole = Role;
            if (previousRole == RaftRole.Stopped)
            {
               return new RequestVoteResponse { Term = _currentTerm, VoteGranted = false };
            }

            if (request.Term > _currentTerm)
            {
               _currentTerm = request.Term;
               _votedFor = null;
               _termStateVersion++;
               termUpdated = true;
               needsPersist = true;

               _role = (int)RaftRole.Follower;
               _leaderId = null;
               _lastHeartbeatTicks = Environment.TickCount64;
               _electionTimeoutMs = GetNextElectionTimeoutMs();

               leaderCts = _leaderCts;
               _leaderCts = null;
               electionCts = _activeElectionCts;
               _activeElectionCts = null;
               electionTcs = _activeElectionTcs;
               _activeElectionTcs = null;
               preVoteCts = _activePreVoteCts;
               _activePreVoteCts = null;
               preVoteTcs = _activePreVoteTcs;
               _activePreVoteTcs = null;
            }

            responseTerm = _currentTerm;

            if (request.Term == _currentTerm)
            {
               var canVote = string.IsNullOrEmpty(_votedFor) || _votedFor == request.CandidateId;
               if (canVote && isUpToDate)
               {
                  voteGranted = true;
                  if (_votedFor != request.CandidateId)
                  {
                     _votedFor = request.CandidateId;
                     _termStateVersion++;
                  }

                  needsPersist = true;
                  _lastHeartbeatTicks = Environment.TickCount64;
               }
            }
         }
      }
      finally
      {
         _logLock.Release();
      }

      AbortWaitersAndCancel(electionTcs, electionCts);
      AbortWaitersAndCancel(preVoteTcs, preVoteCts);
      CancelAndDisposeWhenDrained(leaderCts);

      if (termUpdated && previousRole == RaftRole.Leader)
      {
         FailPendingProposals("Node stepped down from leader before proposal was committed.");
      }

      if (needsPersist)
      {
         await PersistTermAndVoteAsync(CancellationToken.None);
      }

      if (termUpdated && previousRole == RaftRole.Leader && Role != RaftRole.Leader)
      {
         FailPendingProposals("Node stepped down from leader before proposal was committed.");
      }

      if (termUpdated && previousRole != RaftRole.Follower)
      {
         await TryExecuteEventAsync(
            Events.OnRoleChanged,
            new RaftRoleChangedContext(Options.NodeId, previousRole, RaftRole.Follower, responseTerm),
            GetNodeToken());
      }

      return new RequestVoteResponse
      {
         Term = responseTerm,
         VoteGranted = voteGranted
      };
   }

   private async ValueTask<(bool Accepted, ulong CurrentTerm)> TryAcceptLeaderRpcAsync(ulong rpcTerm, string leaderId)
   {
      if (Role == RaftRole.Stopped)
      {
         return (false, CurrentTerm);
      }

      var currentTerm = CurrentTerm;

      if (rpcTerm > currentTerm)
      {
         await StepDownAsync(rpcTerm, CancellationToken.None);
         currentTerm = CurrentTerm;
      }

      if (Role == RaftRole.Stopped)
      {
         return (false, CurrentTerm);
      }

      if (rpcTerm < currentTerm)
      {
         return (false, currentTerm);
      }

      if (Role == RaftRole.Candidate)
      {
         await StepDownAsync(rpcTerm, CancellationToken.None);
      }

      bool rejected;
      var leaderChanged = false;
      CancellationTokenSource? preVoteCts = null;
      TaskCompletionSource? preVoteTcs = null;

      lock (_stateLock)
      {
         currentTerm = _currentTerm;
         if (Role == RaftRole.Stopped)
         {
            return (false, currentTerm);
         }

         rejected = rpcTerm < currentTerm || Role != RaftRole.Follower;
         if (!rejected)
         {
            var now = Environment.TickCount64;
            Volatile.Write(ref _lastHeartbeatTicks, now);
            Volatile.Write(ref _lastLeaderContactTicks, now);

            if (_leaderId != leaderId)
            {
               _leaderId = leaderId;
               leaderChanged = true;
            }

            preVoteCts = _activePreVoteCts;
            _activePreVoteCts = null;
            preVoteTcs = _activePreVoteTcs;
            _activePreVoteTcs = null;
         }
      }

      AbortWaitersAndCancel(preVoteTcs, preVoteCts);

      if (rejected)
      {
         return (false, CurrentTerm);
      }

      if (leaderChanged)
      {
         await TryExecuteEventAsync(
            Events.OnLeaderChanged,
            new RaftLeaderChangedContext(Options.NodeId, leaderId, currentTerm),
            GetNodeToken());
      }

      await EnsureTermAndVotePersistedAsync();

      if (Role == RaftRole.Stopped || rpcTerm < CurrentTerm || Role != RaftRole.Follower)
      {
         return (false, CurrentTerm);
      }

      return (true, currentTerm);
   }

   private async ValueTask<AppendEntriesResponse> HandleAppendEntriesAsync(AppendEntriesRequest request)
   {
      var (accepted, acceptedTerm) = await TryAcceptLeaderRpcAsync(request.Term, request.LeaderId);
      if (!accepted)
      {
         return new AppendEntriesResponse
         {
            Term = acceptedTerm,
            Success = false,
            MatchIndex = Role == RaftRole.Stopped ? 0 : await Storage.GetLastLogIndexAsync()
         };
      }

      ulong latestTerm = acceptedTerm;
      ulong lastLogIndexAfterAppend;
      await _logLock.WaitAsync(CancellationToken.None);
      try
      {
         latestTerm = CurrentTerm;
         if (Role == RaftRole.Stopped)
         {
            return new AppendEntriesResponse { Term = latestTerm, Success = false, MatchIndex = 0 };
         }

         if (request.Term < latestTerm || Role != RaftRole.Follower)
         {
            return new AppendEntriesResponse
            {
               Term = latestTerm,
               Success = false,
               MatchIndex = await Storage.GetLastLogIndexAsync()
            };
         }

         var compactedIndex = await Storage.GetCompactedUntilIndexAsync();
         var compactedTerm = await Storage.GetCompactedUntilTermAsync();

         // Check log consistency at PrevLogIndex
         if (request.PrevLogIndex > 0)
         {
            if (request.PrevLogIndex < compactedIndex)
            {
               if (request.Entries.Count > 0 && request.Entries[^1].Index >= compactedIndex)
               {
                  RaftLogEntry? boundaryEntry = null;
                  for (var i = 0; i < request.Entries.Count; i++)
                  {
                     if (request.Entries[i].Index == compactedIndex)
                     {
                        boundaryEntry = request.Entries[i];
                        break;
                     }
                  }

                  if (!boundaryEntry.HasValue || boundaryEntry.Value.Term != compactedTerm)
                  {
                     return new AppendEntriesResponse
                     {
                        Term = latestTerm,
                        Success = false,
                        MatchIndex = compactedIndex > 0 ? compactedIndex - 1 : 0
                     };
                  }
               }
               else
               {
                  return new AppendEntriesResponse
                  {
                     Term = latestTerm,
                     Success = false,
                     MatchIndex = compactedIndex
                  };
               }
            }
            else if (request.PrevLogIndex == compactedIndex)
            {
               if (compactedTerm != request.PrevLogTerm)
               {
                  return new AppendEntriesResponse
                  {
                     Term = latestTerm,
                     Success = false,
                     MatchIndex = compactedIndex > 0 ? compactedIndex - 1 : 0
                  };
               }
            }
            else
            {
               var prevEntry = await Storage.GetEntryAsync(request.PrevLogIndex);
               if (!prevEntry.HasValue)
               {
                  var lastIdx = await Storage.GetLastLogIndexAsync();
                  return new AppendEntriesResponse { Term = latestTerm, Success = false, MatchIndex = lastIdx };
               }

               if (prevEntry.Value.Term != request.PrevLogTerm)
               {
                  var lastIdx = await Storage.GetLastLogIndexAsync();
                  return new AppendEntriesResponse
                  {
                     Term = latestTerm,
                     Success = false,
                     MatchIndex = Math.Min(lastIdx, request.PrevLogIndex - 1)
                  };
               }
            }
         }

         // Append entries (skipping any prefix already compacted) and resolve conflicts
         if (request.Entries.Count > 0)
         {
            for (var i = 0; i < request.Entries.Count; i++)
            {
               var newEntry = request.Entries[i];
               if (newEntry.Index <= compactedIndex)
               {
                  continue;
               }

               var existing = await Storage.GetEntryAsync(newEntry.Index);
               if (existing != null)
               {
                  if (existing.Value.Term != newEntry.Term)
                  {
                     await Storage.TruncateLogAsync(newEntry.Index);
                     await Storage.AppendEntriesAsync(SliceEntries(request.Entries, i));
                     break;
                  }
               }
               else
               {
                  await Storage.AppendEntriesAsync(SliceEntries(request.Entries, i));
                  break;
               }
            }
         }

         lastLogIndexAfterAppend = await Storage.GetLastLogIndexAsync();

         var termAfterAppend = CurrentTerm;
         if (Role != RaftRole.Follower || request.Term != termAfterAppend)
         {
            return new AppendEntriesResponse
            {
               Term = termAfterAppend,
               Success = false,
               MatchIndex = Role == RaftRole.Stopped ? 0 : lastLogIndexAfterAppend
            };
         }

         latestTerm = termAfterAppend;
      }
      finally
      {
         _logLock.Release();
      }

      var postLockTerm = CurrentTerm;
      if (Role != RaftRole.Follower || request.Term != postLockTerm)
      {
         return new AppendEntriesResponse
         {
            Term = postLockTerm,
            Success = false,
            MatchIndex = Role == RaftRole.Stopped ? 0 : lastLogIndexAfterAppend
         };
      }

      var replicatedMatchIndex = request.Entries.Count > 0
         ? request.Entries[^1].Index
         : request.PrevLogIndex;

      if (request.LeaderCommitIndex > CommitIndex || Volatile.Read(ref _lastApplied) < CommitIndex)
      {
         var leaderCommitBound = Math.Min(request.LeaderCommitIndex, Math.Min(replicatedMatchIndex, lastLogIndexAfterAppend));
         var newCommit = Math.Max(CommitIndex, leaderCommitBound);

         await AdvanceCommitIndexAsync(newCommit, GetNodeToken());
      }

      var finalTerm = CurrentTerm;
      if (Role != RaftRole.Follower || request.Term != finalTerm)
      {
         return new AppendEntriesResponse
         {
            Term = finalTerm,
            Success = false,
            MatchIndex = Role == RaftRole.Stopped ? 0 : lastLogIndexAfterAppend
         };
      }

      return new AppendEntriesResponse
      {
         Term = finalTerm,
         Success = true,
         MatchIndex = replicatedMatchIndex
      };
   }

   private static IReadOnlyList<RaftLogEntry> SliceEntries(IReadOnlyList<RaftLogEntry> entries, int startIndex)
   {
      if (startIndex <= 0)
      {
         return entries;
      }

      var slice = new List<RaftLogEntry>(entries.Count - startIndex);
      for (var i = startIndex; i < entries.Count; i++)
      {
         slice.Add(entries[i]);
      }

      return slice;
   }

   private async ValueTask<InstallSnapshotResponse> HandleInstallSnapshotAsync(InstallSnapshotRequest request)
   {
      var (accepted, acceptedTerm) = await TryAcceptLeaderRpcAsync(request.Term, request.LeaderId);
      if (!accepted)
      {
         return new InstallSnapshotResponse { Term = acceptedTerm, Success = false };
      }

      var localCompactedIndex = await Storage.GetCompactedUntilIndexAsync();
      var localLastLogIndex = await Storage.GetLastLogIndexAsync();

      if (request.LastIncludedIndex <= localCompactedIndex ||
          (request.LastIncludedIndex < CommitIndex && request.LastIncludedIndex <= localLastLogIndex))
      {
         return new InstallSnapshotResponse { Term = CurrentTerm, Success = true };
      }

      await _logLock.WaitAsync(CancellationToken.None);
      try
      {
         await _applyLock.WaitAsync(CancellationToken.None);
         try
         {
            if (Role == RaftRole.Stopped || request.Term < CurrentTerm || Role != RaftRole.Follower)
            {
               return new InstallSnapshotResponse { Term = CurrentTerm, Success = false };
            }

            var currentCompacted = await Storage.GetCompactedUntilIndexAsync();
            var currentLastLogIndex = await Storage.GetLastLogIndexAsync();

            if (request.LastIncludedIndex <= currentCompacted ||
                (request.LastIncludedIndex <= Volatile.Read(ref _lastApplied) && request.LastIncludedIndex <= currentLastLogIndex) ||
                (request.LastIncludedIndex < CommitIndex && request.LastIncludedIndex <= currentLastLogIndex))
            {
               return new InstallSnapshotResponse { Term = CurrentTerm, Success = true };
            }

            await StateMachine.RestoreSnapshotAsync(request.Data, request.LastIncludedIndex, request.LastIncludedTerm);
            await Storage.CompactPrefixAsync(request.LastIncludedIndex, request.LastIncludedTerm);

            lock (_stateLock)
            {
               _commitIndex = Math.Max(_commitIndex, request.LastIncludedIndex);
               _lastApplied = Math.Max(_lastApplied, request.LastIncludedIndex);
            }
         }
         finally
         {
            _applyLock.Release();
         }
      }
      finally
      {
         _logLock.Release();
      }

      return new InstallSnapshotResponse { Term = CurrentTerm, Success = true };
   }

   private int GetNextElectionTimeoutMs()
   {
      var min = (int)Options.ElectionTimeoutMin.TotalMilliseconds;
      var max = (int)Options.ElectionTimeoutMax.TotalMilliseconds;
      if (min >= max)
      {
         return min;
      }

      return Random.Shared.Next(min, max);
   }

   public async ValueTask DisposeAsync()
   {
      if (Interlocked.Exchange(ref _disposed, 1) != 0)
      {
         return;
      }

      await StopAsync();
      await Storage.DisposeAsync();
      await Transport.DisposeAsync();
      _proposalLock.Dispose();
      _logLock.Dispose();
      _applyLock.Dispose();
      _lifecycleLock.Dispose();
   }
}
