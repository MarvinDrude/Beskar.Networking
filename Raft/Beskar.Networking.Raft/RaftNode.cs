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
   private int _electionTimeoutMs;

   private readonly Lock _stateLock = new();
   private readonly SemaphoreSlim _proposalLock = new(1, 1);
   private readonly ConcurrentDictionary<string, RaftPeerTracker> _peerTrackers = new();
   private readonly ConcurrentDictionary<ulong, TaskCompletionSource<ReadOnlyMemory<byte>>> _pendingProposals = new();

   private CancellationTokenSource? _nodeCts;
   private Task? _timerLoopTask;
   private Task? _heartbeatLoopTask;
   private int _disposed;

   public RaftNode(
      RaftNodeOptions options,
      IRaftLogStorage storage,
      IRaftStateMachine stateMachine,
      IRaftTransport transport)
   {
      Options = options;
      Storage = storage;
      StateMachine = stateMachine;
      Transport = transport;
      _electionTimeoutMs = GetNextElectionTimeoutMs();
   }

   public async ValueTask StartAsync(CancellationToken ct = default)
   {
      lock (_stateLock)
      {
         if (Role != RaftRole.Stopped)
         {
            return;
         }

         _role = (int)RaftRole.Follower;
      }

      _currentTerm = await Storage.GetCurrentTermAsync(ct);
      _votedFor = await Storage.GetVotedForAsync(ct);
      _commitIndex = 0;
      _lastApplied = 0;
      _lastHeartbeatTicks = Environment.TickCount64;

      _nodeCts = new CancellationTokenSource();

      await Transport.StartAsync(HandleIncomingRpcAsync, ct);

      _timerLoopTask = Task.Run(() => RunElectionTimerLoopAsync(_nodeCts.Token), CancellationToken.None);
      _heartbeatLoopTask = Task.Run(() => RunHeartbeatLoopAsync(_nodeCts.Token), CancellationToken.None);

      if (Events.OnRoleChanged.Count > 0)
      {
         await Events.OnRoleChanged.ExecuteAsync(new RaftRoleChangedContext(Options.NodeId, RaftRole.Stopped, RaftRole.Follower, _currentTerm), cancellationToken: ct);
      }
   }

   public async ValueTask StopAsync(CancellationToken ct = default)
   {
      if (Interlocked.Exchange(ref _disposed, 1) != 0)
      {
         return;
      }

      var oldRole = Role;
      lock (_stateLock)
      {
         _role = (int)RaftRole.Stopped;
      }

      if (_nodeCts != null)
      {
         await _nodeCts.CancelAsync();
      }

      await Transport.StopAsync(ct);

      foreach (var kvp in _pendingProposals)
      {
         kvp.Value.TrySetCanceled(ct);
      }
      _pendingProposals.Clear();

      if (_timerLoopTask != null)
      {
         try { await _timerLoopTask; } catch { }
      }

      if (_heartbeatLoopTask != null)
      {
         try { await _heartbeatLoopTask; } catch { }
      }

      if (Events.OnRoleChanged.Count > 0)
      {
         await Events.OnRoleChanged.ExecuteAsync(new RaftRoleChangedContext(Options.NodeId, oldRole, RaftRole.Stopped, _currentTerm), cancellationToken: ct);
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

         var lastIndex = await Storage.GetLastLogIndexAsync(ct);
         newIndex = lastIndex + 1;

         var entry = new RaftLogEntry(currentTerm, newIndex, command);
         await Storage.AppendEntriesAsync([entry], ct);

         tcs = new TaskCompletionSource<ReadOnlyMemory<byte>>(TaskCreationOptions.RunContinuationsAsynchronously);
         _pendingProposals[newIndex] = tcs;
      }
      finally
      {
         _proposalLock.Release();
      }

      // If single-node cluster, commit and apply immediately
      if (Options.Peers.Count == 0)
      {
         await AdvanceCommitIndexAsync(newIndex, ct);
         return await tcs.Task;
      }

      // Trigger replication immediately
      _ = TriggerReplicationAsync(CancellationToken.None);

      await using var reg = ct.Register(() =>
      {
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
            await Task.Delay(25, cancellationToken);
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
         if (elapsedMs >= _electionTimeoutMs)
         {
            await StartElectionAsync(cancellationToken);
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
            await TriggerReplicationAsync(cancellationToken);
         }
      }
   }

   private async Task StartElectionAsync(CancellationToken cancellationToken)
   {
      ulong electionTerm;
      RaftRole previousRole;

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
         electionTerm = _currentTerm;
         _leaderId = null;
         _lastHeartbeatTicks = Environment.TickCount64;
         _electionTimeoutMs = GetNextElectionTimeoutMs();
      }

      await Storage.SetTermAndVoteAsync(electionTerm, Options.NodeId, cancellationToken);

      if (Events.OnRoleChanged.Count > 0)
      {
         await Events.OnRoleChanged.ExecuteAsync(new RaftRoleChangedContext(Options.NodeId, previousRole, RaftRole.Candidate, electionTerm), cancellationToken: cancellationToken);
      }

      var lastLogIndex = await Storage.GetLastLogIndexAsync(cancellationToken);
      var lastLogTerm = await Storage.GetLastLogTermAsync(cancellationToken);

      // Single-node cluster wins election instantly
      if (Options.Peers.Count == 0)
      {
         await BecomeLeaderAsync(electionTerm, lastLogIndex, cancellationToken);
         return;
      }

      var votesGranted = 1; // Vote for self
      var totalNodes = Options.Peers.Count + 1;
      var requiredQuorum = (totalNodes / 2) + 1;

      var voteRequest = new RequestVoteRequest
      {
         Term = electionTerm,
         CandidateId = Options.NodeId,
         LastLogIndex = lastLogIndex,
         LastLogTerm = lastLogTerm
      };

      var peerTasks = Options.Peers.Select(async peerId =>
      {
         try
         {
            var response = await Transport.RequestVoteAsync(peerId, voteRequest, cancellationToken);
            if (response == null)
            {
               return;
            }

            if (response.Term > electionTerm)
            {
               await StepDownAsync(response.Term, cancellationToken);
               return;
            }

            if (response.VoteGranted && response.Term == electionTerm)
            {
               var currentVotes = Interlocked.Increment(ref votesGranted);
               if (currentVotes >= requiredQuorum && Role == RaftRole.Candidate && CurrentTerm == electionTerm)
               {
                  await BecomeLeaderAsync(electionTerm, lastLogIndex, cancellationToken);
               }
            }
         }
         catch
         {
            // Peer unreachable
         }
      });

      await Task.WhenAll(peerTasks);
   }

   private async Task BecomeLeaderAsync(ulong term, ulong lastLogIndex, CancellationToken cancellationToken)
   {
      RaftRole previousRole;
      lock (_stateLock)
      {
         if (Role != RaftRole.Candidate || CurrentTerm != term)
         {
            return;
         }

         previousRole = Role;
         _role = (int)RaftRole.Leader;
         _leaderId = Options.NodeId;

         _peerTrackers.Clear();
         foreach (var peer in Options.Peers)
         {
            _peerTrackers[peer] = new RaftPeerTracker(peer, lastLogIndex + 1);
         }
      }

      if (Events.OnRoleChanged.Count > 0)
      {
         await Events.OnRoleChanged.ExecuteAsync(new RaftRoleChangedContext(Options.NodeId, previousRole, RaftRole.Leader, term), cancellationToken: cancellationToken);
      }

      if (Events.OnLeaderChanged.Count > 0)
      {
         await Events.OnLeaderChanged.ExecuteAsync(new RaftLeaderChangedContext(Options.NodeId, Options.NodeId, term), cancellationToken: cancellationToken);
      }

      // Send initial empty AppendEntries (heartbeats) immediately to all peers
      await TriggerReplicationAsync(cancellationToken);
   }

   private async Task StepDownAsync(ulong newTerm, CancellationToken cancellationToken)
   {
      RaftRole previousRole;
      lock (_stateLock)
      {
         if (_currentTerm >= newTerm && Role == RaftRole.Follower)
         {
            return;
         }

         previousRole = Role;
         _role = (int)RaftRole.Follower;
         _currentTerm = newTerm;
         _lastHeartbeatTicks = Environment.TickCount64;
         _electionTimeoutMs = GetNextElectionTimeoutMs();
      }

      await Storage.SetTermAndVoteAsync(newTerm, null, cancellationToken);

      if (previousRole != RaftRole.Follower && Events.OnRoleChanged.Count > 0)
      {
         await Events.OnRoleChanged.ExecuteAsync(new RaftRoleChangedContext(Options.NodeId, previousRole, RaftRole.Follower, newTerm), cancellationToken: cancellationToken);
      }
   }

   private async Task TriggerReplicationAsync(CancellationToken cancellationToken = default)
   {
      if (Role != RaftRole.Leader)
      {
         return;
      }

      var currentTerm = CurrentTerm;
      var commitIndex = CommitIndex;

      var tasks = Options.Peers.Select(async peerId =>
      {
         if (!_peerTrackers.TryGetValue(peerId, out var tracker))
         {
            return;
         }

         var prevLogIndex = tracker.NextIndex > 0 ? tracker.NextIndex - 1 : 0;
         ulong prevLogTerm = 0;

         if (prevLogIndex > 0)
         {
            var prevEntry = await Storage.GetEntryAsync(prevLogIndex, cancellationToken);
            if (prevEntry == null)
            {
               // log entries before tracker.NextIndex have been snapshot-compacted by leader
               // ... i think we send InstallSnapshot RPC to bring follower up to date
               try
               {
                  var snapshotData = await StateMachine.TakeSnapshotAsync(cancellationToken);
                  var lastIncludedIndex = await Storage.GetLastLogIndexAsync(cancellationToken);
                  var lastIncludedTerm = await Storage.GetLastLogTermAsync(cancellationToken);

                  var snapshotReq = new InstallSnapshotRequest
                  {
                     Term = currentTerm,
                     LeaderId = Options.NodeId,
                     LastIncludedIndex = lastIncludedIndex,
                     LastIncludedTerm = lastIncludedTerm,
                     Data = snapshotData
                  };

                  var snapshotResp = await Transport.InstallSnapshotAsync(peerId, snapshotReq, cancellationToken);
                  if (snapshotResp != null)
                  {
                     if (snapshotResp.Term > currentTerm)
                     {
                        await StepDownAsync(snapshotResp.Term, cancellationToken);
                        return;
                     }

                     if (snapshotResp.Success)
                     {
                        tracker.MatchIndex = Math.Max(tracker.MatchIndex, lastIncludedIndex);
                        tracker.NextIndex = tracker.MatchIndex + 1;
                        await CheckAndUpdateCommitIndexAsync(cancellationToken);
                     }
                  }
               }
               catch
               {
                  // Peer unreachable
               }
               return;
            }

            prevLogTerm = prevEntry.Value.Term;
         }

         var entries = await Storage.GetEntriesAsync(tracker.NextIndex, Options.MaxAppendEntriesBatchSize, cancellationToken);

         var request = new AppendEntriesRequest
         {
            Term = currentTerm,
            LeaderId = Options.NodeId,
            PrevLogIndex = prevLogIndex,
            PrevLogTerm = prevLogTerm,
            LeaderCommitIndex = commitIndex,
            Entries = entries
         };

         try
         {
            var response = await Transport.AppendEntriesAsync(peerId, request, cancellationToken);
            if (response == null)
            {
               return;
            }

            if (response.Term > currentTerm)
            {
               await StepDownAsync(response.Term, cancellationToken);
               return;
            }

            if (Role != RaftRole.Leader || CurrentTerm != currentTerm)
            {
               return;
            }

            if (response.Success)
            {
               tracker.MatchIndex = Math.Max(tracker.MatchIndex, response.MatchIndex);
               tracker.NextIndex = tracker.MatchIndex + 1;

               await CheckAndUpdateCommitIndexAsync(cancellationToken);
            }
            else
            {
               // Follower rejected due to log inconsistency, fast-backtrack nextIndex to follower's matchIndex
               if (response.MatchIndex > 0 && response.MatchIndex < tracker.NextIndex)
               {
                  tracker.NextIndex = response.MatchIndex + 1;
               }
               else if (tracker.NextIndex > 1)
               {
                  tracker.NextIndex--;
               }
            }
         }
         catch
         {
            // Peer unreachable
         }
      });

      await Task.WhenAll(tasks);
   }

   private async Task CheckAndUpdateCommitIndexAsync(CancellationToken cancellationToken)
   {
      if (Role != RaftRole.Leader)
      {
         return;
      }

      var lastLogIndex = await Storage.GetLastLogIndexAsync(cancellationToken);
      var currentCommit = CommitIndex;

      var matchIndexes = _peerTrackers.Values.Select(t => t.MatchIndex).Append(lastLogIndex).OrderByDescending(x => x).ToList();
      var quorumIndex = matchIndexes[matchIndexes.Count / 2];

      if (quorumIndex > currentCommit)
      {
         var entry = await Storage.GetEntryAsync(quorumIndex, cancellationToken);
         if (entry != null && entry.Value.Term == CurrentTerm)
         {
            await AdvanceCommitIndexAsync(quorumIndex, cancellationToken);
            _ = TriggerReplicationAsync(CancellationToken.None);
         }
      }
   }

   private async Task AdvanceCommitIndexAsync(ulong newCommitIndex, CancellationToken cancellationToken)
   {
      lock (_stateLock)
      {
         if (newCommitIndex <= _commitIndex)
         {
            return;
         }

         _commitIndex = newCommitIndex;
      }

      while (Volatile.Read(ref _lastApplied) < Volatile.Read(ref _commitIndex))
      {
         var applyIndex = Volatile.Read(ref _lastApplied) + 1;
         var entry = await Storage.GetEntryAsync(applyIndex, cancellationToken);
         if (entry == null)
         {
            break;
         }

         var applyResult = await StateMachine.ApplyAsync(entry.Value.Data, applyIndex, cancellationToken);
         Volatile.Write(ref _lastApplied, applyIndex);

         if (_pendingProposals.TryRemove(applyIndex, out var tcs))
         {
            tcs.TrySetResult(applyResult);
         }

         if (Events.OnEntryCommitted.Count > 0)
         {
            await Events.OnEntryCommitted.ExecuteAsync(new RaftEntryCommittedContext(Options.NodeId, entry.Value, applyResult), cancellationToken: cancellationToken);
         }
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
      var lastLogIndex = await Storage.GetLastLogIndexAsync();
      var lastLogTerm = await Storage.GetLastLogTermAsync();

      var isUpToDate = request.LastLogTerm > lastLogTerm ||
                       (request.LastLogTerm == lastLogTerm && request.LastLogIndex >= lastLogIndex);

      ulong responseTerm;
      var voteGranted = false;
      string? updatedVotedFor;

      lock (_stateLock)
      {
         if (request.Term > _currentTerm)
         {
            _role = (int)RaftRole.Follower;
            _currentTerm = request.Term;
            _votedFor = null;
            _lastHeartbeatTicks = Environment.TickCount64;
            _electionTimeoutMs = GetNextElectionTimeoutMs();
         }

         responseTerm = _currentTerm;

         if (request.Term == _currentTerm)
         {
            var canVote = string.IsNullOrEmpty(_votedFor) || _votedFor == request.CandidateId;
            if (canVote && isUpToDate)
            {
               voteGranted = true;
               _votedFor = request.CandidateId;
               _lastHeartbeatTicks = Environment.TickCount64;
            }
         }

         updatedVotedFor = _votedFor;
      }

      await Storage.SetTermAndVoteAsync(responseTerm, updatedVotedFor);

      return new RequestVoteResponse
      {
         Term = responseTerm,
         VoteGranted = voteGranted
      };
   }

   private async ValueTask<AppendEntriesResponse> HandleAppendEntriesAsync(AppendEntriesRequest request)
   {
      var currentTerm = CurrentTerm;

      if (request.Term > currentTerm)
      {
         await StepDownAsync(request.Term, CancellationToken.None);
         currentTerm = CurrentTerm;
      }

      if (request.Term < currentTerm)
      {
         return new AppendEntriesResponse { Term = currentTerm, Success = false, MatchIndex = await Storage.GetLastLogIndexAsync() };
      }

      // Reset election timeout upon receiving valid RPC from current leader
      Volatile.Write(ref _lastHeartbeatTicks, Environment.TickCount64);

      if (Role == RaftRole.Candidate)
      {
         await StepDownAsync(request.Term, CancellationToken.None);
      }

      if (_leaderId != request.LeaderId)
      {
         _leaderId = request.LeaderId;
         if (Events.OnLeaderChanged.Count > 0)
         {
            await Events.OnLeaderChanged.ExecuteAsync(new RaftLeaderChangedContext(Options.NodeId, request.LeaderId, currentTerm), cancellationToken: CancellationToken.None);
         }
      }

      // Check log consistency at PrevLogIndex
      if (request.PrevLogIndex > 0)
      {
         var prevEntry = await Storage.GetEntryAsync(request.PrevLogIndex);
         if (prevEntry == null || prevEntry.Value.Term != request.PrevLogTerm)
         {
            var lastIdx = await Storage.GetLastLogIndexAsync();
            return new AppendEntriesResponse { Term = currentTerm, Success = false, MatchIndex = lastIdx };
         }
      }

      // Append entries and resolve conflicts
      if (request.Entries.Count > 0)
      {
         for (var i = 0; i < request.Entries.Count; i++)
         {
            var newEntry = request.Entries[i];
            var existing = await Storage.GetEntryAsync(newEntry.Index);
            if (existing != null)
            {
               if (existing.Value.Term != newEntry.Term)
               {
                  await Storage.TruncateLogAsync(newEntry.Index);
                  await Storage.AppendEntriesAsync(request.Entries.Skip(i).ToList());
                  break;
               }
            }
            else
            {
               await Storage.AppendEntriesAsync(request.Entries.Skip(i).ToList());
               break;
            }
         }
      }

      var lastLogIndexAfterAppend = await Storage.GetLastLogIndexAsync();

      if (request.LeaderCommitIndex > CommitIndex)
      {
         var newCommit = Math.Min(request.LeaderCommitIndex, lastLogIndexAfterAppend);
         await AdvanceCommitIndexAsync(newCommit, CancellationToken.None);
      }

      return new AppendEntriesResponse
      {
         Term = currentTerm,
         Success = true,
         MatchIndex = lastLogIndexAfterAppend
      };
   }

   private async ValueTask<InstallSnapshotResponse> HandleInstallSnapshotAsync(InstallSnapshotRequest request)
   {
      var currentTerm = CurrentTerm;

      if (request.Term > currentTerm)
      {
         await StepDownAsync(request.Term, CancellationToken.None);
         currentTerm = CurrentTerm;
      }

      if (request.Term < currentTerm)
      {
         return new InstallSnapshotResponse { Term = currentTerm, Success = false };
      }

      Volatile.Write(ref _lastHeartbeatTicks, Environment.TickCount64);

      await StateMachine.RestoreSnapshotAsync(request.Data, request.LastIncludedIndex, request.LastIncludedTerm);
      await Storage.CompactPrefixAsync(request.LastIncludedIndex);

      lock (_stateLock)
      {
         _commitIndex = Math.Max(_commitIndex, request.LastIncludedIndex);
         _lastApplied = Math.Max(_lastApplied, request.LastIncludedIndex);
      }

      return new InstallSnapshotResponse { Term = currentTerm, Success = true };
   }

   private int GetNextElectionTimeoutMs()
   {
      var min = (int)Options.ElectionTimeoutMin.TotalMilliseconds;
      var max = (int)Options.ElectionTimeoutMax.TotalMilliseconds;
      return Random.Shared.Next(min, max);
   }

   public async ValueTask DisposeAsync()
   {
      await StopAsync();
      await Storage.DisposeAsync();
      await Transport.DisposeAsync();
   }
}
