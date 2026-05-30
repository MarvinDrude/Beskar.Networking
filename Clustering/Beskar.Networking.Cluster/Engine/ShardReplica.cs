using System.Diagnostics.CodeAnalysis;
using Beskar.Memory.Threading;
using Beskar.Networking.Cluster.Protocol.Enums;
using Beskar.Networking.Cluster.Protocol.Options;
using Beskar.Networking.Cluster.Protocol.Packets.Shard;
using Beskar.Utilities.Tracing;

namespace Beskar.Networking.Cluster.Engine;

public sealed class ShardReplica : IAsyncDisposable
{
   public required Guid ShardId { get; init; }
   public required Guid NodeId { get; init; }

   public ClusterNodeRole CurrentRole { get; private set; }

   public long CurrentEpoch { get; private set; } = 0;
   public long LastLogIndex { get; private set; } = 0;

   private PausableAsyncTimer _electionTimer;

   private readonly ShardReplicaOptions _options;
   private readonly ClusterCommunicator _communicator;

   // 0 = not disposed, 1 = disposed
   private int _isDisposed;

   public ShardReplica(
      ClusterCommunicator communicator,
      ShardReplicaOptions options)
   {
      _communicator = communicator;
      _options = options;

      StartNewElectionTimer();
   }

   private async Task ElectionTimerCallback()
   {
      TraceLogger.LogNeutralInfo("[Shard {0}][Node {1}] Election timeout reached, starting new election", ShardId, NodeId);
      CurrentRole = ClusterNodeRole.Candidate;
      CurrentEpoch++;

      var voteRequest = new RequestVoteRequestPayload
      {
         CandidateNodeId = NodeId,
         LastLogIndex = LastLogIndex,
         LastLogEpoch = CurrentEpoch - 1
      };

      // if vote is split
      StartNewElectionTimer();

      try
      {
         await _communicator.BroadcastAsync(ShardId, voteRequest, CurrentEpoch);
      }
      catch (Exception ex)
      {
         TraceLogger.LogNeutralError("[Shard {0}][Node {1}] There was an error during election broadcast {2}", ShardId, NodeId, ex.ToString());
      }
   }

   [MemberNotNull(nameof(_electionTimer))]
   private void StartNewElectionTimer()
   {
      var timeout = GetRandomElectionTimeout();
      TraceLogger.LogNeutralInfo("[Shard {0}][Node {1}] Starting new election timer with timeout {2}", ShardId, NodeId, timeout);

      if (_electionTimer is null)
      {
         _electionTimer = new PausableAsyncTimer(
            timeout, ElectionTimerCallback);
      }

      _electionTimer.Pause();
      _electionTimer.UpdateInterval(timeout);
      _electionTimer.Resume(waitBeforeExecution: true);
   }

   private TimeSpan GetRandomElectionTimeout()
   {
      return TimeSpan.FromMilliseconds(Random.Shared.NextInt64(
         (long)_options.MinElectionTimeout.TotalMilliseconds,
         (long)_options.MaxElectionTimeout.TotalMilliseconds + 1));
   }

   public ValueTask DisposeAsync()
   {
      if (Interlocked.Exchange(ref _isDisposed, 1) == 1)
         return ValueTask.CompletedTask;

      _electionTimer.Dispose();
      return ValueTask.CompletedTask;
   }
}
