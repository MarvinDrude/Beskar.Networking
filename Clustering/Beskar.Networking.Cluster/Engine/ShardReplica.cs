using System.Diagnostics.CodeAnalysis;
using Beskar.Memory.Threading;
using Beskar.Networking.Cluster.Protocol.Enums;
using Beskar.Networking.Cluster.Protocol.Options;

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

   private Task ElectionTimerCallback()
   {
      CurrentRole = ClusterNodeRole.Candidate;
      CurrentEpoch++;


   }

   [MemberNotNull(nameof(_electionTimer))]
   private void StartNewElectionTimer()
   {
      if (_electionTimer is null)
      {
         _electionTimer = new PausableAsyncTimer(
            GetRandomElectionTimeout(), ElectionTimerCallback);
      }

      _electionTimer.Pause();
      _electionTimer.UpdateInterval(GetRandomElectionTimeout());
      _electionTimer.Resume();
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
