using System.Threading;
using Beskar.Networking.Raft.Protocol.Models;

namespace Beskar.Networking.Raft.Storage;

/// <summary>
/// High-performance, thread-safe in-memory implementation of <see cref="IRaftLogStorage"/>.
/// Suitable for testing, simulations, and non-durable cluster topologies.
/// </summary>
public sealed class InMemoryRaftLogStorage : IRaftLogStorage
{
   private readonly Lock _lock = new();
   private readonly List<RaftLogEntry> _entries = [];
   private ulong _currentTerm;
   private string? _votedFor;

   private ulong _compactedUntilIndex;
   private ulong _compactedUntilTerm;

   public ValueTask<ulong> GetCurrentTermAsync(CancellationToken ct = default)
   {
      lock (_lock)
      {
         return ValueTask.FromResult(_currentTerm);
      }
   }

   public ValueTask SetCurrentTermAsync(ulong term, CancellationToken ct = default)
   {
      lock (_lock)
      {
         _currentTerm = term;
         return ValueTask.CompletedTask;
      }
   }

   public ValueTask<string?> GetVotedForAsync(CancellationToken ct = default)
   {
      lock (_lock)
      {
         return ValueTask.FromResult(_votedFor);
      }
   }

   public ValueTask SetVotedForAsync(string? candidateId, CancellationToken ct = default)
   {
      lock (_lock)
      {
         _votedFor = candidateId;
         return ValueTask.CompletedTask;
      }
   }

   public ValueTask SetTermAndVoteAsync(ulong term, string? candidateId, CancellationToken ct = default)
   {
      lock (_lock)
      {
         _currentTerm = term;
         _votedFor = candidateId;
         return ValueTask.CompletedTask;
      }
   }

   public ValueTask<ulong> GetLastLogIndexAsync(CancellationToken ct = default)
   {
      lock (_lock)
      {
         if (_entries.Count == 0)
         {
            return ValueTask.FromResult(_compactedUntilIndex);
         }

         return ValueTask.FromResult(_entries[^1].Index);
      }
   }

   public ValueTask<ulong> GetLastLogTermAsync(CancellationToken ct = default)
   {
      lock (_lock)
      {
         if (_entries.Count == 0)
         {
            return ValueTask.FromResult(_compactedUntilTerm);
         }

         return ValueTask.FromResult(_entries[^1].Term);
      }
   }

   public ValueTask<RaftLogEntry?> GetEntryAsync(ulong index, CancellationToken ct = default)
   {
      lock (_lock)
      {
         return RaftLogStorageHelper.TryGetEntryOffset(index, _entries, out var listIndex)
            ? ValueTask.FromResult<RaftLogEntry?>(_entries[listIndex])
            : ValueTask.FromResult<RaftLogEntry?>(null);
      }
   }

   public ValueTask<IReadOnlyList<RaftLogEntry>> GetEntriesAsync(ulong fromIndex, int maxCount, CancellationToken ct = default)
   {
      lock (_lock)
      {
         return ValueTask.FromResult(RaftLogStorageHelper.GetEntriesSlice(_entries, fromIndex, maxCount));
      }
   }

   public ValueTask AppendEntriesAsync(IReadOnlyList<RaftLogEntry> entries, CancellationToken ct = default)
   {
      lock (_lock)
      {
         for (var i = 0; i < entries.Count; i++)
         {
            var entry = entries[i];
            switch (RaftLogStorageHelper.ClassifyAppendEntry(entry, _entries, _compactedUntilIndex))
            {
               case RaftLogStorageHelper.AppendEntryAction.Skip:
                  continue;

               case RaftLogStorageHelper.AppendEntryAction.TruncateAndAppend:
                  if (RaftLogStorageHelper.TryGetEntryOffset(entry.Index, _entries, out var existingListIndex))
                  {
                     _entries.RemoveRange(existingListIndex, _entries.Count - existingListIndex);
                  }
                  break;
            }

            _entries.Add(entry);
         }

         return ValueTask.CompletedTask;
      }
   }

   public ValueTask TruncateLogAsync(ulong fromIndex, CancellationToken ct = default)
   {
      lock (_lock)
      {
         if (RaftLogStorageHelper.TryGetTruncateAction(fromIndex, _entries, out var clearAll, out var startListIndex))
         {
            if (clearAll)
            {
               _entries.Clear();
            }
            else
            {
               _entries.RemoveRange(startListIndex, _entries.Count - startListIndex);
            }
         }

         return ValueTask.CompletedTask;
      }
   }

   public ValueTask CompactPrefixAsync(ulong untilIndex, ulong untilTerm = 0, CancellationToken ct = default)
   {
      lock (_lock)
      {
         var plan = RaftLogStorageHelper.PlanCompaction(
            untilIndex,
            untilTerm,
            _entries,
            _compactedUntilIndex,
            _compactedUntilTerm);

         if (!plan.ShouldUpdate)
         {
            return ValueTask.CompletedTask;
         }

         _compactedUntilIndex = plan.NewCompactedUntilIndex;
         _compactedUntilTerm = plan.NewCompactedUntilTerm;

         if (plan.ClearAllRemaining)
         {
            _entries.Clear();
         }
         else if (plan.RemoveCount > 0)
         {
            _entries.RemoveRange(0, plan.RemoveCount);
         }

         return ValueTask.CompletedTask;
      }
   }

   public ValueTask<ulong> GetCompactedUntilIndexAsync(CancellationToken ct = default)
   {
      lock (_lock)
      {
         return ValueTask.FromResult(_compactedUntilIndex);
      }
   }

   public ValueTask<ulong> GetCompactedUntilTermAsync(CancellationToken ct = default)
   {
      lock (_lock)
      {
         return ValueTask.FromResult(_compactedUntilTerm);
      }
   }

   public ValueTask DisposeAsync()
   {
      lock (_lock)
      {
         _entries.Clear();
      }

      return ValueTask.CompletedTask;
   }
}
