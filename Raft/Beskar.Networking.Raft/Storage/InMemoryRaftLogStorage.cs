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
         if (index == 0 || _entries.Count == 0)
         {
            return ValueTask.FromResult<RaftLogEntry?>(null);
         }

         var firstIndex = _entries[0].Index;
         if (index < firstIndex)
         {
            return ValueTask.FromResult<RaftLogEntry?>(null);
         }

         var listIndex = (int)(index - firstIndex);
         if (listIndex < _entries.Count)
         {
            return ValueTask.FromResult<RaftLogEntry?>(_entries[listIndex]);
         }

         return ValueTask.FromResult<RaftLogEntry?>(null);
      }
   }

   public ValueTask<IReadOnlyList<RaftLogEntry>> GetEntriesAsync(ulong fromIndex, int maxCount, CancellationToken ct = default)
   {
      lock (_lock)
      {
         if (fromIndex == 0 || _entries.Count == 0 || maxCount <= 0)
         {
            return ValueTask.FromResult<IReadOnlyList<RaftLogEntry>>(Array.Empty<RaftLogEntry>());
         }

         var firstIndex = _entries[0].Index;
         if (fromIndex < firstIndex)
         {
            return ValueTask.FromResult<IReadOnlyList<RaftLogEntry>>(Array.Empty<RaftLogEntry>());
         }

         var startListIndex = (int)(fromIndex - firstIndex);
         if (startListIndex >= _entries.Count)
         {
            return ValueTask.FromResult<IReadOnlyList<RaftLogEntry>>(Array.Empty<RaftLogEntry>());
         }

         var count = Math.Min(maxCount, _entries.Count - startListIndex);
         var result = new List<RaftLogEntry>(count);
         for (var i = 0; i < count; i++)
         {
            result.Add(_entries[startListIndex + i]);
         }

         return ValueTask.FromResult<IReadOnlyList<RaftLogEntry>>(result);
      }
   }

   public ValueTask AppendEntriesAsync(IReadOnlyList<RaftLogEntry> entries, CancellationToken ct = default)
   {
      lock (_lock)
      {
         for (var i = 0; i < entries.Count; i++)
         {
            var entry = entries[i];
            if (entry.Index <= _compactedUntilIndex)
            {
               continue;
            }

            if (_entries.Count > 0)
            {
               var firstIndex = _entries[0].Index;
               var existingListIndex = (int)(entry.Index - firstIndex);
               if (existingListIndex >= 0 && existingListIndex < _entries.Count)
               {
                  if (_entries[existingListIndex].Term != entry.Term)
                  {
                     _entries.RemoveRange(existingListIndex, _entries.Count - existingListIndex);
                  }
                  else
                  {
                     _entries[existingListIndex] = entry;
                     continue;
                  }
               }
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
         if (_entries.Count == 0 || fromIndex == 0)
         {
            return ValueTask.CompletedTask;
         }

         var firstIndex = _entries[0].Index;
         if (fromIndex < firstIndex)
         {
            _entries.Clear();
            return ValueTask.CompletedTask;
         }

         var startListIndex = (int)(fromIndex - firstIndex);
         if (startListIndex < _entries.Count)
         {
            _entries.RemoveRange(startListIndex, _entries.Count - startListIndex);
         }

         return ValueTask.CompletedTask;
      }
   }

   public ValueTask CompactPrefixAsync(ulong untilIndex, ulong untilTerm = 0, CancellationToken ct = default)
   {
      lock (_lock)
      {
         if (_entries.Count == 0 || untilIndex == 0)
         {
            if (untilIndex >= _compactedUntilIndex)
            {
               _compactedUntilIndex = untilIndex;
               if (untilTerm > 0)
               {
                  _compactedUntilTerm = untilTerm;
               }
            }
            return ValueTask.CompletedTask;
         }

         var firstIndex = _entries[0].Index;
         if (untilIndex < firstIndex)
         {
            if (untilIndex == _compactedUntilIndex && untilTerm > 0)
            {
               _compactedUntilTerm = untilTerm;
            }
            return ValueTask.CompletedTask;
         }

         var removeCount = (int)(untilIndex - firstIndex + 1);
         var targetEntry = _entries[Math.Min(removeCount - 1, _entries.Count - 1)];
         _compactedUntilIndex = untilIndex;
         _compactedUntilTerm = untilTerm > 0 ? untilTerm : targetEntry.Term;

         if (removeCount >= _entries.Count)
         {
            _entries.Clear();
         }
         else
         {
            _entries.RemoveRange(0, removeCount);
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
