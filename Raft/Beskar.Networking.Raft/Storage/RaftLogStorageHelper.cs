using Beskar.Networking.Raft.Protocol.Models;

namespace Beskar.Networking.Raft.Storage;

internal static class RaftLogStorageHelper
{
   internal readonly record struct CompactionPlan(
      bool ShouldUpdate,
      int RemoveCount,
      ulong NewCompactedUntilIndex,
      ulong NewCompactedUntilTerm,
      bool ClearAllRemaining);

   public static bool TryGetEntryOffset(ulong index, IReadOnlyList<RaftLogEntry> entries, out int offset)
   {
      if (index == 0 || entries.Count == 0)
      {
         offset = 0;
         return false;
      }

      var firstIndex = entries[0].Index;
      if (index < firstIndex)
      {
         offset = 0;
         return false;
      }

      var delta = index - firstIndex;
      if (delta >= (ulong)entries.Count)
      {
         offset = 0;
         return false;
      }

      offset = (int)delta;
      if (entries[offset].Index != index)
      {
         offset = 0;
         return false;
      }

      return true;
   }

   public static IReadOnlyList<RaftLogEntry> GetEntriesSlice(
      IReadOnlyList<RaftLogEntry> entries,
      ulong fromIndex,
      int maxCount)
   {
      if (fromIndex == 0 || entries.Count == 0 || maxCount <= 0)
      {
         return [];
      }

      var firstIndex = entries[0].Index;
      if (fromIndex < firstIndex)
      {
         return [];
      }

      var delta = fromIndex - firstIndex;
      if (delta >= (ulong)entries.Count)
      {
         return [];
      }

      var startListIndex = (int)delta;
      var count = Math.Min(maxCount, entries.Count - startListIndex);
      var result = new List<RaftLogEntry>(count);
      for (var i = 0; i < count; i++)
      {
         result.Add(entries[startListIndex + i]);
      }

      return result;
   }

   public static bool TryGetTruncateAction(
      ulong fromIndex,
      IReadOnlyList<RaftLogEntry> entries,
      out bool clearAll,
      out int startListIndex)
   {
      if (entries.Count == 0 || fromIndex == 0)
      {
         clearAll = false;
         startListIndex = 0;
         return false;
      }

      var firstIndex = entries[0].Index;
      if (fromIndex <= firstIndex)
      {
         clearAll = true;
         startListIndex = 0;
         return true;
      }

      var delta = fromIndex - firstIndex;
      if (delta >= (ulong)entries.Count)
      {
         clearAll = false;
         startListIndex = 0;
         return false;
      }

      clearAll = false;
      startListIndex = (int)delta;
      return true;
   }

   internal enum AppendEntryAction
   {
      Skip,
      Append,
      TruncateAndAppend
   }

   public static AppendEntryAction ClassifyAppendEntry(
      RaftLogEntry entry,
      IReadOnlyList<RaftLogEntry> currentEntries,
      ulong compactedUntilIndex)
   {
      if (entry.Index <= compactedUntilIndex)
      {
         return AppendEntryAction.Skip;
      }

      if (TryGetEntryOffset(entry.Index, currentEntries, out var existingListIndex))
      {
         return currentEntries[existingListIndex].Term != entry.Term
            ? AppendEntryAction.TruncateAndAppend
            : AppendEntryAction.Skip;
      }

      var expectedNext = currentEntries.Count > 0
         ? currentEntries[^1].Index + 1
         : compactedUntilIndex + 1;

      return entry.Index == expectedNext
         ? AppendEntryAction.Append
         : AppendEntryAction.Skip;
   }

   public static CompactionPlan PlanCompaction(
      ulong untilIndex,
      ulong untilTerm,
      IReadOnlyList<RaftLogEntry> entries,
      ulong compactedUntilIndex,
      ulong compactedUntilTerm)
   {
      if (untilIndex == 0)
      {
         return new CompactionPlan(false, 0, compactedUntilIndex, compactedUntilTerm, false);
      }

      if (entries.Count == 0)
      {
         if (untilIndex > compactedUntilIndex)
         {
            var resolvedTerm = untilTerm > 0 ? untilTerm : compactedUntilTerm;
            return new CompactionPlan(true, 0, untilIndex, resolvedTerm, false);
         }

         if (untilIndex == compactedUntilIndex && untilTerm > 0 && untilTerm != compactedUntilTerm)
         {
            return new CompactionPlan(true, 0, compactedUntilIndex, untilTerm, false);
         }

         return new CompactionPlan(false, 0, compactedUntilIndex, compactedUntilTerm, false);
      }

      var firstIndex = entries[0].Index;
      if (untilIndex < firstIndex)
      {
         if (untilIndex == compactedUntilIndex && untilTerm > 0 && untilTerm != compactedUntilTerm)
         {
            return new CompactionPlan(true, 0, compactedUntilIndex, untilTerm, false);
         }

         return new CompactionPlan(false, 0, compactedUntilIndex, compactedUntilTerm, false);
      }

      var delta = untilIndex - firstIndex;
      int removeCount;
      ulong targetTerm;
      if (delta >= (ulong)entries.Count)
      {
         removeCount = entries.Count;
         targetTerm = entries[^1].Term;
      }
      else
      {
         removeCount = (int)delta + 1;
         targetTerm = entries[removeCount - 1].Term;
      }

      var newTerm = untilTerm > 0 ? untilTerm : targetTerm;
      var clearAll = removeCount >= entries.Count || (untilTerm > 0 && targetTerm != untilTerm);
      return new CompactionPlan(true, removeCount, untilIndex, newTerm, clearAll);
   }
}
