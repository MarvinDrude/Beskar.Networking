using System.Buffers.Binary;
using System.Text;
using Beskar.Networking.Raft.Protocol.Models;
using Beskar.Networking.Raft.Storage;

namespace Beskar.Networking.Raft.Tests;

public class RaftStorageExtendedTests
{
   [Test]
   [Arguments(0)]
   [Arguments(1)]
   [Arguments(100)]
   [Arguments(99999)]
   public async Task InMemoryStorage_SetAndGetTerm_ReturnsCorrectTerm(ulong term)
   {
      await using var storage = new InMemoryRaftLogStorage();
      await storage.SetCurrentTermAsync(term);
      await Assert.That(await storage.GetCurrentTermAsync()).IsEqualTo(term);
   }

   [Test]
   [Arguments(null)]
   [Arguments("")]
   [Arguments("node-alpha")]
   [Arguments("candidate-xyz-999")]
   public async Task InMemoryStorage_SetAndGetVotedFor_ReturnsCorrectCandidate(string? candidateId)
   {
      await using var storage = new InMemoryRaftLogStorage();
      await storage.SetVotedForAsync(candidateId);
      await Assert.That(await storage.GetVotedForAsync()).IsEqualTo(candidateId);
   }

   [Test]
   public async Task InMemoryStorage_GetEntryIndexZero_ReturnsNull()
   {
      await using var storage = new InMemoryRaftLogStorage();
      await Assert.That(await storage.GetEntryAsync(0)).IsNull();
   }

   [Test]
   public async Task InMemoryStorage_GetEntries_OutofBounds_ReturnsEmptyList()
   {
      await using var storage = new InMemoryRaftLogStorage();
      await storage.AppendEntriesAsync([new RaftLogEntry(1, 1, "data"u8.ToArray())]);

      var outOfBounds = await storage.GetEntriesAsync(5, 10);
      await Assert.That(outOfBounds.Count).IsEqualTo(0);

      var zeroCount = await storage.GetEntriesAsync(1, 0);
      await Assert.That(zeroCount.Count).IsEqualTo(0);
   }

   [Test]
   public async Task FileStorage_SetTermAndVoteAsync_PersistsAtomically()
   {
      var testDir = Path.Combine(Path.GetTempPath(), $"raft_atomic_test_{Guid.NewGuid():N}");
      try
      {
         await using (var storage = new FileRaftLogStorage(testDir))
         {
            await storage.SetTermAndVoteAsync(42, "leader-candidate-7");
         }

         await using (var reloaded = new FileRaftLogStorage(testDir))
         {
            await Assert.That(await reloaded.GetCurrentTermAsync()).IsEqualTo(42UL);
            await Assert.That(await reloaded.GetVotedForAsync()).IsEqualTo("leader-candidate-7");
         }
      }
      finally
      {
         if (Directory.Exists(testDir)) Directory.Delete(testDir, true);
      }
   }

   [Test]
   public async Task FileStorage_CorruptTrailingLogRecord_RecoversCleanlyOnStartup()
   {
      var testDir = Path.Combine(Path.GetTempPath(), $"raft_corrupt_test_{Guid.NewGuid():N}");
      try
      {
         await using (var storage = new FileRaftLogStorage(testDir))
         {
            var e1 = new RaftLogEntry(1, 1, "VALID_ENTRY_1"u8.ToArray());
            await storage.AppendEntriesAsync([e1]);
         }

         // Append corrupted partial header bytes to raft.log
         var logFile = Path.Combine(testDir, "raft.log");
         using (var fs = new FileStream(logFile, FileMode.Append, FileAccess.Write))
         {
            // Write valid header with data length 100, but write only 5 bytes of data (torn write)
            Span<byte> header = stackalloc byte[20];
            BinaryPrimitives.WriteUInt64LittleEndian(header[..8], 1);
            BinaryPrimitives.WriteUInt64LittleEndian(header.Slice(8, 8), 2);
            BinaryPrimitives.WriteInt32LittleEndian(header.Slice(16, 4), 100);
            fs.Write(header);
            fs.Write("SHORT"u8.ToArray());
         }

         // Reload storage: engine must recover valid prefix and truncate trailing corruption
         await using (var reloaded = new FileRaftLogStorage(testDir))
         {
            await Assert.That(await reloaded.GetLastLogIndexAsync()).IsEqualTo(1UL);
            var e1Fetched = await reloaded.GetEntryAsync(1);
            await Assert.That(e1Fetched.HasValue).IsTrue();
            await Assert.That(Encoding.UTF8.GetString(e1Fetched!.Value.Data.Span)).IsEqualTo("VALID_ENTRY_1");
            await Assert.That(await reloaded.GetEntryAsync(2)).IsNull();
         }
      }
      finally
      {
         if (Directory.Exists(testDir)) Directory.Delete(testDir, true);
      }
   }

   [Test]
   public async Task FileStorage_ZeroLengthEntryPayload_HandlesWithoutError()
   {
      var testDir = Path.Combine(Path.GetTempPath(), $"raft_zerolen_test_{Guid.NewGuid():N}");
      try
      {
         await using (var storage = new FileRaftLogStorage(testDir))
         {
            var emptyEntry = new RaftLogEntry(5, 1, ReadOnlyMemory<byte>.Empty);
            await storage.AppendEntriesAsync([emptyEntry]);

            var fetched = await storage.GetEntryAsync(1);
            await Assert.That(fetched.HasValue).IsTrue();
            await Assert.That(fetched!.Value.Data.Length).IsEqualTo(0);
         }
      }
      finally
      {
         if (Directory.Exists(testDir)) Directory.Delete(testDir, true);
      }
   }

   [Test]
   public async Task FileStorage_LargeEntryPayload_PersistsAndReadsAccurately()
   {
      var testDir = Path.Combine(Path.GetTempPath(), $"raft_large_test_{Guid.NewGuid():N}");
      try
      {
         var largeBuffer = new byte[256 * 1024]; // 256 KB
         Random.Shared.NextBytes(largeBuffer);

         await using (var storage = new FileRaftLogStorage(testDir))
         {
            var largeEntry = new RaftLogEntry(10, 1, largeBuffer);
            await storage.AppendEntriesAsync([largeEntry]);

            var fetched = await storage.GetEntryAsync(1);
            await Assert.That(fetched.HasValue).IsTrue();
            await Assert.That(fetched!.Value.Data.Span.SequenceEqual(largeBuffer)).IsTrue();
         }
      }
      finally
      {
         if (Directory.Exists(testDir)) Directory.Delete(testDir, true);
      }
   }

   [Test]
   public async Task CompactAllEntries_PreservesLastLogIndexAndTerm_ForElectionSafety()
   {
      var testDir = Path.Combine(Path.GetTempPath(), $"raft_compact_safety_{Guid.NewGuid():N}");
      try
      {
         await using (var storage = new FileRaftLogStorage(testDir))
         {
            var entries = new List<RaftLogEntry>
            {
               new(1, 1, "data1"u8.ToArray()),
               new(2, 2, "data2"u8.ToArray()),
               new(5, 3, "data3"u8.ToArray())
            };
            await storage.AppendEntriesAsync(entries);

            // Compact all entries 1..3
            await storage.CompactPrefixAsync(3);

            // Verified last log index must be 3 and term 5 even when log list is empty
            await Assert.That(await storage.GetLastLogIndexAsync()).IsEqualTo(3UL);
            await Assert.That(await storage.GetLastLogTermAsync()).IsEqualTo(5UL);
         }

         // Reload storage from disk to ensure persisted metadata retains last compacted index/term
         await using (var reloaded = new FileRaftLogStorage(testDir))
         {
            await Assert.That(await reloaded.GetLastLogIndexAsync()).IsEqualTo(3UL);
            await Assert.That(await reloaded.GetLastLogTermAsync()).IsEqualTo(5UL);
         }
      }
      finally
      {
         if (Directory.Exists(testDir)) Directory.Delete(testDir, true);
      }
   }

   [Test]
   public async Task FileStorage_RetransmittedMatchingEntries_DoesNotTruncateTailEntries()
   {
      var testDir = Path.Combine(Path.GetTempPath(), $"raft_retransmit_test_{Guid.NewGuid():N}");
      try
      {
         await using var storage = new FileRaftLogStorage(testDir);

         // Append entries 1..10
         var initial = new List<RaftLogEntry>();
         for (ulong i = 1; i <= 10; i++) initial.Add(new RaftLogEntry(1, i, Encoding.UTF8.GetBytes($"entry_{i}")));
         await storage.AppendEntriesAsync(initial);

         // Retransmit entries 5..7 with SAME term 1
         var retransmit = new List<RaftLogEntry>();
         for (ulong i = 5; i <= 7; i++) retransmit.Add(new RaftLogEntry(1, i, Encoding.UTF8.GetBytes($"entry_{i}")));
         await storage.AppendEntriesAsync(retransmit);

         // Verified tail entries 8, 9, 10 must NOT have been truncated or deleted!
         await Assert.That(await storage.GetLastLogIndexAsync()).IsEqualTo(10UL);
         var entry10 = await storage.GetEntryAsync(10);
         await Assert.That(entry10.HasValue).IsTrue();
         await Assert.That(Encoding.UTF8.GetString(entry10!.Value.Data.Span)).IsEqualTo("entry_10");
      }
      finally
      {
         if (Directory.Exists(testDir)) Directory.Delete(testDir, true);
      }
   }

   [Test]
   public async Task EmptyStorage_CompactPrefixWithUntilTerm_PreservesLastLogIndexAndTerm()
   {
      var testDir = Path.Combine(Path.GetTempPath(), $"raft_empty_compact_test_{Guid.NewGuid():N}");
      try
      {
         await using (var fileStorage = new FileRaftLogStorage(testDir))
         {
            await fileStorage.CompactPrefixAsync(100, 15);
            await Assert.That(await fileStorage.GetLastLogIndexAsync()).IsEqualTo(100UL);
            await Assert.That(await fileStorage.GetLastLogTermAsync()).IsEqualTo(15UL);
         }

         await using (var reloaded = new FileRaftLogStorage(testDir))
         {
            await Assert.That(await reloaded.GetLastLogIndexAsync()).IsEqualTo(100UL);
            await Assert.That(await reloaded.GetLastLogTermAsync()).IsEqualTo(15UL);
         }

         await using var memStorage = new InMemoryRaftLogStorage();
         await memStorage.CompactPrefixAsync(100, 15);
         await Assert.That(await memStorage.GetLastLogIndexAsync()).IsEqualTo(100UL);
         await Assert.That(await memStorage.GetLastLogTermAsync()).IsEqualTo(15UL);
      }
      finally
      {
         if (Directory.Exists(testDir)) Directory.Delete(testDir, true);
      }
   }

   [Test]
   public async Task Storage_AppendEntries_IndexLessThanCompactedIndex_SkippedCleanly()
   {
      // 1. InMemory storage test
      await using (var memStorage = new InMemoryRaftLogStorage())
      {
         await memStorage.AppendEntriesAsync([
            new RaftLogEntry(1, 1, "E1"u8.ToArray()),
            new RaftLogEntry(1, 2, "E2"u8.ToArray()),
            new RaftLogEntry(1, 3, "E3"u8.ToArray())
         ]);
         await memStorage.CompactPrefixAsync(2, 1);

         // Append old entry 1 and 2 again
         await memStorage.AppendEntriesAsync([
            new RaftLogEntry(1, 1, "E1_OLD"u8.ToArray()),
            new RaftLogEntry(1, 2, "E2_OLD"u8.ToArray()),
            new RaftLogEntry(1, 4, "E4"u8.ToArray())
         ]);

         await Assert.That(await memStorage.GetLastLogIndexAsync()).IsEqualTo(4UL);
         var entry3 = await memStorage.GetEntryAsync(3);
         await Assert.That(entry3.HasValue).IsTrue();
         await Assert.That(Encoding.UTF8.GetString(entry3!.Value.Data.Span)).IsEqualTo("E3");
         var entry4 = await memStorage.GetEntryAsync(4);
         await Assert.That(entry4.HasValue).IsTrue();
         await Assert.That(Encoding.UTF8.GetString(entry4!.Value.Data.Span)).IsEqualTo("E4");
      }

      // 2. File storage test
      var testDir = Path.Combine(Path.GetTempPath(), $"raft_skip_compacted_{Guid.NewGuid():N}");
      try
      {
         await using (var fileStorage = new FileRaftLogStorage(testDir))
         {
            await fileStorage.AppendEntriesAsync([
               new RaftLogEntry(1, 1, "E1"u8.ToArray()),
               new RaftLogEntry(1, 2, "E2"u8.ToArray()),
               new RaftLogEntry(1, 3, "E3"u8.ToArray())
            ]);
            await fileStorage.CompactPrefixAsync(2, 1);

            await fileStorage.AppendEntriesAsync([
               new RaftLogEntry(1, 1, "E1_OLD"u8.ToArray()),
               new RaftLogEntry(1, 2, "E2_OLD"u8.ToArray()),
               new RaftLogEntry(1, 4, "E4"u8.ToArray())
            ]);

            await Assert.That(await fileStorage.GetLastLogIndexAsync()).IsEqualTo(4UL);
            var entry3 = await fileStorage.GetEntryAsync(3);
            await Assert.That(entry3.HasValue).IsTrue();
            await Assert.That(Encoding.UTF8.GetString(entry3!.Value.Data.Span)).IsEqualTo("E3");
            var entry4 = await fileStorage.GetEntryAsync(4);
            await Assert.That(entry4.HasValue).IsTrue();
            await Assert.That(Encoding.UTF8.GetString(entry4!.Value.Data.Span)).IsEqualTo("E4");
         }

         // Verify reload
         await using (var reloaded = new FileRaftLogStorage(testDir))
         {
            await Assert.That(await reloaded.GetLastLogIndexAsync()).IsEqualTo(4UL);
            var entry3 = await reloaded.GetEntryAsync(3);
            await Assert.That(entry3.HasValue).IsTrue();
            await Assert.That(Encoding.UTF8.GetString(entry3!.Value.Data.Span)).IsEqualTo("E3");
            var entry4 = await reloaded.GetEntryAsync(4);
            await Assert.That(entry4.HasValue).IsTrue();
            await Assert.That(Encoding.UTF8.GetString(entry4!.Value.Data.Span)).IsEqualTo("E4");
         }
      }
      finally
      {
         if (Directory.Exists(testDir)) Directory.Delete(testDir, true);
      }
   }

   [Test]
   public async Task CompactPrefix_SnapshotTermMismatch_DiscardsConflictingTailLogEntries()
   {
      // 1. InMemory storage
      await using (var memStorage = new InMemoryRaftLogStorage())
      {
         await memStorage.AppendEntriesAsync([
            new RaftLogEntry(1, 1, "E1"u8.ToArray()),
            new RaftLogEntry(1, 2, "E2"u8.ToArray()),
            new RaftLogEntry(1, 3, "E3"u8.ToArray()),
            new RaftLogEntry(1, 4, "E4"u8.ToArray()),
            new RaftLogEntry(1, 5, "E5"u8.ToArray())
         ]);

         // Compact until index 3 with term 2 (which conflicts with local term 1)
         await memStorage.CompactPrefixAsync(3, 2);

         // Entire log must be cleared, leaving no conflicting entries 4 or 5
         await Assert.That(await memStorage.GetEntryAsync(4)).IsNull();
         await Assert.That(await memStorage.GetEntryAsync(5)).IsNull();
         await Assert.That(await memStorage.GetLastLogIndexAsync()).IsEqualTo(3UL);
         await Assert.That(await memStorage.GetLastLogTermAsync()).IsEqualTo(2UL);
      }

      // 2. File storage
      var testDir = Path.Combine(Path.GetTempPath(), $"raft_term_mismatch_{Guid.NewGuid():N}");
      try
      {
         await using (var fileStorage = new FileRaftLogStorage(testDir))
         {
            await fileStorage.AppendEntriesAsync([
               new RaftLogEntry(1, 1, "E1"u8.ToArray()),
               new RaftLogEntry(1, 2, "E2"u8.ToArray()),
               new RaftLogEntry(1, 3, "E3"u8.ToArray()),
               new RaftLogEntry(1, 4, "E4"u8.ToArray()),
               new RaftLogEntry(1, 5, "E5"u8.ToArray())
            ]);

            await fileStorage.CompactPrefixAsync(3, 2);

            await Assert.That(await fileStorage.GetEntryAsync(4)).IsNull();
            await Assert.That(await fileStorage.GetEntryAsync(5)).IsNull();
            await Assert.That(await fileStorage.GetLastLogIndexAsync()).IsEqualTo(3UL);
            await Assert.That(await fileStorage.GetLastLogTermAsync()).IsEqualTo(2UL);
         }

         await using (var reloaded = new FileRaftLogStorage(testDir))
         {
            await Assert.That(await reloaded.GetEntryAsync(4)).IsNull();
            await Assert.That(await reloaded.GetEntryAsync(5)).IsNull();
            await Assert.That(await reloaded.GetLastLogIndexAsync()).IsEqualTo(3UL);
            await Assert.That(await reloaded.GetLastLogTermAsync()).IsEqualTo(2UL);
         }
      }
      finally
      {
         if (Directory.Exists(testDir)) Directory.Delete(testDir, true);
      }
   }

   [Test]
   [Timeout(5000)]
   public async Task FileStorage_LargeCandidateIdOver32KB_PersistsVotedForAcrossRestart(CancellationToken ct)
   {
      var testDir = Path.Combine(Path.GetTempPath(), $"raft_storage_test_{Guid.NewGuid():N}");
      var largeCandidateId = new string('c', 40_000);

      try
      {
         await using (var storage = new FileRaftLogStorage(testDir))
         {
            await storage.SetTermAndVoteAsync(5, largeCandidateId, ct);
            await Assert.That(await storage.GetVotedForAsync(ct)).IsEqualTo(largeCandidateId);
         }

         await using (var reloaded = new FileRaftLogStorage(testDir))
         {
            await Assert.That(await reloaded.GetCurrentTermAsync(ct)).IsEqualTo(5UL);
            await Assert.That(await reloaded.GetVotedForAsync(ct)).IsEqualTo(largeCandidateId);
         }
      }
      finally
      {
         if (Directory.Exists(testDir)) Directory.Delete(testDir, true);
      }
   }

   [Test]
   [Timeout(5000)]
   [Arguments(true)]
   [Arguments(false)]
   public async Task Storage_HighIndexOffsetMultipleOf2Pow32_GetEntriesReturnsEmpty(
      bool useFileStorage, CancellationToken ct)
   {
      var testDir = Path.Combine(Path.GetTempPath(), $"raft_storage_test_{Guid.NewGuid():N}");
      IRaftLogStorage storage = useFileStorage
         ? new FileRaftLogStorage(testDir)
         : new InMemoryRaftLogStorage();

      try
      {
         await storage.AppendEntriesAsync([
            new RaftLogEntry(1, 1, "e1"u8.ToArray()),
            new RaftLogEntry(1, 2, "e2"u8.ToArray())
         ], ct);

         const ulong wrappedIndex = 1UL + (1UL << 32);

         var highEntries = await storage.GetEntriesAsync(wrappedIndex, 10, ct);
         await Assert.That(highEntries.Count).IsEqualTo(0);
      }
      finally
      {
         await storage.DisposeAsync();
         if (Directory.Exists(testDir)) Directory.Delete(testDir, true);
      }
   }

   [Test]
   [Timeout(5000)]
   [Arguments(true)]
   [Arguments(false)]
   public async Task Storage_HighIndexOffsetMultipleOf2Pow32_TruncateDoesNotDeleteLowIndices(
      bool useFileStorage, CancellationToken ct)
   {
      var testDir = Path.Combine(Path.GetTempPath(), $"raft_storage_test_{Guid.NewGuid():N}");
      IRaftLogStorage storage = useFileStorage
         ? new FileRaftLogStorage(testDir)
         : new InMemoryRaftLogStorage();

      try
      {
         await storage.AppendEntriesAsync([
            new RaftLogEntry(1, 1, "e1"u8.ToArray()),
            new RaftLogEntry(1, 2, "e2"u8.ToArray())
         ], ct);

         const ulong wrappedIndex = 1UL + (1UL << 32);

         await storage.TruncateLogAsync(wrappedIndex, ct);
         await Assert.That(await storage.GetLastLogIndexAsync(ct)).IsEqualTo(2UL);
         var entry1 = await storage.GetEntryAsync(1, ct);
         var entry2 = await storage.GetEntryAsync(2, ct);
         await Assert.That(entry1.HasValue).IsTrue();
         await Assert.That(entry2.HasValue).IsTrue();
      }
      finally
      {
         await storage.DisposeAsync();
         if (Directory.Exists(testDir)) Directory.Delete(testDir, true);
      }
   }

   [Test]
   [Timeout(5000)]
   [Arguments(true)]
   [Arguments(false)]
   public async Task Storage_HighIndexOffsetMultipleOf2Pow32_AppendDoesNotOverwriteLowIndex(
      bool useFileStorage, CancellationToken ct)
   {
      var testDir = Path.Combine(Path.GetTempPath(), $"raft_storage_test_{Guid.NewGuid():N}");
      IRaftLogStorage storage = useFileStorage
         ? new FileRaftLogStorage(testDir)
         : new InMemoryRaftLogStorage();

      try
      {
         await storage.AppendEntriesAsync([
            new RaftLogEntry(1, 1, "e1"u8.ToArray()),
            new RaftLogEntry(1, 2, "e2"u8.ToArray())
         ], ct);

         const ulong wrappedIndex = 1UL + (1UL << 32);

         try
         {
            await storage.AppendEntriesAsync([new RaftLogEntry(2, wrappedIndex, "gap"u8.ToArray())], ct);
         }
         catch (Exception)
         {
            // Rejecting a non-contiguous append is acceptable
         }

         var entry1 = await storage.GetEntryAsync(1, ct);
         await Assert.That(entry1.HasValue).IsTrue();
         await Assert.That(entry1!.Value.Term).IsEqualTo(1UL);
         await Assert.That(Encoding.UTF8.GetString(entry1.Value.Data.Span)).IsEqualTo("e1");
      }
      finally
      {
         await storage.DisposeAsync();
         if (Directory.Exists(testDir)) Directory.Delete(testDir, true);
      }
   }

   [Test]
   [Timeout(5000)]
   [Arguments(true)]
   [Arguments(false)]
   public async Task Storage_HighIndexOffsetMultipleOf2Pow32_CompactPrefixDoesNotOverflow(
      bool useFileStorage, CancellationToken ct)
   {
      var testDir = Path.Combine(Path.GetTempPath(), $"raft_storage_test_{Guid.NewGuid():N}");
      IRaftLogStorage storage = useFileStorage
         ? new FileRaftLogStorage(testDir)
         : new InMemoryRaftLogStorage();

      try
      {
         await storage.AppendEntriesAsync([
            new RaftLogEntry(1, 1, "e1"u8.ToArray()),
            new RaftLogEntry(1, 2, "e2"u8.ToArray())
         ], ct);

         await storage.CompactPrefixAsync(1UL << 32, 2, ct);
         await Assert.That(await storage.GetCompactedUntilIndexAsync(ct)).IsEqualTo(1UL << 32);
         await Assert.That(await storage.GetLastLogIndexAsync(ct)).IsEqualTo(1UL << 32);
      }
      finally
      {
         await storage.DisposeAsync();
         if (Directory.Exists(testDir)) Directory.Delete(testDir, true);
      }
   }

   [Test]
   [Timeout(5000)]
   [Arguments(true)]
   [Arguments(false)]
   public async Task Storage_EmptyLog_NonContiguousIndexAppend_DoesNotBlockLowerContiguousEntries(
      bool useFileStorage, CancellationToken ct)
   {
      var testDir = Path.Combine(Path.GetTempPath(), $"raft_storage_test_{Guid.NewGuid():N}");
      IRaftLogStorage storage = useFileStorage
         ? new FileRaftLogStorage(testDir)
         : new InMemoryRaftLogStorage();

      try
      {
         try
         {
            await storage.AppendEntriesAsync([new RaftLogEntry(1, 5, "gap"u8.ToArray())], ct);
         }
         catch (Exception)
         {
            // Rejecting a non-contiguous append is also acceptable
         }

         await storage.AppendEntriesAsync([new RaftLogEntry(1, 1, "e1"u8.ToArray())], ct);
         var entry1 = await storage.GetEntryAsync(1, ct);
         await Assert.That(entry1.HasValue).IsTrue();
         await Assert.That(Encoding.UTF8.GetString(entry1!.Value.Data.Span)).IsEqualTo("e1");
      }
      finally
      {
         await storage.DisposeAsync();
         if (Directory.Exists(testDir)) Directory.Delete(testDir, true);
      }
   }

   [Test]
   [Timeout(5000)]
   public async Task FileStorage_CompactedEntriesRemainingOnDisk_FilteredOnReload(CancellationToken ct)
   {
      var dirUncompacted = Path.Combine(Path.GetTempPath(), $"raft_storage_test_{Guid.NewGuid():N}");
      var dirCompactedMeta = Path.Combine(Path.GetTempPath(), $"raft_storage_test_{Guid.NewGuid():N}");

      try
      {
         await using (var s1 = new FileRaftLogStorage(dirUncompacted))
         {
            var entries = Enumerable.Range(1, 5)
               .Select(i => new RaftLogEntry(2, (ulong)i, Encoding.UTF8.GetBytes($"e{i}")))
               .ToList();
            await s1.AppendEntriesAsync(entries, ct);
         }

         Directory.CreateDirectory(dirCompactedMeta);
         foreach (var file in Directory.GetFiles(dirUncompacted))
            File.Copy(file, Path.Combine(dirCompactedMeta, Path.GetFileName(file)), true);

         await using (var s2 = new FileRaftLogStorage(dirCompactedMeta))
         {
            await s2.CompactPrefixAsync(3, 2, ct);
         }

         // Simulate crash after metadata persisted CompactedUntilIndex=3 while raft.log still retains entries 1..5
         File.Copy(Path.Combine(dirCompactedMeta, "metadata.bin"), Path.Combine(dirUncompacted, "metadata.bin"), true);

         await using (var recovered = new FileRaftLogStorage(dirUncompacted))
         {
            await Assert.That(await recovered.GetCompactedUntilIndexAsync(ct)).IsEqualTo(3UL);
            await Assert.That(await recovered.GetEntryAsync(1, ct)).IsNull();
            await Assert.That(await recovered.GetEntryAsync(3, ct)).IsNull();
            await Assert.That(await recovered.GetEntryAsync(4, ct)).IsNotNull();
            await Assert.That(await recovered.GetLastLogIndexAsync(ct)).IsEqualTo(5UL);
         }
      }
      finally
      {
         if (Directory.Exists(dirUncompacted)) Directory.Delete(dirUncompacted, true);
         if (Directory.Exists(dirCompactedMeta)) Directory.Delete(dirCompactedMeta, true);
      }
   }

   [Test]
   [Timeout(5000)]
   public async Task FileStorage_OperationsAfterDispose_ThrowObjectDisposedException(CancellationToken ct)
   {
      var testDir = Path.Combine(Path.GetTempPath(), $"raft_storage_test_{Guid.NewGuid():N}");

      try
      {
         var storage = new FileRaftLogStorage(testDir);
         await storage.AppendEntriesAsync([new RaftLogEntry(1, 1, "e1"u8.ToArray())], ct);
         await storage.DisposeAsync();

         await Assert.ThrowsAsync<ObjectDisposedException>(async () =>
            await storage.CompactPrefixAsync(1, 1, ct));
         await Assert.ThrowsAsync<ObjectDisposedException>(async () =>
            await storage.TruncateLogAsync(1, ct));
         await Assert.ThrowsAsync<ObjectDisposedException>(async () =>
            await storage.SetTermAndVoteAsync(2, "n1", ct));
      }
      finally
      {
         if (Directory.Exists(testDir)) Directory.Delete(testDir, true);
      }
   }

   [Test]
   [Timeout(5000)]
   public async Task InMemoryStorage_CompactPrefixIndexZero_DoesNotMutateCompactedTerm(CancellationToken ct)
   {
      var storage = new InMemoryRaftLogStorage();
      await storage.CompactPrefixAsync(0, 99, ct);

      await Assert.That(await storage.GetCompactedUntilIndexAsync(ct)).IsEqualTo(0UL);
      await Assert.That(await storage.GetCompactedUntilTermAsync(ct)).IsEqualTo(0UL);
   }
}
