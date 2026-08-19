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
         if (Directory.Exists(testDir)) Directory.Delete(testDir, recursive: true);
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
         for (ulong i = 1; i <= 10; i++)
         {
            initial.Add(new RaftLogEntry(1, i, Encoding.UTF8.GetBytes($"entry_{i}")));
         }
         await storage.AppendEntriesAsync(initial);

         // Retransmit entries 5..7 with SAME term 1
         var retransmit = new List<RaftLogEntry>();
         for (ulong i = 5; i <= 7; i++)
         {
            retransmit.Add(new RaftLogEntry(1, i, Encoding.UTF8.GetBytes($"entry_{i}")));
         }
         await storage.AppendEntriesAsync(retransmit);

         // Verified tail entries 8, 9, 10 must NOT have been truncated or deleted!
         await Assert.That(await storage.GetLastLogIndexAsync()).IsEqualTo(10UL);
         var entry10 = await storage.GetEntryAsync(10);
         await Assert.That(entry10.HasValue).IsTrue();
         await Assert.That(Encoding.UTF8.GetString(entry10!.Value.Data.Span)).IsEqualTo("entry_10");
      }
      finally
      {
         if (Directory.Exists(testDir)) Directory.Delete(testDir, recursive: true);
      }
   }
}
