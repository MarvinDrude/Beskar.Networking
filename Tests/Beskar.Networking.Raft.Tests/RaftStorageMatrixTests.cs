using System.Text;
using Beskar.Networking.Raft.Protocol.Models;
using Beskar.Networking.Raft.Storage;

namespace Beskar.Networking.Raft.Tests;

public class RaftStorageMatrixTests
{
   [Test]
   [Arguments(1UL)]
   [Arguments(2UL)]
   [Arguments(3UL)]
   [Arguments(4UL)]
   [Arguments(5UL)]
   [Arguments(10UL)]
   [Arguments(25UL)]
   [Arguments(50UL)]
   [Arguments(75UL)]
   [Arguments(100UL)]
   [Arguments(500UL)]
   [Arguments(1000UL)]
   [Arguments(10000UL)]
   [Arguments(999999UL)]
   public async Task InMemoryStorage_Terms_MatrixTest(ulong term)
   {
      await using var storage = new InMemoryRaftLogStorage();
      await storage.SetCurrentTermAsync(term);
      await Assert.That(await storage.GetCurrentTermAsync()).IsEqualTo(term);
   }

   [Test]
   [Arguments(1UL)]
   [Arguments(2UL)]
   [Arguments(3UL)]
   [Arguments(4UL)]
   [Arguments(5UL)]
   [Arguments(10UL)]
   [Arguments(25UL)]
   [Arguments(50UL)]
   [Arguments(75UL)]
   [Arguments(100UL)]
   [Arguments(500UL)]
   [Arguments(1000UL)]
   [Arguments(10000UL)]
   [Arguments(999999UL)]
   public async Task FileStorage_Terms_MatrixTest(ulong term)
   {
      var testDir = Path.Combine(Path.GetTempPath(), $"raft_term_mat_{Guid.NewGuid():N}");
      try
      {
         await using (var storage = new FileRaftLogStorage(testDir))
         {
            await storage.SetCurrentTermAsync(term);
         }

         await using (var reloaded = new FileRaftLogStorage(testDir))
         {
            await Assert.That(await reloaded.GetCurrentTermAsync()).IsEqualTo(term);
         }
      }
      finally
      {
         if (Directory.Exists(testDir)) Directory.Delete(testDir, true);
      }
   }

   [Test]
   [Arguments("node-1", 1UL)]
   [Arguments("candidate-alpha", 2UL)]
   [Arguments("node-beta-cluster-01", 5UL)]
   [Arguments("node-gamma", 10UL)]
   [Arguments("candidate-delta", 20UL)]
   [Arguments("node-epsilon-region-us-east-1", 50UL)]
   [Arguments("node-zeta", 100UL)]
   [Arguments("candidate-eta", 200UL)]
   [Arguments("node-theta", 500UL)]
   [Arguments("node-iota", 1000UL)]
   public async Task FileStorage_TermAndVote_MatrixTest(string candidateId, ulong term)
   {
      var testDir = Path.Combine(Path.GetTempPath(), $"raft_tv_mat_{Guid.NewGuid():N}");
      try
      {
         await using (var storage = new FileRaftLogStorage(testDir))
         {
            await storage.SetTermAndVoteAsync(term, candidateId);
         }

         await using (var reloaded = new FileRaftLogStorage(testDir))
         {
            await Assert.That(await reloaded.GetCurrentTermAsync()).IsEqualTo(term);
            await Assert.That(await reloaded.GetVotedForAsync()).IsEqualTo(candidateId);
         }
      }
      finally
      {
         if (Directory.Exists(testDir)) Directory.Delete(testDir, true);
      }
   }

   [Test]
   [Arguments(1)]
   [Arguments(5)]
   [Arguments(10)]
   [Arguments(20)]
   [Arguments(50)]
   [Arguments(100)]
   public async Task InMemoryStorage_AppendAndRetrieve_MatrixTest(int count)
   {
      await using var storage = new InMemoryRaftLogStorage();

      var entries = new List<RaftLogEntry>();
      for (var i = 1; i <= count; i++) entries.Add(new RaftLogEntry(1, (ulong)i, Encoding.UTF8.GetBytes($"CMD_{i}")));

      await storage.AppendEntriesAsync(entries);

      await Assert.That(await storage.GetLastLogIndexAsync()).IsEqualTo((ulong)count);

      for (var i = 1; i <= count; i++)
      {
         var fetched = await storage.GetEntryAsync((ulong)i);
         await Assert.That(fetched.HasValue).IsTrue();
         await Assert.That(Encoding.UTF8.GetString(fetched!.Value.Data.Span)).IsEqualTo($"CMD_{i}");
      }
   }

   [Test]
   [Arguments(1)]
   [Arguments(5)]
   [Arguments(10)]
   [Arguments(20)]
   [Arguments(50)]
   [Arguments(100)]
   public async Task FileStorage_AppendAndRetrieve_MatrixTest(int count)
   {
      var testDir = Path.Combine(Path.GetTempPath(), $"raft_app_mat_{Guid.NewGuid():N}");
      try
      {
         await using (var storage = new FileRaftLogStorage(testDir))
         {
            var entries = new List<RaftLogEntry>();
            for (var i = 1; i <= count; i++)
               entries.Add(new RaftLogEntry(1, (ulong)i, Encoding.UTF8.GetBytes($"CMD_{i}")));

            await storage.AppendEntriesAsync(entries);
            await Assert.That(await storage.GetLastLogIndexAsync()).IsEqualTo((ulong)count);
         }

         await using (var reloaded = new FileRaftLogStorage(testDir))
         {
            await Assert.That(await reloaded.GetLastLogIndexAsync()).IsEqualTo((ulong)count);
            for (var i = 1; i <= count; i++)
            {
               var fetched = await reloaded.GetEntryAsync((ulong)i);
               await Assert.That(fetched.HasValue).IsTrue();
               await Assert.That(Encoding.UTF8.GetString(fetched!.Value.Data.Span)).IsEqualTo($"CMD_{i}");
            }
         }
      }
      finally
      {
         if (Directory.Exists(testDir)) Directory.Delete(testDir, true);
      }
   }

   [Test]
   [Arguments(10, 3)]
   [Arguments(10, 5)]
   [Arguments(10, 8)]
   [Arguments(20, 10)]
   [Arguments(20, 15)]
   [Arguments(50, 25)]
   public async Task FileStorage_TruncateSuffix_MatrixTest(int totalEntries, int truncateFromIndex)
   {
      var testDir = Path.Combine(Path.GetTempPath(), $"raft_trunc_mat_{Guid.NewGuid():N}");
      try
      {
         await using (var storage = new FileRaftLogStorage(testDir))
         {
            var entries = new List<RaftLogEntry>();
            for (var i = 1; i <= totalEntries; i++)
               entries.Add(new RaftLogEntry(1, (ulong)i, Encoding.UTF8.GetBytes($"CMD_{i}")));
            await storage.AppendEntriesAsync(entries);

            await storage.TruncateLogAsync((ulong)truncateFromIndex);

            await Assert.That(await storage.GetLastLogIndexAsync()).IsEqualTo((ulong)(truncateFromIndex - 1));
         }

         await using (var reloaded = new FileRaftLogStorage(testDir))
         {
            await Assert.That(await reloaded.GetLastLogIndexAsync()).IsEqualTo((ulong)(truncateFromIndex - 1));
         }
      }
      finally
      {
         if (Directory.Exists(testDir)) Directory.Delete(testDir, true);
      }
   }

   [Test]
   [Arguments(10, 3)]
   [Arguments(10, 5)]
   [Arguments(10, 8)]
   [Arguments(20, 10)]
   [Arguments(20, 15)]
   [Arguments(50, 25)]
   public async Task FileStorage_CompactPrefix_MatrixTest(int totalEntries, int compactUntilIndex)
   {
      var testDir = Path.Combine(Path.GetTempPath(), $"raft_comp_mat_{Guid.NewGuid():N}");
      try
      {
         await using (var storage = new FileRaftLogStorage(testDir))
         {
            var entries = new List<RaftLogEntry>();
            for (var i = 1; i <= totalEntries; i++)
               entries.Add(new RaftLogEntry(1, (ulong)i, Encoding.UTF8.GetBytes($"CMD_{i}")));
            await storage.AppendEntriesAsync(entries);

            await storage.CompactPrefixAsync((ulong)compactUntilIndex);

            for (var i = 1; i <= compactUntilIndex; i++)
               await Assert.That(await storage.GetEntryAsync((ulong)i)).IsNull();

            for (var i = compactUntilIndex + 1; i <= totalEntries; i++)
            {
               var fetched = await storage.GetEntryAsync((ulong)i);
               await Assert.That(fetched.HasValue).IsTrue();
               await Assert.That(Encoding.UTF8.GetString(fetched!.Value.Data.Span)).IsEqualTo($"CMD_{i}");
            }
         }

         await using (var reloaded = new FileRaftLogStorage(testDir))
         {
            for (var i = 1; i <= compactUntilIndex; i++)
               await Assert.That(await reloaded.GetEntryAsync((ulong)i)).IsNull();

            for (var i = compactUntilIndex + 1; i <= totalEntries; i++)
            {
               var fetched = await reloaded.GetEntryAsync((ulong)i);
               await Assert.That(fetched.HasValue).IsTrue();
               await Assert.That(Encoding.UTF8.GetString(fetched!.Value.Data.Span)).IsEqualTo($"CMD_{i}");
            }
         }
      }
      finally
      {
         if (Directory.Exists(testDir)) Directory.Delete(testDir, true);
      }
   }
}
