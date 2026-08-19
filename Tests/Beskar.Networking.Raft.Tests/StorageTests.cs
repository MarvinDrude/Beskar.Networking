using System.Text;
using Beskar.Networking.Raft.Protocol.Models;
using Beskar.Networking.Raft.Storage;

namespace Beskar.Networking.Raft.Tests;

public class StorageTests
{
   [Test]
   public async Task InMemoryStorage_TermsAndVotes_PersistCorrectly()
   {
      await using var storage = new InMemoryRaftLogStorage();

      await Assert.That(await storage.GetCurrentTermAsync()).IsEqualTo(0UL);
      await Assert.That(await storage.GetVotedForAsync()).IsNull();

      await storage.SetTermAndVoteAsync(5, "node-1");

      await Assert.That(await storage.GetCurrentTermAsync()).IsEqualTo(5UL);
      await Assert.That(await storage.GetVotedForAsync()).IsEqualTo("node-1");

      await storage.SetVotedForAsync("node-2");
      await Assert.That(await storage.GetVotedForAsync()).IsEqualTo("node-2");
   }

   [Test]
   public async Task InMemoryStorage_LogAppendsAndTruncations_OperateAccurately()
   {
      await using var storage = new InMemoryRaftLogStorage();

      var entry1 = new RaftLogEntry(1, 1, Encoding.UTF8.GetBytes("cmd1"));
      var entry2 = new RaftLogEntry(1, 2, Encoding.UTF8.GetBytes("cmd2"));
      var entry3 = new RaftLogEntry(2, 3, Encoding.UTF8.GetBytes("cmd3"));

      await storage.AppendEntriesAsync(new[] { entry1, entry2, entry3 });

      await Assert.That(await storage.GetLastLogIndexAsync()).IsEqualTo(3UL);
      await Assert.That(await storage.GetLastLogTermAsync()).IsEqualTo(2UL);

      var fetched2 = await storage.GetEntryAsync(2);
      await Assert.That(fetched2.HasValue).IsTrue();
      await Assert.That(fetched2!.Value.Term).IsEqualTo(1UL);
      await Assert.That(Encoding.UTF8.GetString(fetched2.Value.Data.Span)).IsEqualTo("cmd2");

      var range = await storage.GetEntriesAsync(2, 2);
      await Assert.That(range.Count).IsEqualTo(2);
      await Assert.That(range[0].Index).IsEqualTo(2UL);
      await Assert.That(range[1].Index).IsEqualTo(3UL);

      // Truncate from index 3
      await storage.TruncateLogAsync(3);
      await Assert.That(await storage.GetLastLogIndexAsync()).IsEqualTo(2UL);
      await Assert.That(await storage.GetEntryAsync(3)).IsNull();
   }

   [Test]
   public async Task FileStorage_PersistAndReload_OperatesAccurately()
   {
      var testDir = Path.Combine(Path.GetTempPath(), $"raft_storage_test_{Guid.NewGuid():N}");
      try
      {
         await using (var storage = new FileRaftLogStorage(testDir))
         {
            await storage.SetTermAndVoteAsync(12, "candidate-x");
            var entry1 = new RaftLogEntry(12, 1, Encoding.UTF8.GetBytes("PERSISTED_COMMAND_1"));
            var entry2 = new RaftLogEntry(12, 2, Encoding.UTF8.GetBytes("PERSISTED_COMMAND_2"));
            await storage.AppendEntriesAsync(new[] { entry1, entry2 });
         }

         await using (var storage = new FileRaftLogStorage(testDir))
         {
            await Assert.That(await storage.GetCurrentTermAsync()).IsEqualTo(12UL);
            await Assert.That(await storage.GetVotedForAsync()).IsEqualTo("candidate-x");
            await Assert.That(await storage.GetLastLogIndexAsync()).IsEqualTo(2UL);
            await Assert.That(await storage.GetLastLogTermAsync()).IsEqualTo(12UL);

            var entry1 = await storage.GetEntryAsync(1);
            await Assert.That(entry1.HasValue).IsTrue();
            await Assert.That(Encoding.UTF8.GetString(entry1!.Value.Data.Span)).IsEqualTo("PERSISTED_COMMAND_1");

            var entry2 = await storage.GetEntryAsync(2);
            await Assert.That(entry2.HasValue).IsTrue();
            await Assert.That(Encoding.UTF8.GetString(entry2!.Value.Data.Span)).IsEqualTo("PERSISTED_COMMAND_2");

            var entry3 = new RaftLogEntry(12, 3, Encoding.UTF8.GetBytes("TO_BE_TRUNCATED"));
            await storage.AppendEntriesAsync([entry3]);
            await Assert.That(await storage.GetLastLogIndexAsync()).IsEqualTo(3UL);

            await storage.TruncateLogAsync(3);
            await Assert.That(await storage.GetLastLogIndexAsync()).IsEqualTo(2UL);
         }

         await using (var storage = new FileRaftLogStorage(testDir))
         {
            await Assert.That(await storage.GetLastLogIndexAsync()).IsEqualTo(2UL);
            await Assert.That(await storage.GetEntryAsync(3)).IsNull();
         }
      }
      finally
      {
         if (Directory.Exists(testDir))
         {
            Directory.Delete(testDir, recursive: true);
         }
      }
   }
}
