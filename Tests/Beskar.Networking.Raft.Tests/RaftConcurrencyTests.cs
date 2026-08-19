using System.Text;
using Beskar.Networking.Raft.Enums;
using Beskar.Networking.Raft.Options;
using Beskar.Networking.Raft.Protocol.Models;
using Beskar.Networking.Raft.Storage;
using Beskar.Networking.Raft.Transport;
using Beskar.Networking.Transports.Memory;

namespace Beskar.Networking.Raft.Tests;

public class RaftConcurrencyTests
{
   [Test]
   [Arguments(10)]
   [Arguments(25)]
   [Arguments(50)]
   public async Task ConcurrentAppends_InMemoryStorage_ThreadSafety(int threadCount)
   {
      await using var storage = new InMemoryRaftLogStorage();

      var tasks = Enumerable.Range(1, threadCount).Select(threadId => Task.Run(async () =>
      {
         for (var i = 1; i <= 20; i++)
         {
            await storage.SetCurrentTermAsync((ulong)(threadId * 100 + i));
            await storage.SetVotedForAsync($"node-{threadId}");
         }
      })).ToList();

      await Task.WhenAll(tasks);

      await Assert.That(await storage.GetCurrentTermAsync()).IsGreaterThan(0UL);
   }

   [Test]
   [Arguments(5)]
   [Arguments(10)]
   [Arguments(20)]
   public async Task RapidStartStop_RaftNode_HandlesChurnCleanly(int churnIterations)
   {
      var options = new RaftNodeOptions
      {
         NodeId = "churn-node",
         Peers = [],
         ElectionTimeoutMin = TimeSpan.FromMilliseconds(50),
         ElectionTimeoutMax = TimeSpan.FromMilliseconds(100),
         HeartbeatInterval = TimeSpan.FromMilliseconds(20)
      };

      var memoryOptions = new MemoryTransportOptions();

      for (var i = 0; i < churnIterations; i++)
      {
         var storage = new InMemoryRaftLogStorage();
         var sm = new TestRaftStateMachine();
         var endPoint = new MemoryEndPoint($"raft-churn-{i}-{Guid.NewGuid():N}");
         var listener = new MemoryNetworkListener(endPoint, memoryOptions);
         var transport = new RaftNetworkTransport(listener, []);

         await using var node = new RaftNode(options, storage, sm, transport);
         await node.StartAsync();
         await Task.Delay(10);
         await node.StopAsync();

         await Assert.That(node.Role).IsEqualTo(RaftRole.Stopped);
      }
   }

   [Test]
   public async Task ConcurrentStorageReadsAndWrites_FileStorage_NoRaceConditions()
   {
      var testDir = Path.Combine(Path.GetTempPath(), $"raft_conc_file_{Guid.NewGuid():N}");
      try
      {
         await using (var storage = new FileRaftLogStorage(testDir))
         {
            var writeTask = Task.Run(async () =>
            {
               for (ulong i = 1; i <= 100; i++)
               {
                  var entry = new RaftLogEntry(1, i, Encoding.UTF8.GetBytes($"DATA_{i}"));
                  await storage.AppendEntriesAsync([entry]);
                  await Task.Yield();
               }
            });

            var readTask = Task.Run(async () =>
            {
               for (var i = 0; i < 50; i++)
               {
                  var lastIdx = await storage.GetLastLogIndexAsync();
                  if (lastIdx > 0) await storage.GetEntryAsync(lastIdx);
                  await Task.Yield();
               }
            });

            await Task.WhenAll(writeTask, readTask);

            await Assert.That(await storage.GetLastLogIndexAsync()).IsEqualTo(100UL);
         }
      }
      finally
      {
         if (Directory.Exists(testDir)) Directory.Delete(testDir, true);
      }
   }
}
