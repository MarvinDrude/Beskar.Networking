using System.Text;
using Beskar.Networking.Raft.Enums;
using Beskar.Networking.Raft.Options;
using Beskar.Networking.Raft.Storage;
using Beskar.Networking.Raft.Transport;
using Beskar.Networking.Transports.Memory;

namespace Beskar.Networking.Raft.Tests;

public class RaftClusterMatrixTests
{
   [Test]
   [Arguments(50, 100, 20)]
   [Arguments(100, 200, 30)]
   [Arguments(150, 300, 50)]
   [Arguments(200, 400, 60)]
   public async Task SingleNodeCluster_CustomOptions_BecomesLeader(int minTimeoutMs, int maxTimeoutMs, int heartbeatMs)
   {
      var options = new RaftNodeOptions
      {
         NodeId = "matrix-node-1",
         Peers = [],
         ElectionTimeoutMin = TimeSpan.FromMilliseconds(minTimeoutMs),
         ElectionTimeoutMax = TimeSpan.FromMilliseconds(maxTimeoutMs),
         HeartbeatInterval = TimeSpan.FromMilliseconds(heartbeatMs)
      };

      var storage = new InMemoryRaftLogStorage();
      var sm = new KeyValueTestStateMachine();
      var memoryOptions = new MemoryTransportOptions();
      var endPoint = new MemoryEndPoint($"raft-mat-{Guid.NewGuid():N}");
      var listener = new MemoryNetworkListener(endPoint, memoryOptions);
      var transport = new RaftNetworkTransport(listener, []);

      await using var node = new RaftNode(options, storage, sm, transport);
      await node.StartAsync();

      await Task.Delay(maxTimeoutMs + 50);

      await Assert.That(node.Role).IsEqualTo(RaftRole.Leader);
   }

   [Test]
   [Arguments("SET k1=v1")]
   [Arguments("SET k2=v2")]
   [Arguments("SET k3=v3")]
   [Arguments("SET key_with_spaces = value with spaces")]
   [Arguments("SET binary_data_key=1234567890")]
   [Arguments("SET json_payload={\"a\":1,\"b\":2}")]
   public async Task SingleNodeCluster_ProposeDifferentPayloads_ExecutesSuccessfully(string proposalCommand)
   {
      var options = new RaftNodeOptions
      {
         NodeId = "solo-proposer",
         Peers = [],
         ElectionTimeoutMin = TimeSpan.FromMilliseconds(50),
         ElectionTimeoutMax = TimeSpan.FromMilliseconds(100),
         HeartbeatInterval = TimeSpan.FromMilliseconds(20)
      };

      var storage = new InMemoryRaftLogStorage();
      var sm = new KeyValueTestStateMachine();
      var memoryOptions = new MemoryTransportOptions();
      var endPoint = new MemoryEndPoint($"raft-prop-{Guid.NewGuid():N}");
      var listener = new MemoryNetworkListener(endPoint, memoryOptions);
      var transport = new RaftNetworkTransport(listener, []);

      await using var node = new RaftNode(options, storage, sm, transport);
      await node.StartAsync();
      await Task.Delay(150);

      var payload = Encoding.UTF8.GetBytes(proposalCommand);
      var result = await node.ProposeAsync(payload);

      await Assert.That(Encoding.UTF8.GetString(result.Span)).IsEqualTo("OK");
   }

   [Test]
   [Arguments(5)]
   [Arguments(10)]
   [Arguments(20)]
   [Arguments(50)]
   public async Task SingleNodeCluster_ConcurrentProposals_AllCommitSequentially(int proposalCount)
   {
      var options = new RaftNodeOptions
      {
         NodeId = "conc-proposer",
         Peers = [],
         ElectionTimeoutMin = TimeSpan.FromMilliseconds(50),
         ElectionTimeoutMax = TimeSpan.FromMilliseconds(100),
         HeartbeatInterval = TimeSpan.FromMilliseconds(20)
      };

      var storage = new InMemoryRaftLogStorage();
      var sm = new KeyValueTestStateMachine();
      var memoryOptions = new MemoryTransportOptions();
      var endPoint = new MemoryEndPoint($"raft-conc-{Guid.NewGuid():N}");
      var listener = new MemoryNetworkListener(endPoint, memoryOptions);
      var transport = new RaftNetworkTransport(listener, []);

      await using var node = new RaftNode(options, storage, sm, transport);
      await node.StartAsync();
      await Task.Delay(150);

      var tasks = new List<Task<ReadOnlyMemory<byte>>>();
      for (var i = 1; i <= proposalCount; i++)
      {
         var cmd = Encoding.UTF8.GetBytes($"SET conc_key_{i}=val_{i}");
         tasks.Add(node.ProposeAsync(cmd).AsTask());
      }

      var results = await Task.WhenAll(tasks);

      await Assert.That(results.Length).IsEqualTo(proposalCount);
      await Assert.That(node.CommitIndex).IsEqualTo((ulong)proposalCount);
      await Assert.That(sm.Store.Count).IsEqualTo(proposalCount);
   }
}
