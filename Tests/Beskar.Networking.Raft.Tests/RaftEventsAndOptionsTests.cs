using Beskar.Networking.Raft.Enums;
using Beskar.Networking.Raft.Events;
using Beskar.Networking.Raft.Options;
using Beskar.Networking.Raft.Protocol.Models;
using Beskar.Networking.Raft.Storage;
using Beskar.Networking.Raft.Transport;
using Beskar.Networking.Transports.Memory;

namespace Beskar.Networking.Raft.Tests;

public class RaftEventsAndOptionsTests
{
   [Test]
   public async Task RaftNodeOptions_Defaults_AreSetSensibly()
   {
      var options = new RaftNodeOptions
      {
         NodeId = "test-node"
      };

      await Assert.That(options.NodeId).IsEqualTo("test-node");
      await Assert.That(options.Peers.Count).IsEqualTo(0);
      await Assert.That(options.ElectionTimeoutMin).IsGreaterThan(TimeSpan.Zero);
      await Assert.That(options.ElectionTimeoutMax).IsGreaterThan(options.ElectionTimeoutMin);
      await Assert.That(options.HeartbeatInterval).IsGreaterThan(TimeSpan.Zero);
      await Assert.That(options.MaxAppendEntriesBatchSize).IsGreaterThan(0);
   }

   [Test]
   public async Task RaftNode_RoleChangedEvent_FiresOnLifecycleTransitions()
   {
      var options = new RaftNodeOptions
      {
         NodeId = "solo-node",
         Peers = [],
         ElectionTimeoutMin = TimeSpan.FromMilliseconds(50),
         ElectionTimeoutMax = TimeSpan.FromMilliseconds(100),
         HeartbeatInterval = TimeSpan.FromMilliseconds(20)
      };

      var storage = new InMemoryRaftLogStorage();
      var sm = new TestRaftStateMachine();
      var memoryOptions = new MemoryTransportOptions();
      var endPoint = new MemoryEndPoint($"raft-evt-{Guid.NewGuid():N}");
      var listener = new MemoryNetworkListener(endPoint, memoryOptions);
      var transport = new RaftNetworkTransport(listener, []);

      await using var node = new RaftNode(options, storage, sm, transport);

      var roleTransitions = new List<(RaftRole OldRole, RaftRole NewRole)>();
      node.Events.OnRoleChanged.Add((ctx, _) =>
      {
         roleTransitions.Add((ctx.OldRole, ctx.NewRole));
         return ValueTask.CompletedTask;
      });

      await node.StartAsync();
      await Task.Delay(150); // Becomes follower -> candidate -> leader

      await node.StopAsync();

      await Assert.That(roleTransitions.Count).IsGreaterThanOrEqualTo(2);
      await Assert.That(roleTransitions[0]).IsEqualTo((RaftRole.Stopped, RaftRole.Follower));
   }

   [Test]
   public async Task RaftNode_EntryCommittedEvent_FiresWithCorrectContext()
   {
      var options = new RaftNodeOptions
      {
         NodeId = "solo-node",
         Peers = [],
         ElectionTimeoutMin = TimeSpan.FromMilliseconds(50),
         ElectionTimeoutMax = TimeSpan.FromMilliseconds(100),
         HeartbeatInterval = TimeSpan.FromMilliseconds(20)
      };

      var storage = new InMemoryRaftLogStorage();
      var sm = new TestRaftStateMachine();
      var memoryOptions = new MemoryTransportOptions();
      var endPoint = new MemoryEndPoint($"raft-evt-commit-{Guid.NewGuid():N}");
      var listener = new MemoryNetworkListener(endPoint, memoryOptions);
      var transport = new RaftNetworkTransport(listener, []);

      await using var node = new RaftNode(options, storage, sm, transport);

      RaftEntryCommittedContext? committedCtx = null;
      node.Events.OnEntryCommitted.Add((ctx, _) =>
      {
         committedCtx = ctx;
         return ValueTask.CompletedTask;
      });

      await node.StartAsync();
      await Task.Delay(150);

      await node.ProposeAsync("TEST_PROPOSAL"u8.ToArray());

      await Assert.That(committedCtx).IsNotNull();
      await Assert.That(committedCtx!.NodeId).IsEqualTo("solo-node");
      await Assert.That(committedCtx.Entry.Index).IsEqualTo(1UL);
   }

   [Test]
   public async Task ContextModels_ConstructAndExposePropertiesCorrectly()
   {
      var roleCtx = new RaftRoleChangedContext("node-1", RaftRole.Follower, RaftRole.Candidate, 5);
      await Assert.That(roleCtx.NodeId).IsEqualTo("node-1");
      await Assert.That(roleCtx.OldRole).IsEqualTo(RaftRole.Follower);
      await Assert.That(roleCtx.NewRole).IsEqualTo(RaftRole.Candidate);
      await Assert.That(roleCtx.Term).IsEqualTo(5UL);

      var leaderCtx = new RaftLeaderChangedContext("node-1", "leader-99", 10);
      await Assert.That(leaderCtx.NodeId).IsEqualTo("node-1");
      await Assert.That(leaderCtx.LeaderId).IsEqualTo("leader-99");
      await Assert.That(leaderCtx.Term).IsEqualTo(10UL);

      var entry = new RaftLogEntry(1, 2, "cmd"u8.ToArray());
      var commitCtx = new RaftEntryCommittedContext("node-1", entry, "RESULT"u8.ToArray());
      await Assert.That(commitCtx.NodeId).IsEqualTo("node-1");
      await Assert.That(commitCtx.Entry.Index).IsEqualTo(2UL);
   }
}
