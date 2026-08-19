using Beskar.Networking.Raft.Internal;

namespace Beskar.Networking.Raft.Tests;

public class RaftPeerTrackerTests
{
   [Test]
   public async Task PeerTracker_Initialization_SetsPropertiesCorrectly()
   {
      var tracker = new RaftPeerTracker("peer-node-1", 101);

      await Assert.That(tracker.PeerId).IsEqualTo("peer-node-1");
      await Assert.That(tracker.NextIndex).IsEqualTo(101UL);
      await Assert.That(tracker.MatchIndex).IsEqualTo(0UL);
   }

   [Test]
   public async Task PeerTracker_UpdateIndices_MutatesStateCorrectly()
   {
      var tracker = new RaftPeerTracker("peer-1", 1);

      tracker.MatchIndex = 50;
      tracker.NextIndex = 51;

      await Assert.That(tracker.MatchIndex).IsEqualTo(50UL);
      await Assert.That(tracker.NextIndex).IsEqualTo(51UL);
   }

   [Test]
   public async Task PeerTracker_DecrementNextIndex_DecrementsSafely()
   {
      var tracker = new RaftPeerTracker("peer-1", 5);

      if (tracker.NextIndex > 1) tracker.NextIndex--;

      await Assert.That(tracker.NextIndex).IsEqualTo(4UL);
   }
}
