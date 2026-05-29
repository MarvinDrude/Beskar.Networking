using System.Collections.Concurrent;
using Beskar.Networking.Abstractions.Interfaces;

namespace Beskar.Networking.Cluster.Engine;

public sealed class ClusterSessionRegistry
{
   private readonly ConcurrentDictionary<Guid, INetworkSession> _activeSessions = [];

   public void Register(Guid nodeId, INetworkSession session)
   {
      _activeSessions[nodeID] = session;
   }
}
