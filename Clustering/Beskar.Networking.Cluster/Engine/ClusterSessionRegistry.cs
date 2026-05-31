using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Cluster.Protocol.Interfaces;

namespace Beskar.Networking.Cluster.Engine;

public sealed class ClusterSessionRegistry : IClusterSessionRegistry
{
   private readonly ConcurrentDictionary<Guid, INetworkStream> _activeSessions = [];

   public void Register(Guid nodeId, INetworkStream session)
   {
      _activeSessions[nodeId] = session;
   }

   public bool TryRemove(Guid nodeId, [MaybeNullWhen(false)] out INetworkStream session)
   {
      return _activeSessions.TryRemove(nodeId, out session);
   }

   public bool TryGetSession(Guid nodeId, [MaybeNullWhen(false)] out INetworkStream session)
   {
      return _activeSessions.TryGetValue(nodeId, out session);
   }

   public async ValueTask DisposeAsync()
   {
      foreach (var session in _activeSessions.Values)
      {
         try
         {
            if (session.Session is IAsyncDisposable asyncSession)
            {
               await asyncSession.DisposeAsync();
            }
         }
         catch
         {
            // ignored
         }
      }
   }
}
