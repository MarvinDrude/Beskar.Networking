using System.Diagnostics.CodeAnalysis;
using Beskar.Networking.Abstractions.Interfaces;

namespace Beskar.Networking.Cluster.Protocol.Interfaces;

public interface IClusterSessionRegistry : IAsyncDisposable
{
   public void Register(Guid nodeId, INetworkStream session);

   public bool TryRemove(Guid nodeId, [MaybeNullWhen(false)] out INetworkStream session);

   public bool TryGetSession(Guid nodeId, [MaybeNullWhen(false)] out INetworkStream session);
}
