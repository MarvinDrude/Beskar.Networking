using Beskar.Networking.Abstractions.Interfaces;

namespace Beskar.Networking.Cluster.Protocol.Models;

public sealed class ClusterMessageContext
{
   public required INetworkSession Session { get; init; }

   public bool IsJoined { get; init; }
}
