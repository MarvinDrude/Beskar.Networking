using System.Net;
using Beskar.Networking.Abstractions.Interfaces;

namespace Beskar.Networking.Raft.Transport;

/// <summary>
/// Represents configuration for connecting to a Raft peer node.
/// </summary>
/// <param name="PeerId">Unique identifier of the peer node.</param>
/// <param name="EndPoint">Network endpoint of the peer listener.</param>
/// <param name="ClientFactory">Factory function to produce network clients for this peer.</param>
public sealed record RaftPeerEndpoint(
   string PeerId,
   EndPoint EndPoint,
   Func<INetworkClient> ClientFactory);
