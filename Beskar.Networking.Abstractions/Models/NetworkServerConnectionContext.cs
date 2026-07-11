using Beskar.Networking.Abstractions.Interfaces;

namespace Beskar.Networking.Abstractions.Models;

/// <summary>
/// Represents the context of an established server-side connection,
/// linking the listener that accepted the connection to the active network session.
/// </summary>
/// <param name="Listener">The network listener that accepted the connection.</param>
/// <param name="Session">The active network session established with the client.</param>
public readonly record struct NetworkServerConnectionContext(
   INetworkListener Listener, INetworkSession Session);
