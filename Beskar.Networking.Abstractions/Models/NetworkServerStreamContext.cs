using Beskar.Networking.Abstractions.Interfaces;

namespace Beskar.Networking.Abstractions.Models;

/// <summary>
/// Represents the context of an active data stream on the server,
/// linking the established server connection context to the specific network stream.
/// </summary>
/// <param name="Connection">The established server connection context containing the listener and session.</param>
/// <param name="Stream">The active network stream on which data is being transmitted.</param>
public readonly record struct NetworkServerStreamContext(
   NetworkServerConnectionContext Connection, INetworkStream Stream);
