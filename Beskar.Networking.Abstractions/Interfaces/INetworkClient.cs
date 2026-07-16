using System.Diagnostics.CodeAnalysis;
using System.Net;
using Beskar.Networking.Abstractions.Errors;
using Beskar.Memory.Results;
using Beskar.Networking.Abstractions.Enums;
using Beskar.Networking.Abstractions.Models;

namespace Beskar.Networking.Abstractions.Interfaces;

/// <summary>
/// Represents a network client.
/// </summary>
public interface INetworkClient : IAsyncDisposable
{
   /// <summary>
   /// The transport kind of the current session.
   /// </summary>
   public TransportKind Transport { get; }

   /// <summary>
   /// Gets a value indicating whether the client is currently connected to a remote endpoint.
   /// </summary>
   [MemberNotNullWhen(true, nameof(Session))]
   public bool IsConnected { get; }

   /// <summary>
   /// Gets the operational statistics for this client.
   /// </summary>
   public NetworkClientStats Stats { get; }

   /// <summary>
   /// Gets the active network session, or null if not connected.
   /// </summary>
   public INetworkSession? Session { get; }

   /// <summary>
   /// Tries to connect to a remote endpoint.
   /// </summary>
   public ValueTask<Result<INetworkSession, NetworkCodeError>> ConnectAsync(
      EndPoint endPoint, CancellationToken ct = default);

   /// <summary>
   /// Disconnects any active session established by this client.
   /// </summary>
   public ValueTask DisconnectAsync(CancellationToken ct = default);
}
