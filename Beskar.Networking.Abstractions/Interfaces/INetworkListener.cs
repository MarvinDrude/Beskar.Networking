using System.Net;
using Beskar.Networking.Abstractions.Errors;
using Beskar.Memory.Results;
using Beskar.Networking.Abstractions.Enums;
using Beskar.Networking.Abstractions.Models;

namespace Beskar.Networking.Abstractions.Interfaces;

/// <summary>
/// Represents a network listener.
/// </summary>
public interface INetworkListener : IAsyncDisposable
{
   /// <summary>
   /// The local address of the listener.
   /// </summary>
   public EndPoint LocalAddress { get; }

   /// <summary>
   /// Gets a value indicating whether the listener is currently bound and listening.
   /// </summary>
   public bool IsBound { get; }

   /// <summary>
   /// The transport kind of the current listener.
   /// </summary>
   public TransportKind Transport { get; }

   /// <summary>
   /// Gets the operational statistics for this listener.
   /// </summary>
   public NetworkListenerStats Stats { get; }

   /// <summary>
   /// Binds the listener to the local address.
   /// </summary>
   public ValueTask<VoidResult<NetworkCodeError>> BindAsync(CancellationToken ct = default);

   /// <summary>
   /// Unbinds the listener from the local address.
   /// </summary>
   public ValueTask<VoidResult<NetworkCodeError>> UnbindAsync(CancellationToken ct = default);

   /// <summary>
   /// Accepts a new network session.
   /// </summary>
   public ValueTask<Result<INetworkSession, NetworkCodeError>> AcceptSessionAsync(CancellationToken ct = default);

}
